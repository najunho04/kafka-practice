"""순서를 지키는 retry/DLQ 컨슈머.

규칙 한 줄: 같은 key에 '아직 안 끝난 앞 메시지'가 있으면, 뒤 메시지는 처리하지 않고 따라간다.
  - 앞 메시지가 retry 중  → 뒤 메시지도 retry 토픽에서 대기
  - 앞 메시지가 DLQ에 있음 → 뒤 메시지도 DLQ에서 대기
구현: 막힌 key마다 '번호표'를 발급하고, serving(현재 차례) 번호만 처리한다. (state.py)
  seq <  serving : 이미 처리됨(중복 전달) → 무시          ← at-least-once 중복 방어
  seq == serving : 내 차례 → 처리. 성공하면 serving+1, 실패하면 retry/DLQ 로
  seq >  serving : 앞이 아직 안 끝남 → 보류(retry 또는 DLQ 로 이동)

순서: '다음 토픽에 발행(flush) → 원본 commit' 은 02와 동일.
버그 수정 시뮬레이션: 같은 폴더에 fixed.flag 파일이 생기면 poison 이 성공한다(재배포 대신)."""
import pathlib
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import state  # noqa: E402
from common import decode, make_consumer, make_producer  # noqa: E402

HERE = pathlib.Path(__file__).resolve().parent
MAIN, RETRY, DLQ = "payments", "payments.retry", "payments.DLQ"
RETRY_DELAY_SEC = {1: 2, 2: 4}  # n번째 재시도 전 대기
MAX_RETRY = 2
PARK_DELAY_SEC = 1  # 보류 중인 뒤 메시지가 retry 토픽을 다시 확인하는 간격


def handle(event: dict, retry_count: int) -> None:
    if event["kind"] == "transient" and retry_count == 0:
        raise RuntimeError("일시적 오류(외부 API 타임아웃)")
    if event["kind"] == "poison" and not (HERE / "fixed.flag").exists():
        raise ValueError("데이터 오류: 재시도해도 계속 실패")
    with open(HERE / "processed.log", "a") as f:  # verify.py 가 key별 순서를 검사한다
        f.write(f"{event['user']} {event['n']}\n")
    print(f"  ✅ 처리 {event['user']} n={event['n']}")


def headers_of(msg) -> dict:
    return {k: v.decode() for k, v in (msg.headers() or [])}


c = make_consumer("payments-ordered-worker")
c.subscribe([MAIN, RETRY])
p = make_producer()
db = state.connect()


def forward(topic: str, msg, h: dict, **updates) -> None:
    h = {**h, **updates}
    p.produce(topic, key=msg.key(), value=msg.value(), headers=[(k, str(v).encode()) for k, v in h.items()])
    p.flush()  # 다음 토픽에 확실히 들어간 뒤에만 commit


def fail(msg, h: dict, key: str, seq: int, retry_count: int, err: Exception) -> None:
    nxt = retry_count + 1
    if nxt > MAX_RETRY:
        state.set_state(db, key, "DLQ")  # ★ 상태를 먼저 바꿔야 뒤 메시지들이 DLQ로 따라온다
        print(f"  ☠️  {key} seq={seq} 재시도 소진 → DLQ ({err}) — 이 key의 뒤 메시지는 DLQ에서 보류")
        forward(DLQ, msg, h, seq=seq, retry_count=nxt, last_error=err, original_topic=MAIN)
    else:
        delay = RETRY_DELAY_SEC[nxt]
        print(f"  ⚠️  {key} seq={seq} 실패({err}) → retry {nxt}회차, {delay}초 뒤")
        forward(RETRY, msg, h, seq=seq, retry_count=nxt, last_error=err, not_before=time.time() + delay)


def on_message(msg) -> None:
    h = headers_of(msg)
    event, key = decode(msg.value()), msg.key().decode()
    retry_count = int(h.get("retry_count", 0))
    seq = int(h["seq"]) if "seq" in h else None
    print(f"[{msg.topic()}] {key} n={event['n']} seq={seq} retry_count={retry_count}")

    if seq is None:  # 번호표 없음 = 본 토픽에서 막 온 새 메시지
        if state.get(db, key) is None:  # 막힌 key가 아님 → 평소처럼 처리
            try:
                handle(event, 0)
                return
            except Exception as e:  # 이 key의 첫 실패 → 이 메시지가 맨 앞(seq 0)이 된다
                fail(msg, h, key, state.take_ticket(db, key, f"{msg.topic()}:{msg.partition()}:{msg.offset()}"), 0, e)
                return
        seq = state.take_ticket(db, key, f"{msg.topic()}:{msg.partition()}:{msg.offset()}")  # 막힌 key → 줄 서기

    st = state.get(db, key)
    if st is None or seq < st[2]:
        print("  (이미 처리된 메시지의 중복 전달 → 무시)")
        return
    key_state, _, serving = st

    if seq > serving or key_state == "DLQ":  # 내 차례가 아님(또는 key가 DLQ에 막힘) → 보류
        if key_state == "DLQ":
            print(f"  ⏸️  {key} seq={seq} DLQ에 앞 메시지가 있어 DLQ로 보류")
            forward(DLQ, msg, h, seq=seq, original_topic=MAIN)
        else:
            print(f"  ⏸️  {key} seq={seq} 앞(seq={serving})이 재시도 중이라 대기")
            forward(RETRY, msg, h, seq=seq, not_before=time.time() + PARK_DELAY_SEC)
        return

    wait = float(h.get("not_before", 0)) - time.time()  # 내 차례 + retry 지연 대기 (실무는 pause()/resume())
    if wait > 0:
        time.sleep(wait)
    try:
        handle(event, retry_count)
        if state.advance(db, key):
            print(f"  🔓 {key} 밀린 메시지 모두 처리 → 막힘 해제")
    except Exception as e:
        fail(msg, h, key, seq, retry_count, e)


print("대기 중... (Ctrl+C로 종료)")
try:
    while True:
        m = c.poll(1.0)
        if m is None or m.error():
            continue
        on_message(m)
        c.commit(message=m, asynchronous=False)
except KeyboardInterrupt:
    pass
finally:
    c.close()
