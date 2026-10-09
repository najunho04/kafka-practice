"""실패 처리 흐름:
  payments ──실패──▶ payments.retry.1 (5초 뒤) ──실패──▶ payments.retry.2 (10초 뒤) ──실패──▶ payments.DLQ
핵심: 실패한 1건 때문에 뒤 메시지가 막히지 않는다(본 흐름은 계속 진행).
순서: '다음 토픽에 발행(flush) → 원본 commit' (at-least-once: 중복은 가능, 유실은 없음)

FIX_BUG=1 로 실행하면 poison 메시지도 성공 처리(버그를 고쳐서 배포한 상황)."""
import os
import pathlib
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from common import decode, make_consumer, make_producer  # noqa: E402

MAIN, RETRY1, RETRY2, DLQ = "payments", "payments.retry.1", "payments.retry.2", "payments.DLQ"
RETRY_TOPICS = {1: RETRY1, 2: RETRY2}
RETRY_DELAY_SEC = {1: 5, 2: 10}
MAX_RETRY = 2
FIX_BUG = os.getenv("FIX_BUG") == "1"


def handle(event: dict, retry_count: int) -> None:
    kind = event["kind"]
    if kind == "transient" and retry_count == 0:
        raise RuntimeError("일시적 오류(예: 외부 API 타임아웃)")
    if kind == "poison" and not FIX_BUG:
        raise ValueError("데이터 오류: 재시도해도 계속 실패")
    print(f"  ✅ 처리 성공 id={event['id']} (retry_count={retry_count})")


def get_header(msg, name, default=None):
    for k, v in msg.headers() or []:
        if k == name:
            return v.decode()
    return default


c = make_consumer("payments-worker")
c.subscribe([MAIN, RETRY1, RETRY2])
p = make_producer()
print("대기 중... (Ctrl+C로 종료)")

try:
    while True:
        msg = c.poll(1.0)
        if msg is None or msg.error():
            continue
        event = decode(msg.value())
        retry_count = int(get_header(msg, "retry_count", "0"))

        # retry 토픽이면 not_before 시각까지 기다린다.
        # (단순화를 위해 sleep. 실무에서는 pause()/resume()으로 파티션만 멈춰 max.poll.interval.ms를 지킨다)
        not_before = float(get_header(msg, "not_before", "0"))
        if not_before > time.time():
            time.sleep(not_before - time.time())

        try:
            print(f"[{msg.topic()}] id={event['id']} kind={event['kind']} retry_count={retry_count}")
            handle(event, retry_count)
        except Exception as e:
            nxt = retry_count + 1
            headers = [("retry_count", str(nxt).encode()), ("last_error", str(e).encode()),
                       ("original_topic", MAIN.encode())]
            if nxt > MAX_RETRY:
                print(f"  ☠️  재시도 소진 → DLQ 이동 id={event['id']} ({e})")
                p.produce(DLQ, key=msg.key(), value=msg.value(), headers=headers)
            else:
                delay = RETRY_DELAY_SEC[nxt]
                print(f"  ⚠️  실패({e}) → {RETRY_TOPICS[nxt]} 로 이동, {delay}초 뒤 재시도")
                headers.append(("not_before", str(time.time() + delay).encode()))
                p.produce(RETRY_TOPICS[nxt], key=msg.key(), value=msg.value(), headers=headers)
            p.flush()  # 다음 토픽에 확실히 들어간 뒤에만
        c.commit(message=msg, asynchronous=False)  # 원본 commit
except KeyboardInterrupt:
    pass
finally:
    c.close()
