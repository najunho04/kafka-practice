"""DLQ 자동 재발행 데몬.
- DLQ 메시지를 일정 시간(지수 백오프)이 지나면 원래 토픽으로 되돌려 보낸다. 사람이 버튼을 누를 필요가 없다.
- 보내기 전에 key 상태를 RETRYING 으로 되돌린다 → worker 가 번호 순서(seq)대로 처리한다.
  (DLQ 안의 도착 순서가 뒤섞여 있어도 worker 의 번호표 검사가 순서를 복원한다)
- redrive_count 가 MAX_REDRIVE 를 넘으면 포기하고 사람에게 넘긴다(자동 재발행의 무한루프 방지).
  그 key는 DLQ 상태로 남아 뒤 메시지도 계속 보류된다."""
import pathlib
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import state  # noqa: E402
from common import make_consumer, make_producer  # noqa: E402

MAIN, DLQ = "payments", "payments.DLQ"
BASE_BACKOFF_SEC = 6
MAX_REDRIVE = 3

c = make_consumer("payments-redriver")
c.subscribe([DLQ])
p = make_producer()
db = state.connect()
print("DLQ 감시 중... (Ctrl+C로 종료)")

try:
    while True:
        msg = c.poll(1.0)
        if msg is None or msg.error():
            continue
        h = {k: v.decode() for k, v in (msg.headers() or [])}
        key, n = msg.key().decode(), int(h.get("redrive_count", 0))

        if n >= MAX_REDRIVE:
            print(f"🚨 {key} seq={h.get('seq')} 자동 재발행 {n}회 실패 → 수동 확인 필요 (last_error={h.get('last_error')})")
            c.commit(message=msg, asynchronous=False)
            continue

        due = msg.timestamp()[1] / 1000 + BASE_BACKOFF_SEC * 2**n  # DLQ에 들어온 시각 + 백오프
        if due > time.time():
            time.sleep(due - time.time())

        if state.get(db, key) is not None:
            state.set_state(db, key, "RETRYING")  # ★ 먼저 상태를 풀어야 worker 가 이 key를 다시 처리한다
        h.update(retry_count="0", redrive_count=str(n + 1))
        h.pop("not_before", None)
        print(f"♻️  재발행 {key} seq={h.get('seq')} ({n + 1}번째 자동 재발행)")
        p.produce(MAIN, key=msg.key(), value=msg.value(), headers=[(k, v.encode()) for k, v in h.items()])
        p.flush()
        c.commit(message=msg, asynchronous=False)
except KeyboardInterrupt:
    pass
finally:
    c.close()
