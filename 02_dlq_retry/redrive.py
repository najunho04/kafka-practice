"""재발행(redrive): DLQ에 쌓인 메시지를 원래 토픽(payments)으로 다시 보낸다.
버그를 고친 뒤 실행하는 운영 도구. retry_count는 0으로 초기화한다.
사용: python redrive.py"""
import pathlib
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from common import make_consumer, make_producer, uniq  # noqa: E402

MAIN, DLQ = "payments", "payments.DLQ"

c = make_consumer(uniq("redrive"))
c.subscribe([DLQ])
p = make_producer()
count, last = 0, time.time()

while time.time() - last < 8:  # 8초간 새 메시지가 없으면 종료
    msg = c.poll(1.0)
    if msg is None or msg.error():
        continue
    last = time.time()
    err = next((v.decode() for k, v in msg.headers() or [] if k == "last_error"), "?")
    print(f"재발행: key={msg.key().decode()} (DLQ 사유: {err})")
    p.produce(MAIN, key=msg.key(), value=msg.value(), headers=[("redriven", b"1")])
    count += 1

p.flush()
c.close()
print(f"총 {count}건을 {MAIN} 으로 재발행")
