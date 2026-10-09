"""1단계 record: 1건 받고 → 처리(DB 10ms) → commit, 반복. 가장 느리다.
사용: python consumer_record.py [기대건수]"""
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from common import Meter, OrderChecker, decode, make_consumer, simulate_io, uniq  # noqa: E402

TOPIC = "orders"
N = int(sys.argv[1]) if len(sys.argv) > 1 else 2000

c = make_consumer(uniq("record"))
c.subscribe([TOPIC])
meter, checker = Meter(), OrderChecker()

while meter.count < N:
    msg = c.poll(1.0)
    if msg is None:
        if meter.idle():
            break
        continue
    if msg.error():
        print(msg.error())
        continue
    event = decode(msg.value())
    simulate_io(10)  # 건별 DB insert
    checker.check(event["key"], event["seq"])
    c.commit(message=msg, asynchronous=False)  # 건별 commit (네트워크 왕복 매번 발생)
    meter.tick()

meter.report("record", checker)
c.close()
