"""2단계 batch: 한 번에 N건 받아서 묶어서 처리(bulk insert) + commit 1번.
한계: 스레드 1개라 I/O 대기 중에는 그냥 논다.
사용: python consumer_batch.py [기대건수]"""
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from common import Meter, OrderChecker, decode, make_consumer, simulate_io, uniq  # noqa: E402

TOPIC = "orders"
N = int(sys.argv[1]) if len(sys.argv) > 1 else 2000

c = make_consumer(uniq("batch"))
c.subscribe([TOPIC])
meter, checker = Meter(), OrderChecker()

while meter.count < N:
    msgs = [m for m in c.consume(num_messages=200, timeout=1.0) if not m.error()]
    if not msgs:
        if meter.idle():
            break
        continue
    events = [decode(m.value()) for m in msgs]
    simulate_io(10 + 0.2 * len(events))  # bulk insert: 고정비용 10ms + 건당 0.2ms
    for e in events:
        checker.check(e["key"], e["seq"])
    c.commit(asynchronous=False)  # 배치당 commit 1번
    meter.tick(len(msgs))

meter.report("batch", checker)
c.close()
