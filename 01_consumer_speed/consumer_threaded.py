"""3단계 multi-thread: 받은 배치를 스레드풀에 '아무렇게나' 나눠 처리.
빠르지만 같은 key의 순서가 깨진다 → 마지막에 '순서 위반' 건수로 확인.
commit은 배치의 모든 작업이 끝난 뒤에만 한다(일부만 끝난 상태에서 commit하면 유실 위험).
사용: python consumer_threaded.py [기대건수]"""
import pathlib
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from common import Meter, OrderChecker, decode, make_consumer, uniq  # noqa: E402

TOPIC = "orders"
N = int(sys.argv[1]) if len(sys.argv) > 1 else 2000

c = make_consumer(uniq("threaded"))
c.subscribe([TOPIC])
meter, checker = Meter(), OrderChecker()
pool = ThreadPoolExecutor(max_workers=8)


def work(event):
    time.sleep(random.uniform(0.005, 0.015))  # 처리 시간이 들쭉날쭉한 DB 호출
    checker.check(event["key"], event["seq"])


while meter.count < N:
    msgs = [m for m in c.consume(num_messages=200, timeout=1.0) if not m.error()]
    if not msgs:
        if meter.idle():
            break
        continue
    futures = [pool.submit(work, decode(m.value())) for m in msgs]
    for f in futures:
        f.result()  # 하나라도 예외면 여기서 터진다 → commit 안 함 → 재시작 시 배치 재처리
    c.commit(asynchronous=False)
    meter.tick(len(msgs))

meter.report("multi-thread", checker)
pool.shutdown()
c.close()
