"""4단계 parallel: 'key 단위'로 병렬화.
같은 key의 메시지는 한 작업 안에서 순서대로, 다른 key끼리는 동시에 처리 → 순서 보장 + 속도.
(Confluent Parallel Consumer(Java)의 ordering=KEY 와 같은 아이디어를 단순화한 것)

더 올리고 싶다면: 이 스크립트를 터미널 여러 개에서 같은 그룹으로 실행(파티션 수 3까지 의미 있음).
사용: python consumer_parallel.py [기대건수]"""
import pathlib
import random
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from common import Meter, OrderChecker, decode, make_consumer, uniq  # noqa: E402

TOPIC = "orders"
N = int(sys.argv[1]) if len(sys.argv) > 1 else 2000

c = make_consumer(uniq("parallel"))
c.subscribe([TOPIC])
meter, checker = Meter(), OrderChecker()
pool = ThreadPoolExecutor(max_workers=10)


def work_one_key(events):
    for e in events:  # 같은 key는 순서대로
        time.sleep(random.uniform(0.005, 0.015))
        checker.check(e["key"], e["seq"])


while meter.count < N:
    msgs = [m for m in c.consume(num_messages=200, timeout=1.0) if not m.error()]
    if not msgs:
        if meter.idle():
            break
        continue
    by_key = defaultdict(list)
    for m in msgs:  # consume()가 파티션 내 offset 순서를 지켜주므로 key별 순서도 유지됨
        e = decode(m.value())
        by_key[e["key"]].append(e)
    futures = [pool.submit(work_one_key, evs) for evs in by_key.values()]
    for f in futures:
        f.result()
    # 장애 처리 정책 예: 여기서 예외가 나면 commit하지 않고 → 배치 전체 재처리(at-least-once)
    # 더 정교하게: 실패한 key만 retry 토픽으로 보내고 나머지는 commit (02_dlq_retry 참고)
    c.commit(asynchronous=False)
    meter.tick(len(msgs))

meter.report("parallel(key별)", checker)
pool.shutdown()
c.close()
