"""일부러 느린 컨슈머(초당 약 20건) → lag이 천천히 줄어드는 것을 lag.py --watch 로 관찰.
여러 터미널에서 이 스크립트를 동시에 띄우면(같은 그룹) lag이 더 빨리 줄어드는 것도 확인 가능.
사용: python slow_consumer.py"""
import pathlib
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from common import make_consumer  # noqa: E402

c = make_consumer("lag-demo-group")
c.subscribe(["lag-demo"])
n = 0
try:
    while True:
        msg = c.poll(1.0)
        if msg is None or msg.error():
            continue
        time.sleep(0.05)  # 건당 50ms
        n += 1
        if n % 10 == 0:
            c.commit(asynchronous=False)
            print(f"처리 {n}건", end="\r", flush=True)
except KeyboardInterrupt:
    pass
finally:
    c.close()
