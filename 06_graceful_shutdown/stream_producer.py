"""배포 시뮬레이션용: deploy-demo 토픽(파티션 4개)에 초당 약 10건 계속 발행. Ctrl+C/SIGTERM으로 종료."""
import pathlib
import signal
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from common import ensure_topic, make_producer  # noqa: E402

TOPIC = "deploy-demo"
ensure_topic(TOPIC, partitions=4)
p = make_producer()
running = True


def stop(*_):
    global running
    running = False


signal.signal(signal.SIGTERM, stop)
signal.signal(signal.SIGINT, stop)

i = 0
while running:
    p.produce(TOPIC, key=f"k-{i % 20}", value=str(i))
    p.poll(0)
    i += 1
    time.sleep(0.1)
p.flush()
print(f"총 {i}건 발행")
