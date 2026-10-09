"""lag-demo 토픽에 메시지를 빠르게 쌓는다. 사용: python fill_topic.py [건수]"""
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from common import make_producer, recreate_topic  # noqa: E402

TOPIC = "lag-demo"
N = int(sys.argv[1]) if len(sys.argv) > 1 else 3000
recreate_topic(TOPIC, partitions=3)
p = make_producer()
for i in range(N):
    p.produce(TOPIC, key=f"k-{i % 30}", value=str(i))
    p.poll(0)
p.flush()
print(f"{TOPIC} 에 {N}건 적재")
