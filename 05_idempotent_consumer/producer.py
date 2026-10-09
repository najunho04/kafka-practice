"""order-events 토픽 생성 + DB 초기화 + 이벤트 발행. e-2, e-4는 일부러 2번 발행(중복 재발행 상황)."""
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import db  # noqa: E402
from common import encode, make_producer, recreate_topic  # noqa: E402

TOPIC = "order-events"
recreate_topic(TOPIC, partitions=3)
db.init()

p = make_producer()
events = ["e-1", "e-2", "e-3", "e-4", "e-5", "e-2", "e-4"]  # 중복 2건
for ev in events:
    p.produce(TOPIC, key=ev, value=encode({"event_id": ev, "order_id": ev.split("-")[1]}))
p.flush()
print(f"{len(events)}건 발행 (e-2, e-4 중복) → 푸시는 5번만 나가야 정상")
