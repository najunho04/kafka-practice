"""부하용 메시지 생성: key 10종(user-0~9), 각 key마다 seq가 1,2,3... 증가.
사용: python producer_load.py [건수]"""
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from common import encode, make_producer, recreate_topic  # noqa: E402

TOPIC = "orders"
N = int(sys.argv[1]) if len(sys.argv) > 1 else 2000

recreate_topic(TOPIC, partitions=3)
# 토픽 생성 직후엔 일부 전송이 실패→재시도되며 순서가 뒤집힐 수 있다.
# 멱등 producer는 재시도해도 key별 순서를 보존한다.
p = make_producer(**{"enable.idempotence": True})
seq = {}
for i in range(N):
    key = f"user-{i % 10}"
    seq[key] = seq.get(key, 0) + 1
    p.produce(TOPIC, key=key, value=encode({"key": key, "seq": seq[key]}))
    p.poll(0)
p.flush()
print(f"{TOPIC} 토픽에 {N}건 발행 완료 (key 10종, 파티션 3개)")
