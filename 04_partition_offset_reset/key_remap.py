"""파티션을 늘리면 hash(key) % 파티션수 결과가 바뀌어 '같은 key가 다른 파티션'으로 간다
→ 증설 전/후에 같은 key의 순서 보장이 깨질 수 있다.
사용: python key_remap.py"""
import pathlib
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from confluent_kafka.admin import NewPartitions  # noqa: E402

from common import admin_client, make_producer, recreate_topic  # noqa: E402

TOPIC = "remap-demo"
KEYS = [f"user-{i}" for i in range(10)]


def where(keys) -> dict:
    """각 key가 어느 파티션에 저장됐는지 (delivery 결과로 확인)."""
    result = {}
    for _ in range(20):
        result.clear()
        p = make_producer(partitioner="murmur2_random")  # Java 클라이언트와 같은 해시
        for k in keys:
            p.produce(TOPIC, key=k, value="x",
                      on_delivery=lambda err, m, k=k: result.__setitem__(k, m.partition()) if not err else None)
        p.flush()
        if len(result) == len(keys):
            return dict(result)
        time.sleep(0.5)
    raise RuntimeError("발행 실패")


recreate_topic(TOPIC, partitions=3)
before = where(KEYS)
for f in admin_client().create_partitions([NewPartitions(TOPIC, 4)]).values():
    f.result()
time.sleep(2)
after = where(KEYS)

print(f"{'key':10s} 3개 파티션일 때 → 4개로 증설 후")
moved = 0
for k in KEYS:
    flag = "  ← 이동!" if before[k] != after[k] else ""
    moved += before[k] != after[k]
    print(f"{k:10s} p{before[k]}            p{after[k]}{flag}")
print(f"\n{len(KEYS)}개 key 중 {moved}개가 다른 파티션으로 이동")
