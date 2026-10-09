"""orders 토픽 안의 데이터 자체가 순서대로 들어있는지 점검 (컨슈머와 무관하게 파티션을 직접 읽는다).
순서 위반이 컨슈머 코드 탓인지, 토픽 데이터 탓인지 가를 때 쓴다.
사용: python diagnose_topic.py"""
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from confluent_kafka import Consumer, TopicPartition  # noqa: E402

from common import BOOTSTRAP, decode  # noqa: E402

TOPIC = "orders"
c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "diagnose-tool", "enable.auto.commit": False})

total, inversions, seq1_count = 0, 0, {}
last = {}
parts = c.list_topics(TOPIC, timeout=10).topics[TOPIC].partitions
for p in sorted(parts):
    lo, hi = c.get_watermark_offsets(TopicPartition(TOPIC, p), timeout=10)
    c.assign([TopicPartition(TOPIC, p, lo)])
    n = 0
    while n < hi - lo:
        m = c.poll(2)
        if m is None or m.error():
            continue
        n += 1
        e = decode(m.value())
        if e["seq"] == 1:
            seq1_count[e["key"]] = seq1_count.get(e["key"], 0) + 1
        if e["seq"] < last.get(e["key"], 0):
            inversions += 1
        last[e["key"]] = max(last.get(e["key"], 0), e["seq"])
    total += n
    print(f"partition {p}: {n}건")
c.close()

print(f"\n총 {total}건 / 토픽 데이터 자체의 순서 역전: {inversions}건")
dup = {k: v for k, v in seq1_count.items() if v > 1}
if dup:
    print(f"⚠️ seq=1이 여러 번 나온 key: {dup}")
    print("   → producer_load.py가 여러 번 실행돼 데이터가 섞였을 가능성 (seq가 처음부터 다시 시작)")
elif inversions == 0:
    print("토픽 데이터는 정상 → 위반은 컨슈머 쪽(재시작/중복 읽기 등)에서 생긴 것")
