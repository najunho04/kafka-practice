"""컨슈머 그룹의 lag 조회 (kafka-consumer-groups.sh --describe 와 같은 계산).
  lag = 파티션의 최신 offset(log end) - 그룹이 commit한 offset
사용: python lag.py <group> <topic> [--watch] [--alert N]"""
import argparse
import pathlib
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from confluent_kafka import TopicPartition  # noqa: E402

from common import make_consumer  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument("group")
ap.add_argument("topic")
ap.add_argument("--watch", action="store_true", help="2초마다 반복")
ap.add_argument("--alert", type=int, default=500, help="총 lag이 이 값을 넘으면 경고")
args = ap.parse_args()

# 같은 group.id로 조회만 한다 (subscribe/poll 하지 않으므로 리밸런스에 끼어들지 않는다)
c = make_consumer(args.group)


def show() -> None:
    meta = c.list_topics(args.topic, timeout=10).topics[args.topic]
    parts = [TopicPartition(args.topic, p) for p in meta.partitions]
    committed = {tp.partition: tp.offset for tp in c.committed(parts, timeout=10)}
    total = 0
    print(f"{time.strftime('%H:%M:%S')}  group={args.group} topic={args.topic}")
    print(f"  {'partition':>9} {'committed':>10} {'log-end':>9} {'lag':>7}")
    for tp in sorted(parts, key=lambda x: x.partition):
        _low, high = c.get_watermark_offsets(tp, timeout=10)
        cur = committed.get(tp.partition, -1001)  # -1001 = commit 기록 없음
        lag = high - (cur if cur >= 0 else _low)
        total += lag
        print(f"  {tp.partition:>9} {cur if cur >= 0 else '-':>10} {high:>9} {lag:>7}")
    flag = "  🚨 임계치 초과 → 컨슈머 증설/파티션 증설/DLQ 확인" if total > args.alert else ""
    print(f"  total lag = {total}{flag}\n")


try:
    while True:
        show()
        if not args.watch:
            break
        time.sleep(2)
except KeyboardInterrupt:
    pass
finally:
    c.close()
