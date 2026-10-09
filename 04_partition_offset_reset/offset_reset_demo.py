"""파티션 증설 시 auto.offset.reset=latest 면 메시지가 유실되는 것을 재현한다.

흐름 (latest / earliest 각각):
  1) 파티션 1개 토픽 + 컨슈머가 commit 기록을 하나 남김
  2) 파티션을 1 → 2 로 증설
  3) 새 파티션(1번)에 메시지 5건 즉시 발행
  4) 컨슈머가 새 파티션을 인식(리밸런스)한 뒤 몇 건을 받는지 센다
     - 새 파티션엔 이 그룹의 commit 기록이 없으므로 auto.offset.reset 정책이 적용된다.
사용: python offset_reset_demo.py   (약 40초)"""
import pathlib
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from confluent_kafka.admin import NewPartitions  # noqa: E402

from common import admin_client, make_consumer, make_producer, recreate_topic, uniq  # noqa: E402

TOPIC = "scale-demo"
NEW_MSGS = 5


def produce_to_partition(partition: int, n: int) -> None:
    """증설 직후엔 프로듀서 메타데이터가 낡았을 수 있어 성공할 때까지 재시도."""
    for _ in range(20):
        failed = []
        p = make_producer()  # 새 프로듀서 = 최신 메타데이터
        for i in range(n):
            p.produce(TOPIC, value=f"new-{i}", partition=partition,
                      on_delivery=lambda err, m: failed.append(err) if err else None)
        p.flush()
        if not failed:
            return
        time.sleep(0.5)
    raise RuntimeError("새 파티션에 발행 실패")


def run(reset: str) -> int:
    recreate_topic(TOPIC, partitions=1)
    c = make_consumer(uniq(f"reset-{reset}"), **{
        "auto.offset.reset": reset,
        "topic.metadata.refresh.interval.ms": 2000,  # 새 파티션을 빨리 감지 (기본은 5분)
    })
    c.subscribe([TOPIC])
    p = make_producer()

    # 1) 파티션 0 할당 + commit 기록 남기기 (할당 전에 보낸 건 latest면 놓칠 수 있어 반복 발행)
    deadline = time.time() + 30
    while time.time() < deadline:
        p.produce(TOPIC, value="warmup", partition=0)
        p.flush()
        msg = c.poll(1.0)
        if msg and not msg.error():
            c.commit(message=msg, asynchronous=False)
            break
    else:
        raise RuntimeError("워밍업 실패")

    # 2) 파티션 증설, 3) 새 파티션에 즉시 발행
    for f in admin_client().create_partitions([NewPartitions(TOPIC, 2)]).values():
        f.result()
    produce_to_partition(1, NEW_MSGS)

    # 4) 새 파티션 인식 후 받은 건수
    received = 0
    end = time.time() + 20
    while time.time() < end:
        msg = c.poll(1.0)
        if msg and not msg.error() and msg.partition() == 1:
            received += 1
    c.close()
    return received


print(f"파티션 증설 직후 새 파티션에 {NEW_MSGS}건 발행 → 컨슈머가 받은 건수")
for reset in ("latest", "earliest"):
    got = run(reset)
    verdict = "✅ 전부 수신" if got == NEW_MSGS else f"❌ {NEW_MSGS - got}건 유실"
    print(f"  auto.offset.reset={reset:9s} → {got}건 수신  {verdict}")
