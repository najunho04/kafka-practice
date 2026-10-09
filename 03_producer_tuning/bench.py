"""브로커 부담 줄이기: 배치 + 압축 설정별 비교.
(librdkafka 설정명 기준. Java의 batch.size / linger.ms / compression.type / buffer.memory 와 대응)
  batch.size                → batch.size, batch.num.messages
  linger.ms                 → linger.ms
  compression.type          → compression.type
  buffer.memory             → queue.buffering.max.kbytes
사용: python bench.py [건수]"""
import json
import pathlib
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from common import make_producer, recreate_topic  # noqa: E402

TOPIC = "tuning-demo"
N = int(sys.argv[1]) if len(sys.argv) > 1 else 50_000

CONFIGS = {
    "1) 건별 전송(배치 X)": {"linger.ms": 0, "batch.num.messages": 1, "compression.type": "none"},
    "2) 배치(linger 20ms)": {"linger.ms": 20, "batch.num.messages": 10000, "batch.size": 1_000_000,
                          "compression.type": "none"},
    "3) 배치 + lz4": {"linger.ms": 20, "batch.num.messages": 10000, "batch.size": 1_000_000,
                    "compression.type": "lz4"},
    "4) 배치 + zstd": {"linger.ms": 20, "batch.num.messages": 10000, "batch.size": 1_000_000,
                     "compression.type": "zstd"},
}


def payload(i: int) -> bytes:
    # 반복 문자열이 많아 압축이 잘 되는 JSON (실제 로그/이벤트도 보통 이렇다)
    return json.dumps({"id": i, "user": f"user-{i % 100}", "memo": "order-created " * 20}).encode()


def run(label: str, conf: dict) -> None:
    stats = {"tx_bytes": 0}
    delivered = {"n": 0}

    def stats_cb(js):
        stats["tx_bytes"] = json.loads(js).get("tx_bytes", 0)

    def on_delivery(err, _msg):
        if err is None:
            delivered["n"] += 1

    p = make_producer(**conf, **{"statistics.interval.ms": 500, "stats_cb": stats_cb})
    start = time.time()
    for i in range(N):
        while True:
            try:
                p.produce(TOPIC, key=str(i % 100), value=payload(i), on_delivery=on_delivery)
                break
            except BufferError:  # 내부 버퍼 가득 → 전송이 따라올 때까지 잠깐 대기
                p.poll(0.05)
        if i % 1000 == 0:
            p.poll(0)
    p.flush()
    elapsed = time.time() - start
    time.sleep(1.2)
    p.poll(0)  # 마지막 통계 수신
    mb = stats["tx_bytes"] / 1024 / 1024
    print(f"{label:22s} {elapsed:6.2f}초 | {N / elapsed:8.0f} msg/s | 네트워크 전송량 {mb:7.1f} MB | 성공 {delivered['n']}건")


recreate_topic(TOPIC, partitions=3)
print(f"메시지 {N}건 발행 비교")
for label, conf in CONFIGS.items():
    run(label, conf)
print("\n※ 배치/압축은 처리량을 올리고 전송량을 줄이는 대신, 메시지가 최대 linger.ms 만큼 늦게 나간다(latency↑).")
