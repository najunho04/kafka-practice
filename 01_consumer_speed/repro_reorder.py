"""가설 검증: '토픽 생성 직후 전송 실패 → 재시도 → 순서 역전' 이 실제로 일어나는가?

조건 3가지를 여러 번 반복해 비교한다. (사용자 실습 토픽 orders와 겹치지 않게 repro-reorder 토픽 사용)
  A) 멱등 OFF + 토픽 만들자마자 발행
  B) 멱등 ON  + 토픽 만들자마자 발행
  C) 멱등 OFF + 토픽 만든 뒤 충분히 기다렸다 발행
각 실행마다: producer 재시도 횟수(txretries), 전송 오류(txerrs), 토픽에 저장된 순서 역전 건수와 그 위치(offset)를 출력.
사용: python repro_reorder.py [반복횟수=8] [건수=2000]"""
import json
import logging
import os
import pathlib
import re
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from confluent_kafka import Consumer, Producer, TopicPartition  # noqa: E402

from common import BOOTSTRAP, decode, encode, recreate_topic  # noqa: E402

TOPIC = os.getenv("REPRO_TOPIC", "repro-reorder")
ONLY = os.getenv("REPRO_ONLY", "ABC")  # 실행할 조건 글자 (예: A, AC)
INTEREST = re.compile(r"retr|REQERR|UNKNOWN_TOPIC|LEADER|requeue|ERR|Reset|leader", re.I)
REPEAT = int(sys.argv[1]) if len(sys.argv) > 1 else 8
N = int(sys.argv[2]) if len(sys.argv) > 2 else 2000


class Capture(logging.Handler):
    def __init__(self):
        super().__init__()
        self.lines = []

    def emit(self, record):
        msg = record.getMessage()
        if INTEREST.search(msg):
            self.lines.append(msg)


def produce_all(idempotent: bool, wait: bool):
    recreate_topic(TOPIC, partitions=3)
    cap = Capture()
    logger = logging.getLogger(f"prod-{time.time()}")
    logger.addHandler(cap)
    logger.setLevel(logging.DEBUG)
    if wait:
        time.sleep(5)  # 리더 지정/메타데이터 전파가 끝나길 충분히 기다림
    stats = {"txretries": 0, "txerrs": 0}
    failed = []

    def stats_cb(js):
        brokers = json.loads(js).get("brokers", {})
        stats["txretries"] = sum(b.get("txretries", 0) for b in brokers.values())
        stats["txerrs"] = sum(b.get("txerrs", 0) for b in brokers.values())

    p = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "enable.idempotence": idempotent,
        "max.in.flight.requests.per.connection": 5,   # 동시에 날리는 배치 수 (역전이 생기려면 >1)
        "batch.num.messages": 10,                     # 배치를 작게 → 배치가 많이 생겨 역전 기회 증가
        "linger.ms": 0,
        "statistics.interval.ms": 200,
        "stats_cb": stats_cb,
        "debug": "topic,metadata,msg",                # 재시도/리더 관련 내부 로그 수집
    }, logger=logger)
    seq = {}
    for i in range(N):
        key = f"user-{i % 10}"
        seq[key] = seq.get(key, 0) + 1
        while True:
            try:
                p.produce(TOPIC, key=key, value=encode({"key": key, "seq": seq[key]}),
                          on_delivery=lambda err, m: failed.append(str(err)) if err else None)
                break
            except BufferError:
                p.poll(0.05)
        p.poll(0)
    p.flush()
    time.sleep(0.6)
    p.poll(0)  # 마지막 통계 반영
    return stats, failed, cap.lines


def read_back():
    """각 파티션을 처음부터 읽어 key별 seq 역전 건수와 위치를 구한다."""
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "repro-reader", "enable.auto.commit": False})
    inversions = []  # (partition, offset)
    total = 0
    last = {}
    for part in range(3):
        lo, hi = c.get_watermark_offsets(TopicPartition(TOPIC, part), timeout=10)
        c.assign([TopicPartition(TOPIC, part, lo)])
        n = 0
        while n < hi - lo:
            m = c.poll(2)
            if m is None or m.error():
                continue
            n += 1
            e = decode(m.value())
            if e["seq"] < last.get(e["key"], 0):
                inversions.append((part, m.offset()))
            last[e["key"]] = max(last.get(e["key"], 0), e["seq"])
        total += n
    c.close()
    return total, inversions


CONDITIONS = [
    ("A) 멱등OFF + 즉시 발행", False, False),
    ("B) 멱등ON  + 즉시 발행", True, False),
    ("C) 멱등OFF + 5초 대기 후", False, True),
]

print(f"각 조건 {REPEAT}회 × {N}건\n")
summary = {}
for label, idem, wait in CONDITIONS:
    if label[0] not in ONLY:
        continue
    print(f"== {label}")
    runs_with_inversion = 0
    for r in range(1, REPEAT + 1):
        stats, failed, log_lines = produce_all(idem, wait)
        total, inv = read_back()
        if inv:
            runs_with_inversion += 1
        where = ""
        if inv:
            max_off = max(o for _, o in inv)
            where = f" | 역전 offset 범위: {min(o for _, o in inv)}~{max_off}"
        print(f"  #{r}: 저장 {total}건, 순서역전 {len(inv):3d}건 | 재시도 {stats['txretries']}, "
              f"전송오류 {stats['txerrs']}, 실패응답 {len(failed)}{where}", flush=True)
        if inv:  # 역전이 난 실행의 producer 내부 로그(재시도/리더/오류 관련)
            print("     [producer 내부 로그 중 관련 줄]")
            for line in log_lines[:15]:
                print("      ", line[:160])
    summary[label] = runs_with_inversion
    print()

print("== 요약 (순서 역전이 발생한 실행 횟수)")
for label, cnt in summary.items():
    print(f"  {label}: {cnt}/{REPEAT}")
