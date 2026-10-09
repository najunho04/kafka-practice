"""원본 producer_load.py(멱등 설정 없음, 기본 producer 설정, 2000건, 파티션 3개)를 그대로 N번 반복 실행하며
  (1) 토픽에 저장된 순서 역전 여부/위치
  (2) producer 내부 로그에 '리더 정보 없이 보냈다가 거절(Not leader / unknown topic ...)'된 흔적
을 실행마다 기록하고 둘의 상관관계를 표로 보여준다.
※ 원본 코드와 다른 점은 로그 수집(debug)뿐이다. 사용자 실습 토픽(orders)과 겹치지 않게 orders-proof 토픽을 쓴다.
사용: python proof_original_producer.py [반복횟수=50]"""
import logging
import pathlib
import re
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from confluent_kafka import Consumer, Producer, TopicPartition  # noqa: E402

from common import BOOTSTRAP, decode, encode, recreate_topic  # noqa: E402

TOPIC = "orders-proof"
N = 2000
RUNS = int(sys.argv[1]) if len(sys.argv) > 1 else 50
OUT = pathlib.Path(__file__).resolve().parent / "proof_logs"
OUT.mkdir(exist_ok=True)
INTEREST = re.compile(r"error|Not leader|UNKNOWN_TOPIC|LEADER_NOT|REQERR|BROKERUA|retr", re.I)
REJECT = re.compile(r"encountered error|Not leader|UNKNOWN_TOPIC|LEADER_NOT_AVAILABLE", re.I)


class Capture(logging.Handler):
    def __init__(self):
        super().__init__()
        self.lines = []

    def emit(self, record):
        self.lines.append(f"{record.created:.3f} {record.getMessage()}")


def original_producer_load(cap: Capture) -> float:
    """producer_load.py 원본 로직 (멱등 X, 기본 설정). 반환: 토픽 생성 완료 ~ 첫 produce까지 걸린 시간"""
    recreate_topic(TOPIC, partitions=3)
    t_created = time.time()
    logger = logging.getLogger(f"p{time.time()}")
    logger.addHandler(cap)
    logger.setLevel(logging.DEBUG)
    p = Producer({"bootstrap.servers": BOOTSTRAP, "debug": "topic,metadata,msg"}, logger=logger)
    seq = {}
    for i in range(N):
        key = f"user-{i % 10}"
        seq[key] = seq.get(key, 0) + 1
        p.produce(TOPIC, key=key, value=encode({"key": key, "seq": seq[key]}))
        p.poll(0)
    p.flush()
    p.poll(0)
    return t_created


def read_back():
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "proof-reader", "enable.auto.commit": False})
    inversions, total, last = [], 0, {}
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


table = {"inv_and_reject": 0, "inv_no_reject": 0, "noinv_and_reject": 0, "noinv_no_reject": 0}
summary_path = OUT / "summary.txt"
with open(summary_path, "w", encoding="utf-8") as summary:
    for r in range(1, RUNS + 1):
        cap = Capture()
        original_producer_load(cap)
        total, inv = read_back()
        rejects = [ln for ln in cap.lines if REJECT.search(ln)]
        has_inv, has_rej = bool(inv), bool(rejects)
        table[("inv" if has_inv else "noinv") + ("_and_reject" if has_rej else "_no_reject")] += 1

        parts = sorted({p for p, _ in inv})
        where = f"파티션 {parts}, offset {min(o for _, o in inv)}~{max(o for _, o in inv)}" if inv else "-"
        line = (f"#{r:02d} 저장 {total}건 | 순서역전 {len(inv):3d}건 ({where}) | "
                f"거절/오류 로그 {len(rejects)}줄")
        print(line, flush=True)
        summary.write(line + "\n")
        if has_inv or has_rej:  # 증거 로그 보관
            with open(OUT / f"run_{r:02d}.log", "w", encoding="utf-8") as f:
                f.write(line + "\n\n[관련 로그]\n")
                f.write("\n".join(ln for ln in cap.lines if INTEREST.search(ln) or "Leader" in ln or "leader" in ln))

    result = (
        f"\n== {RUNS}회 결과 (순서역전 × 거절로그)\n"
        f"  순서역전 O + 거절로그 O : {table['inv_and_reject']}\n"
        f"  순서역전 O + 거절로그 X : {table['inv_no_reject']}   ← 0이어야 '거절 없이는 역전 없음'\n"
        f"  순서역전 X + 거절로그 O : {table['noinv_and_reject']}   ← 거절돼도 역전 안 난 경우(타이밍)\n"
        f"  순서역전 X + 거절로그 X : {table['noinv_no_reject']}\n"
    )
    print(result)
    summary.write(result)
