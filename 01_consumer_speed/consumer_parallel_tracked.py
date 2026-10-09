"""4단계 parallel (심화): 레코드 단위로 완료를 추적하고, '연속으로 끝난 구간까지만' commit 한다.
(Confluent Parallel Consumer의 ordering=KEY 모드 + 처리된 레코드만 커밋 하는 방식을 단순화해 직접 구현)

consumer_parallel.py 와의 차이
  - 배치 전체가 끝나길 기다리지 않는다. 레코드 1건이 끝나면 같은 key의 다음 건을 바로 내보낸다.
  - KEY 순서: 같은 (파티션,key)는 앞 건이 끝나기 전엔 다음 건을 실행하지 않는다 (스레드 고정 X, 스케줄링 규칙).
  - commit: 파티션별로 '빈틈 없이 끝난 offset 구간의 끝+1' 까지만 commit.
    (u2의 offset 20이 u1의 offset 10보다 먼저 끝나도 20을 commit하면 10이 유실되므로)
  - 한계: 먼저 끝난 offset을 commit 메타데이터에 저장하지는 않는다 → 크래시 후 재시작하면 그 건들은 '중복' 처리된다.

검증용 옵션
  --crash-after K   K건 완료 시점에 commit/close 없이 즉시 종료(강제 종료 흉내)
  --naive-commit    '받자마자 commit'(처리 완료 추적 없음) → 크래시 시 유실을 보여주기 위한 나쁜 예
  --group G         같은 그룹으로 재시작해서 이어서 처리 (생략하면 새 그룹 + done 로그 초기화)
실행이 끝나면 done 로그와 토픽 offset을 대조해 유실/중복 건수를 출력한다.
사용: python consumer_parallel_tracked.py [옵션]"""
import argparse
import os
import pathlib
import random
import sys
import threading
import time
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from confluent_kafka import TopicPartition  # noqa: E402

from common import OrderChecker, decode, make_consumer, uniq  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument("--group")
ap.add_argument("--crash-after", type=int, default=0)
ap.add_argument("--naive-commit", action="store_true")
args = ap.parse_args()

TOPIC = "orders"
WORKERS = 10
MAX_INFLIGHT = 300  # 대기+실행 중 레코드 상한 (너무 많이 당겨오지 않도록)
DONE_LOG = pathlib.Path(__file__).resolve().parent / "tracked_done.log"

group = args.group or uniq("tracked")
if args.group is None:
    DONE_LOG.unlink(missing_ok=True)  # 새 그룹이면 검증 로그도 새로
print(f"group={group}  (이어서 실행하려면 --group {group})", flush=True)

lock = threading.Lock()
pool = ThreadPoolExecutor(max_workers=WORKERS)
queues = defaultdict(deque)   # (partition, key) -> 대기 중인 레코드
running = set()               # 실행 중인 (partition, key)  ← key당 동시에 1건
inflight = defaultdict(int)   # partition -> 아직 안 끝난 레코드 수
done = defaultdict(set)       # partition -> 끝났지만 아직 commit 안 된 offset들
commit_next = {}              # partition -> 연속 완료 구간의 다음 offset (= commit 후보)
completed = 0
checker = OrderChecker()
done_fh = open(DONE_LOG, "a", buffering=1)


def schedule(k):
    """(lock 보유 상태) 이 key의 다음 레코드를 내보낼 수 있으면 내보낸다."""
    if k in running or not queues[k]:
        return
    rec = queues[k].popleft()
    running.add(k)
    pool.submit(run, rec)


def run(rec):
    global completed
    p, off, ev = rec
    time.sleep(random.uniform(0.005, 0.015))  # 실제 처리(DB 호출) 흉내
    checker.check(ev["key"], ev["seq"])
    with lock:
        done_fh.write(f"{p} {off}\n")  # 검증용 '처리 완료' 기록
        done[p].add(off)
        inflight[p] -= 1
        running.discard((p, ev["key"]))
        completed += 1
        if args.crash_after and completed >= args.crash_after:
            print(f"💥 {completed}건 완료 시점에 강제 종료 (commit/close 없음)", flush=True)
            os._exit(1)
        schedule((p, ev["key"]))


def advance_and_commit():
    """파티션별로 빈틈 없이 끝난 구간까지만 commit (consumer는 스레드 안전하지 않아 메인 스레드에서만 호출)."""
    offsets = []
    with lock:
        for p, nxt in list(commit_next.items()):
            n = nxt
            while n in done[p]:
                done[p].discard(n)
                n += 1
            if n != nxt:
                commit_next[p] = n
                offsets.append(TopicPartition(TOPIC, p, n))
    if offsets:
        c.commit(offsets=offsets, asynchronous=False)


def on_revoke(consumer, parts):
    """리밸런스: 넘겨줄 파티션의 대기 레코드는 버리고, 실행 중인 건은 끝나길 기다린 뒤 commit."""
    revoked = {tp.partition for tp in parts}
    with lock:
        for k in list(queues):
            if k[0] in revoked:
                inflight[k[0]] -= len(queues[k])
                queues[k].clear()
    while True:
        with lock:
            if not any(k[0] in revoked for k in running):
                break
        time.sleep(0.01)
    advance_and_commit()
    with lock:
        for p in revoked:
            commit_next.pop(p, None)
            done.pop(p, None)
            inflight.pop(p, None)
    print(f"REVOKE {sorted(revoked)} (진행분 commit 후 반납)", flush=True)


# 강제 종료된 이전 컨슈머가 그룹에서 빠지길 기다리는 시간(session.timeout.ms)을 최소값으로 줄임
# (기본 45초. 크래시 후 재시작 실습이 오래 걸리지 않게. 06_graceful_shutdown 참고)
c = make_consumer(group, **{"session.timeout.ms": 6000})
c.subscribe([TOPIC], on_revoke=on_revoke)
start, last_msg = time.time(), time.time()
received_any = False
last_commit = 0.0

while True:
    with lock:
        backlog = sum(inflight.values())
    if backlog >= MAX_INFLIGHT:
        time.sleep(0.02)  # (실무에서는 pause()/resume()으로 파티션 fetch를 멈춘다)
    else:
        msgs = c.consume(num_messages=min(100, MAX_INFLIGHT - backlog), timeout=0.2)
        for m in msgs:
            if m.error():
                continue
            last_msg = time.time()
            received_any = True
            ev = decode(m.value())
            p, off = m.partition(), m.offset()
            k = (p, ev["key"])
            with lock:
                commit_next.setdefault(p, off)
                inflight[p] += 1
                queues[k].append((p, off, ev))
                schedule(k)
        if args.naive_commit and msgs:
            c.commit(asynchronous=False)  # 나쁜 예: 처리 여부와 무관하게 받은 위치까지 commit
    if not args.naive_commit and time.time() - last_commit > 0.3:
        advance_and_commit()
        last_commit = time.time()
    with lock:
        backlog = sum(inflight.values())
    # 첫 메시지를 받기 전엔 그룹 합류/이전 컨슈머 만료를 기다려야 해서 더 길게 기다린다
    if backlog == 0 and time.time() - last_msg > (10 if received_any else 30):
        break

advance_and_commit()
elapsed = time.time() - start
c.close()
pool.shutdown()
done_fh.close()

# ---- 검증: done 로그 vs 토픽에 실제로 있는 offset ----
from common import make_consumer as _mk  # noqa: E402

v = _mk("tracked-verify")
seen, lines = defaultdict(set), 0
for line in DONE_LOG.read_text().split("\n"):
    if line:
        p, off = map(int, line.split())
        seen[p].add(off)
        lines += 1
total = missing = 0
for p in sorted(v.list_topics(TOPIC, timeout=10).topics[TOPIC].partitions):
    lo, hi = v.get_watermark_offsets(TopicPartition(TOPIC, p), timeout=10)
    total += hi - lo
    missing += len(set(range(lo, hi)) - seen[p])
v.close()
distinct = sum(len(s) for s in seen.values())
mode = "naive-commit" if args.naive_commit else "tracked(연속구간 commit)"
print(f"\n[{mode}] 토픽 {total}건 | 처리 완료 기록 {lines}줄 (서로 다른 offset {distinct}) | "
      f"유실 {missing}건 | 중복 처리 {lines - distinct}건 | 같은 key 순서 위반 {checker.violations}건 | {elapsed:.1f}초")
