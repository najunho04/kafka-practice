"""배포(롤링 재시작) 시 컨슈머 동작 관찰.

환경변수
  GRACEFUL=1(기본)  SIGTERM을 받으면 → 현재 메시지 마무리 → commit → consumer.close()(그룹 탈퇴)
  GRACEFUL=0        SIGTERM을 그냥 무시하지 않고 기본 동작(즉시 종료) → commit도 탈퇴도 없음
  ASSIGNOR=cooperative-sticky(기본) | range   파티션 재분배 전략(assigner)

kill -TERM <pid> 와 kill -9 <pid>(SIGKILL) 의 차이, 두 assignor의 리밸런스 로그 차이를 비교한다.
사용: python -u graceful_consumer.py <이름>"""
import os
import pathlib
import signal
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from common import make_consumer  # noqa: E402

NAME = sys.argv[1] if len(sys.argv) > 1 else str(os.getpid())
TOPIC = "deploy-demo"
GRACEFUL = os.getenv("GRACEFUL", "1") == "1"
ASSIGNOR = os.getenv("ASSIGNOR", "cooperative-sticky")
COOP = "cooperative" in ASSIGNOR
COMMIT_EVERY = 5  # 5건마다 commit → 갑자기 죽으면 최대 4건이 재처리(중복)된다

running = True
since_commit = 0


def log(text: str) -> None:
    print(f"{time.strftime('%H:%M:%S')} [{NAME}] {text}", flush=True)


def on_assign(consumer, parts):
    log(f"ASSIGN  {sorted(p.partition for p in parts)}")
    consumer.incremental_assign(parts) if COOP else consumer.assign(parts)


def on_revoke(consumer, parts):
    log(f"REVOKE  {sorted(p.partition for p in parts)}")
    try:
        consumer.commit(asynchronous=False)  # 넘겨주기 전에 내가 처리한 곳까지 commit
    except Exception:
        pass
    consumer.incremental_unassign(parts) if COOP else consumer.unassign()


def on_term(*_):
    global running
    log("SHUTDOWN 신호 수신 → 마무리 시작")
    running = False


if GRACEFUL:
    signal.signal(signal.SIGTERM, on_term)
    signal.signal(signal.SIGINT, on_term)

c = make_consumer("deploy-demo-group", **{
    "partition.assignment.strategy": ASSIGNOR,
    "session.timeout.ms": 10000,  # SIGKILL 시 브로커가 '죽었다'고 판단하기까지 걸리는 시간
})
c.subscribe([TOPIC], on_assign=on_assign, on_revoke=on_revoke)
log(f"시작 (graceful={GRACEFUL}, assignor={ASSIGNOR})")

while running:
    msg = c.poll(0.5)
    if msg is None or msg.error():
        continue
    time.sleep(0.3)  # 처리 시간 (이 도중에 종료 신호가 와도 현재 메시지는 끝까지 처리)
    print(f"[{NAME}] processed p={msg.partition()} off={msg.offset()}", flush=True)
    since_commit += 1
    if since_commit >= COMMIT_EVERY:
        c.commit(asynchronous=False)
        since_commit = 0

# graceful 종료: 남은 offset commit + 그룹 탈퇴(LeaveGroup) → 다른 컨슈머가 즉시 파티션을 받는다
try:
    c.commit(asynchronous=False)
except Exception:
    pass
c.close()
log("정상 종료 (commit + 그룹 탈퇴 완료)")
