"""토픽 3개(payments / payments.retry / payments.DLQ) 생성 + user별 이벤트 발행.
user-1 : n=2 가 일시 오류(transient)      → 잠깐 retry, 뒤의 n=3,4 는 같이 보류됐다가 순서대로 처리
user-2 : n=2 가 독약(poison)              → DLQ로 가고, 뒤의 n=3,4,5 도 DLQ에서 같이 보류
user-3 : 전부 정상                         → 다른 key는 영향 없이 계속 처리"""
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import state  # noqa: E402
from common import encode, make_producer, recreate_topic  # noqa: E402

MAIN, RETRY, DLQ = "payments", "payments.retry", "payments.DLQ"
for t in (MAIN, RETRY, DLQ):
    recreate_topic(t, partitions=1)
state.init()
(pathlib.Path(__file__).resolve().parent / "fixed.flag").unlink(missing_ok=True)
(pathlib.Path(__file__).resolve().parent / "processed.log").unlink(missing_ok=True)

plan = {
    "user-1": ["ok", "transient", "ok", "ok"],
    "user-2": ["ok", "poison", "ok", "ok", "ok"],
    "user-3": ["ok", "ok", "ok"],
}
p = make_producer()
for n in range(5):  # 라운드 로빈으로 섞어서 발행 (key별 순서만 유지)
    for user, kinds in plan.items():
        if n < len(kinds):
            p.produce(MAIN, key=user, value=encode({"user": user, "n": n + 1, "kind": kinds[n]}))
p.flush()
print("발행 완료. user-1: n=2 일시오류 / user-2: n=2 독약 / user-3: 정상")
