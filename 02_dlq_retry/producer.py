"""payments 토픽 + retry/DLQ 토픽을 만들고 테스트 메시지 12건 발행.
kind: ok(정상) / transient(처음 1번만 실패 = 일시 오류) / poison(계속 실패 = 데이터 오류)"""
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from common import encode, make_producer, recreate_topic  # noqa: E402

MAIN, RETRY1, RETRY2, DLQ = "payments", "payments.retry.1", "payments.retry.2", "payments.DLQ"
for t in (MAIN, RETRY1, RETRY2, DLQ):
    recreate_topic(t, partitions=1)  # 순서가 눈에 보이도록 파티션 1개

p = make_producer()
for i in range(1, 13):
    kind = "poison" if i == 7 else ("transient" if i % 4 == 0 else "ok")
    p.produce(MAIN, key=f"pay-{i}", value=encode({"id": i, "kind": kind}))
p.flush()
print("발행 완료: 4, 8, 12번=일시 오류(transient), 7번=독약 메시지(poison)")
