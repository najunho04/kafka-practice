# ⑤ 중복 처리: 멱등한 컨슈머 (idempotency key)

## A. Kafka 없이: race condition 재현
```bash
python race_demo.py
```
서버 A, B가 같은 주문을 동시에 처리할 때
- 패턴1(읽고-판단하고-쓰기) → 푸시 2번 ❌ (DB의 `status=PAID` 쓰기가 멱등해도 소용없다)
- 패턴2(조건부 UPDATE), 패턴3(unique INSERT) → 푸시 1번 ✅

## B. Kafka와 함께: 중복 재발행 방어
```bash
python producer.py     # e-2, e-4를 일부러 2번 발행
python consumer.py     # 중복은 "건너뜀" 로그, Ctrl+C로 종료
python verify.py       # 모든 target이 1번인지 확인
```
- consumer.py의 선점 로직을 지우고 그냥 푸시하게 바꿔서 다시 돌려 보면(`verify.py`) 중복 발송이 확인된다.

## C. 남는 구멍: 선점 후 크래시
```bash
python producer.py
CRASH_AFTER_CLAIM=1 python consumer.py   # 선점 직후 프로세스가 죽음
python consumer.py                       # 재시작 → "이미 처리 중(SENDING)"이라 건너뜀 → 푸시가 영영 안 나감!
python verify.py
```
→ 반대로 "푸시 호출 후 SENT 기록 전" 에 죽으면 중복 발송이 가능하다. 둘을 동시에 완벽히 막는 건 불가능.
→ 현실적인 대응: ① 외부 API에 idempotency key를 같이 전달(결제 PG 등) ② `SENDING`이 N분 넘게 남은 건을 점검/재시도하는 보정 배치

## 도전 과제
- `SELECT ... FOR UPDATE`, version 컬럼(낙관적 락) 방식을 MySQL/PostgreSQL로 옮겨서 구현해 보기
- `SENDING`이 60초 넘은 행을 찾아 재처리하는 `recover_stale.py` 작성해 보기
- 슬라이드의 materialized view: `notification_log`를 "이벤트 처리 상태를 모아둔 조회용 테이블"로 보고 조회 API를 만들어 보기
