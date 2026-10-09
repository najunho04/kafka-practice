"""멱등한 컨슈머: 외부 호출(푸시) '전에' DB에서 원자적으로 선점한다.

  1) INSERT notification_log(event_id, 'SENDING')   ← unique 제약, 이긴 쪽만 성공
  2) 성공한 쪽만 푸시 호출
  3) UPDATE ... 'SENT'
  4) Kafka commit

CRASH_AFTER_CLAIM=1 : 선점 직후 죽는 상황을 재현. 재시작하면 '이미 선점됨'으로 보여 푸시가 영영 안 나간다
                      → 이게 '완벽한 exactly-once는 없다'는 구멍. 해결: SENDING이 오래 남은 건을 점검하는
                        보정 로직 + 외부 API의 idempotency key (README 참고)
사용: python consumer.py"""
import os
import pathlib
import sqlite3
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import db  # noqa: E402
from common import decode, make_consumer  # noqa: E402

TOPIC = "order-events"
CRASH_AFTER_CLAIM = os.getenv("CRASH_AFTER_CLAIM") == "1"

conn = db.connect()
c = make_consumer("order-notifier")
c.subscribe([TOPIC])
print("대기 중... (Ctrl+C로 종료)")

try:
    while True:
        msg = c.poll(1.0)
        if msg is None or msg.error():
            continue
        event_id = decode(msg.value())["event_id"]
        key = f"{event_id}-push"
        try:
            conn.execute("INSERT INTO notification_log(idempotency_key, status) VALUES (?, 'SENDING')", (key,))
        except sqlite3.IntegrityError:
            status = conn.execute("SELECT status FROM notification_log WHERE idempotency_key=?", (key,)).fetchone()[0]
            print(f"⏭️  {event_id}: 이미 처리됨/처리 중({status}) → 건너뜀")
            c.commit(message=msg, asynchronous=False)
            continue

        if CRASH_AFTER_CLAIM:
            print(f"💥 {event_id}: 선점 직후 크래시 (푸시 전)")
            os._exit(1)
        db.external_push(conn, event_id)
        conn.execute("UPDATE notification_log SET status='SENT' WHERE idempotency_key=?", (key,))
        print(f"📨 {event_id}: 푸시 발송")
        c.commit(message=msg, asynchronous=False)
except KeyboardInterrupt:
    pass
finally:
    c.close()
