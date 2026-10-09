"""외부 푸시 시스템이 받은 호출 기록 확인: 같은 target이 2번 이상이면 중복 발송."""
import db

conn = db.connect()
rows = conn.execute("SELECT target, COUNT(*) FROM push_calls GROUP BY target ORDER BY target").fetchall()
for target, n in rows:
    print(f"{target}: {n}번 {'❌ 중복' if n > 1 else '✅'}")
print(f"\nnotification_log 상태: {conn.execute('SELECT idempotency_key, status FROM notification_log').fetchall()}")
