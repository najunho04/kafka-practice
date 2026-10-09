"""Kafka 없이 실행 가능. 서버 A, B가 '같은 주문 1234'를 동시에 처리할 때 푸시가 몇 번 나가는가?

  패턴1 읽고-판단하고-쓰기 (check-then-act)  → 깨짐: 2번 발송
  패턴2 조건부 UPDATE로 선점                 → 1번
  패턴3 unique 제약 INSERT로 선점            → 1번
사용: python race_demo.py"""
import sqlite3
import threading
import time

import db

DB = "race.db"
ORDER_ID = 1234


def push_count(conn) -> int:
    return conn.execute("SELECT COUNT(*) FROM push_calls").fetchone()[0]


def broken(barrier):
    conn = db.connect(DB)
    barrier.wait()
    status = conn.execute("SELECT status FROM orders WHERE id=?", (ORDER_ID,)).fetchone()[0]
    if status == "CREATED":  # A, B 둘 다 CREATED를 읽고 통과
        time.sleep(0.1)  # 로직 처리 시간
        db.external_push(conn, f"order-{ORDER_ID}")  # ← 푸시 2번
        conn.execute("UPDATE orders SET status='PAID' WHERE id=?", (ORDER_ID,))


def conditional_update(barrier):
    conn = db.connect(DB)
    barrier.wait()
    # 읽기+판단+쓰기를 한 문장으로 → DB가 원자적으로 처리. 이긴 쪽만 rowcount == 1
    cur = conn.execute("UPDATE orders SET status='SENDING' WHERE id=? AND status='CREATED'", (ORDER_ID,))
    if cur.rowcount == 1:
        time.sleep(0.1)
        db.external_push(conn, f"order-{ORDER_ID}")
        conn.execute("UPDATE orders SET status='PAID' WHERE id=?", (ORDER_ID,))


def unique_insert(barrier):
    conn = db.connect(DB)
    barrier.wait()
    try:  # 같은 idempotency_key를 두 번 INSERT하면 DB가 한쪽을 거부
        conn.execute("INSERT INTO notification_log(idempotency_key, status) VALUES (?, 'SENDING')",
                     (f"order-{ORDER_ID}-push",))
    except sqlite3.IntegrityError:
        return
    time.sleep(0.1)
    db.external_push(conn, f"order-{ORDER_ID}")
    conn.execute("UPDATE notification_log SET status='SENT' WHERE idempotency_key=?", (f"order-{ORDER_ID}-push",))


for label, fn in [("패턴1 읽고-판단-쓰기", broken),
                  ("패턴2 조건부 UPDATE 선점", conditional_update),
                  ("패턴3 unique INSERT 선점", unique_insert)]:
    db.init(DB)
    conn = db.connect(DB)
    conn.execute("INSERT INTO orders VALUES (?, 'CREATED')", (ORDER_ID,))
    barrier = threading.Barrier(2)  # A, B를 정확히 동시에 출발
    threads = [threading.Thread(target=fn, args=(barrier,)) for _ in range(2)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    n = push_count(conn)
    print(f"{label:26s} → 푸시 {n}번 {'❌ 중복!' if n > 1 else '✅'}")
