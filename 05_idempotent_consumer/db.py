"""SQLite 헬퍼. (실무의 MySQL/PostgreSQL 역할)
push_calls 테이블은 '외부 푸시 시스템'이 받은 호출 기록 — 같은 id가 2번 들어있으면 중복 발송."""
import pathlib
import sqlite3

BASE = pathlib.Path(__file__).resolve().parent


def connect(name: str = "idempotency.db") -> sqlite3.Connection:
    # autocommit(isolation_level=None): 문장 하나하나가 곧 트랜잭션 → 동시성 동작이 눈에 보이기 쉬움
    return sqlite3.connect(BASE / name, timeout=10, isolation_level=None)


def init(name: str = "idempotency.db") -> None:
    conn = connect(name)
    conn.executescript(
        """
        DROP TABLE IF EXISTS orders;
        DROP TABLE IF EXISTS notification_log;
        DROP TABLE IF EXISTS push_calls;
        CREATE TABLE orders (id INTEGER PRIMARY KEY, status TEXT);
        CREATE TABLE notification_log (
            idempotency_key TEXT PRIMARY KEY,   -- ★ unique 제약이 곧 '선점 락'
            status TEXT NOT NULL,               -- SENDING → SENT
            created_at REAL DEFAULT (strftime('%s','now'))
        );
        CREATE TABLE push_calls (seq INTEGER PRIMARY KEY AUTOINCREMENT, target TEXT);
        """
    )
    conn.close()


def external_push(conn: sqlite3.Connection, target: str) -> None:
    """외부 시스템 호출을 흉내: 호출될 때마다 한 줄 기록."""
    conn.execute("INSERT INTO push_calls(target) VALUES (?)", (target,))
