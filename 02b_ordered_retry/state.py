"""key별 '막힘' 상태 저장소. (실무에서는 Redis / DB 테이블. 여기서는 SQLite)

key_state : 이 key에 실패한 메시지가 있어 뒤 메시지를 보류 중인가?
    state    RETRYING(retry 토픽에서 재시도 중) / DLQ(DLQ에 있음, 사람 또는 redrive 대기)
    next_seq 다음에 발급할 번호표
    serving  지금 처리해야 할 번호(= 이 key의 가장 앞 미처리 메시지)
tickets   : 번호표 중복 발급 방지 (같은 원본 메시지가 재전달돼도 같은 번호를 받게 한다)
"""
import pathlib
import sqlite3

DB = pathlib.Path(__file__).resolve().parent / "key_state.db"


def connect() -> sqlite3.Connection:
    return sqlite3.connect(DB, timeout=10, isolation_level=None)  # autocommit, 트랜잭션은 직접 BEGIN


def init() -> None:
    conn = connect()
    conn.executescript(
        """
        DROP TABLE IF EXISTS key_state;
        DROP TABLE IF EXISTS tickets;
        CREATE TABLE key_state (key TEXT PRIMARY KEY, state TEXT NOT NULL,
                                next_seq INTEGER NOT NULL, serving INTEGER NOT NULL);
        CREATE TABLE tickets (origin TEXT PRIMARY KEY, key TEXT NOT NULL, seq INTEGER NOT NULL);
        """
    )
    conn.close()


def get(conn, key: str):
    """(state, next_seq, serving) 또는 None(막히지 않은 key)."""
    return conn.execute("SELECT state, next_seq, serving FROM key_state WHERE key=?", (key,)).fetchone()


def take_ticket(conn, key: str, origin: str) -> int:
    """번호표 발급. key가 처음 막히는 순간이면 key_state 행도 만든다 (첫 번호 = 0 = 실패한 그 메시지)."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        done = conn.execute("SELECT seq FROM tickets WHERE origin=?", (origin,)).fetchone()
        if done:  # 재전달된 같은 메시지 → 같은 번호
            conn.execute("COMMIT")
            return done[0]
        conn.execute("INSERT OR IGNORE INTO key_state VALUES (?, 'RETRYING', 0, 0)", (key,))
        (seq,) = conn.execute("SELECT next_seq FROM key_state WHERE key=?", (key,)).fetchone()
        conn.execute("UPDATE key_state SET next_seq = next_seq + 1 WHERE key=?", (key,))
        conn.execute("INSERT INTO tickets VALUES (?, ?, ?)", (origin, key, seq))
        conn.execute("COMMIT")
        return seq
    except Exception:
        conn.execute("ROLLBACK")
        raise


def set_state(conn, key: str, state: str) -> None:
    conn.execute("UPDATE key_state SET state=? WHERE key=?", (state, key))


def advance(conn, key: str) -> bool:
    """맨 앞 메시지를 처리 완료. 밀린 게 더 없으면 key 막힘을 풀고 True."""
    conn.execute("BEGIN IMMEDIATE")
    conn.execute("UPDATE key_state SET serving = serving + 1 WHERE key=?", (key,))
    (done,) = conn.execute("SELECT serving = next_seq FROM key_state WHERE key=?", (key,)).fetchone()
    if done:
        conn.execute("DELETE FROM key_state WHERE key=?", (key,))
        conn.execute("DELETE FROM tickets WHERE key=?", (key,))
    conn.execute("COMMIT")
    return bool(done)
