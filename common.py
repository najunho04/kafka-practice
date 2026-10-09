"""모든 실습이 공유하는 헬퍼: 접속 설정, 토픽 생성, 컨슈머 생성, 측정 도구."""
import json
import os
import threading
import time

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")


def admin_client() -> AdminClient:
    return AdminClient({"bootstrap.servers": BOOTSTRAP})


def create_topic(name: str, partitions: int = 3, retries: int = 1) -> None:
    admin = admin_client()
    for attempt in range(retries):
        try:
            for f in admin.create_topics([NewTopic(name, partitions, 1)]).values():
                f.result()
            return
        except Exception:
            if attempt == retries - 1:
                raise
            time.sleep(0.5)  # 직전에 지운 토픽이 아직 정리 중일 수 있음


def recreate_topic(name: str, partitions: int = 3) -> None:
    """토픽을 지우고 다시 만든다 (실습을 처음부터 반복하기 위함)."""
    admin = admin_client()
    if name in admin.list_topics(timeout=10).topics:
        for f in admin.delete_topics([name], operation_timeout=30).values():
            f.result()
    create_topic(name, partitions, retries=30)


def ensure_topic(name: str, partitions: int = 3) -> None:
    if name not in admin_client().list_topics(timeout=10).topics:
        create_topic(name, partitions)


def make_producer(**overrides) -> Producer:
    conf = {"bootstrap.servers": BOOTSTRAP}
    conf.update(overrides)
    return Producer(conf)


def make_consumer(group: str, **overrides) -> Consumer:
    """수동 commit + 처음부터 읽기(earliest)가 기본값."""
    conf = {
        "bootstrap.servers": BOOTSTRAP,
        "group.id": group,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    }
    conf.update(overrides)
    return Consumer(conf)


def uniq(name: str) -> str:
    """실행할 때마다 새 컨슈머 그룹(= commit 기록 없음)을 쓰기 위한 이름."""
    return f"{name}-{int(time.time())}"


def encode(obj) -> bytes:
    return json.dumps(obj, ensure_ascii=False).encode()


def decode(raw: bytes):
    return json.loads(raw.decode())


def simulate_io(ms: float) -> None:
    """DB 호출 / 외부 API 호출처럼 '기다리는 시간'을 흉내낸다."""
    time.sleep(ms / 1000)


class Meter:
    """처리량 측정: 첫 메시지부터 마지막 메시지까지 걸린 시간으로 계산."""

    def __init__(self):
        self.count = 0
        self.first = None
        self.last = time.time()

    def tick(self, n: int = 1) -> None:
        now = time.time()
        if self.first is None:
            self.first = now
        self.count += n
        self.last = now

    def idle(self, limit: float = 10.0) -> bool:
        return time.time() - self.last > limit

    def report(self, label: str, checker=None) -> None:
        if self.first is None:
            print(f"[{label}] 처리한 메시지 없음")
            return
        elapsed = max(self.last - self.first, 1e-9)
        line = f"[{label}] {self.count}건 / {elapsed:.2f}초 = {self.count / elapsed:.0f} msg/s"
        if checker is not None:
            line += f" | 같은 key 순서 위반: {checker.violations}건"
        print(line)


class OrderChecker:
    """같은 key 안에서 seq가 거꾸로 처리되면 '순서 위반'으로 센다."""

    def __init__(self):
        self.last = {}
        self.violations = 0
        self._lock = threading.Lock()

    def check(self, key: str, seq: int) -> None:
        with self._lock:
            if seq < self.last.get(key, 0):
                self.violations += 1
            self.last[key] = max(seq, self.last.get(key, 0))
