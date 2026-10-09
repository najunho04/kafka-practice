# Kafka 실습 (토스 백엔드 세션 슬라이드 ①~⑦)

언어: **Python + confluent-kafka** (개념에 집중하기 쉬워서). 슬라이드의 Java/Spring 개념과 설정명이 1:1로 대응하는 부분은 코드 주석에 적어 두었다.

## 준비
```bash
cd /home/najunho/toss-iwanttogo/kafka/실습
docker compose up -d                       # Kafka 1대 (localhost:9092)
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```
먼저 [00_cli_basics.md](00_cli_basics.md) 로 코드 없이 파티션/그룹/리밸런스를 눈으로 확인해 보자.

## 슬라이드 ↔ 실습 폴더
| 슬라이드 질문 | 폴더 | 핵심 키워드 |
|---|---|---|
| ① 컨슈머 속도를 높이려면? | [01_consumer_speed](01_consumer_speed) | record→batch→multi-thread→parallel, 순서 보장, rebalance |
| ② 처리에 실패하면? | [02_dlq_retry](02_dlq_retry) | retry 토픽, DLQ, 재발행(redrive) |
| ③ 브로커 성능을 개선하려면? | [03_producer_tuning](03_producer_tuning) | batch, linger, 압축, 버퍼 |
| ④ 처리 속도를 높이려면? | [04_partition_offset_reset](04_partition_offset_reset) | 파티션 증설, latest→earliest |
| ⑤ 중복 처리를 막으려면? | [05_idempotent_consumer](05_idempotent_consumer) | 멱등 컨슈머, idempotency key, check-then-act |
| ⑥ 안정적으로 배포하려면? | [06_graceful_shutdown](06_graceful_shutdown) | assigner, SIGTERM→SIGKILL |
| ⑦ 모니터링·장애 대응은? | [07_lag_monitoring](07_lag_monitoring) | lag, producer/consumer config |

공통 헬퍼는 [common.py](common.py). 접속 주소는 `KAFKA_BOOTSTRAP` 환경변수로 바꿀 수 있다(기본 `localhost:9092`).
실습 종료 후 정리: `docker compose down`
