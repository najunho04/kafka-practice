# ⑦ 모니터링: lag + config 점검

```bash
python fill_topic.py 3000                         # 터미널 1: 메시지 3000건 적재
python slow_consumer.py                           # 터미널 2: 느린 컨슈머
python lag.py lag-demo-group lag-demo --watch     # 터미널 3: lag 관찰
```
- 컨슈머를 하나 더 띄우면(터미널 4) lag이 줄어드는 속도가 빨라지는지, 3개 넘게 띄우면(파티션 3개) 더 이상 안 빨라지는지 확인
- 컨슈머를 모두 끄면 lag이 그대로인지(commit이 멈춤) 확인
- 같은 값을 CLI로도 확인: `docker exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group lag-demo-group`

## 운영 config 체크리스트 (슬라이드: producer & consumer config)
| 대상 | 설정 | 의미 / 확인 포인트 |
|---|---|---|
| Producer | `acks` | 0/1/all — 유실 허용 여부 vs 속도 |
| Producer | `retries`, `enable.idempotence` | 재시도 시 브로커 저장 중복 방지 |
| Producer | `batch.size`, `linger.ms`, `compression.type` | ③번 실습 |
| Consumer | `max.poll.records` | 한 번에 가져오는 건수 → 처리 시간 결정 |
| Consumer | `max.poll.interval.ms` | poll 간격이 이보다 길면 죽은 것으로 간주 → 리밸런스 |
| Consumer | `session.timeout.ms`, `heartbeat.interval.ms` | 장애 감지 속도 (⑥번 실습) |
| Consumer | `auto.offset.reset` | commit 기록 없을 때 시작 위치 (④번 실습) |
| Consumer | `enable.auto.commit` | 수동 commit 여부 → 유실/중복 성격 결정 |

## 후속 대책 runbook 연습
lag이 `--alert` 값을 넘었을 때 순서를 직접 써보자: ① 컨슈머 에러 로그 확인 → ② DLQ 급증 여부 → ③ 컨슈머 증설(파티션 수까지) → ④ 파티션 증설(④번 주의사항) → ⑤ 처리 로직 병목 개선(①번)
