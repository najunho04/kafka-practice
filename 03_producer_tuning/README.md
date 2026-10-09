# ③ 브로커 성능: 배치 + 압축 (버퍼 최적화)

```bash
python bench.py 50000
```
설정 4가지로 같은 메시지를 보내서 **처리량(msg/s)** 과 **네트워크 전송량(MB)** 을 비교한다.

## 직접 바꿔보기
- `linger.ms` 를 0 / 5 / 50 / 200 으로 바꾸고 처리량 변화 관찰
- 메시지를 1건만 보낼 때 `linger.ms=200` 이면 전송이 200ms 늦어지는 것 확인 (latency 트레이드오프)
- `acks=all` / `acks=1` / `acks=0` 을 추가해 신뢰성 ↔ 속도 비교 (`make_producer(acks="all")`)
- `enable.idempotence=true` 를 켜면 재시도로 인한 브로커 저장 중복이 사라진다 (⑤의 "Kafka 쪽 멱등성")
