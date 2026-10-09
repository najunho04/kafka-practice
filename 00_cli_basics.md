# 0단계: 코드 없이 CLI로 개념 체험

## 1. Kafka 띄우기
```bash
docker compose up -d          # 이 폴더의 docker-compose.yml 사용
# 또는: docker run -d --name kafka -p 9092:9092 apache/kafka:latest
```

## 2. 토픽 만들기 (파티션 3개)
```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic test --partitions 3
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic test
```

## 3. Producer / Consumer (터미널 2개)
```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic test --property parse.key=true --property key.separator=:
# 입력 예: user1:hello   (key:value)

docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning --property print.partition=true --property print.key=true
```

## 4. 직접 확인해볼 것
- 같은 `--group myg` 로 consumer를 2~3개 띄우면 → 파티션이 **분배**된다 (그룹 안에서는 한 파티션을 1명만 읽음)
- 다른 `--group` 으로 띄우면 → 모두 전체 메시지를 받는다 (**팬아웃**)
- 같은 key는 항상 같은 partition 번호로 찍히는지 (`print.partition=true`)
- consumer를 하나 강제 종료(Ctrl+C)하면 남은 consumer에게 파티션이 넘어가는 것 (= **리밸런스**)

## 5. consumer group / lag 보기
```bash
docker exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
docker exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group myg
# CURRENT-OFFSET, LOG-END-OFFSET, LAG 컬럼이 슬라이드 ⑦의 lag
```
