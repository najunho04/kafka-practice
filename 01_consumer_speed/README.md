# ① 컨슈머 속도: record → batch → multi-thread → parallel

```bash
python producer_load.py 2000        # orders 토픽에 2000건 (key 10종)
python consumer_record.py           # 건별 처리 + 건별 commit
python consumer_batch.py            # 배치 처리 + 배치당 commit
python consumer_threaded.py         # 스레드풀 (빠르지만 순서 위반 발생)
python consumer_parallel.py         # key별 병렬 (빠르고 순서 위반 0)
```
각 스크립트는 새 컨슈머 그룹으로 처음부터 읽으므로 producer는 한 번만 돌리면 된다.

## 관찰 포인트
| 방식 | 속도 | 순서 위반 | 비고 |
|---|---|---|---|
| record | 가장 느림 | 0 | 건마다 DB + commit 왕복 |
| batch | 빨라짐 | 0 | commit/DB 호출 횟수가 줄어서 |
| multi-thread | 더 빠름 | **발생** | 같은 key가 서로 다른 스레드에서 처리돼 뒤바뀜 |
| parallel(key별) | 빠름 | 0 | 같은 key는 한 작업 안에서 순차 처리 |

## 생각해볼 질문 (슬라이드 "고려해야 할 것")
1. multi-thread에서 3번 메시지만 실패했는데 4, 5번은 성공했다면 offset을 어디까지 commit해야 할까? (코드 주석 참고)
2. 컨슈머를 2~3개 같은 그룹으로 동시에 켜면(리밸런스) 처리 중이던 배치는 어떻게 될까? → 06번에서 직접 관찰
3. `max.poll.interval.ms`(기본 5분)보다 한 배치 처리가 오래 걸리면? → 컨슈머가 죽은 것으로 간주되어 리밸런스
