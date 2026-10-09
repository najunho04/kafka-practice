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

## 심화: 레코드 단위 추적 + 연속 구간 commit (`consumer_parallel_tracked.py`)
`consumer_parallel.py`는 배치 전체가 끝나야 commit하는 단순 버전이다. 실제 Parallel Consumer가 하는 방식
(KEY 순서 스케줄링, 레코드별 완료 추적, 빈틈 없이 끝난 구간까지만 commit)을 직접 구현한 것이 이 파일이다.
```bash
python producer_load.py 2000
python consumer_parallel_tracked.py                               # 정상 완주: 유실 0 / 중복 0
python consumer_parallel_tracked.py --crash-after 700             # 700건 완료 시점에 강제 종료
python consumer_parallel_tracked.py --group <위에 출력된 그룹명>    # 같은 그룹으로 재시작
python consumer_parallel_tracked.py --naive-commit --crash-after 700   # 나쁜 예: 받자마자 commit
python consumer_parallel_tracked.py --naive-commit --group <그룹명>
```
| 방식 | 크래시 후 재시작 결과 (직접 실행) |
|---|---|
| 연속 구간 commit | 유실 **0**, 중복 287건 (끝났지만 commit 못 한 건 재처리) |
| 받자마자 commit | 유실 **293건**, 중복 0 (받았지만 처리 못 한 건 영영 사라짐) |

→ 중복은 멱등 컨슈머(⑤)로 막을 수 있지만, 유실은 되돌릴 수 없다. 그래서 "처리된 레코드만 commit" 한다.
※ 크래시 후 재시작이 바로 이어지지 않고 몇 초 걸리는 건 이전 컨슈머가 그룹에서 빠지길(session.timeout) 기다리기 때문이다(⑥).

## 생각해볼 질문 (슬라이드 "고려해야 할 것")
1. multi-thread에서 3번 메시지만 실패했는데 4, 5번은 성공했다면 offset을 어디까지 commit해야 할까? (코드 주석 참고)
2. 컨슈머를 2~3개 같은 그룹으로 동시에 켜면(리밸런스) 처리 중이던 배치는 어떻게 될까? → 06번에서 직접 관찰
3. `max.poll.interval.ms`(기본 5분)보다 한 배치 처리가 오래 걸리면? → 컨슈머가 죽은 것으로 간주되어 리밸런스
