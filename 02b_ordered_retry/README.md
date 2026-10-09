# ②-b 순서를 지키는 retry / DLQ + 자동 redrive

02번의 한계: 같은 key의 앞 메시지가 retry/DLQ에 있어도 뒤 메시지가 먼저 처리돼 순서가 뒤집힌다.
여기서는 **같은 key에 안 끝난 앞 메시지가 있으면 뒤 메시지도 같이 보류**하고, DLQ는 **자동으로 재발행**한다.

```bash
python producer.py                 # 토픽 생성 + user-1/2/3 이벤트 발행
python worker.py                   # 터미널 A
python redrive_daemon.py           # 터미널 B (DLQ 자동 재발행)
touch fixed.flag                   # 버그 수정 배포 시뮬레이션 (worker/데몬 재시작 불필요)
python verify.py                   # key별 처리 순서 검사
```

## 핵심 아이디어 (번호표)
| 파일 | 역할 |
|---|---|
| [state.py](state.py) | key별 막힘 상태(RETRYING/DLQ) + 번호표(next_seq, serving). 실무에서는 Redis/DB |
| [worker.py](worker.py) | `seq < serving` 중복 무시 / `== serving` 처리 / `> serving` 보류(retry 또는 DLQ로) |
| [redrive_daemon.py](redrive_daemon.py) | 지수 백오프 후 자동 재발행, `MAX_REDRIVE` 넘으면 사람에게 넘김 |

## 알아둘 점
- 보류된 메시지가 DLQ에 뒤섞여 들어와도 괜찮다: 순서는 토픽 순서가 아니라 번호표(seq)가 복원한다.
- 앞 메시지가 DLQ에서 사람을 기다리면 그 key는 계속 막힌다(다른 key는 영향 없음). 이게 순서 보장의 대가다.
- 상태 저장소 갱신과 Kafka 발행/commit은 한 트랜잭션이 아니다. 중간에 죽어도 번호표 중복 발급 방지(`tickets`)와
  `seq < serving` 무시로 at-least-once 중복을 흡수한다.
- 인스턴스가 여러 개여도 같은 key는 같은 파티션 → 한 컨슈머만 처리하므로 안전. 리밸런스/좀비 구간은 `BEGIN IMMEDIATE`와 seq 검사가 마지막 방어선.
- 데모는 `sleep`으로 지연한다. 실무는 `pause()/resume()`.
