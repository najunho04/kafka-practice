# ② 실패 처리: retry 토픽 → DLQ → 재발행

```bash
python producer.py        # 토픽 4개 생성 + 12건 발행
python consumer.py        # 터미널 A: 실패 흐름 관찰 (약 15~20초)
```
관찰할 것
- 4, 8, 12번(일시 오류): 한 번 실패 → retry.1 로 이동 → 5초 뒤 성공
- 7번(poison): retry.1 → retry.2 → **DLQ**. 그동안 다른 메시지는 막히지 않고 계속 처리됨

버그를 고친 상황 재현
```bash
python redrive.py                 # DLQ → payments 로 재발행 (그냥 하면 또 실패해서 DLQ로 감)
FIX_BUG=1 python consumer.py      # 버그 수정본 배포 → 이번엔 7번이 성공
```
(순서: consumer를 `FIX_BUG=1`로 다시 띄워둔 상태에서 `redrive.py` 실행)

## 생각해볼 질문
- 재시도 사이 5초를 `sleep`으로 기다리면 그동안 같은 파티션의 다른 retry 메시지는? → 코드 주석의 `pause()` 방식
- "원본 commit"과 "retry 토픽 발행" 순서를 바꾸면(commit 먼저) 어떤 장애에서 유실될까?
- DLQ 메시지에 어떤 헤더를 남겨야 운영자가 원인을 알 수 있을까? (`last_error`, `original_topic` …)
