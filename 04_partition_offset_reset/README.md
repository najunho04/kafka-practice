# ④ 처리 속도: 파티션 증설과 latest → earliest

```bash
python offset_reset_demo.py   # latest는 유실, earliest는 전부 수신 (약 1분)
python key_remap.py           # 증설하면 같은 key가 다른 파티션으로 이동
```

## 핵심
- 파티션을 늘리면 병렬성의 상한(컨슈머 수)이 올라간다.
- 하지만 새 파티션엔 **이 컨슈머 그룹의 commit 기록이 없다** → `auto.offset.reset` 정책이 적용
  - `latest`: 컨슈머가 새 파티션을 인식한 **이후**의 메시지부터 → 그 사이에 들어온 건 유실
  - `earliest`: 처음부터 → 유실 없음 (기존 파티션은 commit 기록이 있으니 영향 없음)
- 대가: 파티션 수는 줄일 수 없고, key → 파티션 매핑이 바뀌어 순서 보장이 증설 시점에 한 번 깨질 수 있다.

## 확인해볼 것
- 실습 결과에서 latest의 유실 건수가 매번 같은가? (메타데이터 갱신 타이밍에 따라 달라질 수 있음 → "인식 전에 들어온 건 유실")
- `topic.metadata.refresh.interval.ms` 를 기본(5분)으로 두면 새 파티션 인식까지 얼마나 걸릴까?
