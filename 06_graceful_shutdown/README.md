# ⑥ 안정적 배포: assigner + graceful shutdown

```bash
chmod +x run_demo.sh
./run_demo.sh term                    # SIGTERM → graceful: 중복 0, 리밸런스 즉시
./run_demo.sh kill                    # SIGKILL: 최대 4건 중복 + 리밸런스 지연(약 10초, session.timeout.ms)
GRACEFUL=0 ./run_demo.sh term         # 정리 없이 종료: SIGKILL과 비슷한 결과
ASSIGNOR=range ./run_demo.sh term     # eager: 리밸런스 때 전 파티션 회수 → ASSIGN/REVOKE 로그 비교
```
각 실행은 약 55초 걸린다. 결과는 `logs/` 에도 남는다.

## 관찰 포인트
| | graceful (SIGTERM + close) | SIGKILL |
|---|---|---|
| 현재 처리 중 메시지 | 끝까지 처리 후 commit | 처리됐지만 commit 안 됨 → **중복** |
| 그룹 탈퇴 | 즉시(LeaveGroup) | 브로커가 session.timeout(10초) 뒤에 알아챔 → 그동안 그 파티션은 **lag 누적** |

| | cooperative-sticky | range (eager) |
|---|---|---|
| 리밸런스 시 | 옮겨갈 파티션만 REVOKE | 가진 **모든** 파티션 REVOKE 후 재할당 (전체 일시 정지) |

## 쿠버네티스에서는?
Pod 종료 시 `SIGTERM` → `terminationGracePeriodSeconds`(기본 30초) 안에 안 끝나면 `SIGKILL`.
그래서 컨슈머의 "한 건 처리 시간 + commit + close" 가 이 시간 안에 끝나도록 설계해야 한다.
