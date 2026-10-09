#!/usr/bin/env bash
# 롤링 배포 시뮬레이션: 컨슈머 A, B 실행 → 중간에 B를 종료 → 리밸런스/중복 확인
#   ./run_demo.sh term                       B에 SIGTERM (graceful)
#   GRACEFUL=0 ./run_demo.sh term            B에 SIGTERM인데 정리 로직 없음
#   ./run_demo.sh kill                       B에 SIGKILL (강제 종료)
#   ASSIGNOR=range ./run_demo.sh term        eager 방식(기본 range)과 비교
cd "$(dirname "$0")" || exit 1
MODE=${1:-term}
mkdir -p logs && rm -f logs/*

python -u stream_producer.py > logs/producer.log 2>&1 &
PROD=$!
sleep 3
python -u graceful_consumer.py A > logs/A.log 2>&1 &
A=$!
python -u graceful_consumer.py B > logs/B.log 2>&1 &
B=$!

sleep 25
echo "== 25초 경과: B에 ${MODE} 전송 =="
if [ "$MODE" = kill ]; then kill -9 $B; else kill -TERM $B; fi

sleep 25
kill -TERM $A $PROD
wait 2>/dev/null

echo
echo "== 리밸런스/종료 로그 (시간순) =="
grep -h -E "ASSIGN|REVOKE|SHUTDOWN|정상 종료" logs/A.log logs/B.log | sort
echo
echo "== 중복 처리된 메시지(같은 partition/offset을 2번 이상 처리) =="
grep -h processed logs/A.log logs/B.log | awk '{print $3, $4}' | sort | uniq -d
echo "(출력이 없으면 중복 0건)"
