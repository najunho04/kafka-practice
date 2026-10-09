"""processed.log 를 읽어 key별 처리 순서가 n 오름차순인지 + 누락/중복이 없는지 검사."""
import pathlib
from collections import defaultdict

seen = defaultdict(list)
for line in (pathlib.Path(__file__).resolve().parent / "processed.log").read_text().splitlines():
    user, n = line.split()
    seen[user].append(int(n))
ok = True
for user, ns in sorted(seen.items()):
    good = ns == sorted(ns) and len(ns) == len(set(ns))
    ok &= good
    print(f"{user}: {ns} {'✅ 순서 유지' if good else '❌ 순서 위반/중복'}")
print("결과:", "모든 key 순서 보장" if ok else "위반 있음")
