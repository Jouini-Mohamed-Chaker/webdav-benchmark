#!/bin/bash
# bench.sh - baseline network throughput (iperf3) then WebDAV throughput,
# with the iperf3 number automatically fed into webdav-benchmark so its
# upload/download tables print an "overhead vs iperf3" column directly -
# no manual cross-referencing of two separate outputs needed.
#
# Usage:
#   ./bench.sh <target-host> <webdav-url> [size_mb] [repeats] [max_level]
#
# Examples:
#   ./bench.sh 192.168.95.1 http://192.168.95.1:8080/dav 200 3        # through proxy
#   ./bench.sh 192.168.95.2 http://192.168.95.2:8080/dav 200 3        # direct to backend
#   ./bench.sh 192.168.95.2 http://192.168.95.2:8080/dav 200 3 64     # if you raise -levels above 32

set -euo pipefail

TARGET_HOST="${1:?usage: bench.sh <target-host> <webdav-url> [size_mb] [repeats] [max_level]}"
WEBDAV_URL="${2:?usage: bench.sh <target-host> <webdav-url> [size_mb] [repeats] [max_level]}"
SIZE_MB="${3:-200}"
REPEATS="${4:-3}"
MAX_LEVEL="${5:-32}"   # match iperf3 parallelism to the highest WebDAV
                        # concurrency level tested, so the baseline is a
                        # fair ceiling instead of an apples-to-oranges
                        # 4-stream number being compared against 32
                        # concurrent WebDAV streams.

echo "=================================================="
echo " STEP 1/2: iperf3 baseline (raw network, $MAX_LEVEL parallel streams) -> $TARGET_HOST"
echo "=================================================="
echo "NOTE: the iperf3 server is started/managed by Ansible - if this fails,"
echo "check 'systemctl status iperf3' on $TARGET_HOST."

IPERF_JSON=$(iperf3 -c "$TARGET_HOST" -t 10 -P "$MAX_LEVEL" -J)
echo "$IPERF_JSON" | python3 -c '
import json, sys
data = json.load(sys.stdin)
bits_per_second = data["end"]["sum_received"]["bits_per_second"]
print(f"iperf3 measured: {bits_per_second / 1e9:.3f} Gbit/s (sum of parallel streams, receiver-side)")
'
BASELINE_GBIT=$(echo "$IPERF_JSON" | python3 -c '
import json, sys
data = json.load(sys.stdin)
print(data["end"]["sum_received"]["bits_per_second"] / 1e9)
')

echo
echo "=================================================="
echo " STEP 2/2: WebDAV benchmark -> $WEBDAV_URL"
echo "=================================================="
./webdav-benchmark -url "$WEBDAV_URL" -size "$SIZE_MB" -repeats "$REPEATS" -baseline-gbit "$BASELINE_GBIT"