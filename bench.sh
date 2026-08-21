#!/bin/bash
# bench.sh - baseline network throughput (iperf3) then WebDAV throughput,
# so the gap between the two shows exactly how much overhead WebDAV/nginx
# add on top of the raw network.
#
# Usage:
#   ./bench.sh <target-host> <webdav-url> [size_mb] [repeats]
#
# Examples:
#   ./bench.sh 192.168.95.1 http://192.168.95.1:8080/dav 200 3   # through proxy
#   ./bench.sh 192.168.95.2 http://192.168.95.2:8080/dav 200 3   # direct to backend

set -euo pipefail

TARGET_HOST="${1:?usage: bench.sh <target-host> <webdav-url> [size_mb] [repeats]}"
WEBDAV_URL="${2:?usage: bench.sh <target-host> <webdav-url> [size_mb] [repeats]}"
SIZE_MB="${3:-200}"
REPEATS="${4:-3}"

echo "=================================================="
echo " STEP 1/2: iperf3 baseline (raw network) -> $TARGET_HOST"
echo "=================================================="
echo "NOTE: run 'iperf3 -s' on $TARGET_HOST first if it isn't already listening."
iperf3 -c "$TARGET_HOST" -t 10 -P 4

echo
echo "=================================================="
echo " STEP 2/2: WebDAV benchmark -> $WEBDAV_URL"
echo "=================================================="
./webdav-benchmark -url "$WEBDAV_URL" -size "$SIZE_MB" -repeats "$REPEATS"

echo
echo "Compare the iperf3 'sender'/'receiver' Gbits/sec above to the"
echo "WEBDAV UPLOAD/DOWNLOAD Gbit/s summary - the difference is WebDAV overhead."