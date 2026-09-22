#!/bin/bash
# Offline smoke test: build + cloud_bridge(dry-run) + iot-collector
# Usage: bash tools/path2_local_test.sh
set -e
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
make
rm -f /tmp/bridge.out /tmp/iot_p2.out cache.dat cache.dat.cursor
./cloud_bridge -l 9000 -d >/tmp/bridge.out 2>&1 &
BPID=$!
sleep 0.4
./iot-collector -s -p 127.0.0.1:9000 >/tmp/iot_p2.out 2>&1 &
CPID=$!
sleep 5
kill -INT $CPID 2>/dev/null || true
sleep 1
kill -INT $BPID 2>/dev/null || true
wait $CPID 2>/dev/null || true
wait $BPID 2>/dev/null || true
echo "----- bridge tail -----"
tail -8 /tmp/bridge.out
echo "----- collector link -----"
grep -E "link|unicast|egress" /tmp/iot_p2.out | tail -8
if grep -q "platform=ONLINE" /tmp/iot_p2.out; then
  echo "SMOKE_OK"
else
  echo "SMOKE_CHECK_LOGS"
  exit 1
fi
