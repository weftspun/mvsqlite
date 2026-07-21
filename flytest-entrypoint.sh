#!/bin/bash
set -e

NPROC=$(nproc)
echo "=== dedicated cores available: $NPROC ==="

mkdir -p /etc/foundationdb /var/lib/foundationdb/data /var/log/foundationdb
chown -R foundationdb:foundationdb /etc/foundationdb /var/lib/foundationdb /var/log/foundationdb || true

RANDSTR=$(tr -dc 'A-Za-z0-9' </dev/urandom | head -c 8)
echo "test:${RANDSTR}@127.0.0.1:4500" > /etc/foundationdb/fdb.cluster
chmod 0664 /etc/foundationdb/fdb.cluster
chown foundationdb:foundationdb /etc/foundationdb/fdb.cluster || true

CONF=/etc/foundationdb/foundationdb.conf
cat > "$CONF" <<EOF
[fdbmonitor]
user = foundationdb
group = foundationdb

[general]
restart-delay = 10
cluster-file = /etc/foundationdb/fdb.cluster

[fdbserver]
command = /usr/sbin/fdbserver
public-address = auto:\$ID
listen-address = public
datadir = /var/lib/foundationdb/data/\$ID
logdir = /var/log/foundationdb

[fdbserver.4500]
EOF
for i in $(seq 1 $((NPROC - 1))); do
  { echo ""; echo "[fdbserver.$((4500 + i))]"; } >> "$CONF"
done
cat "$CONF"

/usr/lib/foundationdb/fdbmonitor --conffile "$CONF" --lockfile /var/run/fdbmonitor.pid &
sleep 5
fdbcli -C /etc/foundationdb/fdb.cluster --exec "configure new single memory" || true
sleep 2
fdbcli -C /etc/foundationdb/fdb.cluster --exec "status"

run_case() {
  local num_procs=$1
  local num_shards=$2
  local label=$3
  echo ""
  echo "=== CASE: $label (num_procs=$num_procs num_shards=$num_shards) ==="

  DATA_PLANES=""
  for i in $(seq 0 $((num_procs - 1))); do
    DP_PORT=$((7000 + i))
    ADMIN_PORT=$((8000 + i))
    /usr/local/bin/mvstore --data-plane 127.0.0.1:$DP_PORT --admin-api 127.0.0.1:$ADMIN_PORT \
      --metadata-prefix "flytest-$label" --raw-data-prefix "f$label" &
    if [ -z "$DATA_PLANES" ]; then
      DATA_PLANES="http://localhost:$DP_PORT"
    else
      DATA_PLANES="$DATA_PLANES,http://localhost:$DP_PORT"
    fi
  done
  sleep 2

  # RUST_LOG=off, not info: under 64-way concurrency with real contention (200
  # entities, 64 workers), mvclient logs a warn!/error! line per 409/500 - with
  # tracing_subscriber's default stdout writer locking per line, that's dozens
  # of mutex-contended synchronous writes/sec that have nothing to do with
  # FDB/mvSQLite's real throughput. Isolating that variable directly instead
  # of assuming it away.
  RUST_LOG=off /usr/local/bin/mvsqlite-shard-bench \
    --data-planes "$DATA_PLANES" --admin-api "http://localhost:8000" \
    --ns-prefix "fly_$label" --num-shards $num_shards --num-entities 200 \
    --concurrency 64 --iterations 150 --mode scaling

  pkill -f /usr/local/bin/mvstore || true
  sleep 1
}

mpstat -P ALL 2 > /tmp/mpstat.log 2>&1 &
MPSTAT_PID=$!

# Use exactly as many processes/shards as this machine's hard-allocated vCPU
# count (Fly enforces this at the hypervisor level, so nproc here is a real
# limit, not a soft one) - deploy this same image on differently-sized
# machines (performance-1x, performance-4x, ...) to get real per-core-count
# data points instead of simulating multiple core counts on one machine.
run_case $NPROC $NPROC thismachine

kill $MPSTAT_PID || true
echo ""
echo "=== CPU utilization ==="
cat /tmp/mpstat.log

echo ""
echo "=== DONE - shutting down ==="
sleep 2
