#!/bin/bash
set -e
# Usage: flytest-multihost-driver.sh "[ip1]:7000,[ip2]:7000,..." "[ip1]:8000"
DATA_PLANES_RAW="$1"
ADMIN_API_IP="$2"
NUM_SHARDS="$3"

DATA_PLANES=""
IFS=',' read -ra ADDRS <<< "$DATA_PLANES_RAW"
for addr in "${ADDRS[@]}"; do
  if [ -z "$DATA_PLANES" ]; then
    DATA_PLANES="http://$addr"
  else
    DATA_PLANES="$DATA_PLANES,http://$addr"
  fi
done

echo "=== driving $NUM_SHARDS shards across: $DATA_PLANES ==="
RUST_LOG=off,mvsqlite_shard_bench=info /usr/local/bin/mvsqlite-shard-bench \
  --data-planes "$DATA_PLANES" --admin-api "http://$ADMIN_API_IP" \
  --ns-prefix "mh_$NUM_SHARDS" --num-shards "$NUM_SHARDS" --num-entities 200 \
  --concurrency 64 --iterations 150 --mode scaling
