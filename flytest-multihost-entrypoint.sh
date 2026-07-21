#!/bin/bash
set -e

# ROLE=coordinator or ROLE=worker (worker also needs COORDINATOR_IP set).
ROLE="${ROLE:?ROLE must be set}"

MY_IP=$(getent ahostsv6 fly-local-6pn | head -n1 | awk '{print $1}')
if [ -z "$MY_IP" ]; then
  echo "could not resolve fly-local-6pn" >&2
  exit 1
fi
echo "=== role=$ROLE my_ip=$MY_IP cores=$(nproc) ==="

mkdir -p /etc/foundationdb /var/lib/foundationdb/data /var/log/foundationdb
chown -R foundationdb:foundationdb /etc/foundationdb /var/lib/foundationdb /var/log/foundationdb || true

if [ "$ROLE" = "coordinator" ]; then
  RANDSTR=$(tr -dc 'A-Za-z0-9' </dev/urandom | head -c 8)
  echo "test:${RANDSTR}@[$MY_IP]:4500" > /etc/foundationdb/fdb.cluster
else
  : "${COORDINATOR_CLUSTER_STRING:?COORDINATOR_CLUSTER_STRING must be set for workers}"
  echo "$COORDINATOR_CLUSTER_STRING" > /etc/foundationdb/fdb.cluster
fi
chmod 0664 /etc/foundationdb/fdb.cluster
chown foundationdb:foundationdb /etc/foundationdb/fdb.cluster || true
cat /etc/foundationdb/fdb.cluster

NPROC=$(nproc)
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
public-address = [$MY_IP]:\$ID
listen-address = public
datadir = /var/lib/foundationdb/data/\$ID
logdir = /var/log/foundationdb

[fdbserver.4500]
EOF
for i in $(seq 1 $((NPROC - 1))); do
  { echo ""; echo "[fdbserver.$((4500 + i))]"; } >> "$CONF"
done

/usr/lib/foundationdb/fdbmonitor --conffile "$CONF" --lockfile /var/run/fdbmonitor.pid &
FDBMON_PID=$!
sleep 5

if [ "$ROLE" = "coordinator" ]; then
  fdbcli -C /etc/foundationdb/fdb.cluster --exec "configure new single memory" || true
  sleep 2
fi
fdbcli -C /etc/foundationdb/fdb.cluster --exec "status" || true

# Every role also runs its own mvstore, bound to its own private IP, so the
# benchmark driver (run separately via SSH exec) can address each machine's
# mvstore directly for a real one-process-per-machine data-plane list.
/usr/local/bin/mvstore --data-plane "[$MY_IP]:7000" --admin-api "[$MY_IP]:8000" \
  --metadata-prefix mvsqlite-multihost --raw-data-prefix mh &

echo "=== ready: ip=$MY_IP cluster=$(cat /etc/foundationdb/fdb.cluster) ==="

wait $FDBMON_PID
