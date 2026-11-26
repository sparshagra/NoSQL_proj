#!/usr/bin/env bash
set -euo pipefail

export HOME=/tmp
export USER=hive
export JAVA_TOOL_OPTIONS="-Duser.home=/tmp"
export HADOOP_CLIENT_OPTS="-Xmx1G -Duser.home=/tmp"
export HADOOP_CLASSPATH="/opt/tez/*:/opt/tez/lib/*:"

RUN_ID="${1:?run_id required}"
shift

CONF_DIR="/tmp/hive_conf_${RUN_ID}"
DERBY_DIR="/tmp/derby_metastore"
DB_URL="jdbc:derby:;databaseName=${DERBY_DIR}/metastore_db;create=true"

mkdir -p "$CONF_DIR" /tmp/hive_scratch /tmp/hive_warehouse "$DERBY_DIR" /tmp/.beeline

cat <<XML > "${CONF_DIR}/hive-site.xml"
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property><name>fs.defaultFS</name><value>hdfs://localhost:9000</value></property>
  <property><name>javax.jdo.option.ConnectionURL</name><value>${DB_URL}</value></property>
  <property><name>javax.jdo.option.ConnectionDriverName</name><value>org.apache.derby.jdbc.EmbeddedDriver</value></property>
  <property><name>hive.metastore.warehouse.dir</name><value>file:///tmp/hive_warehouse</value></property>
  <property><name>hive.metastore.warehouse.external.dir</name><value>file:///tmp/hive_warehouse</value></property>
  <property><name>hive.exec.scratchdir</name><value>/tmp/hive/scratch</value></property>
  <property><name>hive.user.install.directory</name><value>/tmp/hive/scratch</value></property>
  <property><name>hive.server2.thrift.bind.host</name><value>127.0.0.1</value></property>
  <property><name>hive.server2.thrift.port</name><value>10000</value></property>
  <property><name>hive.server2.enable.doAs</name><value>false</value></property>
  <property><name>hive.server2.authentication</name><value>NONE</value></property>
  <property><name>hive.query.results.cache.enabled</name><value>false</value></property>
  <property><name>hive.execution.engine</name><value>tez</value></property>
  <property><name>tez.local.mode</name><value>true</value></property>
  <property><name>tez.runtime.optimize.local.fetch</name><value>true</value></property>
  <property><name>hive.tez.exec.inplace.progress</name><value>false</value></property>
  <property><name>hive.exec.submit.local.task.via.child</name><value>false</value></property>
  <property><name>metastore.metastore.event.db.notification.api.auth</name><value>false</value></property>
</configuration>
XML

export HIVE_CONF_DIR="$CONF_DIR"
cd "$DERBY_DIR"

/opt/hive/bin/schematool -dbType derby -initSchema >/tmp/schematool.log 2>&1

/opt/hive/bin/hive --skiphadoopversion --skiphbasecp --service hiveserver2 >/tmp/hs2.log 2>&1 &
HS2_PID=$!
trap 'kill "$HS2_PID" 2>/dev/null || true' EXIT

HS2_READY=0
for _ in $(seq 1 180); do
  if (echo > /dev/tcp/127.0.0.1/10000) >/dev/null 2>&1; then
    HS2_READY=1
    break
  fi
  if ! kill -0 "$HS2_PID" 2>/dev/null; then
    echo "HiveServer2 exited early:" >&2
    tail -n 120 /tmp/hs2.log >&2
    exit 1
  fi
  sleep 1
done

if [ "$HS2_READY" -ne 1 ]; then
  echo "Timed out waiting for HiveServer2 on 127.0.0.1:10000." >&2
  echo "========== PROCESSES ==========" >&2
  ps -efww >&2
  echo "========== TCP SOCKETS ==========" >&2
  cat /proc/net/tcp /proc/net/tcp6 >&2
  echo "========== JAVA PROCESS STATE ==========" >&2
  cat "/proc/$HS2_PID/status" >&2
  echo "WAIT CHANNEL:" >&2
  cat "/proc/$HS2_PID/wchan" >&2 || true
  echo "========== TEMP LOG FILES ==========" >&2
  find /tmp /opt/hive/logs -maxdepth 3 -type f -printf '%p (%s bytes)\\n' 2>/dev/null | head -100 >&2
  echo "========== HIVE SERVER LOG ==========" >&2
  cat /tmp/hs2.log >&2
  echo "========== HIVE APPLICATION LOG ==========" >&2
  tail -n 180 /tmp/hive/hive.log >&2
  echo "========== DERBY METASTORE LOG ==========" >&2
  tail -n 100 /tmp/derby_metastore/derby.log >&2
  exit 1
fi

COMBINED="/tmp/combined_${RUN_ID}.hql"
cat /hive_sql/01_create_tables.hql /hive_sql/02_parse_and_tag.hql /hive_sql/03_analytics.hql > "$COMBINED"

/opt/hive/bin/beeline \
  --silent=true \
  --showHeader=false \
  --outputformat=tsv2 \
  -n hive -p "" \
  -u 'jdbc:hive2://127.0.0.1:10000/default' \
  "$@" \
  -f "$COMBINED"
