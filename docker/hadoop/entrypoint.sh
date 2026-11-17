#!/bin/bash
set -e

echo "======================================"
echo " Starting NoSQL Hadoop Environment"
echo "======================================"

export HDFS_NAMENODE_USER=root
export HDFS_DATANODE_USER=root
export YARN_RESOURCEMANAGER_USER=root
export YARN_NODEMANAGER_USER=root

mkdir -p /hadoop/dfs/name /hadoop/dfs/data /hadoop/tmp /hadoop/logs

# Format NameNode only on first startup
if [ ! -f /hadoop/dfs/name/current/VERSION ]; then
    echo "Formatting HDFS NameNode..."
    hdfs namenode -format -force -nonInteractive
fi

echo "Starting HDFS NameNode..."
hadoop-daemon.sh start namenode

echo "Starting HDFS DataNode..."
hadoop-daemon.sh start datanode

echo "Waiting for HDFS..."
HDFS_READY=false

for i in $(seq 1 30); do
    if hdfs dfsadmin -report >/dev/null 2>&1; then
        HDFS_READY=true
        break
    fi
    sleep 2
done

if [ "$HDFS_READY" != "true" ]; then
    echo "ERROR: HDFS did not become ready within 60 seconds."
    exit 1
fi

echo "Starting YARN ResourceManager..."
yarn-daemon.sh start resourcemanager

echo "Starting YARN NodeManager..."
yarn-daemon.sh start nodemanager

echo "Starting MapReduce JobHistory Server..."
mr-jobhistory-daemon.sh start historyserver
echo "======================================"
echo " Hadoop services started"
echo "======================================"

cleanup() {
    mr-jobhistory-daemon.sh stop historyserver || true
    echo "Stopping Hadoop services..."
    yarn-daemon.sh stop nodemanager || true
    yarn-daemon.sh stop resourcemanager || true
    hadoop-daemon.sh stop datanode || true
    hadoop-daemon.sh stop namenode || true
}

trap cleanup TERM INT

tail -f /dev/null &
wait $!
