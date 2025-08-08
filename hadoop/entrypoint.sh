#!/bin/bash

/usr/sbin/sshd -D & #The & puts it into the background.

# Check if namenode is already formatted
if [ ! -d "${HADOOP_HOME}/data/dfs/namenode/current" ]; then
    echo "Formatting namdenode..."
    ${HADOOP_HOME}/bin/hdfs namenode -format -force
fi

# Start HDFS service
echo "Starting Hadoop services..."
${HADOOP_HOME}/sbin/start-dfs.sh

# Keep the container running
tail -f /dev/null