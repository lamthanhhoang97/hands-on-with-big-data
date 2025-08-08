#!/bin/bash
if [ "$SPARK_MODE" == "master" ]; then
    echo "Starting Spark in master mode";
    /opt/spark/sbin/start-master.sh && tail -f /dev/null
elif [ "$SPARK_MODE" == "worker" ]; then
    echo "Starting Spark in worker mode";
    /opt/spark/sbin/start-worker.sh $SPARK_MASTER_URL --cores $SPARK_WORKER_CORES && tail -f /dev/null
elif [ "$SPARK_MODE" == "history" ]; then
    echo "Staring Spark in history mode";
    /opt/spark/sbin/start-history-server.sh && tail -f /dev/null
fi