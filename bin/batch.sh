#!/usr/bin/env bash
#Author: Alfer weifeng
#Date: 2017-12-29
#Description:

run () {
    if [ -f "$RUN_PATH/$PID_FILE" ]; then
        echo "$RUN_PATH/$PID_FILE already exists."
        echo "Now exiting ..."
        exit 1
    fi
    $@ > $LOG_PATH/"$LOG_FILE" 2>&1 &
    PID=$!
    echo "$PID" > "$RUN_PATH/$PID_FILE"
    wait "$PID"
    rm -f "$RUN_PATH/$PID_FILE"
}

Usage='''Usage: \n
$0 <doc | docType> \ttransform document from elp data. \n
docType: PEOPLE VEHICLE TELPHONE ORGANIZATION CASE ADDRESS \n
'''

ROOT="$(dirname $(cd $(dirname $0); pwd))"
echo "$ROOT"

. $ROOT/bin/env.sh

if [ $# -lt 2 ]; then
    echo -e "$Usage"
    exit 1
fi

LOG_PATH="$ROOT"/logs
RUN_PATH="$ROOT"/run

if [ "$JAVA_HOME" != "" ] ; then
    JAVA="$JAVA_HOME/bin/java"
else
    echo "Environment variable \$JAVA_HOME is not set."
    exit 1
fi

if [ ! -d "$LOG_PATH" ];then
    mkdir -p "$LOG_PATH"
fi

if [ ! -d "$RUN_PATH" ];then
    mkdir -p "$RUN_PATH"
fi

case $1 in
    doc)
        CLASS="com.zqykj.batch.document.job.DocDataCollectJob"
        CONF="$ROOT/conf/zqy-doc.properties"
        LOG_FILE="DocDataCollectJob.out"
        PID_FILE="DocDataCollectJob.pid"
        ;;
    trans)
        CLASS="com.zqykj.batch.transform.DataTransformJob"
        CONF="$ROOT/conf/zqy-doc.properties"
        LOG_FILE="DataTransformJob.out"
        PID_FILE="DataTransformJob.pid"
        ;;
    bj)
        CLASS="com.zqykj.batch.transform.HiveTransJob"
        CONF="$ROOT/conf/zqy-bj.properties"
        LOG_FILE="HiveTransJob.out"
        PID_FILE="HiveTransJob.pid"
        ;;
    *)
        echo -e "$Usage"
        exit 1
        ;;
esac

docType=$2

echo "SPARK_SUBMIT=$SPARK_SUBMIT"

CMD="$SPARK_SUBMIT \
    --class $CLASS \
    --master yarn-cluster \
    --driver-memory 3G \
    --executor-cores 2 \
    --executor-memory 12G \
    --conf spark.yarn.driver.memoryOverhead=1024 \
    --conf spark.yarn.executor.memoryOverhead=4096 \
    --conf spark.task.maxFailures=2 \
    --conf spark.network.timeout=120 \
    --conf spark.shuffle.io.maxRetries=4 \
    --conf spark.shuffle.io.retryWait=60s \
    --conf spark.core.connection.ack.wait.timeout=60000 \
    --conf spark.dynamicAllocation.enabled=true \
    --conf spark.shuffle.service.enabled=true \
    --conf spark.dynamicAllocation.executorIdleTimeout=60s \
    --conf spark.dynamicAllocation.initialExecutors=1 \
    --conf spark.dynamicAllocation.maxExecutors=5 \
    --conf spark.dynamicAllocation.minExecutors=0 \
    --conf spark.memory.userLegacyMode=false \
    --conf spark.memory.fraction=0.8 \
    --conf spark.memory.storageFraction=0.5 \
    --queue root.work \
    --properties-file $CONF \
    $ROOT/streaming-2.0.3-bj-SNAPSHOT.jar $docType"

echo -e "$CMD"
run "$CMD" &
