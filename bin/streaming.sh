#!/usr/bin/env bash
#Author: Alfer weifeng
#Date: 2017-09-18
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
$0 <et | p | w> \n
et\telptransform from kafka data.\n
p\tpersist data transformed with elp.\n
'''

ROOT="$(dirname $(cd $(dirname $0); pwd))"
echo "$ROOT"

. $ROOT/bin/env.sh

if [ $# -lt 1 ]; then
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

echo="JAVA_HOME=$JAVA_HOME"

if [ ! -d "$LOG_PATH" ];then
    mkdir -p "$LOG_PATH"
fi

if [ ! -d "$RUN_PATH" ];then
    mkdir -p "$RUN_PATH"
fi

case $1 in
    et)
        CLASS="com.zqykj.streaming.business.TransformJob"
        CONF="$ROOT/conf/zqy-app.properties"
        LOG_FILE="TransformJob.out"
        PID_FILE="TransformJob.pid"
        ;;
    p)
        CLASS="com.zqykj.streaming.business.LoadJob"
        CONF="$ROOT/conf/zqy-app.properties"
        LOG_FILE="LoadJob.out"
        PID_FILE="LoadJob.pid"
        ;;
    *)
        echo -e "$Usage"
        exit 1
        ;;
esac

echo "SPARK_SUBMIT=$SPARK_SUBMIT"

CMD="$SPARK_SUBMIT \
    --class $CLASS \
    --master yarn-cluster \
    --executor-memory 2G \
    --num-executors 2 \
    --properties-file $CONF \
    $ROOT/streaming-2.0.2-SNAPSHOT.jar"

echo -e "$CMD"
run "$CMD" &
