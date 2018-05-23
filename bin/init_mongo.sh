#!/usr/bin/env bash

ROOT="$(dirname $(cd $(dirname $0); pwd))"
echo "$ROOT"

. $ROOT/bin/env.sh

echo "$MONGODB_HOME"

init_mongodb(){

    if [ "$MONGODB_IS_AUTHENTICATE" = "true" ]; then
        "$MONGODB_HOME"/bin/mongo -u zqykj -p zqykj localhost/hyjj ../resources/service/scripts/insert_docRules.js
    elif [ "$MONGODB_IS_AUTHENTICATE" = "false" ]; then
        "$MONGODB_HOME"/bin/mongo localhost/hyjj "$ROOT"/scripts/insert_docRules.js
    fi

}

init_mongodb
