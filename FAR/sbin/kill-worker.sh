#!/usr/bin/env bash

JAVA_HOME="/usr/lib/jvm-oracle/jdk1.8.0_231"

WORKER_ID=`$JAVA_HOME/bin/jps | grep -i 'Worker' | grep -Eo '[0-9]+'`
if [ "${WORKER_ID}" == "" ]; then
	echo "Can't find Worker process."
	exit 1
fi

kill -9 $WORKER_ID
echo "Killed Worker $WORKER_ID"