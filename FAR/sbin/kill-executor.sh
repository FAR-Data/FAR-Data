#!/usr/bin/env bash

if [ -z "${JAVA_HOME}" ]; then
  echo "Need to set JAVA_HOME."
  exit 1
fi


EXEC_ID=`$JAVA_HOME/bin/jps | grep -i 'CoarseGrainedExecutorBackend' | grep -Eo '[0-9]+' | head -n 1`
if [ "${EXEC_ID}" == "" ]; then
	echo "Can't find Worker process."
	exit 1
fi

kill -9 $EXEC_ID
echo "Killed CoarseGrainedExecutor $EXEC_ID"