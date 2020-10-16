#!/usr/bin/env bash

usage="Usage: collie.sh --wait period"

# if no args specified, show usage
if [ $# -le 0 ]; then
  echo $usage
  exit 1
fi

if [ "$1" == "--wait" ]; then
  PERIOD="$2"
  echo "Sleep ${PERIOD}s"
else
  echo $usage
  exit 1
fi

# Wait for SparkSubmit start
sleep 10

SUBMIT_ID=`jps | grep -i 'SparkSubmit' | grep -Eo '[0-9]+'`
if [ "${SUBMIT_ID}" == "" ]; then
  echo "Can't find SparkSubmit process."
  exit 1
else 
  echo "SparkSubmit PID: ${SUBMIT_ID}"
fi

sleep $PERIOD

ALIVE=`jps | grep -i ${SUBMIT_ID}`

if [ "${ALIVE}" == "" ]; then
  echo "Finished. Exit."
else
  echo "Still alive, kill the SparkSubmit!"
  kill -9 $SUBMIT_ID
fi

exit 0
