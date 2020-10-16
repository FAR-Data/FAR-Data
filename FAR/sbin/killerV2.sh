#!/usr/bin/env bash

usage="Usage: killerV2.sh --server server-url --jobid id [--sleep period]"

# if no args specified, show usage
if [ $# -le 0 ]; then
  echo $usage
  exit 1
fi

if [ "$1" == "--server" ] && [ "$3" == "--jobid" ]; then
  URL="$2"
  JOB_ID="$4"
  shift 4
else
  echo $usage
  exit 1
fi

if [ "$1" == "--sleep" ]; then
  PERIOD="$2"
  echo "Sleep ${PERIOD}s"
  sleep $PERIOD
fi

HOST="http://${URL}"
APP_ID=`curl -s ${HOST}/api/v1/applications/ | grep -Eo 'app-[0-9]+-[0-9]+'`

if [ "$APP_ID" == "" ]; then
  echo "Can't find running application."
  exit 1
fi

JOB_LINK="${HOST}/api/v1/applications/${APP_ID}/jobs"
EXECUTOR_LINK="${HOST}/api/v1/applications/${APP_ID}/executors"

CURRENT_JOB_ID=`curl -s ${JOB_LINK} | grep -Eom1 '\"jobId\"\s:\s[0-9]+' | grep -Eo '[0-9]+'`

if [[ "$CURRENT_JOB_ID" -gt "$JOB_ID" ]]; then
  echo "Current job id: ${CURRENT_JOB_ID}, too late."
  exit 1
fi

LAST_SEEN_ID=0
while [[ "$CURRENT_JOB_ID" -lt "$JOB_ID" ]]; do 
	CURRENT_JOB_ID=`curl -s ${JOB_LINK} | grep -Eom1 '\"jobId\"\s:\s[0-9]+' | grep -Eo '[0-9]+'`
	if [[ "$CURRENT_JOB_ID" -gt "$LAST_SEEN_ID" ]]; then
		echo "Current job id: ${CURRENT_JOB_ID}, waiting ${JOB_ID}..."
	fi
	LAST_SEEN_ID=${CURRENT_JOB_ID}
	sleep 0.5
done

TARGET_WORKER=`curl -s ${EXECUTOR_LINK} | getMIE.py`

# Stop one slave
echo "Killing the worker ${TARGET_WORKER} !"

if [ -z "${SPARK_HOME}" ]; then
  export SPARK_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi
. "${SPARK_HOME}/sbin/spark-config.sh"
. "${SPARK_HOME}/bin/load-spark-env.sh"
"${SPARK_HOME}/sbin/slave.sh" ${TARGET_WORKER} cd "${SPARK_HOME}" \; "${SPARK_HOME}/sbin"/kill-worker.sh

# Stop one slave
echo "The worker ${TARGET_WORKER} has been killed!"
