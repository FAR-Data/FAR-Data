#!/usr/bin/env bash

USAGE="collect-results.sh DEST TARGET-FOLDER BACKUP-FOLDER"

if [ $# -lt 3 ]; then
  echo $USAGE
  exit 1
fi

DEST="$1"
TARGET="$2"
BACKUP="$3"

RESULTS_PATH="/home/xpmei/deployed/spark-external"
BACKUP_PATH="/home/xpmei/exec-results/${BACKUP}"
mkdir -p ${BACKUP_PATH}


if [ ! -z "$(ls -A ${RESULTS_PATH}/loggers/)" ]; then
	mv ${RESULTS_PATH}/loggers/* ${BACKUP_PATH}/
fi
if [ ! -z "$(ls -A ${RESULTS_PATH}/work/)" ]; then
	mv ${RESULTS_PATH}/work/* ${BACKUP_PATH}/
fi

MYNAME=`hostname`
rsync -av ${BACKUP_PATH}/ ${DEST}:${TARGET}/${MYNAME}/

