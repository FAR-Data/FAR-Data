#!/usr/bin/env bash

if [ $# -lt 1 ]; then
  echo "Usage: kill-by-name.sh procedure_name"
  exit 1
fi

PROCESS=`ps -ef|grep -v grep|grep -v kill-by-name|grep $1|grep -v PPID|awk '{ print $2}'`

for PROC in $PROCESS
do
  echo "Kill the $1 process [ $PROC ]"
  kill -9 $PROC
done