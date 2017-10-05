#!/bin/sh

#export SPARK_HOME=...
NUM=$1
export SERVICE_HOME="$(cd "`dirname "$0"`"/..; pwd)"
export OUTPUT_PATH=${SERVICE_HOME}/outputs/${NUM}

spark-submit ${SERVICE_HOME}/sbin/spark_app.py ${OUTPUT_PATH}
