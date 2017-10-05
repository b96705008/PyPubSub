#!/bin/sh
export SERVICE_HOME="$(cd "`dirname "$0"`"/..; pwd)"
export PYTHONPATH=${PYTHONPATH}:/${SERVICE_HOME}/lib
export PYTHONPATH=${PYTHONPATH}:/${SERVICE_HOME}/app

export ENV=env
export CONFIG_PATH=${SERVICE_HOME}/etc/${ENV}.cfg

python ${SERVICE_HOME}/sbin/client_app.py ${CONFIG_PATH}
