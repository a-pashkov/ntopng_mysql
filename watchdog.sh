#!/bin/bash

cd ${0%${0##*/}}
BASEDIR=$(pwd)
WD_FILE="${BASEDIR}/rel/ntopng_mysql/watchdog"
WD_TIMEOUT=300

if test -f $WD_FILE; then 
  if (( $(date +%s) - $(date +%s -r $WD_FILE) > $WD_TIMEOUT )); then
    echo "$(date) Restart system" >&2
    ${BASEDIR}/stop.sh
    rm -f $WD_FILE
    service ntopng stop
    ${BASEDIR}/start.sh
    service ntopng start
  fi
else 
  echo "$(date) File '${WD_FILE}' not found"
fi 
