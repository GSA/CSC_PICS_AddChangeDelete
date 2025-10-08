#!/bin/bash

# Generate a timestamp
timestamp=$(date +"%Y%m%d_%H%M")
RTDIR="/opt/python_scripts/gss"
APPDIR="$RTDIR/apps/CSC_PICSAddChangeReport"
LOGDIR="$APPDIR/logs"
cd "$APPDIR"
find $LOGDIR -type f -name "*.log" -mtime +7 -exec rm {} \;
export PYTHONPATH=$RTDIR/apps/d2d-pandas-etl:$RTDIR/apps/commonUtils:$PYTHONPATH
echo $PYTHONPATH
python3.12 $APPDIR/PICSAddChange.py | tee $LOGDIR/"PICSAddChange_$timestamp.log"
