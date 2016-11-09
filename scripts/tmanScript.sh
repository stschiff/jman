#!/usr/bin/env bash

if [ "$#" -ne 2 ]; then
    echo "Usage: tmanScript ScriptFile ProjectPath"
else
    SCRIPTFILE=$1
    PROJECTPATH=$2
    export TMAN_PROJECT_FILE=.$(basename $SCRIPTFILE).tman
    if [ -e $TMAN_PROJECT_FILE ] && \
        [ $(date -r $TMAN_PROJECT_FILE +%s) -gt $(date -r $SCRIPTFILE +%s) ]; then
        export TMAN_DO_NOT_UPDATE_PROJECT=True
    else
        export TMAN_DO_NOT_UPDATE_PROJECT=False
        tman init -p $PROJECTPATH 
    fi
fi
