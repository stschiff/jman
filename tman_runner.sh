#!/usr/bin/env bash

LOGFILE=$1
CMD=$2

printf "STARTING\t$(date)\n" > $LOGFILE
printf "COMMAND\t$CMD\n" >> $LOGFILE
printf "OUTPUT\n" >> $LOGFILE
($CMD 2>&1) >> $LOGFILE
EXIT_CODE=$?
printf "FINISHED\t$(date)\n" >> $LOGFILE
printf "EXIT CODE\t$EXIT_CODE\n" >> $LOGFILE
