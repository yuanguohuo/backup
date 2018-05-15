#!/bin/bash

#make sure $LOG_FILE is defined when call this function;

function log()
{
    local time_stamp=`date "+%Y-%m-%d %H:%M:%S"`

    local logdir=`dirname $LOG_FILE`
    [ ! -d $logdir ] && mkdir -p $logdir

    echo "$time_stamp    $*" | tee -a $LOG_FILE
    return 0
}
