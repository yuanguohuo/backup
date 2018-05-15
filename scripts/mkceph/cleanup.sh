#!/bin/bash

[ -z "$CEPH_HOME" ] && CEPH_HOME=/usr/local/ceph
[ -z "$TARGET_CONF" ] && TARGET_CONF=/etc/ceph/ceph.conf

LOG_FILE=$1
. $CEPH_HOME/log.sh

#delete old logs, pid files, because we are redeploying a new cluster
logf=`ceph-conf -c $TARGET_CONF -n osd.0 "log file"`
pidf=`ceph-conf -c $TARGET_CONF -n osd.0 "pid file"`
logdir=`dirname $logf`
piddir=`dirname $pidf`

rm -f $logdir/* $piddir/*

log "kill suspending ceph process (if there is any) on this node"

i=0    #if we have checked 3 times and there is no suspending ceph process, return SUCCESS
j=0    #if we have killed 100 times and there is still suspending ceph process, return ERROR

anykilled=0

while [ 1 ] ; do
    process=`ps -ef | grep -e ceph | egrep -v grep | grep -v deploy.sh | egrep -v "$CEPH_HOME/cleanup.sh" | tr -s ' ' | cut -d ' ' -f 2`
    if [ -z "$process" ] ; then
        i=`expr $i + 1`
        if [ $i -eq 3 ] ; then
            break
        fi
        sleep 1
    else
        i=0

        log "suspending ceph process: $process"
        for proc in $process ; do
            log "kill process $proc"
            kill -9 $proc > /dev/null 2>&1
        done

        anykilled=1

        j=`expr $j + 1`
        if [ $j -eq 100 ] ; then
            break
        fi
    fi
done

if [ $i -eq 3 ] ; then
    if [ $anykilled -eq 0 ] ; then
        log "no suspending ceph process to kill"
    else
        log "succeeded to kill suspending ceph process"
    fi
    exit 0    #SUCCESS
else
    log "failed to kill suspending ceph process" 
    exit 1    #ERROR
fi
