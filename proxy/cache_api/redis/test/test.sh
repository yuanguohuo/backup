#!/bin/bash

logdir=`dirname $0`/log

stamp=`date +%Y%m%d%H%M%S`


param1=$1

HOST="127.0.0.1"
PORT=8191

if [[ -z "$param1" ]] || [[ $param1 -eq 1 ]] ; then
    #basic test
    echo "Run basic tests ..."
    
    time1=`date +%s`
    
    curl "http://$HOST:$PORT/redis/test/basic" > $logdir/basic_$stamp.log 2>&1
    
    time2=`date +%s`
    elapse=`expr $time2 - $time1`
    
    grep "All basic tests have succeeded" $logdir/basic_$stamp.log > /dev/null 2>&1
    if [ $? -ne 0 ] ; then
        echo "basic test failed, try to find cause from $logdir/basic_$stamp.log"
        exit 1
    else
        echo "Basic test succeeded. elapse=$elapse"
    fi
fi


if [[ -z "$param1" ]] || [[ $param1 -eq 2 ]] ; then
    #concurrent test
    echo "Run concurrent tests ..."
    redis-cli -c -p 7000 set foobar 0 > /dev/null 2>&1
    if [ $? -ne 0 ] ; then
        echo "failed to reset foobar"
        exit 1
    fi
    
    steps=123456
    total=`expr $steps \* 9`
    
    time1=`date +%s`
    
    curl -s "http://$HOST:$PORT/redis/test/concurrent1?c=$steps" > $logdir/concurrent1_$stamp.log 2>&1 &
    curl -s "http://$HOST:$PORT/redis/test/concurrent1?c=$steps" > $logdir/concurrent2_$stamp.log 2>&1 &
    curl -s "http://$HOST:$PORT/redis/test/concurrent1?c=$steps" > $logdir/concurrent3_$stamp.log 2>&1 &
    curl -s "http://$HOST:$PORT/redis/test/concurrent2?c=$steps" > $logdir/concurrent4_$stamp.log 2>&1 &
    curl -s "http://$HOST:$PORT/redis/test/concurrent2?c=$steps" > $logdir/concurrent5_$stamp.log 2>&1 &
    curl -s "http://$HOST:$PORT/redis/test/concurrent2?c=$steps" > $logdir/concurrent6_$stamp.log 2>&1 &
    curl -s "http://$HOST:$PORT/redis/test/concurrent1?c=$steps" > $logdir/concurrent7_$stamp.log 2>&1 &
    curl -s "http://$HOST:$PORT/redis/test/concurrent2?c=$steps" > $logdir/concurrent8_$stamp.log 2>&1 &
    curl -s "http://$HOST:$PORT/redis/test/concurrent1?c=$steps" > $logdir/concurrent9_$stamp.log 2>&1 &
    
    wait
    
    time2=`date +%s`
    elapse=`expr $time2 - $time1`
    
    ret=`redis-cli -c -p 7000 get foobar 2>/dev/null`
    
    if [ $ret -ne $total ] ; then
        echo "concurrent test failed. expected result is 1111104, but actual is $ret"
        exit 1
    else
        echo "Concurrent test succeeded. elapse=$elapse"
    fi
fi

if [[ -z "$param1" ]] || [[ $param1 -eq 3 ]] ; then
    #consistency test
    # the idea is like this:
    # when we run consistency.lua, we crash a master node and then restart it.
    # During this period of time, many access errors will occur (because slotmap is
    # outdated). The cluster will refresh the slotmap when too many errors have been
    # found. So, after the last refresh, error numbers should stop increasing. 
    # Thus, after restart, we clear the log; in the new log, error num should not
    # increase;
    echo "Run consistency tests ..."
    
    time1=`date +%s`
    curl -s "http://$HOST:$PORT/redis/test/consistency?round=100" | tee $logdir/consistency_$stamp.log | tee /tmp/consistency_$stamp.log > /dev/null 2>&1 &
    
    sleep 1
    
    #seletet a master node (not 7000) to kill
    pid=`redis-cli -p 7000 cluster nodes | grep master | grep -v ":7000" | cut -d ' ' -f 2 | cut -d ':' -f 2 | head -1`
    
    #kill the selected master node
    redis-cli -p $pid  debug segfault > /dev/null 2>&1
    
    sleep 1
    
    #start the killed master node
    redis-server /usr/local/redis-3.2.8/etc/redis_$pid.conf
    
    wait
    
    time2=`date +%s`
    elapse=`expr $time2 - $time1`
    
    for x in {1..20} ; do
        sed -i -e '1,/======================/ d' /tmp/consistency_$stamp.log
    done
    
    reads=`head -n 6 /tmp/consistency_$stamp.log | grep "Num Read" | cut -d ":" -f 2`
    writes=`head -n 6 /tmp/consistency_$stamp.log | grep "Num Write" | cut -d ":" -f 2`
    rfails=`head -n 6 /tmp/consistency_$stamp.log | grep "Read Fail" | cut -d ":" -f 2`
    wfails=`head -n 6 /tmp/consistency_$stamp.log | grep "Write Fail" | cut -d ":" -f 2`
    wlosts=`head -n 6 /tmp/consistency_$stamp.log | grep "Lost Write" | cut -d ":" -f 2`
    wnoack=`head -n 6 /tmp/consistency_$stamp.log | grep "Un-acked Write" | cut -d ":" -f 2`
    
    reads1=`tail -n 6 /tmp/consistency_$stamp.log | grep "Num Read" | cut -d ":" -f 2`
    writes1=`tail -n 6 /tmp/consistency_$stamp.log | grep "Num Write" | cut -d ":" -f 2`
    rfails1=`tail -n 6 /tmp/consistency_$stamp.log | grep "Read Fail" | cut -d ":" -f 2`
    wfails1=`tail -n 6 /tmp/consistency_$stamp.log | grep "Write Fail" | cut -d ":" -f 2`
    wlosts1=`tail -n 6 /tmp/consistency_$stamp.log | grep "Lost Write" | cut -d ":" -f 2`
    wnoack1=`tail -n 6 /tmp/consistency_$stamp.log | grep "Un-acked Write" | cut -d ":" -f 2`
    
    if [[ $reads1 -gt 0 ]] &&
       [[ $writes1 -eq $reads1 ]] && 
       [[ $rfails1 -eq $rfails ]] &&  #read error number should be equal
       [[ $wfails1 -eq $wfails ]] &&  #write error number should be equal
       [[ $wlosts1 -eq 0 ]] &&        #lost writes should be 0
       [[ $wnoack1 -eq 0 ]]           #write with no ack should be 0
    then
        echo "Consistency test succeeded. elapse=$elapse"
    else
        echo "Consistency test failed:"
        echo "Statistics after last refresh"
        head -n 6 /tmp/consistency_$stamp.log 
        echo "Statistics at last"
        tail -n 6 /tmp/consistency_$stamp.log 
    fi
    
    rm -f /tmp/consistency_$stamp.log
fi
