#!/bin/bash

CURDIR=`dirname $0`
TESTDIR=$CURDIR/..

USER_ACC_ID=JTLA6O1TF69Z0YB4I7O1
USER_SEC_KEY=cwoNbM7TZLxYeMcmQxfiwL7n4Pv0JhPRlNG6m1dq

timestamp=`date +%Y%m%d%H%M%S`

LOGDIR=$CURDIR/logs_${timestamp}
rm -fr $LOGDIR
mkdir $LOGDIR

testbuck=buck_$timestamp

function put_bucket()
{
    local buck=$1

    local log=$LOGDIR/put_bucket.log
    $TESTDIR/put_buck.sh -b $buck -i $USER_ACC_ID -k $USER_SEC_KEY > $log 2>&1

    grep "HTTP/1.1 200 OK" $log > /dev/null

    if [ $? -ne 0 ] ; then
        echo "ERROR: put_bucket failed. See $log for details."
        return 1
    else
        echo "put_bucket succeeded."
    fi

    local log=$LOGDIR/put_bucket_check.log
    $TESTDIR/list_bucks.sh -i $USER_ACC_ID -k $USER_SEC_KEY > $log 2>&1

    grep "$buck" $log > /dev/null

    if [ $? -ne 0 ] ; then
        echo "ERROR: put_bucket failed. check bucket failed. See $log for details."
        return 1
    else
        echo "put_bucket succeeded. check bucket succeeded."
    fi

    return 0
}

function put_object()
{
    local buck=$1

    local empty_file=/tmp/empty_file_$timestamp
    local small_file=/tmp/small_file_$timestamp
    local large_file=/tmp/large_file_$timestamp

    rm -f $empty_file
    touch $empty_file
    dd if=/dev/urandom of=$small_file bs=1024 count=1     # 1KB
    dd if=/dev/urandom of=$large_file bs=1024 count=10240 # 10MB

    local log=$LOGDIR/put_empty_file.log
    $TESTDIR/put_obj.sh -b $buck -o a/b/empty_file -f $empty_file -i $USER_ACC_ID -k $USER_SEC_KEY > $log 2>&1

    grep "HTTP/1.1 200 OK" $log > /dev/null
    if [ $? -ne 0 ] ; then
        echo "ERROR: put_empty_file failed. See $log for details."
        return 1
    else
        echo "put_empty_file succeeded."
    fi

    local log=$LOGDIR/put_small_file.log
    $TESTDIR/put_obj.sh -b $buck -o x/y/small_file -f $small_file -i $USER_ACC_ID -k $USER_SEC_KEY > $log 2>&1

    grep "HTTP/1.1 200 OK" $log > /dev/null
    if [ $? -ne 0 ] ; then
        echo "ERROR: put_small_file failed. See $log for details."
        return 1
    else
        echo "put_small_file succeeded."
    fi

    local log=$LOGDIR/put_large_file.log
    $TESTDIR/put_obj.sh -b $buck -o x/y/large_file -f $large_file -i $USER_ACC_ID -k $USER_SEC_KEY > $log 2>&1

    grep "HTTP/1.1 200 OK" $log > /dev/null
    if [ $? -ne 0 ] ; then
        echo "ERROR: put_large_file failed. See $log for details."
        return 1
    else
        echo "put_large_file succeeded."
    fi


    local get_empty_file=/tmp/get_empty_file_$timestamp
    local get_small_file=/tmp/get_small_file_$timestamp
    local get_large_file=/tmp/get_large_file_$timestamp

    local log=$LOGDIR/get_empty_file.log
    $TESTDIR/get_obj.sh -b $buck -o a/b/empty_file -f $get_empty_file -i $USER_ACC_ID -k $USER_SEC_KEY > $log 2>&1

    grep "HTTP/1.1 200 OK" $log > /dev/null
    if [ $? -ne 0 ] ; then
        echo "ERROR: get_empty_file failed. See $log for details."
        return 1
    else
        echo "get_empty_file succeeded."
    fi


    local log=$LOGDIR/get_small_file.log
    $TESTDIR/get_obj.sh -b $buck -o x/y/small_file -f $get_small_file -i $USER_ACC_ID -k $USER_SEC_KEY > $log 2>&1

    grep "HTTP/1.1 200 OK" $log > /dev/null
    if [ $? -ne 0 ] ; then
        echo "ERROR: get_small_file failed. See $log for details."
        return 1
    else
        echo "get_small_file succeeded."
    fi

    local log=$LOGDIR/get_large_file.log
    $TESTDIR/get_obj.sh -b $buck -o x/y/large_file -f $get_large_file -i $USER_ACC_ID -k $USER_SEC_KEY > $log 2>&1

    grep "HTTP/1.1 200 OK" $log > /dev/null
    if [ $? -ne 0 ] ; then
        echo "ERROR: get_large_file failed. See $log for details."
        return 1
    else
        echo "get_large_file succeeded."
    fi


    local empty_file_md5=`md5sum $empty_file | cut -d ' ' -f 1`
    local small_file_md5=`md5sum $small_file | cut -d ' ' -f 1`
    local large_file_md5=`md5sum $large_file | cut -d ' ' -f 1`

    local get_empty_file_md5=`md5sum $get_empty_file | cut -d ' ' -f 1`
    local get_small_file_md5=`md5sum $get_small_file | cut -d ' ' -f 1`
    local get_large_file_md5=`md5sum $get_large_file | cut -d ' ' -f 1`

    if [ "$empty_file_md5" != "$get_empty_file_md5" ] ; then
        echo "ERROR: empty file md5sum incorrect. expected:$empty_file_md5; actual:$get_empty_file_md5"
        return 1
    else
        echo "empty file md5sum correct. expected:$empty_file_md5; actual:$get_empty_file_md5"
    fi

    if [ "$small_file_md5" != "$get_small_file_md5" ] ; then
        echo "ERROR: small file md5sum incorrect. expected:$small_file_md5; actual:$get_small_file_md5"
        return 1
    else
        echo "small file md5sum correct. expected:$small_file_md5; actual:$get_small_file_md5"
    fi

    if [ "$large_file_md5" != "$get_large_file_md5" ] ; then
        echo "ERROR: large file md5sum incorrect. expected:$large_file_md5; actual:$get_large_file_md5"
        return 1
    else
        echo "large file md5sum correct. expected:$large_file_md5; actual:$get_large_file_md5"
    fi

    return 0
}

put_bucket $testbuck || exit 1
put_object $testbuck || exit 1

echo "Sanity Test Succeeded"

