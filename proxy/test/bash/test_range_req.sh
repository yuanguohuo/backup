#!/bin/bash

bucket=$1
object=$2
src_file=$3

if [[ -z "$bucket" ]] || [[ -z "$object" ]] || [[ -z "$src_file" ]] ; then
    echo "Error: missing arguments"
    echo "Usage: $0 {bucket} {object} {source-file}"
    exit 1
fi

size=`ls -l $src_file | tr -s ' ' | cut -d ' ' -f 5`

stride=$(echo "$size/10"|bc)

# range like: start-end
function range_test_1()
{
    local start=$1
    [ -z "$start" ] && start=0

    local end=$start

    local tmpRange=/tmp/range_test_1_range_`date +%s`.tmp
    local tmpPiece=/tmp/range_test_1_piece_`date +%s`.tmp

    while [ $end -lt $size ] ; do

        local len=`expr $end - $start + 1`

        rm -f $tmpRange $tmpPiece 
        ./get_range.sh $bucket $object $start $end $tmpRange 2> /dev/null

        dd ibs=1 skip=$start count=$len if=$src_file of=$tmpPiece

        local md5_1=`md5sum $tmpRange | tr -s ' ' | cut -d ' ' -f 1`
        local md5_2=`md5sum $tmpPiece | tr -s ' ' | cut -d ' ' -f 1`

        if [ "$md5_1" != "$md5_2" ] ; then
            echo "$tmpRange and $tmpPiece don't match"
            return 1
        fi

        echo "Range $start-$end, md5sum: $md5_1"
        echo "Piece $start-$end, md5sum: $md5_2"

        rm -f $tmpRange $tmpPiece

        if [ $end -lt 131072 ] ; then  #in the fist 128K, add 40K each time
            end=`expr $end + 40960`
        else
            end=`expr $end + $stride`  #after the first 128K, add stride each time
        fi
    done

    return 0
}

# range like: start-,  means from 'start' to EOF
function range_test_2()
{
    local start=0
    local end=`expr $size - 1`

    local tmpRange=/tmp/range_test_1_range_`date +%s`.tmp
    local tmpPiece=/tmp/range_test_1_piece_`date +%s`.tmp

    while [ $start -lt $size ] ; do
        local len=`expr $end - $start + 1`

        rm -f $tmpRange $tmpPiece 
        ./get_range.sh $bucket $object $start "" $tmpRange 2> /dev/null  # -1 means ""

        dd ibs=1 skip=$start count=$len if=$src_file of=$tmpPiece

        local md5_1=`md5sum $tmpRange | tr -s ' ' | cut -d ' ' -f 1`
        local md5_2=`md5sum $tmpPiece | tr -s ' ' | cut -d ' ' -f 1`

        if [ "$md5_1" != "$md5_2" ] ; then
            echo "$tmpRange and $tmpPiece don't match"
            return 1
        fi

        echo "Range $start-$end, md5sum: $md5_1"
        echo "Piece $start-$end, md5sum: $md5_2"

        rm -f $tmpRange $tmpPiece

        if [ $start -lt 131072 ] ; then  #in the fist 128K, add 40K each time
            start=`expr $start + 40960`
        else
            start=`expr $start + $stride`  #after the first 128K, add stride each time
        fi
    done

    return 0
}

# range like: -len, means the last 'len' bytes
function range_test_3()
{
    local len=1
    local end=`expr $size - 1`

    local tmpRange=/tmp/range_test_1_range_`date +%s`.tmp
    local tmpPiece=/tmp/range_test_1_piece_`date +%s`.tmp

    while [ $len -le $size ] ; do
        local start=`expr $size - $len`

        rm -f $tmpRange $tmpPiece 
        ./get_range.sh $bucket $object "" $len $tmpRange 2> /dev/null  # -1 means ""

        dd ibs=1 skip=$start count=$len if=$src_file of=$tmpPiece

        local md5_1=`md5sum $tmpRange | tr -s ' ' | cut -d ' ' -f 1`
        local md5_2=`md5sum $tmpPiece | tr -s ' ' | cut -d ' ' -f 1`

        if [ "$md5_1" != "$md5_2" ] ; then
            echo "$tmpRange and $tmpPiece don't match"
            return 1
        fi

        echo "Range $start-$end, md5sum: $md5_1"
        echo "Piece $start-$end, md5sum: $md5_2"

        rm -f $tmpRange $tmpPiece

        len=`expr $len + $stride`
    done

    return 0
}


range_test_1 0
if [ $? -ne 0 ] ; then
    echo "Test 0 failed"
    exit 1
fi

range_test_1 65536
if [ $? -ne 0 ] ; then
    echo "Test 64K failed"
    exit 1
fi

range_test_1 131072 
if [ $? -ne 0 ] ; then
    echo "Test 128K failed"
    exit 1
fi


range_test_1 5242880 
if [ $? -ne 0 ] ; then
    echo "Test 5M failed"
    exit 1
fi

range_test_1 31457280 
if [ $? -ne 0 ] ; then
    echo "Test 30M failed"
    exit 1
fi

range_test_2
if [ $? -ne 0 ] ; then
    echo "Test range_test_2 failed"
    exit 1
fi

range_test_3
if [ $? -ne 0 ] ; then
    echo "Test range_test_3 failed"
    exit 1
fi

echo "All tests have succeeded"
exit 0


