#!/bin/bash
function write_stat()
{
    local stat_file=$1
    local stat=$2
    local outf=$3

    case $stat in
        0)
            echo "STAT====SUCCESS====STAT" > $stat_file
            ;;
        1)
            echo "STAT====FAILURE====STAT" > $stat_file
            ;;
        *)
            echo "STAT====INTERNAL-ERROR====STAT" > $stat_file
            ;;
    esac

    echo "OUTPUT====BEGIN====OUTPUT" >> $stat_file
    if [ -f $outf ] ; then
        cat $outf >> $stat_file
    fi
    echo "OUTPUT====END====OUTPUT" >> $stat_file

    return 0
}

function execute_cmd_locally()
{
    local stat_file=$1
    shift
    local command=$*

    local outf=/tmp/ceph-execute-cmd-output-`date +%s`

    $command > $outf 2>&1

    write_stat $stat_file $? $outf

    rm -f $outf
}

if [ -z "$1" ] ; then
    outf=/tmp/ceph-execute-cmd-output-`date +%s`
    echo "missing return status file" > $outf
    write_stat /tmp/ceph-remote-command-status.default 1 $outf
    rm -f $outf
    exit 1
fi

if [ -z "$2" ] ; then
    outf=/tmp/ceph-execute-cmd-output-`date +%s`
    echo "missing command to run" > $outf
    write_stat $1 1 $outf 
    rm -f $outf
    exit 1
fi

execute_cmd_locally $*

exit 0
