#!/bin/bash

[ -z "$CEPH_HOME" ] && CEPH_HOME=/usr/local/ceph

. $CEPH_HOME/log.sh
. $CEPH_HOME/ssh-conf.sh

function execute_remote_command_ssh()
{
    local check=$1
    local host=$2
    local statf=$3
    shift 3
    local cmd=$*

    local userhost=$host
    [ -n "$CEPH_REMOTE_SSH_USER" ] && userhost=$CEPH_REMOTE_SSH_USER@$host

    ssh $SSH_PORT_OPT $userhost $CEPH_HOME/execute-cmd.sh $statf $cmd > /dev/null  

    if [ $check -eq 0 ] ; then
        return 0
    fi

    if [ $? -ne 0 ] ; then
        log "in execute_remote_command_ssh(), ssh error: failed to execute remote command: $cmd"
        return 1
    fi

    local output=`ssh $SSH_PORT_OPT $userhost cat $statf`
    ssh $SSH_PORT_OPT $userhost rm -f $statf

    echo $output | grep "STAT====SUCCESS====STAT" > /dev/null 2>&1
    local ret=$?

    if [ $ret -eq 0 ] ; then
        log "in execute_remote_command_ssh(), command \"ssh $SSH_PORT_OPT $userhost $CEPH_HOME/execute-cmd.sh $statf $cmd\" succeeded. SSH-REMOTE-OUTPUT: $output"
    else
        log "in execute_remote_command_ssh(), command \"ssh $SSH_PORT_OPT $userhost $CEPH_HOME/execute-cmd.sh $statf $cmd\" failed. SSH-REMOTE-OUTPUT: $output"
    fi

    return $ret
}

function execute_remote_command_truck()
{
    local check=$1
    local host=$2
    local statf=$3
    shift 3
    local cmd=$*

    python /opt/truck/remote.py call -H $host -c "$CEPH_HOME/execute-cmd.sh $statf $cmd" -d -t 3600 > /dev/null 2>&1

    if [ $check -eq 0 ] ; then
        return 0
    fi

    if [ $? -ne 0 ] ; then
        log "in execute_remote_command_truck(), truck error: failed to execute remote command: $cmd"
        return 1
    fi

    local output=`python /opt/truck/remote.py call -H $host -c "cat $statf" -d`
    python /opt/truck/remote.py call -H $host -c "rm -f $statf" -d > /dev/null 2>&1

    echo $output | grep "STAT====SUCCESS====STAT" > /dev/null 2>&1
    local ret=$?

    if [ $ret -eq 0 ] ; then 
        log "in execute_remote_command_truck(), command python /opt/truck/remote.py call -H $host -c \"$CEPH_HOME/execute-cmd.sh $statf $cmd\" -d -t 3600 succeeded. TRUCK-REMOTE-OUTPUT: $output"
    else
        log "in execute_remote_command_truck(), command python /opt/truck/remote.py call -H $host -c \"$CEPH_HOME/execute-cmd.sh $statf $cmd\" -d -t 3600 failed. TRUCK-REMOTE-OUTPUT: $output"
    fi

    return $ret 
}

function execute_remote_command()
{
    local host=$1
    if [ -z "$host" ] ; then
        log "in execute_remote_command(), host missing"
        return 1
    fi

    shift
    local cmd=$*

    if [ -z "$cmd" ] ; then
        log "in execute_remote_command(), command missing"
        return 1
    fi

    local statf=/tmp/remote-command-status.`date +%s`

    if [ "$CEPH_REMOTE_TOOL" == "truck" ] ; then
        execute_remote_command_truck 1 $host $statf $cmd
        return $?
    elif [ "$CEPH_REMOTE_TOOL" == "ssh" ] ; then
        execute_remote_command_ssh 1 $host $statf $cmd
        return $?
    else
        log "invalid CEPH_REMOTE_TOOL:$CEPH_REMOTE_TOOL"
        return 1
    fi
    return 0
}

function execute_remote_command_batch()
{
    if [ -z "$1" ] ; then
        log "in execute_remote_command_batch(), hosts missing"
        return 1
    fi

    local hosts=`echo $1 | sed -e 's/,/ /g'`

    shift
    local cmd=$*

    if [ -z "$cmd" ] ; then
        log "in execute_remote_command_batch(), command missing"
        return 1
    fi

    local statf=/tmp/remote-command-status.`date +%s`

    for host in $hosts ; do 
        if [ "$CEPH_REMOTE_TOOL" == "truck" ] ; then
            log "run command $cmd on host $host in background by truck"
            execute_remote_command_truck 0 $host $statf $cmd &
        elif [ "$CEPH_REMOTE_TOOL" == "ssh" ] ; then
            log "run command $cmd on host $host in background by ssh"
            execute_remote_command_ssh 0 $host $statf $cmd &
        else
            log "invalid CEPH_REMOTE_TOOL:$CEPH_REMOTE_TOOL"
            return 1
        fi
    done

    log "wait for the background processes to complete ..."
    wait

    for host in $hosts ; do 
        if [ "$CEPH_REMOTE_TOOL" == "truck" ] ; then
            local c=0
            local output=""
            while [ $c -lt 5 ] ; do # wait for some more time although we have waited 
                output=`python /opt/truck/remote.py call -H $host -c "cat $statf" -d`

                echo $output | grep "STAT====SUCCESS====STAT" > /dev/null 2>&1
                [ $? -eq 0 ] && break

                echo $output | grep "cat:.*: No such file or directory" > /dev/null 2>&1
                if [ $? -eq 0 ] ; then
                    log "in execute_remote_command_batch(), wait for host $host to complete command $cmd; retry=$c"
                else
                    break  #break when status file exists
                fi

                c=`expr $c + 1`
                sleep 60
            done

            output=`python /opt/truck/remote.py call -H $host -c "cat $statf" -d`

            echo $output | grep "STAT====FAILURE====STAT" > /dev/null 2>&1 
            if [ $? -eq 0 ] ; then
                log "command $cmd failed on $host, SSH-REMOTE-OUTPUT: $output"
                return 1
            fi

            echo $output | grep "STAT====SUCCESS====STAT" > /dev/null 2>&1
            if [ $? -eq 0 ] ; then
                log "command $cmd succeeded on $host, SSH-REMOTE-OUTPUT: $output"
            else
                log "command $cmd failed with unknown reason on $host, SSH-REMOTE-OUTPUT: $output"
                return 1
            fi

            python /opt/truck/remote.py call -H $host -c "rm -f $statf" -d > /dev/null 2>&1

        elif [ "$CEPH_REMOTE_TOOL" == "ssh" ] ; then
            local userhost=$host
            [ -n "$CEPH_REMOTE_SSH_USER" ] && userhost=$CEPH_REMOTE_SSH_USER@$host
            local c=0
            local output=""
            while [ $c -lt 5 ] ; do # wait for some more time although we have waited 
                output=`ssh $SSH_PORT_OPT $userhost cat $statf 2>&1`

                echo $output | grep "STAT====SUCCESS====STAT" > /dev/null 2>&1
                [ $? -eq 0 ] && break

                echo $output | grep "cat:.*: No such file or directory" > /dev/null 2>&1
                if [ $? -eq 0 ] ; then
                    log "in execute_remote_command_batch(), wait for host $host to complete command $cmd; retry=$c"
                else
                    break  #break when status file exists
                fi

                c=`expr $c + 1`
                sleep 60
            done

            output=`ssh $SSH_PORT_OPT $userhost cat $statf 2>&1`

            echo $output | grep "STAT====FAILURE====STAT" > /dev/null 2>&1 
            if [ $? -eq 0 ] ; then
                log "command $cmd failed on $host, SSH-REMOTE-OUTPUT: $output"
                return 1
            fi

            echo $output | grep "STAT====SUCCESS====STAT" > /dev/null 2>&1
            if [ $? -eq 0 ] ; then
                log "command $cmd succeeded on $host, SSH-REMOTE-OUTPUT: $output"
            else
                log "command $cmd failed with unknown reason on $host, SSH-REMOTE-OUTPUT: $output"
                return 1
            fi

            ssh $SSH_PORT_OPT $userhost rm -f $statf
        else
            log "invalid CEPH_REMOTE_TOOL:$CEPH_REMOTE_TOOL"
            return 1
        fi
    done

    return 0
}

function send_file_ssh()
{
    local host=$1
    local src_file=$2
    local dst_file=$3

    local userhost=$host
    [ -n "$CEPH_REMOTE_SSH_USER" ] && userhost=$CEPH_REMOTE_SSH_USER@$host

    scp $SCP_PORT_OPT $src_file $userhost:$dst_file
    if [ $? -ne 0 ] ; then
        log "in send_file_ssh(), ssh error: scp command failed when copy file $src_file"
        return 1
    fi

    local lsout=`ssh $SSH_PORT_OPT $userhost ls -l $dst_file`

    echo $lsout | grep "No such file or directory" > /dev/null 2>&1
    if [ $? -eq 0 ] ; then
        log "in send_file_ssh(), ssh error: failed to copy file $src_file (No such file or directory)"
        return 1
    fi

    local lsize=`ls -l $src_file | grep "$src_file" | sed -e 's/.*[-r][-w][-x][-r][-w][-x][-r][-w][-x][.]* //' | tr -s ' ' | cut -d ' ' -f 4`
    local rsize=`echo $lsout | grep "$dst_file" | sed -e 's/.*[-r][-w][-x][-r][-w][-x][-r][-w][-x][.]* //' | tr -s ' ' | cut -d ' ' -f 4`

    if [ -z "$rsize" ] ; then
        log "in send_file_ssh(), ssh error: failed to copy file $src_file (cannot get remote size)"
        return 1
    fi

    if [ $rsize -eq 0 ] ; then
        log "in send_file_ssh(), ssh error: failed to copy file $src_file (remote size is 0)"
        return 1
    fi

    if [ $rsize -ne $lsize ] ; then
        log "in send_file_ssh(), ssh error: failed to copy file $src_file (remote size $rsize does not equal to lcal size $lsize)"
        return 1
    fi

    log ssh $rsize  $lsize
    return 0
}

function send_file_truck()
{
    local host=$1
    local src_file=$2
    local dst_file=$3

    python /opt/truck/remote.py truck -H $host -s $src_file -o $dst_file -d > /dev/null 2>&1
    if [ $? -ne 0 ] ; then
        log "in send_file_truck(), truck error: truck command failed when copy file $src_file"
        return 1
    fi

    local truckout=`python /opt/truck/remote.py call -H $host -c "ls -l $dst_file" -d`

    echo $truckout | grep "No such file or directory" > /dev/null 2>&1
    if [ $? -eq 0 ] ; then
        log "in send_file_truck(), truck error: failed to copy file $src_file (No such file or directory)"
        return 1
    fi

    local lsize=`ls -l $src_file | grep "$src_file" | sed -e 's/.*[-r][-w][-x][-r][-w][-x][-r][-w][-x][.]* //' | tr -s ' ' | cut -d ' ' -f 4`
    local rsize=`echo $truckout | grep "$dst_file" | sed -e 's/.*[-r][-w][-x][-r][-w][-x][-r][-w][-x][.]* //' | tr -s ' ' | cut -d ' ' -f 4`

    if [ -z "$rsize" ] ; then
        log "in send_file_truck(), ssh error: failed to scp file $src_file (cannot get remote size)"
        return 1
    fi

    if [ $rsize -eq 0 ] ; then
        log "in send_file_truck(), ssh error: failed to scp file $src_file (remote size is 0)"
        return 1
    fi

    if [ $rsize -ne $lsize ] ; then
        log "in send_file_truck(), ssh error: failed to copy file $src_file (remote size $rsize does not equal to local size $lsize)"
        return 1
    fi

    log truck $rsize  $lsize
    return 0
}

function send_file()
{
    local host=$1
    local src_file=$2
    local dst_file=$3

    if [ -z "$host" ] ; then
        log "in send_file(), host missing"
        return 1
    fi

    if [ -z "$src_file" ] ; then
        log "in send_file(), source file missing"
        return 1
    fi

    if [ ! -f $src_file ] ; then
        log "in send_file(), source file does not exist"
        return 1
    fi

    if [ -z "$dst_file" ] ; then
        log "in send_file(), dest file missing"
        return 1
    fi

    if [ "$CEPH_REMOTE_TOOL" == "truck" ] ; then
        send_file_truck $host $src_file $dst_file
        return $?
    elif [ "$CEPH_REMOTE_TOOL" == "ssh" ] ; then
        send_file_ssh $host $src_file $dst_file
        return $?
    else
        log "invalid CEPH_REMOTE_TOOL:$CEPH_REMOTE_TOOL"
        return 1
    fi
    return 0
}

# return: 
#    for file:  0: exist and size!=0;  1: not exist;  2: exist but size=0;  3: exist and it's block file
#    for dir:   0: exist;              1: not exist;
function file_exist_ssh() 
{
    local host=$1
    local dst_file=$2
    local isdir=$3  #0: file;  1: dir

    local userhost=$host
    [ -n "$CEPH_REMOTE_SSH_USER" ] && userhost=$CEPH_REMOTE_SSH_USER@$host

    local lsout=`ssh $SSH_PORT_OPT $userhost ls -l $dst_file`

    echo $lsout | grep "No such file or directory" > /dev/null 2>&1 && return 1

    log "$lsout"

    if [ $isdir -eq 0 ] ; then
        echo "$lsout" | grep "^b" > /dev/null 2>&1  #first char is 'b', block device
        if [ $? -eq 0 ] ; then  #found 'b' at the beginning
            log "$dst_file is a block device"
            return 3
        fi

        local rsize=`echo $lsout | grep "$dst_file" | sed -e 's/.*[-r][-w][-x][-r][-w][-x][-r][-w][-x][.]* //' | tr -s ' ' | cut -d ' ' -f 4`
        log "$host:$dst_file size=$rsize"
        if [ -z  "$rsize" ] ; then
            log "WARN: failed to get size of remote file"
        else
            [ $rsize -eq 0 ] && return 2
        fi
    fi

    return 0
}

# return: 
#    for file:  0: exist and size!=0;  1: not exist;  2: exist but size=0 
#    for dir:   0: exist;              1: not exist;
function file_exist_truck() 
{
    local host=$1
    local dst_file=$2
    local isdir=$3  #0: file;  1: dir

    local truckout=`python /opt/truck/remote.py call -H $host -c "ls -l $dst_file" -d`

    echo $truckout | grep "No such file or directory" > /dev/null 2>&1 && return 1

    if [ $isdir -eq 0 ] ; then
        local rsize=`echo $truckout | grep "$dst_file" | sed -e 's/.*[-r][-w][-x][-r][-w][-x][-r][-w][-x][.]* //' | tr -s ' ' | cut -d ' ' -f 4`
        rsize=`echo $rsize | sed -e 's/[^0-9]//g'`
        [ $rsize -eq 0 ] && return 2
    fi

    return 0
}

function file_dir_exist()
{
    local host=$1
    local dst_file=$2
    local isdir=$3  #0: file;  1: dir

    if [ -z "$host" ] ; then
        log "in file_dir_exist(), host missing"
        return 1
    fi

    if [ -z "$dst_file" ] ; then
        log "in file_dir_exist(), file missing"
        return 1
    fi

    if [ -z "$isdir" ] ; then
        log "in file_dir_exist(), isdir missing"
        return 1
    fi

    if [ "$CEPH_REMOTE_TOOL" == "truck" ] ; then
        file_exist_truck $host $dst_file $isdir
        return $?
    elif [ "$CEPH_REMOTE_TOOL" == "ssh" ] ; then
        file_exist_ssh $host $dst_file $isdir
        return $?
    else
        log "invalid CEPH_REMOTE_TOOL:$CEPH_REMOTE_TOOL"
        return 1
    fi

    return 0
}
