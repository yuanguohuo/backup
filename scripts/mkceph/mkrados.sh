#!/bin/bash

[ -z "$CEPH_HOME" ] && CEPH_HOME=/usr/local/ceph

LOG_FILE=$CEPH_HOME/deploy/log/mkrados.log

. $CEPH_HOME/log.sh
. $CEPH_HOME/myhostname.sh
. $CEPH_HOME/getallhosts.sh
. $CEPH_HOME/ssh-conf.sh

CONF=/etc/ceph/ceph.conf
HOSTNAME=`my_hostname $CONF`

usage_exit()
{
    log "Usage: $0"
    log "       --dir <tmp-dir-for-run>"
    log "       --op do_it_all [--crushmapsrc <source-file-of-crushmap>] | init_daemons | prepare_osds"
    log "       [--stat <file-to-write-status>]"
    log "       [--args <args>]"
    exit 1
}

function write_stat()
{
    local stat=$1
    shift

    if [ $stat -eq 0 ] ; then
        log "SUCCESS:$*"
        echo "==STAT==SUCCESS:$*==STAT==" >> $stat_file
    else
        log "FAILURE:$*"
        echo "==STAT==FAILURE:$*==STAT==" >> $stat_file
    fi

    return 0
}

function check_remote_stat()
{
    local statf=$1
    shift
    local hosts=$*

    for host in $hosts ; do

        local userhost=$host
        [ -n "$ssh_user" ] && userhost=$ssh_user@$host

        local st=""
        local c=0
        while [ $c -lt 15 ] ; do
            if [ "$remote_tool" == "truck" ] ; then
                st=`python /opt/truck/remote.py call -H $host -c "cat $statf" -d -t 5 | grep "==STAT==" | sed -e 's/==STAT==\(.*\)==STAT==/\1/'`
            else #ssh
                st=`ssh $SSH_PORT_OPT $userhost cat $statf | grep "==STAT==" | sed -e 's/==STAT==\(.*\)==STAT==/\1/'`
            fi
            [ -n "$st" ] && break

            log "wait for host $host to finish operation ..."
            c=`expr $c + 1`
            sleep 60
        done

        if [ -z "$st" ] ; then
            log "didn't get return status from $host"
            return 1
        fi

        if [ "$remote_tool" == "truck" ] ; then
            python /opt/truck/remote.py call -H $host -c "rm -f $statf" -d -t 5 
        else
            ssh $SSH_PORT_OPT $userhost rm -f $statf
        fi

        log "host: $host, return status:$st"
        echo "$st" | grep SUCCESS: > /dev/null 2>&1
        if [ $? -ne 0 ] ; then
            return 1
        fi
    done

    return 0
}

function diskmap_put()
{
    local diskmap=$1
    local key=$2
    local value=$3

    cat $diskmap | grep "^$key=" > /dev/null 2>&1
    if [ $? -eq 0 ] ; then
        sed -i -e "/^$key=/ s/^\(.*\)$/\1,$value/" $diskmap
    else
        echo "$key=$value" >> $diskmap
    fi
}


function prepare_monmap()
{
    log "preparing monmap in $tmpdir/monmap"

    #first, make a list of monitors
    local mons=`ceph-conf -c $CONF -l mon | egrep -v '^mon$' | sort`
    local args=""

    local type="mon"
    for name in $mons; do
        local id=`echo $name | cut -c 4- | sed 's/^\\.//'`
        local addr=`ceph-conf -c $CONF -n $name "mon addr"`

        if [ -z "$addr" ]; then
            log "monitor $name has no address defined."
            return 1
        fi
        args=$args" --add $id $addr"
    done

    if [ -z "$args" ]; then
        log "no monitors found in config, aborting."
        return 1
    fi

    # build monmap
    local monmap="$tmpdir/monmap"
    log "monmaptool --create --clobber $args --print $monmap"
    monmaptool --create --clobber $args --print $monmap
    if [ $? -ne 0 ] ; then
        log "failed to build monmap"
        return 1
    fi

    return 0
}

function prepare_osdmap
{
    local mapsrc=$1

    log "building osdmap from $CONF"
    osdmaptool --create-from-conf $tmpdir/osdmap -c $CONF

    if [ $? -ne 0 ] ; then
        log "failed to build osdmap"
        return 1
    fi

    if [ -z "$mapsrc" ] ; then
        log "no crushmap specified, thus use the default crushmap"
        return 0
    fi

    if [ ! -s $mapsrc ] ; then
        log "the crushmap specified ($mapsrc) does not exist or is an empty file, thus use the default crushmap"
        return 0
    fi

    log "compile crushmap $mapsrc"
    crushtool -c $mapsrc -o $tmpdir/crushmap
    if [ $? -ne 0 ] ; then
        log "WARN: failed to compile crushmap src, thus use the default crushmap"
        return 0
    fi

    osdmaptool --import-crush $tmpdir/crushmap $tmpdir/osdmap
    if [ $? -ne 0 ] ; then
        log "failed to import crushmap"
        return 1
    fi

    return 0
}

function dispatch_maps()
{
    log "dispatch monmap and osdmap to other hosts"

    local tarname=`date +%s`.tar

    if [ "$remote_tool" == "truck" ] ; then
        cd $tmpdir/ ; tar cvf $tarname * ; cd -
        if [ ! -f $tmpdir/$tarname ] ; then
            log "failed to tar monmap and osdmap"
            return 1
        fi
    fi

    local allhosts=`get_allhosts $CONF`
    for host in $allhosts ; do 
        if [ "$host" != "$HOSTNAME" ] ; then
            log "dispatch to $host ..."
            if [ "$remote_tool" == "truck" ] ; then
                python /opt/truck/remote.py call -H $host -c "rm -fr $tmpdir ; mkdir -p $tmpdir" -d
                python /opt/truck/remote.py truck -H $host -s $tmpdir/$tarname -o $tmpdir/$tarname -d
                python /opt/truck/remote.py call -H $host -c "cd $tmpdir ; tar xvf $tarname ; rm -f $tarname ; cd -" -d
            else
                local userhost=$host
                [ -n "$ssh_user" ] && userhost=$ssh_user@$host

                ssh $SSH_PORT_OPT $userhost rm -fr $tmpdir 
                ssh $SSH_PORT_OPT $userhost mkdir -p $tmpdir
                scp $SCP_PORT_OPT $tmpdir/* $userhost:$tmpdir/
            fi
        fi
    done

    [ "$remote_tool" == "truck" ] && rm -f $tmpdir/$tarname

    return 0
}

#parameter like: 
#    osd.0,osd.1,osd.2
#    mon.a
#    mds.1,mds.2
function init_local_daemons()
{
    rm -f $stat_file

    local dmn_names=`echo $1 | sed -e 's/,/ /g'`

    log "daemons to init: $dmn_names"

    local total_num=0
    local succ_num=0

    for dmn in $dmn_names ; do
        total_num=`expr $total_num + 1`

        log "init $dmn"

        local dmn_type=`echo $dmn | cut -c 1-3`   # e.g. 'mon', if $dmn is 'mon1'
        local dmn_id=`echo $dmn | cut -c 4- | sed 's/^\\.//'`
        local dmn_name="$dmn_type.$dmn_id"

        log "create dir for log file"
        local logf=`ceph-conf -c $CONF -n $dmn_name "log file"`
        [ -z "$logf" ] && logf=/data/proclog/ceph/$dmn_name.log
        rm -f $logf
        local logdir=`dirname $logf` 
        mkdir -p $logdir
        if [ $? -ne 0 ] ; then
            write_stat 1 "create dir for log file"
            return 1
        fi 

        log "create dir for pid file"
        local pidf=`ceph-conf -c $CONF -n $dmn_name "pid file"`
        [ -z "$pidf" ] && pidf=/var/run/ceph/$dmn_name.pid
        rm -f $pidf
        local piddir=`dirname $pidf` 
        mkdir -p $piddir
        if [ $? -ne 0 ] ; then
            write_stat 1 "create dir for pid file"
            return 1
        fi 

        log "create dir for admin socket"
        local sockf=`ceph-conf -c $CONF -n $dmn_name "admin socket"`
        [ -z "$sockf" ] && sockf=/var/run/ceph/$dmn_name.asok
        rm -f $sockf
        local sockdir=`dirname $sockf` 
        mkdir -p $sockdir 
        if [ $? -ne 0 ] ; then
            write_stat 1 "create dir for socket file"
            return 1
        fi

        if [ $dmn_type = "osd" ]; then
            log "ceph-osd -c $CONF --monmap $tmpdir/monmap -i $dmn_id --mkfs --mkkey"
            ceph-osd -c $CONF --monmap $tmpdir/monmap -i $dmn_id --mkfs --mkkey
            if [ $? -ne 0 ] ; then
                log "WARN: failed to mkfs for $dmn_name, so skip it"
                continue  # skip this osd
            fi

            local osd_data=`ceph-conf -c $CONF -n $dmn_name "osd data"`
            [ -z "$osd_data" ] && osd_data=/data/cache/osd$dmn_id

            local osd_keyring=`ceph-conf -c $CONF -n $dmn_name "keyring"`
            [ -z "$osd_keyring" ] && osd_keyring=$osd_data/keyring

            # cat $osd_keyring
            # [osd.2]
            #         key = AQCoHw5XjyQaGxAADBR8AFk7ppzDEU/q3tAjxQ==

            ceph-authtool -p -n $dmn_name $osd_keyring > $tmpdir/key.$dmn_name
            if [ $? -ne 0 ] ; then
                log "WARN: failed to create keyring for $dmn_name"
                #no need to skip this osd
            fi

            systemctl enable ceph-osd@${dmn_id}.service

            # cat $tmpdir/key.$dmn_name 
            # AQCoHw5XjyQaGxAADBR8AFk7ppzDEU/q3tAjxQ==
        fi

        if [ $dmn_type = "mds" ]; then
            local mds_data=`ceph-conf -c $CONF -n $dmn_name "mds data"`
            [ -z "$mds_data" ] && mds_data=/var/lib/ceph/mds/mds.$dmn_id
            test -d $mds_data || mkdir -p $mds_data

            local mds_keyring=`ceph-conf -c $CONF -n $dmn_name "keyring"`
            [ -z "$mds_keyring" ] && mds_keyring=$mds_data/keyring

            log "creating private key for $dmn_name keyring $mds_keyring"
            ceph-authtool --create-keyring --gen-key -n $dmn_name $mds_keyring
            if [ $? -ne 0 ] ; then
                log "WARN: failed to create keyring for $dmn_name"
                #no need to skip this mds
            fi

            ceph-authtool -p -n $dmn_name $mds_keyring > $tmpdir/key.$dmn_name
            if [ $? -ne 0 ] ; then
                log "WARN: failed to print keyring for $dmn_name"
                #no need to skip this mds
            fi

            systemctl enable ceph-mds@${dmn_id}.service

        fi

        if [ $dmn_type = "mon" ]; then
            local mon_data=`ceph-conf -c $CONF -n $dmn_name "mon data"`
            [ -z "$mon_data" ] && mon_data=/var/lib/ceph/mon/mon.$dmn_id

            log "remove and recreate $mon_data"
            rm -fr "$mon_data" 
            if [ $? -ne 0 ] ; then
                write_stat 1 "remove legacy mon data for $dmn_name"
                return 1
            fi
            mkdir -p "$mon_data"
            if [ $? -ne 0 ] ; then
                write_stat 1 "create dir for mon data for $dmn_name"
                return 1
            fi

            ceph-mon -c $CONF --mkfs -i $dmn_id --monmap $tmpdir/monmap --osdmap $tmpdir/osdmap -k $tmpdir/keyring.mon
            if [ $? -ne 0 ] ; then
                write_stat 1 "mkfs for $dmn_name"
                return 1
            fi

            systemctl enable ceph-mon@${dmn_id}.service
        fi

        succ_num=`expr $succ_num + 1`
    done

    #half=`expr $total_num / 2` #half

    if [ $succ_num -lt $total_num ] ; then 
        write_stat 1 "init $total_num daemons, success $succ_num"
        return 1
    fi

    write_stat 0 "init $total_num daemons, success $succ_num"
    return 0
}

function config_daemons()
{
    local dmn_type=$1 #mds, mon or osd

    if [[ "$dmn_type" != "mds" ]] && [[ "$dmn_type" != "osd" ]] && [[ "$dmn_type" != "mon" ]] ; then
        log "daemon type should be mds, osd or mon"
        return 1
    fi

    local diskmap=diskmap-`date +%s`
    rm -f $diskmap || return 1
    touch $diskmap || return 1

    local daemons=`ceph-conf -c $CONF -l $dmn_type | egrep -v "^$dmn_type$" | sort`

    for dmn in $daemons; do
        local id=`echo $dmn | cut -c 4- | sed 's/^\\.//'`
        local name="$dmn_type.$id"
        local host=`ceph-conf -c $CONF -n $name "host"`

        if [[ -z "$id" ]] || [[ -z "$host" ]] ; then
            log "WARN: id ($id) or host ($host) of $daemon is null, so skip it"
            continue
        fi

        log "found $name on $host ..."

        diskmap_put $diskmap $host $name
    done

    if [ "$dmn_type" == "osd" ] ; then

        local statf=/tmp/mkrados-stat-`date +%s`

        local hosts=""

        log "prepare osds"
        for line in `cat $diskmap` ; do
            local key=`echo $line | sed -e 's/\(.*\)=\(.*\)/\1/'`    #key is the host
            local value=`echo $line | sed -e 's/\(.*\)=\(.*\)/\2/'`  #value is the osd list that run on the host

            hosts="$key $hosts"

            local userhost=$key
            [ -n "$ssh_user" ] && userhost=$ssh_user@$key

            log "prepare osds on $key in background"
            if [ "$remote_tool" == "truck" ] ; then
                python /opt/truck/remote.py call -H $key -c "$0 --dir $tmpdir --op prepare_osds --args $value --stat $statf" -d -t 1800 &
            else
                ssh $SSH_PORT_OPT $userhost $0 --dir $tmpdir --op prepare_osds --args $value --stat $statf &
            fi
        done

        log "wait for background processes to complete ..."
        wait

        log "check return status of the background processes"
        check_remote_stat $statf $hosts

        if [ $? -ne 0 ] ; then
            log "check_remote_stat return with failure"
            return 1
        fi
    fi

    local statf=/tmp/mkrados-stat-`date +%s`

    local hosts=""

    log "config $dmn_type"
    for line in `cat $diskmap` ; do
        local key=`echo $line | sed -e 's/\(.*\)=\(.*\)/\1/'`    #key is the host
        local value=`echo $line | sed -e 's/\(.*\)=\(.*\)/\2/'`  #value is the mon, osd or mds list that run on the host

        hosts="$key $hosts"

        log "config $dmn_type on $key in background"

        local userhost=$key
        [ -n "$ssh_user" ] && userhost=$ssh_user@$key

        if [ "$remote_tool" == "truck" ] ; then
            python /opt/truck/remote.py call -H $key -c "$0 --dir $tmpdir --op init_daemons --args $value --stat $statf" -d -t 1800 &
        else
            ssh $SSH_PORT_OPT $userhost $0 --dir $tmpdir --op init_daemons --args $value --stat $statf &
        fi
    done

    log "wait for background processes to complete ..."
    wait 

    log "check return status of the background processes"
    check_remote_stat $statf $hosts

    if [ $? -ne 0 ] ; then
        log "check_remote_stat return with failure"
        return 1
    fi

    rm -f $diskmap

    return 0
}

function config_mdss()
{
    log "start to config mds ..."
    config_daemons mds 
    local ret=$?
    log "finished to config mds, ret=$ret"
    return $ret 

    return 0
}

#parameter like: 
#    osd.0,osd.1,osd.2
function prepare_local_osds()
{
    rm -f $stat_file

    local osds=`echo $1 | sed -e 's/,/ /g'`

    #Yuanguo: to resolve the issue that systemd silently umounts the 
    #   devices, we need to 
    #            1. remove the corresponding item from /etc/fstab
    #            2. systemctl daemon-reload
    #   We do it for all mounpoints in advance here:
    sed -i -e '/osds\/osd/ d' /etc/fstab
    systemctl daemon-reload

    log "osds to prepare locally: $osds"

    local total_num=0
    local succ_num=0
    for osd in $osds ; do
        total_num=`expr $total_num + 1`

        local id=`echo $osd | cut -c 4- | sed 's/^\\.//'`
        local name="osd.$id"

        log "preparing $name ..."

        local osd_data=`ceph-conf -c $CONF -n $name "osd data"`
        [ -z "$osd_data" ] && osd_data=/data/cache/osd$dmn_id
        if [ -f $osd_data ] ; then
            rm -f $osd_data
            if [ $? -ne 0 ] ; then
                log "ERROR: $osd_data is a file and we failed to delete it, thus we cannot mkdir. skip $name"
                return 1
            fi
        fi

        mkdir -p $osd_data
        if [ $? -ne 0 ] ; then
            log "ERROR: failed to create dir $osd_data, skip $name"
            return 1
        fi

        local fs_devs=`ceph-conf -c $CONF -n $name "devs"`
        if [ -z "$fs_devs" ]; then
            log "ERROR: no devs defined for $name, so skip it"
            return 1
        fi

        #we only use the first device;
        local first_dev=`echo $fs_devs | cut '-d ' -f 1`
        local fs_dev=`echo "$first_dev" | sed -e 's#[0-9]*$##'`

        mount | grep "/boot" | grep $fs_dev > /dev/null 2>&1
        if [ $? -eq 0 ] ; then
            log "ERROR: $fs_dev seems to be the system device, you CANNOT use it as ceph osd"
            return 1
        fi

        local info=`umount $fs_dev`
        if [ $? -ne 0 ] ; then
            echo $info | grep "not mounted" > /dev/null 2>&1
            if [ $? -ne 0 ] ; then
                log "ERROR: unable to umount $fs_dev, so skip $name"
                return 1
            fi
        fi

        local osd_journal=`ceph-conf -c $CONF -n $name "osd journal"`
        [ -z "$osd_journal" ] && osd_journal=$osd_data/journal

        echo $osd_journal | grep "/dev/"  > /dev/null 2>&1
        if [ $? -eq 0 ] ; then   #match, so joural is block device 
            #if journal and filestore are 2 partitions of $first_dev, and if we have not partitioned yet, 
            #then make the partitions

            local j_dev=`echo "$osd_journal" | sed -e 's#[0-9]*$##'`
            mount | grep "/boot" | grep $j_dev > /dev/null 2>&1
            if [ $? -eq 0 ] ; then
                log "ERROR: $j_dev seems to be the system device, you CANNOT use it as osd journal"
                return 1
            fi

            if [ "$fs_dev" == "$j_dev" ] ; then
                if [  "$osd_journal" != "${fs_dev}1" ] ; then
                    log "ERROR: journal should use partition 1 (which is faster) for $name"
                    return 1
                fi
                first_dev=${fs_dev}2   #we will make 2 partiions later, 1 is for journal; 2 is for osd data;

                local j_size=`ceph-conf -c $CONF -n $name "osd_journal_size"`
                [ -z "$j_size" ] && j_size=10240
                j_size=`expr $j_size + 1000`   #reserve a bit more space, because we get less than we request!

                local info=`umount $first_dev`
                if [ $? -ne 0 ] ; then
                    echo $info | grep "not mounted" > /dev/null 2>&1
                    if [ $? -ne 0 ] ; then
                        log "ERROR: unable to umount $first_dev, so skip $name"
                        return 1
                    fi
                fi

                local info=`umount $osd_journal`
                if [ $? -ne 0 ] ; then
                    echo $info | grep "not mounted" > /dev/null 2>&1
                    if [ $? -ne 0 ] ; then
                        log "ERROR: unable to umount $osd_journal, so skip $name"
                        return 1
                    fi
                fi

                #make 2 partiions later, 1 is for journal; 2 is for osd data;
                parted -s $fs_dev mklabel gpt
                parted  -s -a opt $fs_dev mkpart primary 0% ${j_size}MB
                parted  -s -a opt $fs_dev mkpart primary  ${j_size}MB 100%

                parted  /dev/sdb align-check optimal 1 | grep "not aligned" > /dev/null 2>&1
                [ $? -eq 0 ] && log "WARN: journal partition of $name is not aligned"

                parted  /dev/sdb align-check optimal 2 | grep "not aligned" > /dev/null 2>&1
                [ $? -eq 0 ] && log "WARN: filestore partition of $name is not aligned"
            fi

            sleep 1

            if [[ ! -b $first_dev ]] ; then
                log "ERROR: filestore of $name is not a block device"
                return 1
            fi

            if [[ ! -b  $osd_journal ]] ; then
                log "ERROR: journal of $name is not a block device"
                return 1
            fi
        else  #not match, so, journal is a file
            local jdir=`dirname $osd_journal`

            if [ -f $jdir ] ; then
                rm -f $jdir
                if [ $? -ne 0 ] ; then
                    log "ERROR: $jdir is a file and we failed to delete it, thus we cannot mkdir. skip $name"
                    return 1
                fi
            fi  
            mkdir -p $jdir 
            if [ $? -ne 0 ] ; then
                log "ERROR: failed to create dir $jdir, skip $name"
                return 1
            fi
        fi

        local fs_type=`ceph-conf -c $CONF -n $name "osd mkfs type"`
        if [ -z "$fs_type" ]; then
            log "ERROR: no filesystem type defined for $name, so skip it"
            return 1
        fi

        local fs_opt=`ceph-conf -c $CONF -n $name "osd mount options $fs_type"`
        if [ -z "$fs_opt" ]; then
            if [ "$fs_type" = "btrfs" ]; then
                #try to fallback to old keys
                fs_opt=`ceph-conf -c $CONF -n $name "btrfs options"`
            fi

            if [ -z "$fs_opt" ]; then
                if [ "$fs_type" = "xfs" ]; then
                    fs_opt="rw,noatime,nobarrier,inode64,logbsize=256k,delaylog"
                else
                    #fallback to use at least rw,noatime
                    fs_opt="rw,noatime"
                fi
            fi
        fi

        local osd_user=`ceph-conf -c $CONF -n $name "user"`
        [ -z "$osd_user" ] &&  osd_user=root

        #umount the path
        local info=`umount $osd_data`
        if [ $? -ne 0 ] ; then
            echo $info | grep "not mounted" > /dev/null 2>&1
            if [ $? -ne 0 ] ; then
                log "ERROR: unable to umount $osd_data, so skip $name"
                return 1
            fi
        fi

        #set read ahead size
        #TODO: this is lost after reboot; how to persist it?
        local devname=`echo $first_dev | cut -d '/' -f 3 | sed -e 's/[0-9]*$//'`
        log "echo 1024 > /sys/block/$devname/queue/read_ahead_kb"
        echo 1024 > /sys/block/$devname/queue/read_ahead_kb

        #format the disk
        local mkfs_opt=`ceph-conf -c $CONF -n $name "osd mkfs options $fs_type"`
        if [ "$fs_type" = "xfs" ] && [ -z "$mkfs_opt" ]; then
            mkfs_opt="-f"
        fi

        modprobe $fs_type || true

        log "create filesystem for $name: mkfs.$fs_type $mkfs_opt $first_dev"
        mkfs.$fs_type $mkfs_opt $first_dev
        if [ $? -ne 0 ] ; then
            log "ERROR: failed to create filesystem for $name, skip it"
            return 1
        fi

        log "mount filesystem for $name"
        rm -fr $osd_data/*
        log "$CEPH_HOME/mnt.sh $LOG_FILE mount_by_uuid $first_dev $osd_data $fs_type $fs_opt"
        $CEPH_HOME/mnt.sh $LOG_FILE mount_by_uuid $first_dev $osd_data $fs_type $fs_opt
        if [ $? -ne 0 ] ; then
            log "ERROR: unable to mount device $first_dev on $osd_data, so skip $name"
            return 1
        fi

        chown $osd_user $osd_data
        chmod +w $osd_data

        rm -fr $osd_data/*

        succ_num=`expr $succ_num + 1`
    done

    write_stat 0 "prepare $total_num osds, success $succ_num"
    return 0
}

function config_osds()
{
    log "start to config osds ..."
    config_daemons osd
    local ret=$?
    log "finished to config osds, ret=$ret"
    return $ret 
}

function gather_keys()
{
    # this function should gather mds keys, osd keys generated in function init_local_daemons() on all hosts
    # as file  
    #      $tmpdir/key.$dmn_name
    #
    # take an osd for example: command 
    #      ceph-osd -c $CONF --monmap $tmpdir/monmap -i 2 --mkfs --mkkey 
    # will generate file  $osd_data/keyring
    #
    # $ cat $osd_data/keyring
    # [osd.2]
    #        key = AQCoHw5XjyQaGxAADBR8AFk7ppzDEU/q3tAjxQ==
    #
    # the key file for osd.2 is key.osd.2
    # $ cat key.osd.2
    # AQCoHw5XjyQaGxAADBR8AFk7ppzDEU/q3tAjxQ==
    # 
    # See function init_local_daemons() for details;

    if [ "$remote_tool" == "truck" ] ; then
        echo "gathering keys with truck is not supported"
    else
        local osdhosts=`get_allhosts $CONF osd`
        local mdshosts=`get_allhosts $CONF mds`
        for host in $osdhosts $mdshosts ; do
            if [ "$host" != "$HOSTNAME" ] ; then
                local userhost=$host
                [ -n "$ssh_user" ] && userhost=$ssh_user@$host
                scp $SCP_PORT_OPT $userhost:$tmpdir/key.osd.* $tmpdir
                scp $SCP_PORT_OPT $userhost:$tmpdir/key.mds.* $tmpdir
            fi
        done
    fi
    return 0
}

function prepare_mon()
{
    # admin keyring
    log "generating admin key at $tmpdir/keyring.admin"
    ceph-authtool --create-keyring --gen-key -n client.admin $tmpdir/keyring.admin

    # $ cat $tmpdir/keyring.admin
    # [client.admin]
    #         key = AQAVQg9XlZYSABAA/ZFg6S3qkwskN5b1kDZ7dg==

    # mon keyring
    log "building initial monitor keyring"

    cp -f $tmpdir/keyring.admin $tmpdir/keyring.mon

    # $ cat $tmpdir/keyring.mon
    # [client.admin]
    #         key = AQAVQg9XlZYSABAA/ZFg6S3qkwskN5b1kDZ7dg==

    ceph-authtool -n client.admin --set-uid=0 \
    --cap mon 'allow *' \
    --cap osd 'allow *' \
    --cap mds 'allow' \
    $tmpdir/keyring.mon

    # $ cat $tmpdir/keyring.mon
    # [client.admin]
    #         key = AQAVQg9XlZYSABAA/ZFg6S3qkwskN5b1kDZ7dg==
    #         auid = 0
    #         caps mds = "allow"
    #         caps mon = "allow *"
    #         caps osd = "allow *"

    ceph-authtool --gen-key -n mon. $tmpdir/keyring.mon --cap mon 'allow *'

    # $ cat $tmpdir/keyring.mon
    # [mon.]
    #         key = AQCEQw9XTO9lKBAA0QNDoO2TUaQDN2BkDGbw7w==
    #         caps mon = "allow *"
    # [client.admin]
    #         key = AQAVQg9XlZYSABAA/ZFg6S3qkwskN5b1kDZ7dg==
    #         auid = 0
    #         caps mds = "allow"
    #         caps mon = "allow *"
    #         caps osd = "allow *"

    #the keys gathered from other hosts;
    #since authentication is disabled, no keys should be gathered here. See function gather_keys.
    #if authentication enabled, gather_keys will gather files like:
    #   key.osd.0
    #   key.osd.1
    #   key.osd.2
    #   ...
    #   key.mds.0
    #   key.mds.1
    #   ...
    # the content should be something like:
    #   $ cat key.osd.2
    #   AQCoHw5XjyQaGxAADBR8AFk7ppzDEU/q3tAjxQ==
    for k in $tmpdir/key.* ; do
        #kname is "osd.2", "osd.3", "mds.0" ...
        local kname=`echo $k | sed 's/.*key\.//'`
        local ktype=`echo $kname | cut -c 1-3`
        local kid=`echo $kname | cut -c 4- | sed 's/^\\.//'`
        local kname="$ktype.$kid"
        #secret is content of the key file, like "AQCoHw5XjyQaGxAADBR8AFk7ppzDEU/q3tAjxQ=="
        local secret=`cat $k`

        log "add key $k"

        if [ -z "$kname" ] ; then
            log "WARN: kname is empty, maybe $k is an empty file"
            continue
        fi

        if [ -z "$secret" ] ; then
            log "WARN: secret is empty, maybe $k is an empty file"
            continue
        fi

        # add something like the following to $tmpdir/keyring.mon
        # [osd.2]
        #         key = AQCoHw5XjyQaGxAADBR8AFk7ppzDEU/q3tAjxQ==
        #         caps mds = "allow"
        #         caps mon = "allow rwx"
        #         caps osd = "allow *"

        if [ "$ktype" = "osd" ]; then
            log "ceph-authtool $tmpdir/keyring.mon -n $kname --add-key $secret --cap mon 'allow rwx' --cap osd 'allow *'"
            ceph-authtool $tmpdir/keyring.mon -n $kname --add-key $secret \
            --cap mon 'allow rwx' \
            --cap osd 'allow *'
        fi

        if [ "$ktype" = "mds" ]; then
            ceph-authtool $tmpdir/keyring.mon -n $kname --add-key $secret \
            --cap mon "allow rwx" \
            --cap osd 'allow *' \
            --cap mds 'allow'
        fi
    done

    #finally, $tmpdir/keyring.mon looks like:
    #
    #[mon.]
    #       key = AQCEQw9XTO9lKBAA0QNDoO2TUaQDN2BkDGbw7w==
    #       caps mon = "allow *"
    #[osd.0]
    #       key = AQXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX==
    #       caps mds = "allow"
    #       caps mon = "allow rwx"
    #       caps osd = "allow *"
    #[osd.1]
    #       key = AQYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY==
    #       caps mds = "allow"
    #       caps mon = "allow rwx"
    #       caps osd = "allow *"
    #[osd.2]
    #       key = AQCoHw5XjyQaGxAADBR8AFk7ppzDEU/q3tAjxQ==
    #       caps mds = "allow"
    #       caps mon = "allow rwx"
    #       caps osd = "allow *"
    # ......
    #[mds.0]
    #       key = AQZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ==
    #       caps mds = "allow"
    #       caps mon = "allow rwx"
    #       caps osd = "allow *"
    # ......
    #[client.admin]
    #       key = AQAVQg9XlZYSABAA/ZFg6S3qkwskN5b1kDZ7dg==
    #       auid = 0
    #       caps mds = "allow"
    #       caps mon = "allow *"
    #       caps osd = "allow *"
    #
    #the $tmpdir/keyring.mon is dispatched to all hosts that run a monitor by 
    #function dispatch_mon_keyring() and then used in  
    #   config_mons()       --> 
    #   config_daemons mon  -->
    #   init_local_daemons  -->
    #         ceph-mon -c $CONF --mkfs -i $dmn_id --monmap $tmpdir/monmap --osdmap $tmpdir/osdmap -k $tmpdir/keyring.mon
    #it defines "who has what priviledge"

    #note that content of $tmpdir/keyring.admin is
    # [client.admin]
    #         key = AQAVQg9XlZYSABAA/ZFg6S3qkwskN5b1kDZ7dg==
    #it is the /etc/ceph/keyring; see dispatch_admin_keyring(); 

    return 0
}

function dispatch_mon_keyring()
{
    local monhosts=`get_allhosts $CONF mon`

    for monhost in $monhosts ; do
        if [ "$remote_tool" == "truck" ] ; then
            python /opt/truck/remote.py truck -H $monhost -s $tmpdir/keyring.mon -o $tmpdir/keyring.mon -d
        else
            local userhost=$monhost
            [ -n "$ssh_user" ] && userhost=$ssh_user@$monhost
            scp $SCP_PORT_OPT $tmpdir/keyring.mon $userhost:$tmpdir/keyring.mon
        fi
    done

    return 0
}

function config_mons()
{
    log "start to config mons ..."
    config_daemons mon
    local ret=$?
    log "finished to config mons, ret=$ret"
    return $ret 
}

function dispatch_admin_keyring()
{
    #just copy to local dir, cms_Ceph.sh will dispatch it later

    local adminkeyring=`ceph-conf -c $CONF "keyring"`
    [ -z "$adminkeyring" ] && adminkeyring=/etc/ceph/keyring

    cp -f $tmpdir/keyring.admin $adminkeyring

    return 0
}

function do_it_all()
{
    local mapsrc=$1

    prepare_monmap || return 1
    prepare_osdmap $mapsrc || return 1
    dispatch_maps || return 1

    #mdss
    config_mdss || return 1

    #osds
    config_osds || return 1

    #gather keys that will be used in function prepare_mon
    gather_keys || return 1

    #mons
    prepare_mon || return 1

    #when init monitors, keyring.mon needs to be present (see function init_local_daemons), so we dispatch it to other hosts;
    dispatch_mon_keyring || return 1

    config_mons || return 1

    dispatch_admin_keyring || return 1

    return 0
}

tmpdir=""
op=""
args=""
stat_file=""
remote_tool=""
ssh_user=""
crushmapsrc=""

while [ $# -ge 1 ]; do
    case $1 in
        --dir | -d)
            [ -z "$2" ] && usage_exit
            shift
            tmpdir=$1
            ;;
        --op | -o)
            [ -z "$2" ] && usage_exit
            shift
            op=$1
            ;;
        --args | -r)
            [ -z "$2" ] && usage_exit
            shift
            args=$1
            ;;
        --stat | -s)
            [ -z "$2" ] && usage_exit
            shift
            stat_file=$1
            ;;
        --remote-tool | -t)
            [ -z "$2" ] && usage_exit
            shift
            remote_tool=$1
            ;;
        --ssh-user | -u)
            [ -z "$2" ] && usage_exit
            shift
            ssh_user=$1
            ;;
        --crushmapsrc | -m)
            [ -z "$2" ] && usage_exit
            shift
            crushmapsrc=$1
            ;;
        *)
            log "ERROR: unrecognized arg $1"
            usage_exit
            ;;
    esac

    shift
done

log "call $0 with op=$op, args=$args, tmpdir=$tmpdir, stat_file=$stat_file, remote_tool=$remote_tool, ssh_user=$ssh_user"

if [ -z "$tmpdir" ] ; then
    log "tmpdir cannot be null"
    usage_exit
fi

if [ -z "$op" ] ; then
    log "op cannot be null"
    usage_exit
fi

if [[ "$op" == "do_it_all" ]] && [[ -z "$remote_tool" ]] ; then
    log "remote-tool cannot be null for do_it_all"
    usage_exit
fi

ret=0
case $op in
    do_it_all)
        log "$0: start do_it_all"
        do_it_all $crushmapsrc
        ret=$?
        log "$0: end do_it_all"
        ;;
    init_daemons)
        log "$0: start init_daemons"
        init_local_daemons $args
        ret=$?
        log "$0: end init_daemons"
        ;;
    prepare_osds)
        log "$0: start prepare_osds"
        prepare_local_osds $args
        ret=$?
        log "$0: end prepare_osds"
        ;;
    *)
        log "ERROR: invalid op $op"
        usage_exit
        ;;
esac
exit $ret
