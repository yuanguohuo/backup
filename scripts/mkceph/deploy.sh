#!/bin/bash

[ -z "$CEPH_HOME" ] && CEPH_HOME=/usr/local/ceph

RUN_DIR=$CEPH_HOME/deploy
ORIGIN_CONF=$RUN_DIR/ceph.conf
BACKUP_CONF=$RUN_DIR/bak/ceph.conf
TARGET_DIR=/etc/ceph
TARGET_CONF=$TARGET_DIR/ceph.conf
LOG_FILE=$RUN_DIR/log/deploy.log

TARGET_PROGRAM=hpcc
CEPH_REMOTE_TOOL=ssh

. $CEPH_HOME/log.sh
. $CEPH_HOME/myhostname.sh
. $CEPH_HOME/remote-tool.sh
. $CEPH_HOME/getallhosts.sh
. $CEPH_HOME/ssh-conf.sh

osd_weight_map=$RUN_DIR/osd_weight_map

mkdir -p $RUN_DIR
mkdir -p $TARGET_DIR
mkdir -p `dirname $BACKUP_CONF`
mkdir -p `dirname $LOG_FILE`

if [ ! -f $ORIGIN_CONF ] ; then
    log "no configuration file (ceph.conf) found, thus cannot deploy ceph" 
    echo "10203"
    exit 1
fi

echo $ORIGIN_CONF
HOSTNAME=`my_hostname $ORIGIN_CONF`
#if [ $? -ne 0 ] ; then
#    log "failed to get IP address of localhost, thus $0 aborted!"
#    echo "10203"
#    exit 1
#fi

function is_ceph_running()
{
    ceph --connect-timeout 30 -s > /dev/null 2>&1
    return $?
}

function is_mon_running()
{
    local mon_id=$1

    i=0
    while [ $i -lt 30 ] ; do
        local mon_stat=`ceph --connect-timeout 5 mon stat | grep "quorum"`
        local mons_run=${mon_stat##*quorum}
        mons_run=${mons_run//,/' '}
        for id in $mons_run ; do
            if [ "$mon_id" == "$id" ] ; then
                log "mon.$mon_id is running"
                return 0
            fi
        done

        i=`expr $i + 1`
        sleep 1
    done

    log "mon.$mon_id is down"
    return 1
}


function get_osd_tree()
{
    local tmpfile=$1
    local retry=0

    while [ $retry -lt 10 ] ; do
        ceph --connect-timeout 15 osd tree > $tmpfile 2> /dev/null
        cat $tmpfile | grep -i -e "id[[:space:]][[:space:]]*weight[[:space:]][[:space:]]*type[[:space:]][[:space:]]*name[[:space:]][[:space:]]*up\/down[[:space:]][[:space:]]*reweight" > /dev/null 2>&1
        if [ $? -eq 0 ] ; then
            log "got osd tree"
            return 0
        fi

        retry=`expr $retry + 1`
        sleep 1
    done

    return 1
}

function is_osd_running()
{
    local osd=$1

    local tmpfile=/tmp/osdtree-`date +%s`

    get_osd_tree $tmpfile
    if [ $? -ne 0 ] ; then
        log "failed to get osd tree, so we don't know if the osd is running or not" 
        rm -f $tmpfile
        return 2            #return false because we don't know.
    fi

    cat $tmpfile | grep "\<$osd\>" | grep up > /dev/null 2>&1
    if [ $? -eq 0 ] ; then
        log "$osd is running"
        rm -f $tmpfile
        return 0
    fi

    log "$osd is down"
    rm -f $tmpfile
    return 1
}

function is_osd_out_cluster()
{
    local osd=$1

    local tmpfile=/tmp/osdtree-`date +%s`

    get_osd_tree $tmpfile
    if [ $? -ne 0 ] ; then  #failed to run command "ceph --connect-timeout 30 osd tree"
        log "failed to get osd tree, so we don't know if $osd is in cluster or not temporarily" 
        rm -f $tmpfile
        return 2            #return false because we don't know.
    fi

    #we succeeded to get osd tree

    #1. if the given osd cannot be found in osd tree, it is "out";
    cat $tmpfile | grep "\<$osd\>" > /dev/null 2>&1
    if [ $? -ne 0 ] ; then
        log "$osd is not in osd tree, so it is out of the cluster"
        rm -f $tmpfile
        return 0
    fi

    #2. if the given osd is "in", its reweight should be greater than 0;
    local inout=`cat $tmpfile | grep "\<$osd\>" | sed -e 's/^ *//' | tr -s ' ' | cut -d ' ' -f 5`
    if [ $(echo "$inout>0"|bc) -eq 1 ] ; then
        log "$osd is in the cluster"
        rm -f $tmpfile
        return 1
    fi

    #3. otherwise, it is "out"
    log "$osd is out of the cluster"
    rm -f $tmpfile
    return 0
}

function is_osd_out_crush()
{
    local osd=$1

    local tmpfile=/tmp/osdtree-`date +%s`

    get_osd_tree $tmpfile
    if [ $? -ne 0 ] ; then  #failed to run command "ceph --connect-timeout 30 osd tree"
        log "failed to get osd tree, so we don't know if $osd is in crushmap or not temporarily" 
        rm -f $tmpfile
        return 2            #return false because we don't know.
    fi

    #we succeeded to get osd tree

    #1. if the given osd cannot be found in osd tree, it is out of the crushmap;
    cat $tmpfile | grep "\<$osd\>" > /dev/null 2>&1
    if [ $? -ne 0 ] ; then
        log "$osd is not in osd tree, so it is out of the crushmap"
        rm -f $tmpfile
        return 0
    fi

    #2. if its weight is 0, it's out of the crushmap;
    local weight=`cat $tmpfile | grep "\<$osd\>" | sed -e 's/^ *//' | tr -s ' ' | cut -d ' ' -f 2`
    if [ $(echo "$weight==0"|bc) -eq 1 ] ; then  #weight is float in 0.94, so we have to compare it like this
        log "$osd is out of the crush map"
        rm -f $tmpfile
        return 0
    fi

    #3. otherwise, it is in the crushmap;
    log "$osd is in crush map"
    rm -f $tmpfile
    return 1
}

function is_osd_out_osdmap()
{
    local osd=$1

    local tmpfile=/tmp/osdtree-`date +%s`

    get_osd_tree $tmpfile
    if [ $? -ne 0 ] ; then  #failed to run command "ceph --connect-timeout 30 osd tree"
        log "failed to get osd tree, so we don't know if $osd is in osdmap or not temporarily" 
        rm -f $tmpfile
        return 2            #return false because we don't know.
    fi

    #we succeeded to get osd tree

    #1. if the given osd cannot be found in osd tree, it is out of the osdmap;
    cat $tmpfile | grep "\<$osd\>" > /dev/null 2>&1
    if [ $? -ne 0 ] ; then
        log "$osd is not in osd tree, so it is out of the osdmap"
        rm -f $tmpfile
        return 0
    fi

    #2. otherwise, it is in the osdmap;
    log "$osd is in osd map"
    rm -f $tmpfile
    return 1
}

function is_osd_created()
{
    local osd_id=$1
    local tmpfile=/tmp/osdtree-`date +%s`

    get_osd_tree $tmpfile
    if [ $? -ne 0 ] ; then
        log "failed to get osd tree"
        rm -f $tmpfile
        return 2
    fi

    cat $tmpfile | grep "^$osd_id[[:space:]][[:space:]]*0[[:space:]][[:space:]]*osd.$osd_id[[:space:]][[:space:]]*down[[:space:]][[:space:]]*0" >/dev/null 2>&1
    if [ $? -eq 0 ] ; then
        log "found osd $osd_id"
        rm -f $tmpfile
        return 0
    fi

    log "didn't find osd $osd_id"
    rm -f $tmpfile
    return 1
}

function rm_mon_data()
{
    local name=$1
    local conf_file=$2  #in which conf file the monitor to be removed is defined
    [ -z "$conf_file" ] && log "2nd param cannot be null for rm_mon_data(), it is the conf" && return 1
    [ ! -f "$conf_file" ] && log "conf (2nd param) for rm_mon_data() doesn't exist" && return 1

    local mon_data=`ceph-conf -c $conf_file -n $name "mon data"`
    local mon_host=`ceph-conf -c $conf_file -n $name "host"`

    execute_remote_command $mon_host rm -fr $mon_data
    if [ $? -ne 0 ] ; then
        log "in rm_mon_data(), failed to remove $mon_data on host $mon_host"
        return 1
    fi

    return 0
}

function rm_legacy_data()
{
    log "remove legacy monitor data on monitors on every node"
    local mons=`ceph-conf -c $TARGET_CONF -l mon | egrep -v '^mon$' | sort`
    for name in $mons; do
        rm_mon_data "$name" "$TARGET_CONF"
        [ $? -ne 0 ] && return 1
    done

    return 0;
}

function gen_crushmap()
{
    local crushmapsrc=$RUN_DIR/crushmapsrc
    rm -f $crushmapsrc
    local suf=`date +%s`
    local bucket_weight_map=/tmp/bucket-weight-map-$suf.txt
    local rack_osds_map=/tmp/rack-osds-map-$suf.txt

    local hosts=`get_allhosts $ORIGIN_CONF osd`
    local num_hosts=`echo $hosts | wc -w`
    if [ $num_hosts -lt 3 ] ; then
        log "will not generate crushmap because number of hosts is less than 3"
        return 0
    fi

    log "total host num: $num_hosts"

    #shuffle the hosts
    local host_shuf_src=/tmp/host-shuffle-src-$suf.txt
    local host_shuf_dst=/tmp/host-shuffle-dst-$suf.txt

    for host in $hosts ; do
        echo $host >> $host_shuf_src
    done
    cat $host_shuf_src | shuf --random-source=$host_shuf_src > $host_shuf_dst 

    #seperate the osds into 3 racks (make sure that the osds in the same hosts will be in the same rack)

    unset rack0  
    unset rack1  
    unset rack2  
    declare -a rack0 
    declare -a rack1 
    declare -a rack2 

    local idx0=0
    local idx1=0
    local idx2=0

    local i=0
    local mod=0
    for host in `cat $host_shuf_dst` ; do
        #osds in this host
        local osds=`ceph-conf -c $ORIGIN_CONF -l osd --filter-key-value host=$host`

        mod=`expr $i % 3`  #spread among the 3 racks

        if [ $mod -eq 0 ] ; then
            log "host $host is in rack0"
            for osd in $osds ; do
                rack0[$idx0]="$osd"
                idx0=`expr $idx0 + 1`
            done
        elif [ $mod -eq 1 ] ; then
            log "host $host is in rack1"
            for osd in $osds ; do
                rack1[$idx1]="$osd"
                idx1=`expr $idx1 + 1`
            done
        else #mod==2
            log "host $host is in rack2"
            for osd in $osds ; do
                rack2[$idx2]="$osd"
                idx2=`expr $idx2 + 1`
            done
        fi

        i=`expr $i + 1`
    done

    local osd_num=`expr $idx0 + $idx1 + $idx2`
    log "total osd num:$osd_num"
    log "rack0: $idx0: ${rack0[@]}"
    log "rack1: $idx1: ${rack1[@]}"
    log "rack2: $idx2: ${rack2[@]}"

    #number of rows: on everage, every 36 osds in each rack forms a row; that's 3 hosts since there're 12 osds in a host
    local row_num=`expr $idx2 / 36`
    mod=`expr $idx2 % 36`
    [ $mod -ne 0 ] && row_num=`expr $row_num + 1`

    log "row num: $row_num"

    local last_row=`expr $row_num - 1`

    #each rack is cut into slices as below:

    #          |  rack0     rack1     rack2
    #----------+---------------------------
    #    row0: | rack00    rack10    rack20
    #    row1: | rack01    rack11    rack21
    #    row2: | rack02    rack12    rack22
    #    ...   | ...       ...       ...
    #    rowN: | rack0N    rack1N    rack2N

    local osd_wght=0.0

    #cut rack0 into splices
    local row_size=`expr $idx0 / $row_num`
    local row_osds=""
    local row_no=0
    local counter=0
    local rack_weight=0.0
    for osd in ${rack0[@]} ; do
        #current row is full and it's not the last row, then save this row in map file and start a new row; 
        #for the last row, we put all the remaining osds there, thus it may have several more osds;
        if [[ $counter -eq $row_size ]] && [[ $row_no -lt $last_row ]] ; then
            echo "rack0$row_no:$row_osds" >> $rack_osds_map
            echo "rack0$row_no:$rack_weight"  >> $bucket_weight_map
            row_osds="" 
            row_no=`expr $row_no + 1`
            counter=0
            rack_weight=0.0
        fi
        row_osds="$row_osds $osd"
        counter=`expr $counter + 1`
        osd_wght=`grep -w $osd $osd_weight_map 2> /dev/null | cut -d ' ' -f 2`
        [ -z "$osd_wght" ] && osd_wght=1.000
        rack_weight=$(echo "$rack_weight+$osd_wght"|bc)
    done
    #last row
    echo "rack0$row_no:$row_osds" >> $rack_osds_map
    echo "rack0$row_no:$rack_weight"  >> $bucket_weight_map

    #cut rack1 into splices
    row_size=`expr $idx1 / $row_num`
    row_osds=""
    row_no=0
    counter=0
    rack_weight=0.0
    for osd in ${rack1[@]} ; do
        if [[ $counter -eq $row_size ]] && [[ $row_no -lt $last_row ]] ; then
            echo "rack1$row_no:$row_osds" >> $rack_osds_map
            echo "rack1$row_no:$rack_weight"  >> $bucket_weight_map
            row_osds="" 
            row_no=`expr $row_no + 1`
            counter=0
            rack_weight=0.0
        fi
        row_osds="$row_osds $osd"
        counter=`expr $counter + 1`
        osd_wght=`grep -w $osd $osd_weight_map 2> /dev/null | cut -d ' ' -f 2`
        [ -z "$osd_wght" ] && osd_wght=1.000
        rack_weight=$(echo "$rack_weight+$osd_wght"|bc)
    done
    #last row
    echo "rack1$row_no:$row_osds" >> $rack_osds_map
    echo "rack1$row_no:$rack_weight"  >> $bucket_weight_map

    #cut rack2 into splices
    row_size=`expr $idx2 / $row_num`
    row_osds=""
    row_no=0
    counter=0
    rack_weight=0.0
    for osd in ${rack2[@]} ; do
        if [[ $counter -eq $row_size ]] && [[ $row_no -lt $last_row ]] ; then
            echo "rack2$row_no:$row_osds" >> $rack_osds_map
            echo "rack2$row_no:$rack_weight"  >> $bucket_weight_map
            row_osds="" 
            row_no=`expr $row_no + 1`
            counter=0
            rack_weight=0.0
        fi
        row_osds="$row_osds $osd"
        counter=`expr $counter + 1`
        osd_wght=`grep -w $osd $osd_weight_map 2> /dev/null | cut -d ' ' -f 2`
        [ -z "$osd_wght" ] && osd_wght=1.000
        rack_weight=$(echo "$rack_weight+$osd_wght"|bc)
    done
    #last row
    echo "rack2$row_no:$row_osds" >> $rack_osds_map
    echo "rack2$row_no:$rack_weight"  >> $bucket_weight_map

    #generate the crushmap file

    #step 1: header of the crushmap
    cat << EOF > $crushmapsrc
# begin crush map
tunable choose_local_tries 0
tunable choose_local_fallback_tries 0
tunable choose_total_tries 50
tunable chooseleaf_descend_once 1
tunable chooseleaf_vary_r 1
tunable straw_calc_version 1

EOF

    #step 2: devices in the crushmap
    local osd_ids=`ceph-conf -c $ORIGIN_CONF -l osd | egrep -v '^osd$' | sed -e 's/osd\.//' | sort --general-numeric-sort | uniq`
    local osd_num1=`echo $osd_ids | wc -w`
    if [ $osd_num -ne $osd_num1 ] ; then
        log "error: there might be duplicated OSD IDs in ceph.conf ($osd_num != $osd_num1)"
        rm -f $bucket_weight_map $rack_osds_map $crushmapsrc $host_shuf_src $host_shuf_dst
        return 1
    fi

    echo  "# devices" >> $crushmapsrc 
    for id in $osd_ids ; do
        echo "device $id osd.$id" >> $crushmapsrc
    done

    #step 3: types in the crushmap
    cat << EOF >> $crushmapsrc

# types
type 0 osd
type 1 host
type 2 chassis
type 3 rack
type 4 row
type 5 pdu
type 6 pod
type 7 room
type 8 datacenter
type 9 region
type 10 root

EOF

    #step 4: buckets in the crushmap
    local bucket_id=1
    local bak=0

    #step 4b: rack 
    cat $rack_osds_map | while read line ; do
        local rackname=`echo $line | sed -e 's/:.*//'`
        local osd_list=`echo $line | sed -e 's/.*://'`
        echo "rack $rackname {"                   >> $crushmapsrc
        echo "        id -$bucket_id"             >> $crushmapsrc
        echo "        alg straw2"                 >> $crushmapsrc
        echo "        hash 0 # rjenkins1"         >> $crushmapsrc
        for osd in $osd_list ; do
            osd_wght=`grep -w $osd $osd_weight_map 2> /dev/null | cut -d ' ' -f 2`
            [ -z "$osd_wght" ] && osd_wght=1.000
            echo "        item $osd weight $osd_wght" >> $crushmapsrc
        done
        echo "}"                                  >> $crushmapsrc

        bucket_id=`expr $bucket_id + 1`
    done

    #I don't know why the modification on bucket_id in the loop above is lost here; so
    bucket_id=`cat $rack_osds_map | wc -l`
    bucket_id=`expr $bucket_id + 1`

    #step 4b: row 
    local row=0
    while [ $row -lt $row_num ] ; do
        local rowweight=0
        echo "row row$row {"                               >> $crushmapsrc
        echo "        id -$bucket_id"                      >> $crushmapsrc
        echo "        alg straw2"                          >> $crushmapsrc
        echo "        hash 0 # rjenkins1"                  >> $crushmapsrc
        log "row$row:"
        for s in {0..2} ; do
            local wgt=`grep "\<rack$s$row\>" $bucket_weight_map | sed -e 's/.*://'`
            log "    rack$s$row weight: $wgt"
            echo "        item rack$s$row weight $wgt" >> $crushmapsrc
            rowweight=$(echo "$rowweight+$wgt"|bc)
        done
        echo "}"                                           >> $crushmapsrc

        echo "row$row:$rowweight" >> $bucket_weight_map
        bucket_id=`expr $bucket_id + 1`
        row=`expr $row + 1`
    done

    #step 4c: root
    echo "root default {"                 >> $crushmapsrc
    echo "        id -$bucket_id"                      >> $crushmapsrc
    echo "        alg straw2"                          >> $crushmapsrc
    echo "        hash 0 # rjenkins1"                  >> $crushmapsrc

    log "root:"
    row=0
    while [ $row -lt $row_num ] ; do
        local wgt=`grep "\<row$row\>" $bucket_weight_map | sed -e 's/.*://'`
        echo "        item row$row weight $wgt"    >> $crushmapsrc
        log "    row$row weight: $wgt"
        row=`expr $row + 1`
    done
    echo "}"                              >> $crushmapsrc

    #step 5: rules
    cat << EOF >> $crushmapsrc

# rules
rule replicated_ruleset {
        ruleset 0
        type replicated
        min_size 1
        max_size 10
        step take default
        step choose firstn 1 type row 
        step choose firstn 0 type rack
        step choose firstn 1 type osd 
        step emit
}
rule erasure-code {
        ruleset 1
        type erasure
        min_size 3
        max_size 3
        step set_chooseleaf_tries 5
        step set_choose_tries 100
        step take default
        step choose indep 1 type row
        step choose indep 0 type rack
        step choose indep 1 type osd
        step emit
}

# end crush map
EOF

    rm -f $bucket_weight_map $rack_osds_map $host_shuf_src $host_shuf_dst
    return 0
}

function init_system()
{
    #log "generate crushmap if the cluster is too large (more than 3 hosts)"
    #gen_crushmap

    log "start to init ceph cluster -- prepare osds in parallel. start time: `date`"

    local tmpdir=`mktemp -u /tmp/mkfs.ceph.XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX` || return 1

    mkdir -p $tmpdir || return 1

    local crushmapsrc=$RUN_DIR/crushmapsrc
    local crushopt=""
    [ -s $crushmapsrc ] && crushopt="--crushmapsrc $crushmapsrc"

    $CEPH_HOME/mkrados.sh --dir $tmpdir --op do_it_all $crushopt --remote-tool $CEPH_REMOTE_TOOL 

    if [ $? -eq 0 ] ; then
        rm -fr $tmpdir
        log "succeeded to init ceph cluster -- prepare osds in parallel. end time: `date`"
        return 0
    else
        log "failed to init ceph cluster -- prepare osds in parallel. end time: `date`"
        return 1
    fi
    return 0
}

function init_rados_gw()
{
    log "initialize rados gateways"

    local gways=`ceph-conf -c $TARGET_CONF -l client.radosgw | grep client.radosgw`

    if [ -z "$gways" ] ; then
        log "no rados gateway configured in $TARGET_CONF"
        return 1
    fi

    log "rados gateways configured: $gways"

    local tmpdir=`mktemp -d /tmp/ceph.radosgw.XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX`         
    if [ ! -d $tmpdir ] ; then
        log "failed to create temp dir"
        return 1
    fi
    log "created tmp dir $tmpdir"

    for gw in $gways ; do
        log "start to initialize $gw"

        local sock=`ceph-conf -c $TARGET_CONF -n $gw "rgw socket path"`
        if [ -z "$sock" ] ; then
            log "failed to get socket path for $gw from $TARGET_CONF, check the configuration"
            log "skip this gateway: $gw; thus it may fail to start up later"
            continue
        fi
        log "socket path for $gw: $sock"

        local sockfolder=`dirname $sock`

        local kring=`ceph-conf -c $TARGET_CONF -n $gw "keyring"`
        if [ -z "$kring" ] ; then
            log "failed to get keyring for $gw from $TARGET_CONF, check the configuration"
            log "skip this gateway: $gw; thus it may fail to start up later"
            continue
        fi
        log "keyring for $gw: $kring"

        local krfolder=`dirname $kring`
        local krfile=`basename $kring`

        local gwlog=`ceph-conf -c $TARGET_CONF -n $gw "log file"`
        if [ -z "$gwlog" ] ; then
            log "failed to get logfile for $gw from $TARGET_CONF, check the configuration"
            log "skip this gateway: $gw; thus it may fail to start up later"
            continue
        fi
        log "logfile for $gw: $gwlog"

        local logfolder=`dirname $gwlog`

        sudo ceph-authtool --create-keyring $tmpdir/$krfile
        sudo chmod +r $tmpdir/$krfile

        sudo ceph-authtool $tmpdir/$krfile -n $gw --gen-key
        sudo ceph-authtool $tmpdir/$krfile -n $gw --cap osd 'allow rwx' --cap mon 'allow rwx'
        sudo ceph --connect-timeout 5 -k $TARGET_DIR/keyring auth add $gw -i $tmpdir/$krfile
        [ $? -ne 0 ] && log "WARN: failed to add keyring"

        local allhosts=`get_allhosts $ORIGIN_CONF`
        for gwhost in $allhosts ; do
            log "create dir $sockfolder, $krfolder and $logfolderon host $gwhost"
            execute_remote_command $gwhost mkdir -p $sockfolder $krfolder $logfolder 
            if [ $? -ne 0 ] ; then
                log "in init_rados_gw(), failed to create dir $sockfolder on host $gwhost"
                return 1
            fi

            local gwid=`echo $gw | sed -e 's/client.radosgw.gateway//'`
            log "enable client.radosgw.gateway$gwid on $gwhost"
            execute_remote_command $gwhost systemctl enable ceph-radosgw@${gwid}.service
            if [ $? -ne 0 ] ; then
                log "in init_rados_gw(), failed to enable client.radosgw.gateway$gwid on $gwhost"
                return 1
            fi

            log "copy $tmpdir/$krfile to $gwhost:$krfolder/$krfile"
            send_file $gwhost $tmpdir/$krfile $krfolder/$krfile
            if [ $? -ne 0 ] ; then
                log "in init_rados_gw(), failed to copy $krfile to host $gwhost"
                return 1
            fi
        done
        log "fininshed initializing rados gateway $gw"
    done

    rm -fr $tmpdir
    return 0
}

function init_rbd_cephfs()
{
    log "initialize ceph rbd authentication and cephfs used by cloud"

    local admin=`ceph-conf -c $TARGET_CONF -l client.admin`
    local cinder=`ceph-conf -c $TARGET_CONF -l client.cinder`
    local cinder_bakcup=`ceph-conf -c $TARGET_CONF -l client.cinder-backup`
    local glance=`ceph-conf -c $TARGET_CONF -l client.glance`

    [ -z "$admin" ] && log "no client.admin configured in $TARGET_CONF"
    [ -z "$cinder" ] && log "no client.cinder configured in $TARGET_CONF"
    [ -z "$cinder_bakcup" ] && log "no client.cinder-backup configured in $TARGET_CONF"
    [ -z "$glance" ] && log "no client.glance configured in $TARGET_CONF"

    local auth_array=($admin $cinder $cinder_backup $glance)
    local allhosts=`get_allhosts $ORIGIN_CONF`

    for auth_name in ${auth_array[@]} ; do
        log "rbd configured: $auth_name"
        kring=`ceph-conf -c $TARGET_CONF -n $auth_name "keyring"`
        if [ -z "$kring" ] ; then
            log "no keyring configured for $auth_name, skip"
            continue
        fi

        log "keyring for $auth_name: $kring"
        krfolder=`dirname $kring`
        krfile=`basename $kring`
        mkdir -p $krfolder

        sudo echo > $kring
        sudo ceph-authtool --create-keyring $kring
        sudo chmod +r $kring
        sudo ceph-authtool $kring -n $auth_name --gen-key

        case $auth_name in
            client.admin)
                sudo ceph-authtool $kring -n $auth_name --cap osd 'allow *' --cap mon 'allow rw' --cap mds 'allow r'
                ;;
            client.cinder)
                sudo ceph-authtool $kring -n $auth_name --cap mon 'allow r' --cap osd 'allow class-read object_prefix rbd_children, allow rwx pool=volumes, allow rwx pool=vms, allow rx pool=images'
                ;;
            client.cinder-backup)
                sudo ceph-authtool $kring -n $auth_name --cap mon 'allow r' --cap osd 'allow class-read object_prefix rbd_children, allow rwx pool=backups'
                ;;
            client.glance)
                sudo ceph-authtool $kring -n $auth_name --cap mon 'allow r' --cap osd 'allow class-read object_prefix rbd_children, allow rwx pool=images'
                ;;
            *)
                log "invalid param1 ($auth_name), do nothing ..."
                ;;
        esac

        sudo ceph auth add $auth_name -i $kring
        [ $? -ne 0 ] && log "WARN: failed to add keyring of $auth_name"

        # copy this auth file to all other hosts
        for other_host in $allhosts ; do
            if [ "$other_host" != "$HOSTNAME" ] ; then
                log "create dir $krfolder on $other_host"
                execute_remote_command $other_host mkdir -p $krfolder
                if [ $? -ne 0 ] ; then
                    log "in init_rbd_cephfs(), failed to create dir $krfolder on host $other_host"
                    return 1
                fi

                log "copy $krfile to $other_host:$krfolder/$krfile"
                send_file $other_host $kring $krfolder/$krfile
                if [ $? -ne 0 ] ; then
                    log "in init_rbd_cephfs(), failed to copy $kring to host $other_host"
                    return 1
                fi
            fi
        done

    done

    local osd_num=`ceph-conf -c $TARGET_CONF -l osd | egrep -v '^osd$' | wc -l`
    if [ $osd_num -eq 0 ] ; then
        log "configuration error (no osd configured), so skip"
        return 1
    fi

    # reserve some more OSDs, 1.5 times
    osd_num=`expr $osd_num \* 3`
    osd_num=`expr $osd_num / 2`

    replica_num=3
    pg_num=`expr $osd_num \* 100 / $replica_num`
    pg_num=`expr $osd_num / 4`

    # create related pools used by rbd application
    if [ -z "$cinder" ]; then
        log "no client.cinder configured, do nothing"
    else
        log "create pools used by rbd application"
        sudo ceph osd pool create volumes $pg_num
        sudo ceph osd pool create images $pg_num
        sudo ceph osd pool create backups $pg_num
        sudo ceph osd pool create vms $pg_num
    fi

    # create related pools used by cephfs and one cephfs
    local conf_mds=`ceph-conf -c $TARGET_CONF -l mds`
    if [ -z "$conf_mds" ]; then
        log "not configure mds, do nothing"
    else
        log "create pools and one cephfs"
        local cephfs_name="cephfs"
        sudo ceph osd pool create cephfs_data $pg_num
        sudo ceph osd pool create cephfs_metadata $pg_num
        sudo ceph fs new $cephfs_name cephfs_metadata cephfs_data

        # mount the cephfs on all hosts
        for other_host in $allhosts ; do
            local mntdir="/datapool"
            local monaddr=`ceph-conf -c $TARGET_CONF -n mon.a "mon addr"`

            log "umount $mntdir on $other_host"
            execute_remote_command $other_host sudo umount $mntdir

            log "create cephfs mount dir $mntdir on $other_host"
            execute_remote_command $other_host mkdir -p $mntdir
            if [ $? -ne 0 ] ; then
                log "in init_rbd_cephfs(), failed to create cephfs mount dir $mntdir on host $other_host"
                return 1
            fi

            log "mount cephfs on $mntdir on $other_host"
            execute_remote_command $other_host sudo ceph-fuse -m $monaddr $mntdir
            if [ $? -ne 0 ] ; then
                log "in init_rbd_cephfs(), failed to mount cephfs on $mntdir on host $other_host"
                return 1
            fi
        done
    fi

    log "fininshed initializing of ceph rbd authentication used by cloud"
    return 0
}



# 0x00000001 : HEALTH_OK
# 0x00000002 : HEALTH_WARN
# 0x00000004 : HEALTH_ERR
#    at least one of the above 3 bits is set
# 0x00000008 : Recovery in progress, it may be set when HEALTH_OK
# 0x00000010 : clock skew
# 0x00000020 : noscrub set 
# 0x00000040 : nodeep-scrub set 
# 0x00000080 : too many PGs per OSD
# ...
# 0x80000000 : unknown, because "ceph -s" failed
function ceph_stat()
{
  log "check ceph health status"
  local tmpfile=/tmp/cephstat-`date +%s`

  local ret=0

  ceph --connect-timeout 10 -s > $tmpfile 2> /dev/null

  if [ -s $tmpfile ] ; then
      grep "health HEALTH_OK" $tmpfile > /dev/null 2>&1
      if [ $? -eq 0 ] ; then
          ret=$((0x00000001|$ret))
      fi 

      grep "health HEALTH_WARN" $tmpfile > /dev/null 2>&1
      if [ $? -eq 0 ] ; then
          ret=$((0x00000002|$ret))
      fi 

      grep "health HEALTH_ERR" $tmpfile > /dev/null 2>&1
      if [ $? -eq 0 ] ; then
          ret=$((0x00000004|$ret))
      fi 

      grep "recovery io" $tmpfile > /dev/null 2>&1
      if [ $? -eq 0 ] ; then
          ret=$((0x00000008|$ret))
      fi 

      grep "clock skew detected on mon" $tmpfile > /dev/null 2>&1
      if [ $? -eq 0 ] ; then
          ret=$((0x00000010|$ret))
      fi 

      grep "\<noscrub\>" $tmpfile > /dev/null 2>&1
      if [ $? -eq 0 ] ; then
          ret=$((0x00000020|$ret))
      fi 

      grep "\<nodeep-scrub\>" $tmpfile > /dev/null 2>&1
      if [ $? -eq 0 ] ; then
          ret=$((0x00000040|$ret))
      fi 

      grep "too many PGs per OSD" $tmpfile > /dev/null 2>&1
      if [ $? -eq 0 ] ; then
          ret=$((0x00000080|$ret))
      fi
  else
      ret=$((0x80000000|$ret))
  fi

  rm -f $tmpfile
  return $ret
}

function wait_health_ok()
{
    log "wait for ceph cluster to get into HEALTH_OK state ..."

    local timeout=$1 #seconds to wait
    [ -z "$timeout" ] && timeout=600

    local tmpfile=/tmp/cephstat-`date +%s`

    local i=0
    while [ $i -lt $timeout ] ; do

        ceph --connect-timeout 10 -s > $tmpfile 2> /dev/null

        if [ -s $tmpfile ] ; then
            grep "health HEALTH_OK" $tmpfile > /dev/null 2>&1
            if [ $? -eq 0 ] ; then
                rm -f $tmpfile
                log "ceph cluster is in HEALTH_OK state now"
                return 0
            fi

            grep "health HEALTH_WARN" $tmpfile > /dev/null 2>&1
            if [ $? -eq 0 ] ; then
                sed -i -e '1,/health HEALTH_WARN/ d' $tmpfile  #delete lines above "health HEALTH_WARN" (inclusive)
                sed -i -e "/ *monmap e[0-9][0-9]*/,$ d" $tmpfile  #delete lines below "monmap e1: 1 mons at {a=192.168.100.65:6789/0}" (inclusive)

                #only warnings left 
                #we don't care about "too many PGs per OSD"
                sed -i -e '/too many PGs per OSD/ d' $tmpfile 
                #we don't care about "clock skew"
                sed -i -e '/clock skew/ d' $tmpfile 
                #we don't care about "noscrub,nodeep-scrub" 
                sed -i -e '/noscrub.*set/ d' $tmpfile
                sed -i -e '/nodeep-scrub.*set/ d' $tmpfile

                #delete blank lines
                sed -i -e '/^ *$/ d' $tmpfile

                if [ ! -s $tmpfile ] ; then   #tmpfile size is 0, that means there is no other warnings, we treat it as HEALTH_OK
                    rm -f $tmpfile
                    log "ceph cluster is in HEALTH_WARN state, but there is no serious warnings"
                    return 0
                fi
            fi
        else
            log "WARN: cannot check ceph cluster health state this time, ceph -s didn't return"
        fi

        i=`expr $i + 1`
        echo -n "."

        sleep 1
    done

    log "ceph cluster didn't get into HEALTH_OK in $timeout seconds"
    return 1
}

function start_rados_gw()
{
    local create_pool=$1

    log "before create pools for radosgw"
    wait_health_ok
    if [ $? -ne 0 ] ; then
        log "warn: ceph cluster did not get into HEALTH_OK state"
    fi

    log "creating pools for rados gateways ..."

    local osd_num=`ceph-conf -c $TARGET_CONF -l osd | egrep -v '^osd$' | wc -l`
    if [ $osd_num -eq 0 ] ; then
        log "configuration error (no osd configured), so skip starting radosgw."
        return 1
    fi

    # reserve some more OSDs, 1.2 times
    osd_num=`expr $osd_num \* 6`
    osd_num=`expr $osd_num / 5`

    local meta_replica_num=2
    local data_replica_num=2

    #if the hosts are less than 3, set replia num to host num, because if replica num > host num, there 
    #will be a warning  
    local osdhosts=`get_allhosts $ORIGIN_CONF osd`
    local num_osdhosts=`echo $osdhosts | wc -w`
    if [ $num_osdhosts -lt 3 ] ; then
        log "osd host num is $num_osdhosts (<3), so set replica num to $num_osdhosts"
        meta_replica_num=$num_osdhosts
        data_replica_num=$num_osdhosts
    fi
    log "meta_replica_num=$meta_replica_num, data_replica_num=$data_replica_num"

    local meta_pg_num=`expr $osd_num \* 4 / $meta_replica_num`
    local data_pg_num=`expr $osd_num \* 100 / $data_replica_num`

    local rgw_pools=".rgw.root .rgw.control .rgw.gc .rgw.buckets .rgw.buckets.index .rgw.buckets.extra .log .intent-log .usage .users .users.email .users.swift .users.uid default.rgw.control default.rgw.data.root default.rgw.gc default.rgw.log default.rgw.users.uid default.rgw.users.email default.rgw.users.keys default.rgw.meta default.rgw.buckets.index default.rgw.buckets.data"

    if [ $create_pool -eq 1 ] ; then
        for rgw_pool in $rgw_pools ; do

            wait_health_ok

            local pg_num=""
            local replica_num=""
            local expected_objs=0

            if [ "$rgw_pool" == "default.rgw.buckets.data" ] ; then
                pg_num=$data_pg_num
                replica_num=$data_replica_num

                #Yuanguo: we want PGs in pool .rgw.buckets to pre-split, so we need to set expected number of objects;
                #and, we want each PG to have {leaf_folder} leaf folders, so the expected number of objects should be:
                #         pg_num * {leaf_folder} * max-objects-per-folder
                #where 
                #         max-objects-per-folder = abs(filestore_merge_threshold) * filestore_split_multiple * 16
                local leaf_folder=256

                local merge_threshold=`ceph-conf -c $ORIGIN_CONF -n osd.0 "filestore_merge_threshold"`
                if [ -z "$merge_threshold" ] ; then
                    merge_threshold=10
                elif [ $merge_threshold -eq 0 ] ; then
                    merge_threshold=10
                elif [ $merge_threshold -lt 0 ] ; then
                    merge_threshold=`expr 0 - $merge_threshold`
                fi

                local split_multiple=`ceph-conf -c $ORIGIN_CONF -n osd.0 "filestore_split_multiple"`
                if [ -z "$split_multiple" ] ; then
                    split_multiple=2
                elif [ $split_multiple -eq 0 ] ; then
                    split_multiple=2
                fi

                expected_objs=`expr $pg_num \* $leaf_folder \* $merge_threshold \* $split_multiple \* 16`

                log "Pool=.rgw.buckets, merge_threshold=$merge_threshold, split_multiple=$split_multiple"
            else
                pg_num=$meta_pg_num
                replica_num=$meta_replica_num
            fi

            log "create pool $rgw_pool with pg_num=$pg_num, expected_objs=$expected_objs"
            local retry=0
            while [ $retry -lt 10 ] ; do
                result=`ceph --connect-timeout 10 osd pool create $rgw_pool $pg_num $pg_num replicated replicated_ruleset replicated_ruleset $expected_objs 2>&1`

                echo "$result" | grep "pool.*created" 
                if [ $? -eq 0 ] ; then
                    break
                fi

                echo "$result" | grep "pool.*already exists"
                if [ $? -eq 0 ] ; then
                    break
                fi

                log "WARN: failed to create pool $rgw_pool, retry=$retry"

                retry=`expr $retry + 1`
                sleep 1
            done

            sleep 1
            rados lspools | grep -e "^$rgw_pool$"

            if [ $? -ne 0 ] ; then
                log "ERROR: failed to create pool $rgw_pool"
                return 1
            fi

            log "set size of pool $rgw_pool to $replica_num"
            local retry=0
            while [ $retry -lt 10 ] ; do
                result=`ceph --connect-timeout 10 osd pool set $rgw_pool size $replica_num 2>&1`
                echo "$result" | grep "set pool.*size to $replica_num"
                if [ $? -eq 0 ] ; then
                    break
                fi

                retry=`expr $retry + 1`
                sleep 1
            done

            if [ $retry -eq 10 ] ; then
                log "WARN: failed to set size of pool $rgw_pool to $replica_num"
            fi

        done

        #remove pool rbd
        local retry=0
        while [ $retry -lt 10 ] ; do
            rados rmpool rbd rbd --yes-i-really-really-mean-it
            rados lspools | grep "rbd" > /dev/null 2>&1
            if [ $? -ne 0 ] ; then
                log "succeeded to delete pool rbd"
                break
            fi
            retry=`expr $retry + 1`
            sleep 1
        done

        if [ $retry -ge 10 ] ; then
            log "WARN: failed to delete pool rbd"
        fi
    fi

    gways=`ceph-conf -c $TARGET_CONF -l client.radosgw | grep client.radosgw`

    if [ -z "$gways" ] ; then
        log "no rados gateway configured in $TARGET_CONF"
        return 0
    fi

    log "before starting radosgw"

    wait_health_ok
    if [ $? -ne 0 ] ; then
        log "skip starting radosgw because ceph cluster is not in HEALTH_OK state"
        return 1
    fi

    local allhosts=`get_allhosts $ORIGIN_CONF`

    log "starting rados gateways: $gways on every host: $allhosts"

    for gwhost in $allhosts ; do
        execute_remote_command $gwhost systemctl start ceph-radosgw.target
        if [ $? -ne 0 ] ; then
            log "in start_rados_gw(), failed to start up radosgw on $gwhost"
            return 1
        fi
    done

    return 0
}

function start_rados_gw_ecpool()
{
    local create_pool=$1

    log "before create pools for radosgw"
    wait_health_ok
    if [ $? -ne 0 ] ; then
        log "warn: ceph cluster did not get into HEALTH_OK state"
    fi

    log "creating pools for rados gateways ..."

    local osd_num=`ceph-conf -c $TARGET_CONF -l osd | egrep -v '^osd$' | wc -l`
    if [ $osd_num -eq 0 ] ; then
        log "configuration error (no osd configured), so skip starting radosgw."
        return 1
    fi

    # reserve some more OSDs, 1.2 times
    osd_num=`expr $osd_num \* 6`
    osd_num=`expr $osd_num / 5`

    local meta_replica_num=3
    local data_replica_num=2

    #if the hosts are less than 3, set replia num to host num, because if replica num > host num, there 
    #will be a warning  
    local osdhosts=`get_allhosts $ORIGIN_CONF osd`
    local num_osdhosts=`echo $osdhosts | wc -w`
    if [ $num_osdhosts -lt 3 ] ; then
        log "Cannot create ec pool, because number of hosts is less than 3"
        exit 1
    fi
    log "meta_replica_num=$meta_replica_num, data_replica_num=$data_replica_num"

    local meta_pg_num=`expr $osd_num \* 4 / $meta_replica_num`
    local data_pg_num=`expr $osd_num \* 100 / $data_replica_num`

    local rgw_pools=".rgw.root .rgw.control .rgw.gc .rgw.buckets .rgw.buckets.index .rgw.buckets.extra .log .intent-log .usage .users .users.email .users.swift .users.uid default.rgw.control default.rgw.data.root default.rgw.gc default.rgw.log default.rgw.users.uid default.rgw.users.email default.rgw.users.keys default.rgw.meta default.rgw.buckets.index default.rgw.buckets.data"


    if [ $create_pool -eq 1 ] ; then
        ceph --connect-timeout 10 osd pool create dumbecpool 16 16 erasure
        for rgw_pool in $rgw_pools ; do

            wait_health_ok

            local pg_num=""
            local replica_num=""
            local expected_objs=0

            local pool_type=

            if [ "$rgw_pool" == "default.rgw.buckets.data" ] ; then
                pg_num=$data_pg_num
                replica_num=$data_replica_num

                #Yuanguo: we want PGs in pool .rgw.buckets to pre-split, so we need to set expected number of objects;
                #and, we want each PG to have {leaf_folder} leaf folders, so the expected number of objects should be:
                #         pg_num * {leaf_folder} * max-objects-per-folder
                #where 
                #         max-objects-per-folder = abs(filestore_merge_threshold) * filestore_split_multiple * 16
                local leaf_folder=256

                local merge_threshold=`ceph-conf -c $ORIGIN_CONF -n osd.0 "filestore_merge_threshold"`
                if [ -z "$merge_threshold" ] ; then
                    merge_threshold=10
                elif [ $merge_threshold -eq 0 ] ; then
                    merge_threshold=10
                elif [ $merge_threshold -lt 0 ] ; then
                    merge_threshold=`expr 0 - $merge_threshold`
                fi

                local split_multiple=`ceph-conf -c $ORIGIN_CONF -n osd.0 "filestore_split_multiple"`
                if [ -z "$split_multiple" ] ; then
                    split_multiple=2
                elif [ $split_multiple -eq 0 ] ; then
                    split_multiple=2
                fi

                expected_objs=`expr $pg_num \* $leaf_folder \* $merge_threshold \* $split_multiple \* 16`

                log "Pool=.rgw.buckets, merge_threshold=$merge_threshold, split_multiple=$split_multiple"

                pool_type="erasure default erasure-code $expected_objs"
            else
                pg_num=$meta_pg_num
                replica_num=$meta_replica_num

                pool_type="replicated replicated_ruleset replicated_ruleset $expected_objs"
            fi

            log "create pool $rgw_pool with pg_num=$pg_num, expected_objs=$expected_objs"
            local retry=0
            while [ $retry -lt 10 ] ; do
                log "ceph --connect-timeout 10 osd pool create $rgw_pool $pg_num $pg_num $pool_type 2>&1"
                result=`ceph --connect-timeout 10 osd pool create $rgw_pool $pg_num $pg_num $pool_type 2>&1`

                echo "$result" | grep "pool.*created" 
                if [ $? -eq 0 ] ; then
                    break
                fi

                echo "$result" | grep "pool.*already exists"
                if [ $? -eq 0 ] ; then
                    break
                fi

                log "WARN: failed to create pool $rgw_pool, retry=$retry"

                retry=`expr $retry + 1`
                sleep 1
            done

            sleep 1
            rados lspools | grep -e "^$rgw_pool$"

            if [ $? -ne 0 ] ; then
                log "ERROR: failed to create pool $rgw_pool"
                return 1
            fi

            log "set size of pool $rgw_pool to $replica_num"
            local retry=0
            while [ $retry -lt 10 ] ; do
                result=`ceph --connect-timeout 10 osd pool set $rgw_pool size $replica_num 2>&1`
                echo "$result" | grep "set pool.*size to $replica_num"
                if [ $? -eq 0 ] ; then
                    break
                fi

                retry=`expr $retry + 1`
                sleep 1
            done

            if [ $retry -eq 10 ] ; then
                log "WARN: failed to set size of pool $rgw_pool to $replica_num"
            fi

        done

        #remove pool rbd
        local retry=0
        while [ $retry -lt 10 ] ; do
            rados rmpool rbd rbd --yes-i-really-really-mean-it
            rados lspools | grep "rbd" > /dev/null 2>&1
            if [ $? -ne 0 ] ; then
                log "succeeded to delete pool rbd"
                break
            fi
            retry=`expr $retry + 1`
            sleep 1
        done

        if [ $retry -ge 10 ] ; then
            log "WARN: failed to delete pool rbd"
        fi

        #remove pool dumbecpool
        local retry=0
        while [ $retry -lt 10 ] ; do
            rados rmpool dumbecpool dumbecpool --yes-i-really-really-mean-it
            rados lspools | grep "dumbecpool" > /dev/null 2>&1
            if [ $? -ne 0 ] ; then
                log "succeeded to delete pool dumbecpool"
                break
            fi
            retry=`expr $retry + 1`
            sleep 1
        done

        if [ $retry -ge 10 ] ; then
            log "WARN: failed to delete pool dumbecpool"
        fi
    fi

    gways=`ceph-conf -c $TARGET_CONF -l client.radosgw | grep client.radosgw`

    if [ -z "$gways" ] ; then
        log "no rados gateway configured in $TARGET_CONF"
        return 0
    fi

    log "before starting radosgw"

    wait_health_ok
    if [ $? -ne 0 ] ; then
        log "skip starting radosgw because ceph cluster is not in HEALTH_OK state"
        return 1
    fi

    local allhosts=`get_allhosts $ORIGIN_CONF`

    log "starting rados gateways: $gways on every host: $allhosts"

    for gwhost in $allhosts ; do
        execute_remote_command $gwhost systemctl start ceph-radosgw.target
        if [ $? -ne 0 ] ; then
            log "in start_rados_gw(), failed to start up radosgw on $gwhost"
            return 1
        fi
    done

    return 0
}

function start_ceph_daemon()
{
    local daemon=$1
    local allhosts=`get_allhosts $ORIGIN_CONF $1`
    allhosts=`echo $allhosts | tr -s ' ' | sed -e 's/ /,/g'`

    if [ -z "$allhosts" ] ; then
        log "no $daemon configured, thus skip starting $daemon"
        return 0
    fi

    log "start $daemon on hosts:" $allhosts

    execute_remote_command_batch $allhosts systemctl start ceph-${daemon}.target

    if [ $? -ne 0 ] ; then
        log "in start_ceph_daemon(), failed to start $daemon on some host"
        return 1
    else
        log "in start_ceph_daemon(), succeeded to start $daemon on all hosts: $allhosts"
    fi

    return 0;
}

function start_ceph()
{
    local logf=`ceph-conf -c $TARGET_CONF "log file"`
    local logd=`dirname $logf`
    if [ -n "$logd" ] ; then 
        log "create dir $logd for log"
        mkdir -p $logd
    fi

    local pidf=`ceph-conf -c $TARGET_CONF "pid file"`
    local pidd=`dirname $pidf`
    if [ -n "$pidd" ] ; then 
        log "create dir $pidd for pid"
        mkdir -p $pidd
    fi

    start_ceph_daemon mon
    if [ $? -ne 0 ] ; then
        log "failed to start ceph mon"
        return 1
    fi

    start_ceph_daemon osd 
    if [ $? -ne 0 ] ; then
        log "failed to start ceph osd"
        return 1
    fi

    start_ceph_daemon mds 
    if [ $? -ne 0 ] ; then
        log "failed to start ceph mds"
        return 1
    fi

    return 0
}

function stop_rados_gw()
{
    log "stop rados gateways"

    local allhosts=`get_allhosts $ORIGIN_CONF`
    for host in $allhosts ; do
        execute_remote_command $host killall radosgw
        #trick, killall reports error if there is no process, so cannot check
    done
    return 0
}

function stop_ceph()
{
    local allhosts=`get_allhosts $ORIGIN_CONF`
    allhosts=`echo $allhosts | tr -s ' ' | sed -e 's/ /,/g'`

    log "stop ceph on every host: $allhosts"
    execute_remote_command_batch $allhosts systemctl stop ceph.target
    if [ $? -ne 0 ] ; then
        log "in stop_ceph(), failed to stop ceph on some host"
        return 1
    fi

    sleep 30

    log "kill suspending ceph processes (if there is any) on every host: $allhosts"
    execute_remote_command_batch $allhosts $CEPH_HOME/cleanup.sh $LOG_FILE
    if [ $? -ne 0 ] ; then
        log "in stop_ceph(), failed to suspending ceph processes on some host"
        return 1
    fi

    return 0
}

function copy_keyring()
{
    allhosts=`get_allhosts $ORIGIN_CONF`

    log "copy $TARGET_DIR/keyring to other nodes"

    for host in $allhosts ; do
        if [ "$host" != "$HOSTNAME" ] ; then
            log "copy $TARGET_DIR/keyring to node $host"
            send_file $host $TARGET_DIR/keyring $TARGET_DIR/keyring
            if [ $? -ne 0 ] ; then
                log "in copy_keyring(), failed to copy keyring to $host"
                return 1
            fi
        fi
    done

    return 0
}

function dispatch_conf()
{
    local baktime=$1
    cp -f $ORIGIN_CONF /tmp/ceph.conf.tmp

    local allhosts=`get_allhosts $ORIGIN_CONF`

    for host in $allhosts ; do
        sed -i -e '/client.radosgw.gateway/,$s/^[[:space:]]*rgw[[:space:]][[:space:]]*dns[[:space:]][[:space:]]*name[[:space:]]*=.*$/        rgw dns name = '"$host"'/' /tmp/ceph.conf.tmp
        sed -i -e '/client.radosgw.gateway/,$s/^[[:space:]]*host[[:space:]]*=.*$/        host = '"$host"'/' /tmp/ceph.conf.tmp 

        execute_remote_command $host mkdir -p $TARGET_DIR 
        if [ $? -ne 0 ] ; then
            log "in dispatch_conf(), failed to create dir $TARGET_DIR on $host"
            return 1
        fi

        file_dir_exist $host $TARGET_CONF 0
        if [ $? -ne 1 ] ; then
            execute_remote_command $host cp -f $TARGET_CONF $TARGET_CONF.$baktime.bak
        fi

        send_file $host /tmp/ceph.conf.tmp $TARGET_CONF
        if [ $? -ne 0 ] ; then
            log "in dispatch_conf(), failed to copy conf to $host"
            return 1
        fi
    done

    rm -f /tmp/ceph.conf.tmp

    return 0
}

function recover_conf_file()
{
    local baktime=$1
    local allhosts=`get_allhosts $ORIGIN_CONF`
    for host in $allhosts ; do
        file_dir_exist $host $TARGET_CONF.$baktime.bak 0
        if [ $? -ne 1 ] ; then
            execute_remote_command $host cp -f $TARGET_CONF.$baktime.bak $TARGET_CONF
            if [ $? -ne 0 ] ; then
                log "in recover_conf_file(), failed to recover conf on $host"
                return 1
            fi
        else
            log "warn: in recover_conf_file(), $TARGET_CONF.$baktime.bak doesn't exist"
        fi
    done

    return 0
}

function back_conf_file()
{
    local allhosts=`get_allhosts $ORIGIN_CONF`
    local backupdir=`dirname $BACKUP_CONF`
    for host in $allhosts ; do
        execute_remote_command $host mkdir -p $backupdir
        if [ $? -ne 0 ] ; then
            log "in back_conf_file(), failed to create dir $backupdir on $host"
            return 1
        fi

        send_file $host $ORIGIN_CONF $BACKUP_CONF 
        if [ $? -ne 0 ] ; then
            log "in back_conf_file(), failed to copy $ORIGIN_CONF on $host"
            return 1
        fi
    done

    return 0
}

function check_ceph_stat()
{
    log "check ceph health status ..."

    local cnt=0
    while [ $cnt -lt 5 ] ; do
        local health=`ceph --connect-timeout 5 -s 2> /dev/null | grep health`

        echo "$health" | grep HEALTH_OK > /dev/null
        if [ $? -eq 0 ] ; then
            log "OK: ceph started successfully and in ok state: $health" 
            return 0 
        fi

        echo "$health" | grep HEALTH_WARN > /dev/null
        if [ $? -eq 0 ] ; then
            log "WARN: ceph started successfully but in warn state: $health"
            return 0
        fi

        if [ -z "$health" ] ; then
            log "ceph health status is empty. retry=$cnt"
        else
            log "ceph health status is $health. retry=$cnt"
        fi

        cnt=`expr $cnt + 1`
        sleep 3
    done

    log "ERROR: ceph failed to start" 
    return 1
}


function reload_nginx()
{
    log "reload nginx..."

    local ngx_home=/usr/local/nginx-1.6.0
    local ngx_conf=$ngx_home/conf/nginx.conf
    local rgw_conf=$ngx_home/conf/rgw.conf

    if [ ! -f $ngx_conf ] ; then
        log "failed to reload nginx because $ngx_conf does NOT exist"  
        return 1
    fi 

    local gways=`ceph-conf -c $TARGET_CONF -l client.radosgw | grep client.radosgw`
    local rgw_sock=""
    for gw in $gways ; do
        rgw_sock=`ceph-conf -c $TARGET_CONF -n $gw "rgw socket path"`
        [ -n "$rgw_sock" ] && break
    done
    [ -z "$rgw_sock" ] && rgw_sock=/var/run/ceph/ceph-client.radosgw.gateway0.asok

    rm -f $rgw_conf

    cat << EOF > $rgw_conf 
server {
    listen 36666;

    server_name $HOSTNAME;

    client_max_body_size 10g;
    # This is the important option that tengine has, but nginx does not
    #fastcgi_request_buffering off;

    location / {
            root   /usr/share/nginx/html;
            index  index.html index.htm;
            fastcgi_pass_header     Authorization;
            fastcgi_pass_request_headers on;

            if (\$request_method  = PUT ) {
              rewrite ^ /PUT\$request_uri;
            }
            include fastcgi_params;

            fastcgi_pass unix:$rgw_sock;
    }

    location /PUT/ {
            internal;
            fastcgi_pass_header     Authorization;
            fastcgi_pass_request_headers on;

            include fastcgi_params;
            fastcgi_param  CONTENT_LENGTH   \$content_length;
            fastcgi_pass unix:$rgw_sock;
    }
}
EOF

    grep rgw.conf $ngx_conf > /dev/null 2>&1
    if [ $? -ne 0 ] ; then
        sed -i -e "1,/server/{
            /server/i include $rgw_conf;
        }" $ngx_conf
    fi

    $ngx_home/sbin/nginx -s quit > /dev/null 2>&1
    sleep 2

    local retry=0
    local ngxpid=""
    while [ $retry -lt 5 ] ; do
        local info=`$ngx_home/sbin/nginx 2>&1`

        #return error if port conflict
        echo "$info" | grep "Address already in use" > /dev/null 2>&1
        if [ $? -eq 0 ] ; then
            log "Failed to reload nginx because of port confliction:"
            log "$info"
            return 1
        fi

        ngxpid=`ps -ef | grep "master process.*$ngx_home/sbin/nginx" | grep -v grep | tr -s ' ' | cut -d ' ' -f 2`
        if [ -n "$ngxpid" ] ; then
            log "nginx is started with pid $ngxpid"
            break
        fi

        #ngxpid is empty, retry
        log "failed to start nginx, retry=$retry"
        retry=`expr $retry + 1`
        sleep 2
    done

    if [ -z "$ngxpid" ] ; then
        log "failed to restart nginx"
        return 1
    fi

    netstat -anpl | grep "^tcp.*\<36666\>.*LISTEN.*$ngxpid\/nginx" > /dev/null 2>&1
    if [ $? -ne 0 ] ; then
        log "nginx is not listening on port 36666"
        return 1
    fi

    log "nginx is reloaded successfully with pid $ngxpid and it's listening on port 36666"
    return 0
}

function config_hpcc()
{
    log "config hpcc..."

    local rgw_conf=/usr/local/nginx-1.6.0/conf/rgw.conf

    local rgw_sock=""
    for sock in `grep "fastcgi_pass unix:" $rgw_conf | sed -e 's/fastcgi_pass unix://' -e 's/;.*$//'` ; do
        if [ -n "$sock" ] ; then
            rgw_sock=$sock
            break
        fi
    done

    if [ -z "$rgw_sock" ] ; then
        log "failed to config hpcc, because cannot find rgw sock from $rgw_conf"
        return 1
    fi

    if [ ! -S $rgw_sock ] ; then
        log "failed to config hpcc, because $rgw_sock is not a unix socket file"
        return 1
    fi

    local access_key=`radosgw-admin user create --uid=cephtest --display-name="ceph test" --email=ceph.test@chinacache.com 2>/dev/null | grep access_key | cut -d '"' -f 4`
    local secret_key=`radosgw-admin user create --uid=cephtest --display-name="ceph test" --email=ceph.test@chinacache.com 2>/dev/null | grep secret_key | cut -d '"' -f 4`

    [ -z "$access_key" ] && log "failed to create a user for radosgw-admin" && return 1

    #'/' in seceret key will cause some error, so we re-create the user if secret key contains '/'
    while [ `echo $secret_key | egrep "/"` ] ; do
        radosgw-admin user rm --uid=cephtest

        access_key=`radosgw-admin user create --uid=cephtest --display-name="ceph test" --email=ceph.test@chinacache.com 2>/dev/null | grep access_key | cut -d '"' -f 4`
        secret_key=`radosgw-admin user create --uid=cephtest --display-name="ceph test" --email=ceph.test@chinacache.com 2>/dev/null | grep secret_key | cut -d '"' -f 4`
    done

    local s3curl=/root/.s3curl

    cat << EOF1 >  $s3curl
%awsSecretAccessKeys = (
# personal account
admin => {
    id => '$access_key',
    key => '$secret_key',
},
);
EOF1

    chmod 600 $s3curl
    [ $? -ne 0 ] && log "failed to change mode for $s3curl" && return 1

    /usr/bin/radosgw-admin user modify --max-buckets=5000 --uid=cephtest

    log "create 1024 buckets ..."
    $CEPH_HOME/create_bucket.sh 1024

    if [ $? -eq 0 ] ; then
        return 0
    fi

    log "failed to create 1024 buckets"
    return 1
}

#function enable_log_rotate()
#{
#    log "enable log rotate on every host"
#    local logrotate=/etc/logrotate.d/ceph
#
#    cat << EOF3 > $logrotate
#/data/proclog/ceph/*.log {
#    rotate 14
#    daily
#    compress
#    sharedscripts
#    postrotate
#        #re-open logs for osd, mds and mon; see /etc/init.d/ceph
#        service ceph reload >/dev/null
#
#        #re-open logs for radosgw;
#        for rgw_pid in \`ps -ef | grep radosgw | grep -v grep | awk -F ' ' '{print \$2}'\` ; do
#            kill -1 \$rgw_pid
#        done
#    endscript
#    missingok
#    notifempty
#}
#EOF3
#
#    local allhosts=`get_allhosts $ORIGIN_CONF`
#
#    for host in $allhosts ; do
#        if [ "$host" != "$HOSTNAME" ] ; then
#            send_file $host $logrotate $logrotate
#            if [ $? -ne 0 ] ; then
#                log "in enable_log_rotate(), failed to enable logrotate on host $host"
#                return 1
#            fi
#        fi
#    done
#
#    return 0
#}

function start_on_reboot()
{
    local rcfile=/etc/rc.d/rc.local

    if [ -x $CEPH_HOME/ceph-start-on-reboot.sh ] ; then

        sed -i -e '/ceph-start-on-reboot/d' $rcfile
        echo "$CEPH_HOME/ceph-start-on-reboot.sh &" >> $rcfile 

        local allhosts=`get_allhosts $ORIGIN_CONF`

        for host in $allhosts ; do
            if [ "$host" != "$HOSTNAME" ] ; then
                if [ "$CEPH_REMOTE_TOOL" == "truck" ] ; then
                    python /opt/truck/remote.py call -H $host -c "sed -i -e '/ceph-start-on-reboot/d' $rcfile" -d -t 60
                    python /opt/truck/remote.py call -H $host -c "echo $CEPH_HOME/'ceph-start-on-reboot.sh &' >> $rcfile" -d -t 60
                else
                    ssh $SSH_PORT_OPT $host sed -i -e '/ceph-start-on-reboot/d' $rcfile
                    ssh $SSH_PORT_OPT $host "echo $CEPH_HOME/'ceph-start-on-reboot.sh &' >> $rcfile"
                fi
            fi
        done
    fi
}

function check_conf()
{
    local fconf=$1
    [ -z "$fconf" ] && return 0

    local tmpoutf=/tmp/check-conf-`date +%s`

    ceph-conf -c $fconf -l osd 2>&1 | grep -i -e "warning" -e "error" > $tmpoutf 2>&1
    if [ $? -eq 0 ] ; then
        log "there are syntax errors in $fconf"
        cat $tmpoutf
        cat $tmpoutf >> $LOG_FILE
        rm -f $tmpoutf
        return 1
    fi

    rm -f $tmpoutf
    return 0
}

function overwrite_conf()
{
    local destfile=$ORIGIN_CONF
    local overfile=$CEPH_HOME/ceph.overwrite.conf

    log "start to overwrite ceph.conf"

    if [ ! -f $destfile ] ; then
        log "skip overwriting ceph.conf because it is not found"
        return 0
    fi
    dos2unix $destfile

    if [ ! -f $overfile ] ; then
        log "skip overwriting ceph.conf because ceph.overwrite.conf is not found"
        return 0
    fi
    dos2unix $overfile

    local section=""
    local dumb_name=""
    cat $overfile | while read line ; do
        echo $line | grep [0-9a-zA-Z]  > /dev/null 2>&1 || continue  #skip line that doesn't contain alpha and digit;
        echo $line | grep -P "^[ \t]*;" > /dev/null 2>&1 && continue #skip the comment line;

        # we only care about configurations in section: global, mon, osd, mds, client.admin, client.radosgw.gateway0 and client.radosgw.gateway1. Others
        # configurations in sections like [osd.2], [mon.a] and etc are ignored.

        echo $line | grep -P "^[ \t]*\[[\t]*global[ \t]*\][ \t]*" > /dev/null 2>&1
        if [ $? -eq 0 ] ; then
            log "start to merge global section..."
            section="global"
            dumb_name=""
            continue
        fi

        echo $line | grep -P "^[ \t]*\[[\t]*mon[ \t]*\][ \t]*" > /dev/null 2>&1
        if [ $? -eq 0 ] ; then
            log "start to merge mon section..."
            section="mon"
            dumb_name="-n mon.x"
            continue
        fi

        echo $line | grep -P "^[ \t]*\[[\t]*osd[ \t]*\][ \t]*" > /dev/null 2>&1
        if [ $? -eq 0 ] ; then
            log "start to merge osd section..."
            section="osd"
            dumb_name="-n osd.1000"
            continue
        fi

        echo $line | grep -P "^[ \t]*\[[\t]*mds[ \t]*\][ \t]*" > /dev/null 2>&1
        if [ $? -eq 0 ] ; then
            log "start to merge mds section..."
            section="mds"
            dumb_name="-n mds.1000"
            continue
        fi

        echo $line | grep -P "^[ \t]*\[[\t]*client\.admin[ \t]*\][ \t]*" > /dev/null 2>&1
        if [ $? -eq 0 ] ; then
            log "start to merge client.admin section..."
            section="client.admin"
            dumb_name="-n client.admin"
            continue
        fi

        echo $line | grep -P "^[ \t]*\[[\t]*client\.radosgw\.gateway0[ \t]*\][ \t]*" > /dev/null 2>&1
        if [ $? -eq 0 ] ; then
            log "start to merge client.radosgw.gateway0 section..."
            section="client.radosgw.gateway0"
            dumb_name="-n client.radosgw.gateway0"
            continue
        fi

        echo $line | grep -P "^[ \t]*\[[\t]*client\.radosgw\.gateway1[ \t]*\][ \t]*" > /dev/null 2>&1
        if [ $? -eq 0 ] ; then
            log "start to merge client.radosgw.gateway1 section..."
            section="client.radosgw.gateway1"
            dumb_name="-n client.radosgw.gateway1"
            continue
        fi

        echo $line | grep -P "^[ \t]*\[[\t]*[a-zA-Z0-9\.]*[ \t]*\][ \t]*" > /dev/null 2>&1  #other sections, such as [osd.2], [mon.a] and the like
        if [ $? -eq 0 ] ; then
            section=`echo $line | sed -e "s/^[ \t]*\[[\t]*\([a-zA-Z0-9\.]*\)[ \t]*\][ \t]*/\1/"` 
            log "will ignore configurations in section $section"
            continue
        fi

        if [[ "$section" != "global" ]] && [[ "$section" != "mon" ]] && [[ "$section" != "osd" ]] && [[ "$section" != "mds" ]] && [[ "$section" != "client.admin" ]] && [[ "$section" != "client.radosgw.gateway0" ]] && [[ "$section" != "client.radosgw.gateway1" ]] ; then
            log "skip line $line in section $section"
            continue
        fi 

        echo $line | grep "=" > /dev/null 2>&1
        if [ $? -ne 0 ] ; then
            log "invalid ceph.overwrite.conf file: $line should be in key=value format"
            return 1
        fi

        local tmpline=`echo $line | sed -e 's/^[ \t][ \t]*//' -e 's/[ \t]*=[ \t]*/=/'`
        local key=`echo $tmpline | cut -d '=' -f 1`
        local value=`echo $tmpline | cut -d '=' -f 2-`

        local over_value=`ceph-conf -c $overfile $dumb_name "$key"`
        local dest_value=`ceph-conf -c $destfile $dumb_name "$key"`

        if [ "$over_value" == "$dest_value" ] ; then
            log "skip $key because it has the expected value"
            continue
        fi

        if [ -z "$dest_value" ] ; then
            log "add config $line because it's absent in $destfile"
            sed -i -e "/^[ \t]*\[[\t]*$section[ \t]*\][ \t]*/a $line" $destfile
            continue
        fi


        if [ "$over_value" != "$dest_value" ] ; then
            # we will try to find the key in 3 formats: key, key1 and key2 
            local key1=`echo $key | sed -e 's/_/ /g'`
            local key2=`echo $key1 | tr -s ' ' | sed -e 's/ /_/g'`

            #test first, to see if the replace will succeed or not in given section (start from [$section], end with [.*]). If tmp1 is empty
            #the replace fails, otherwise succeeds;
            local tmp=`sed -n -e "/^[ \t]*\[[ \t]*$section[ \t]*\]/,/^[ \t]*\[.*\]/ s|^\([ \t]*$key[ \t]*=\).*|\1 $value|p" $destfile`
            if [ -n "$tmp" ] ; then
                sed -i -e "/^[ \t]*\[[ \t]*$section[ \t]*\]/,/^[ \t]*\[.*\]/ s|^\([ \t]*$key[ \t]*=\).*|\1 $value|" $destfile
                continue
            fi

            local tmp1=`sed -n -e "/^[ \t]*\[[ \t]*$section[ \t]*\]/,/^[ \t]*\[.*\]/ s|^\([ \t]*$key1[ \t]*=\).*|\1 $value|p" $destfile`
            if [ -n "$tmp1" ] ; then
                sed -i -e "/^[ \t]*\[[ \t]*$section[ \t]*\]/,/^[ \t]*\[.*\]/ s|^\([ \t]*$key1[ \t]*=\).*|\1 $value|" $destfile
                continue
            fi

            local tmp2=`sed -n -e "/^[ \t]*\[[ \t]*$section[ \t]*\]/,/^[ \t]*\[.*\]/ s|^\([ \t]*$key2[ \t]*=\).*|\1 $value|p" $destfile`
            if [ -n "$tmp2" ] ; then
                sed -i -e "/^[ \t]*\[[ \t]*$section[ \t]*\]/,/^[ \t]*\[.*\]/ s|^\([ \t]*$key2[ \t]*=\).*|\1 $value|" $destfile
                continue
            fi
            log "WARN: failed to overwrite $key"
        fi 
    done

    rm -f $osd_weight_map
    for osd in `ceph-conf -c $ORIGIN_CONF -l osd | grep -v "^osd$" | sort` ; do
        local oweight=`ceph-conf -c $ORIGIN_CONF -n $osd "weight"`
        if [ -n "$oweight" ] ; then
            echo "$osd $oweight" >> $osd_weight_map
        fi
    done

    sed -i -e '/weight/d' $ORIGIN_CONF

    log "finished to overwrite ceph.conf"
    return 0
}

function re_deploy()
{
    log "this is the master node, thus deploy ceph cluster by this node"

    check_conf $ORIGIN_CONF || return 1

    #TODO: stop monitor scripts which start up ceph processes automatically  

    #overwrite_conf || return 1

    local baktime=`date +%Y-%m-%d-%H-%M`
    dispatch_conf $baktime || return 1

    #TODO: check kernel version

    stop_rados_gw || return 1
    stop_ceph || return 1
    rm_legacy_data || return 1

    init_system || return 1

    start_ceph || return 1

    back_conf_file || return 1  #back up the conf file, so we can get the diff when apply changes (function apply_conf_change)

    init_rados_gw

    copy_keyring || return 1

    start_rados_gw 1


    #TODO: start monitor scripts which start up ceph processes automatically  

    check_ceph_stat || return 1

    return 0 
}

function rm_mon()
{
    local mon=$1
    log "start to remove monitor: $mon"

    local mon_id=`echo $mon | cut -c 4- | sed 's/^\\.//'`
    local host=`ceph-conf -c $BACKUP_CONF -n $mon "host"`

    log "remove $mon (id:$mon_id) on host $host"
    local c=0
    while [ $c -lt 10 ] ; do
        execute_remote_command $host ceph --connect-timeout 20 mon remove $mon_id 
        if [ $? -eq 0 ] ; then
            log "$mon (id:$mon_id) is removed"
            break
        fi
        c=`expr $c + 1`
        sleep 1
    done
    if [ $c -ge 10 ] ; then
        log "in rm_mon(), failed to remove $mon (id:$mon_id) on host $host"
        return 1
    fi

    rm_mon_data "$mon" "$BACKUP_CONF" || return 1 

    log "disable $mon (id:$mon_id) on host $host"
    execute_remote_command $host systemctl disable ceph-mon@${mon_id}.service

    return 0
}

function add_mon()
{
    local mon=$1
    log "start to add monitor: $mon"

    local mon_id=`echo $mon | cut -c 4- | sed 's/^\\.//'`
    local host=`ceph-conf -c $ORIGIN_CONF -n $mon "host"`
    local mon_addr=`ceph-conf -c $ORIGIN_CONF -n $mon "mon addr"`

    local mon_data=`ceph-conf -c $ORIGIN_CONF -n $mon "mon data"`
    local mon_dir=`dirname $mon_data`

    local auth_req=`ceph-conf -c $ORIGIN_CONF "auth cluster required"`

    log "monitor $mon is on node $host"
    execute_remote_command $host rm -fr $mon_data || return 1
    execute_remote_command $host mkdir -p $mon_dir || return 1

    local rdir=`mktemp -u /tmp/add_mon.XXXXXXXXXXX` || exit 1
    execute_remote_command $host mkdir -p $rdir || return 1

    local c=0
    local keyring_opt=""
    if [ "$auth_req" != "none" ] ; then
        log "get monitor keyring"
        c=0
        while [ $c -lt 10 ] ; do
            execute_remote_command $host ceph --connect-timeout 20 auth get mon. -o $rdir/mon_key
            file_dir_exist $host $rdir/mon_key 0
            if [ $? -eq 0 ] ; then
                keyring_opt="--keyring $rdir/mon_key"
                break
            fi
            c=`expr $c + 1`
            sleep 1
        done
        if [ $c -ge 10 ] ; then
            log "failed to get monitor keyring"
            return 1
        fi
    fi

    log "get monitor map"
    c=0
    while [ $c -lt 10 ] ; do
        execute_remote_command $host ceph --connect-timeout 20 mon getmap -o $rdir/mon_map
        file_dir_exist $host $rdir/mon_map 0
        [ $? -eq 0 ] && break
        c=`expr $c + 1`
        sleep 1
    done
    if [ $c -ge 10 ] ; then
        log "failed to get monitor map"
        return 1
    fi

    log "add the new monitor $mon to monitor map"
    c=0
    while [ $c -lt 10 ] ; do
        execute_remote_command $host monmaptool $rdir/mon_map --add $mon_id $mon_addr
        [ $? -eq 0 ] && break
        c=`expr $c + 1`
        sleep 1
    done
    if [ $c -ge 10 ] ; then
        log "failed to add new monitor $mon to monitor map"
        return 1
    fi

    log "create monfs for the new monitor $mon"
    c=0
    while [ $c -lt 10 ] ; do
        execute_remote_command $host ceph-mon -i $mon_id --mkfs --monmap $rdir/mon_map $keyring_opt
        file_dir_exist $host $mon_data/store.db 1  
        [ $? -eq 0 ] && break
        c=`expr $c + 1`
        sleep 1
    done
    if [ $c -ge 10 ] ; then
        log "failed to create monfs for the new monitor $mon"
        return 1
    fi

    log "add the monitor $mon to cluster and start it"
    c=0
    while [ $c -lt 5 ] ; do
        log "add the monitor $mon to cluster"
        #it often reports "Error ETIMEDOUT", but it succeeded actually
        execute_remote_command $host ceph --connect-timeout 120 mon add $mon_id $mon_addr 

        log "start the new monitor $mon"
        execute_remote_command $host systemctl start ceph-mon@${mon_id}.service

        sleep 5

        ceph --connect-timeout 10 -s | grep -w "$mon_addr" > /dev/null 2>&1
        if [ $? -eq 0 ] ; then
            log "monitor $mon is added"
            break 
        fi
        c=`expr $c + 1`
        sleep 1
    done
    if [ $c -ge 5 ] ; then
        log "failed to add the monitor $mon to cluster"
        return 1
    fi

    log "enable $mon on host $host"
    execute_remote_command $host systemctl enable ceph-mon@${mon_id}.service

    execute_remote_command $host rm -fr $rdir

    is_mon_running $mon_id  #we may check if the monitor is runing locally, not need to check it on remote host;
    if [ $? -ne 0 ] ; then
        log "WARN: monitor $mon is added but it failed to start up"
    else
        log "monitor $mon is added successfully"
    fi

    return 0
}

function rm_osd_id()
{
    local osd_id=$1

    local retry=0
    while [ $retry -lt 20 ] ; do
        ceph --connect-timeout 20 osd rm $osd_id

        if is_osd_out_osdmap "osd.$osd_id" ; then
            log "osd.$osd_id has been removed from osd map"
            break
        fi

        log "osd.$osd_id fails to be removed from osd map, retry=$retry"
        retry=`expr $retry + 1`
        sleep 6
    done

    if [ $retry -ge 20 ] ;  then 
        log "failed to remove osd.$osd_id from osd map"
        return 1
    fi

    return 0
}

function rm_osds()
{
    local osds=$*
    log "in rm_osds(), start to remove osds: $osds"

    local to_remove=""
    local retry=0

    log "step1-begin: verify that osds to remove are down and out"
    local tmpfile=/tmp/osdtree-`date +%s`
    get_osd_tree $tmpfile
    if [ $? -ne 0 ] ; then  #failed to run command "ceph --connect-timeout 30 osd tree"
        log "failed to remove osds: failed to get osd tree"
        rm -f $tmpfile
        return 1
    fi

    for osd in $osds ; do
        cat $tmpfile | grep "\<$osd\>" > /dev/null 2>&1
        if [ $? -ne 0 ] ; then
            log "$osd is not in cluster, omit it"
            continue
        fi

        cat $tmpfile | grep "\<$osd\>" | grep "\<up\>" > /dev/null 2>&1
        if [ $? -eq 0 ] ; then
            log "failed to remove osds: $osd is up. Note: osds cannot be removed unless they are down and out!!!"
            rm -f $tmpfile
            return 1
        fi

        local inout=`cat $tmpfile | grep "\<$osd\>" | sed -e 's/^ *//' | tr -s ' ' | cut -d ' ' -f 5`
        if [ $(echo "$inout>0"|bc) -eq 1 ] ; then
            log "failed to remove osds: $osd is in the cluster. Note: osds cannot be removed unless they are down and out!!!"
            rm -f $tmpfile
            return 1
        fi

        to_remove="$osd $to_remove"
    done
    rm -f $tmpfile
    log "step1-end: all osds are down and out, continue ..."

    log "step2-begin: remove osds from crushmap "
    for osd in $to_remove ; do 
        rm_osd_from_crushmap $osd
        if [ $? -ne 0 ] ;  then
            log "failed to remove $osd from crush map, abort (without trying to rollback)"
            return 2
        fi
    done
    log "step2-end: succeeded to remove osds from crushmap"

    log "step3-begin: remove authentication key (if auth is enabled)"
    for osd in $to_remove ; do
        retry=0
        while [ $retry -lt 10 ] ; do
            ceph --connect-timeout 20 auth del $osd
            if [ $? -eq 0 ] ; then
                log "authentication key of $osd is deleted"
                break
            fi

            log "failed to delete authentication key of $osd, retry=$retry"
            retry=`expr $retry + 1`
            sleep 6
        done

        if [ $retry -ge 10 ] ; then
            log "warn: failed to delete authentication key of $osd, try to continue instead of aborting"
        fi
    done
    log "step3-end: finished to remove authentication key"


    log "step4-begin: remove osds from osdmap"
    for osd in $to_remove ; do
        local osd_id=`echo $osd | cut -c 4- | sed 's/^\\.//'`
        rm_osd_id $osd_id

        if [ $? -ne 0 ] ; then
            log "warn: failed to remove $osd from osdmap, try to continue instead of aborting"
        fi
    done
    log "step4-end: finished to remove osds from osdmap"


    log "step5-begin: umount device of osds"
    for osd in $to_remove ; do
        local host=`ceph-conf -c $BACKUP_CONF -n $osd "host"`
        local osd_dev=`ceph-conf -c $BACKUP_CONF -n $osd "devs"`
        local osd_data=`ceph-conf -c $BACKUP_CONF -n $osd "osd data"`

        execute_remote_command $host $CEPH_HOME/mnt.sh $LOG_FILE umount_by_uuid $osd_dev 
        #execute_remote_command $host rm -fr $osd_data   #dont' remove it
    done
    log "step5-end: finished to umount device of osds"

    log "step6-begin: disable the removed osds"
    for osd in $to_remove ; do
        local osd_id=`echo $osd | cut -c 4- | sed 's/^\\.//'`
        local host=`ceph-conf -c $BACKUP_CONF -n $osd "host"`
        execute_remote_command $host systemctl disable ceph-osd@${osd_id}.service
    done
    log "step6-end: finished to disable the removed osds"

    return 0
}

function rm_osd()
{
    local osd=$1

    local osd_id=`echo $osd | cut -c 4- | sed 's/^\\.//'`
    local host=`ceph-conf -c $BACKUP_CONF -n $osd "host"`
    local osd_dev=`ceph-conf -c $BACKUP_CONF -n $osd "devs"`
    local osd_data=`ceph-conf -c $BACKUP_CONF -n $osd "osd data"`

    log "start to remove $osd (id:$osd_id, host:$host, dev:$osd_dev, data:$osd_data)"

    log "remove-osd-step1: wait until cluster gets to HEALTH_OK state ..."
    wait_health_ok 300
    if [ $? -ne 0 ] ; then
        log "abort removing osd because cluster cannot get into HEALTH_OK state"
        return 1
    fi

    log "remove-osd-step2: take $osd out of the cluster ..."
    local retry=0
    while [ $retry -lt 10 ] ; do
        ceph --connect-timeout 20 osd out $osd_id
        if is_osd_out_cluster $osd ; then
            log "$osd has been taken out of the cluster"
            break
        fi
        log "$osd fails to be taken out, retry=$retry"
        retry=`expr $retry + 1`
        sleep 1
    done

    if [ $retry -ge 10 ] ; then 
        log "failed to take $osd out of the cluster"
        return 1
    fi

    log "remove-osd-step3: wait for cluster to get into HEALTH_OK state after take $osd out ..."
    retry=0
    while [ $retry -lt 20 ] ; do
        wait_health_ok && break
        retry=`expr $retry + 1`
    done

    if [ $retry -ge 20 ] ; then
        log "cluster fails to get into HEALTH_OK state, roll back (take $osd in cluster) and abort"
        ceph --connect-timeout 20 osd in $osd_id
        return 1
    fi

    log "remove-osd-step4: stop the osd $osd"
    retry=0
    while [ $retry -lt 10 ] ; do
        execute_remote_command $host /etc/init.d/ceph -c $BACKUP_CONF stop $osd
        is_osd_running $osd
        if [ $? -ne 0 ] ; then  #not running
            log "osd $osd has been stopped"
            break
        fi

        log "osd $osd fails to stop, retry=$retry"
        retry=`expr $retry + 1`
        sleep 1
    done

    if [ $retry -ge 10 ] ; then
        log "failed to stop osd $osd, roll back (take it in the cluster) and abort"
        ceph --connect-timeout 10 osd in $osd_id
        return 1
    fi

    log "remove-osd-step5: remove $osd from CRUSH map"
    rm_osd_from_crushmap $osd
    if [ $? -ne 0 ] ;  then
        log "failed to remove $osd from crush map, abort (without trying to roll back)"
        return 1
    fi

    log "remove-osd-step6: remove authentication key of $osd (if there is, e.g. auth is enabled)"
    ceph --connect-timeout 20 auth del $osd

    log "remove-osd-step7: remove $osd from osd map"
    rm_osd_id $osd_id

    log "remove-osd-step8: umount device of $osd ($osd_dev) on $host"
    execute_remote_command $host $CEPH_HOME/mntfstab.sh umount_by_label $osd_dev 
    execute_remote_command $host rm -fr $osd_data 

    #we will not format the disk here, because it is a dangerous action. it will not be formatted until it's used by new osd 

    return 0
}

#find the rack{x}{y} that contains the given osd;
function find_osd_in_tree()
{
    local tree=$1
    local osd=$2

    local rack=""
    cat $tree | while read line ; do
        echo $line | grep rack > /dev/null 2>&1
        if [ $? -eq 0 ] ; then
            rack=`echo $line | sed -e 's/^ *//' | tr -s ' ' | cut -d ' ' -f 4`
            continue  #current line is rack{x}{y}, save it, then check if one of the following lines matches the given osd;
        fi

        echo $line | grep "\<$osd\>" > /dev/null 2>&1
        if [ $? -eq 0 ] ; then   
            local weight=`echo $line | sed -e 's/^ *//' | tr -s ' ' | cut -d ' ' -f 2`
            if [ $(echo "$weight>0"|bc) -eq 1 ] ; then   #weight is float in 0.94, so we have to compare it like this
                echo $rack
                return 0
            fi
        fi
    done
    return 1
}

#get number of osds in a rack:
#    param1: osd tree file
#    param2: rack no
#    param3: row no
#param3 can be omitted, in such case, return the sum of all rows in the same rack;
#e.g.   get_rack_size rack 1 2 : return number of osds in rack12;
#       get_rack_size rack 1   : return number of osds in rack10, rack11, ..., rack1N
#          |  rack0     rack1     rack2
#----------+---------------------------
#    row0: | rack00    rack10    rack20
#    row1: | rack01    rack11    rack21
#    row2: | rack02    rack12    rack22
#    ...   | ...       ...       ...
#    rowN: | rack0N    rack1N    rack2N
function get_rack_size()
{
    local tree=$1
    local rack_no=$2
    local row_no=$3

    local pattern=rack$rack_no$row_no

    local osds_tmp=/tmp/osds_in_rack_`date +%s`.txt
    local count=0
    cat $tree | while read line ; do
        echo $line | grep rack > /dev/null 2>&1
        if [ $? -eq 0 ] ; then  #rack line
            echo $line | grep $pattern > /dev/null 2>&1
            if [ $? -eq 0 ] ; then #entering the target block, start to count 
                count=1
            else                   #leaving the target block, stop to count 
                count=0
            fi
            continue
        fi

        echo $line | grep osd > /dev/null 2>&1
        if [ $? -eq 0 ] ; then #osd line
            if [ $count -eq 1 ] ; then

                local weight=`echo $line | sed -e 's/^ *//' | tr -s ' ' | cut -d ' ' -f 2`
                if [ $(echo "$weight>0"|bc) -eq 1 ] ; then   #weight is float in 0.94, so we have to compare it like this
                    echo "find an osd in $pattern" >> $osds_tmp
                fi
            fi
        fi
    done

    local num=0
    [ -f $osds_tmp ] && num=`cat $osds_tmp | wc -l`

    rm -f $osds_tmp

    echo $num
}

function add_osd_to_crushmap()
{
    local osd=$1
    local rack_to_add=$2   # 0 means not specified
    local keyring_opt=$3
    local weight=`grep -w $osd $osd_weight_map 2> /dev/null | cut -d ' ' -f 2`
    [ -z "$weight" ] && weight=1.00000

    log "in add_osd_to_crushmap: add $osd to crushmap, rack_to_add=$rack_to_add"

    local location=""
    if [ "$rack_to_add" != "0" ] ; then  
        #rack row is specified, find out the location
        local row_no=`echo $rack_to_add | sed -e 's/rack[0-9]//'`
        location="root=default row=row$row_no rack=$rack_to_add"
    else
        local tmpfile=/tmp/osdtree-`date +%s`
        get_osd_tree $tmpfile
        if [ $? -ne 0 ] ; then
            log "in add_osd_to_crushmap: failed to get osd tree"
            rm -f $tmpfile
            return 1
        fi

        local host=`ceph-conf -c $ORIGIN_CONF -n $osd "host"`
        local osd_id=`echo $osd | cut -c 4- | sed 's/^\\.//'`
    
        grep unknownrack $tmpfile > /dev/null 2>&1
        if [ $? -eq 0 ] ; then   #didn't generate crushmap when the cluster is deployed, the default crushmap was used;
            log "the ceph cluster is using default crushmap"
            location="root=default rack=unknownrack host=$host"
        else #we have had genreated the crushmap, the osd should be put in the same rack as its siblings
            log "the ceph cluster is NOT using default crushmap, analyze it and find a location to add the osd"
            local siblings=`ceph-conf -c $ORIGIN_CONF -l osd --filter-key-value host=$host`
            local rack_row=""
            for sib in $siblings ; do 
                rack_row=`find_osd_in_tree $tmpfile $sib`
                [ -n "$rack_row" ] && break
            done

            if [ -z "$rack_row" ] ; then  # no siblings has been added to crushmap
                log "no siblings of $osd is in the crushmap, so we find the minimum rack and minimun row"

                #choose the minimum rack
                local rack_no=0
                local min=`get_rack_size $tmpfile $rack_no`
                local r=1
                while [ true ] ; do
                    local tmp=`get_rack_size $tmpfile $r`
                    [ $tmp -eq 0 ] && break
                    [ $tmp -lt $min ] && min=$tmp && rack_no=$r
                    r=`expr $r + 1`
                done

                log "minimum rack: $rack_no, size: $min"

                #choose the minimum row
                local row_no=0
                min=`get_rack_size $tmpfile $rack_no $row_no`
                local i=1
                while [ true ] ; do
                    local tmp=`get_rack_size $tmpfile $rack_no $i`
                    [ $tmp -eq 0 ] && break  # don't have the row, or rows greater than it
                    [ $tmp -lt $min ] && min=$tmp && row_no=$i
                    i=`expr $i + 1`
                done
                log "minimum row: $row_no, size: $min"

                location="root=default row=row$row_no rack=rack$rack_no$row_no"
            else  # one of the siblings has been added to $rack
                local rack_no=`echo $rack_row | sed -e 's/rack\([0-9]\)[0-9].*/\1/'`

                log "one of $osd's siblings is in the crushmap, so we find the minimun row in the same rack (rack$rack_no)"

                #choose the minimum row
                local row_no=0
                min=`get_rack_size $tmpfile $rack_no $row_no`
                local i=1
                while [ true ] ; do
                    local tmp=`get_rack_size $tmpfile $rack_no $i`
                    [ $tmp -eq 0 ] && break  # don't have the row, or rows greater than it
                    [ $tmp -lt $min ] && min=$tmp && row_no=$i
                    i=`expr $i + 1`
                done
                log "minimum row: $row_no, size: $min"

                location="root=default row=row$row_no rack=rack$rack_no$row_no"
            fi 
        fi
    fi

    log "the final crush localtion to add $osd: $location"

    log "add to crushmap: ceph --connect-timeout 20 osd crush create-or-move --name=$osd $keyring_opt -- $osd_id $weight $location"
    local cnt=0
    while [ $cnt -lt 20 ] ; do
        ceph --connect-timeout 20 osd crush create-or-move --name=$osd $keyring_opt -- $osd_id $weight $location
        sleep 2
        is_osd_out_crush $osd
        if [ $? -eq 1 ] ; then
            log "osd $osd_id is successfully added to crushmap"
            break
        fi

        cnt=`expr $cnt + 1`
        sleep 6
    done

    if [ $cnt -ge 20 ] ; then
        log "failed to add osd $osd_id to crush map" 
        rm -f $tmpfile
        return 1
    fi

    rm -f $tmpfile
    return 0
}

function rm_osd_from_crushmap()
{
    local osd=$1

    local count=0
    while [ $count -lt 20 ] ; do
        ceph --connect-timeout 20 osd crush remove $osd
        if is_osd_out_crush $osd ; then
            log "$osd has been removed from crush map"
            return 0
        fi

        log "osd $osd fails to be removed from crush map, retry=$count"
        count=`expr $count + 1`
        sleep 6
    done

    if [ $count -ge 20 ] ;  then 
        log "failed to remove $osd from crush map"
        return 1
    fi

    return 0
}

function add_osds()
{
    local rack_to_add=$1  # 0 means not specified
    shift

    local osds=$*
    log "in add_osds(), start to add osds: $osds"

    local retry=0

    log "step1-begin: verify that disks exist"
    for osd in $osds ; do
        local host=`ceph-conf -c $ORIGIN_CONF -n $osd "host"`
        local osd_dev=`ceph-conf -c $ORIGIN_CONF -n $osd "devs"`  #osd_dev may be a disk partition or a disk
        file_dir_exist $host $osd_dev 0   # 0 means file
        if [ $? -ne 3 ] ; then
            log "failed to add osds: osd devs $osd_dev is not present on $host or it's not a block device"
            return 1
        fi
    done
    log "step1-end: all disks exist, continue ..."

    log "step2-begin: verify that these osds don't exist in the osdmap already"
    for osd in $osds ; do
        is_osd_out_osdmap $osd
        local retcode=$?
        if [ $retcode -eq 2 ] ; then
            log "failed to add osds: cannot get osd tree"
            return 1
        fi
        if [ $retcode -eq 1 ] ; then
            log "failed to add osds: $osd is already in osdmap"
            return 1
        fi
    done
    log "step2-end: these osds don't exist in the osdmap, so they can be added, continue ..."

    log "step3-begin: create id for osds"
    local ids_created=""
    for osd in $osds ; do
        local osd_id=`echo $osd | cut -c 4- | sed 's/^\\.//'`
        retry=0
        while [ $retry -lt 10 ] ; do
            #for "ceph --connect-timeout 30 osd create" operation below, if we want to specify osd id, we must also specify uuid;
            #so we generate a uuid here. we save it in file $RUN_DIR/${osd}.fsid, and we copy it to destination osd data dir in 
            #step 7 before mkfs;
            local uuid=`uuidgen`  
            echo $uuid > $RUN_DIR/${osd}.fsid
            local create_id=`ceph --connect-timeout 30 osd create $uuid $osd_id 2>/dev/null`

            if [ "$create_id" == "$osd_id" ] ; then
                log "id $osd_id is created successfully"
                ids_created="$osd_id $ids_created"
                break
            fi

            sleep 5

            if is_osd_created $osd_id ; then
                log "id $osd_id is created successfully, because it's found in osdmap now"
                ids_created="$osd_id $ids_created"
                break
            fi

            retry=`expr $retry + 1`
            sleep 6
        done 

        if [ $retry -ge 10 ] ; then
            log "failed to add osds: cannot create osd id $osd_id. rollback (remove created osd ids) and abort"
            for rm_id in $ids_created ; do
                rm_osd_id $rm_id
            done
            return 1
        fi
    done
    log "step3-end: succeeded to create id for osds, continue ..."

    log "step4-begin: create directories for data, journal, log, pid and sock"
    for osd in $osds ; do
        local host=`ceph-conf -c $ORIGIN_CONF -n $osd "host"`

        local osd_id=`echo $osd | cut -c 4- | sed 's/^\\.//'`
        local osd_data=`ceph-conf -c $ORIGIN_CONF -n $osd "osd data"`
        [ -z "$osd_data" ] && osd_data=/data/ceph/osds/osd.$osd_id

        local osd_journal=`ceph-conf -c $ORIGIN_CONF -n $osd "osd journal"`
        [ -z "$osd_journal" ] && osd_journal=$osd_data/journal

        local jdir=""
        echo $osd_journal | grep "/dev/"  > /dev/null 2>&1
        if [ $? -eq 0 ] ; then   #journal is block device, make sure the device exist
            file_dir_exist $host $osd_journal 0  # 0 means file
            if [ $? -ne 3 ] ; then
                log "failed to add osds: osd journal $osd_journal is not present on $host or it's not a block device"
                return 1
            fi
            execute_remote_command $host umount $osd_journal
        else #journal is a file, then we shoud make sure its containing dir exist
            jdir=`dirname $osd_journal`
        fi

        local logf=`ceph-conf -c $ORIGIN_CONF -n $osd "log file"`
        [ -z "$logf" ] && logf=/data/proclog/ceph/$osd.log
        local logdir=`dirname $logf`

        local pidf=`ceph-conf -c $ORIGIN_CONF -n $osd "pid file"`
        [ -z "$pidf" ] && pidf=/var/run/ceph/$osd.pid
        local piddir=`dirname $pidf`

        local sockf=`ceph-conf -c $ORIGIN_CONF -n $osd "admin socket"`
        [ -z "$sockf" ] && sockf=/var/run/ceph/$osd.asok
        local sockdir=`dirname $sockf`

        execute_remote_command $host mkdir -p $osd_data $jdir $logdir $piddir $sockdir

        for dir in $osd_data $jdir $logdir $piddir $sockdir ; do
            file_dir_exist $host $dir 1   # 1 means directory
            if [ $? -eq 1 ] ; then
                log "failed to add osds: cannot create $dir on $host; rollback (remove created osd ids) and abort"
                for rm_id in $ids_created ; do
                    rm_osd_id $rm_id
                done
                return 1
            fi
        done
    done
    log "step4-end: succeeded to create directories for data, journal, log, pid and sock, continue ..."

    log "step5-begin: format the disks"
    for osd in $osds ; do
        local host=`ceph-conf -c $ORIGIN_CONF -n $osd "host"`
        local osd_dev=`ceph-conf -c $ORIGIN_CONF -n $osd "devs"`

        local fs_type=`ceph-conf -c $ORIGIN_CONF -n $osd "osd mkfs type"`
        [ -z "$fs_type" ] && fs_type=xfs

        local mkfs_opt=`ceph-conf -c $ORIGIN_CONF -n $osd "osd mkfs options $fs_type"`
        [ -z "$mkfs_opt" ] && mkfs_opt="-f -i size=1024"

        local osd_id=`echo $osd | cut -c 4- | sed 's/^\\.//'`

        execute_remote_command $host $CEPH_HOME/mnt.sh $LOG_FILE umount_by_uuid $osd_dev
        if [ $? -ne 0 ] ; then
            log "failed to add osds: cannot umount $osd_dev on $host; rollback (remove created osd ids) and abort"
            for rm_id in $ids_created ; do
                rm_osd_id $rm_id
            done
            return 1
        fi

        retry=0
        while [ $retry -lt 5 ] ; do
            execute_remote_command $host mkfs.$fs_type $mkfs_opt $osd_dev
            [ $? -eq 0 ] && break
            retry=`expr $retry + 1`
            sleep 3
        done
    
        if [ $retry -ge 5 ] ; then
            log "failed to add osds: cannot format $osd_dev on $host; rollback (remove created osd ids) and abort"
            for rm_id in $ids_created ; do
                rm_osd_id $rm_id
            done
            return 1
        fi
    done
    log "step5-end: succeeded to format the disks, continue ..."

    log "step6-begin: mount the disks"
    for osd in $osds ; do
        local host=`ceph-conf -c $ORIGIN_CONF -n $osd "host"`
        local osd_dev=`ceph-conf -c $ORIGIN_CONF -n $osd "devs"`

        local osd_id=`echo $osd | cut -c 4- | sed 's/^\\.//'`
        local osd_data=`ceph-conf -c $ORIGIN_CONF -n $osd "osd data"`
        [ -z "$osd_data" ] && osd_data=/data/ceph/osds/osd.$osd_id

        local fs_type=`ceph-conf -c $ORIGIN_CONF -n $osd "osd mkfs type"`
        [ -z "$fs_type" ] && fs_type=xfs

        local mount_ops=`ceph-conf -c $ORIGIN_CONF -n $osd "osd mount options $fs_type"`
        [ -z "$mount_ops" ] && mount_ops="rw,noatime,nobarrier,inode64,logbsize=256k,delaylog"

        execute_remote_command $host umount $osd_data

        execute_remote_command $host $CEPH_HOME/mnt.sh $LOG_FILE mount_by_uuid $osd_dev $osd_data $fs_type $mount_ops
        if [ $? -ne 0 ] ; then
            log "failed to add osds: cannot mount $osd_dev to $osd_data on $host; rollback (remove created osd ids) and abort"
            for rm_id in $ids_created ; do
                rm_osd_id $rm_id
            done
            return 1
        fi
    done
    log "step6-end: succeeded to mount the disks, continue ..."

    log "step7-begin: initialize the osd data"
    for osd in $osds ; do
        local host=`ceph-conf -c $ORIGIN_CONF -n $osd "host"`

        local osd_id=`echo $osd | cut -c 4- | sed 's/^\\.//'`
        local osd_data=`ceph-conf -c $ORIGIN_CONF -n $osd "osd data"`
        [ -z "$osd_data" ] && osd_data=/data/ceph/osds/osd.$osd_id

        retry=0
        while [ $retry -lt 5 ] ; do
            send_file $host $RUN_DIR/${osd}.fsid $osd_data/fsid
            if [ $? -ne 0 ] ; then
                log "WARN: failed to copy $RUN_DIR/${osd}.fsid to $osd_data/fsid"
                retry=`expr $retry + 1`
                continue
            fi

            execute_remote_command $host ceph-osd -i $osd_id --mkfs --mkkey
            [ $? -eq 0 ] && break

            retry=`expr $retry + 1`
            execute_remote_command $host rm -fr $osd_data/*
            sleep 6
        done

        if [ $retry -ge 5 ] ; then
            log "failed to add osds: cannot initialize osd data for $osd on $host; rollback (remove created osd ids) and abort"
            for rm_id in $ids_created ; do
                rm_osd_id $rm_id
            done
            return 1
        fi
    done
    log "step7-end: succeeded to initialize the osd data, continue ..."


    log "step8-begin: add osds to crushmap"
    local added_crush=""
    for osd in $osds ; do
        local host=`ceph-conf -c $ORIGIN_CONF -n $osd "host"`
        local osd_id=`echo $osd | cut -c 4- | sed 's/^\\.//'`
        local osd_data=`ceph-conf -c $ORIGIN_CONF -n $osd "osd data"`
        [ -z "$osd_data" ] && osd_data=/data/ceph/osds/osd.$osd_id

        local keyring_opt=""
        file_dir_exist $host $osd_data/keyring 0  #0 means file
        if [ $? -ne 1 ] ; then   # keyring exists
            keyring_opt="--keyring=$osd_data/keyring"

            retry=0
            while [ $retry -lt 5 ] ; do
                execute_remote_command $host "ceph --connect-timeout 120 auth add $osd osd \'allow *\' mon \'allow rwx\' -i $osd_data/keyring"
                if [ $? -eq 0 ] ; then
                    log "successful to add keyring for $osd"
                    break
                fi 
                retry=`expr $retry + 1`
                sleep 6
            done

            if [ $retry -ge 5 ] ; then
                log "warn: failed to add keyring for $osd"
                #return 1   don't return error, because keyring is not necessary in our env (auth is disabled)
            fi
        fi

        add_osd_to_crushmap $osd $rack_to_add $keyring_opt  #rack_to_add: 0 means not specified

        if [ $? -ne 0 ] ; then
            log "failed to add osds: cannot add $osd to crushmap; rollback (remove created osd ids) and abort"
            for o in $added_crush ; do
                rm_osd_from_crushmap $o
            done
            for rm_id in $ids_created ; do
                rm_osd_id $rm_id
            done
            return 1
        else
            added_crush="$osd $added_crush"
        fi
    done
    log "step8-end: succeeded to add osds to crushmap, continue ..."


    log "step9-begin: enable the osds"
    for osd in $osds ; do
        local host=`ceph-conf -c $ORIGIN_CONF -n $osd "host"`
        local osd_id=`echo $osd | cut -c 4- | sed 's/^\\.//'`
        execute_remote_command $host systemctl enable ceph-osd@${osd_id}.service
    done
    log "step9-end: finished to enable the osds"


    log "step10-begin: start the osds"
    local fhosts=/tmp/add-osds-hosts-`date +%s`.txt
    for osd in $osds ; do
        local host=`ceph-conf -c $ORIGIN_CONF -n $osd "host"`
        echo $host >> $fhosts
    done

    for osdhost in `cat $fhosts | uniq` ; do 
        log "start the osds on $osdhost"
        execute_remote_command $osdhost systemctl start ceph-osd.target
    done
    log "step10-end: finished to start the osds"

    echo "sleep 120 seconds ..."
    sleep 120 


    log "step11-begin: check the status of the added osds"
    local all=`echo $osds | wc -w`
    local running=0
    for osd in $osds ; do
        retry=0
        while [ $retry -lt 15 ] ; do
            is_osd_running $osd
            if [ $? -eq 0 ] ; then
                running=`expr $running + 1`
                break
            fi
            retry=`expr $retry + 1`
            sleep 6
        done
    done
    log "step11-end: finished to check the status of the added osds"

    rm -f $fhosts

    if [ $running -lt $all ] ; then
        log "WARN: some osds added are not successfully started"
        return 2
    fi

    if [ $running -eq $all ] ; then
        log "SUCCESS: all osds ($running) added are running now"
        return 0
    fi

    return 0
}

function add_osd()
{
    local osd=$1
    log "in add_osd(), start to add osd: $osd"

    local osd_id=`echo $osd | cut -c 4- | sed 's/^\\.//'`
    local host=`ceph-conf -c $ORIGIN_CONF -n $osd "host"`
    local osd_dev=`ceph-conf -c $ORIGIN_CONF -n $osd "devs"`
    local osd_data=`ceph-conf -c $ORIGIN_CONF -n $osd "osd data"`
    local osd_journal=`ceph-conf -c $ORIGIN_CONF -n $osd "osd journal"`

    local mount_ops=`ceph-conf -c $ORIGIN_CONF -n $osd "osd mount options xfs"`

    local logf=`ceph-conf -c $ORIGIN_CONF -n $osd "log file"`
    [ -z "$logf" ] && logf=/data/proclog/ceph/$osd.log
    local logdir=`dirname $logf`

    local pidf=`ceph-conf -c $ORIGIN_CONF -n $osd "pid file"`
    [ -z "$pidf" ] && pidf=/var/run/ceph/$osd.pid
    local piddir=`dirname $pidf`

    local sockf=`ceph-conf -c $ORIGIN_CONF -n $osd "admin socket"`
    [ -z "$sockf" ] && sockf=/var/run/ceph/$osd.asok
    local sockdir=`dirname $sockf`

    is_osd_out_osdmap $osd
    if [ $? -eq 2 ] ; then
        log "in add_osd(), something is wrong, cannot get osd tree, abort"
        return 1
    fi
    if [ $? -eq 1 ] ; then
        log "in add_osd(), $osd is already in osdmap, abort"
        return 1
    fi

    local retry=0
    local create_id=""
    while [ $retry -lt 10 ] ; do
        create_id=`ceph --connect-timeout 30 osd create $osd_id 2>&1`

        if [ "$create_id" == "$osd_id" ] ; then
            log "in add_osd(), osd $osd_id is created successfully"
            break
        fi

        sleep 5

        if is_osd_created $osd_id ; then
            log "in add_osd(), osd $osd_id is created successfully, because it's found in osdmap now"
            break
        fi

        retry=`expr $retry + 1`
        sleep 6
    done 

    if [ $retry -ge 10 ] ; then
        log "in add_osd(), failed to create osd id"
        return 1
    fi

    log "in add_osd(), add osd on node $host"

    execute_remote_command $host mkdir -p $osd_data
    if [ $? -ne 0 ] ; then
        log "in add_osd(), failed to create dir $osd_data on $host"
        rm_osd_id $osd_id
        return 1
    fi

    execute_remote_command $host mkdir -p $logdir
    if [ $? -ne 0 ] ; then
        log "in add_osd(), failed to create dir $logdir on $host"
        rm_osd_id $osd_id
        return 1
    fi

    execute_remote_command $host mkdir -p $piddir
    if [ $? -ne 0 ] ; then
        log "in add_osd(), failed to create dir $piddir on $host"
        rm_osd_id $osd_id
        return 1
    fi

    execute_remote_command $host mkdir -p $sockdir
    if [ $? -ne 0 ] ; then
        log "in add_osd(), failed to create dir $sockdir on $host"
        rm_osd_id $osd_id
        return 1
    fi

    execute_remote_command $host umount $osd_data
    execute_remote_command $host $CEPH_HOME/mntfstab.sh umount_by_label $osd_dev

    log "in add_osd(), format $osd_dev on $host ..."
    #no need to format the disk; because it's already formatted by OP team;
#    retry=0
#    while [ $retry -lt 5 ] ; do
#        execute_remote_command $host mkfs.xfs -f -i size=2048 $osd_dev
#        [ $? -eq 0 ] && break
#
#        retry=`expr $retry + 1`
#        sleep 3
#    done
#
#    if [ $retry -ge 5 ] ; then
#        log "in add_osd(), failed to format $osd_dev on $host"
#        rm_osd_id $osd_id
#        return 1
#    fi

    log "in add_osd(), mount $osd_dev on directory $osd_data on $host"
    execute_remote_command $host $CEPH_HOME/mntfstab.sh mount_by_label $osd_dev $osd_data xfs $mount_ops 

    if [ $? -ne 0 ] ; then
        log "in add_osd(), failed to mount the device $osd_dev on directory $osd_data on $host" 
        rm_osd_id $osd_id
        return 1
    fi
    rm -fr $osd_data/*

    local journal_dir=`dirname $osd_journal`
    if [ "$journal_dir" != "$osd_data" ] ; then
        execute_remote_command $host mkdir -p $journal_dir
        if [ $? -ne 0 ] ; then
            log "in add_osd(), failed to create journal dir $journal_dir on $host"
            rm_osd_id $osd_id
            return 1
        fi
    fi

    log "in add_osd(), initialize osd data"
    retry=0
    while [ $retry -lt 5 ] ; do
        execute_remote_command $host ceph-osd -i $osd_id --mkfs --mkkey
        [ $? -eq 0 ] && break

        retry=`expr $retry + 1`
        execute_remote_command $host rm -fr $osd_data/*
        sleep 6
    done

    if [ $retry -ge 5 ] ; then
        log "in add_osd(), failed to initialize osd data" 
        rm_osd_id $osd_id
        return 1
    fi

    local keyring_opt=""
    file_dir_exist $host $osd_data/keyring 0  #0 means file
    if [ $? -eq 0 ] ; then   # keyring exists
        keyring_opt="--keyring=$osd_data/keyring"

        retry=0
        while [ $retry -lt 5 ] ; do
            execute_remote_command $host "ceph --connect-timeout 120 auth add $osd osd \'allow *\' mon \'allow rwx\' -i $osd_data/keyring"
            if [ $? -eq 0 ] ; then
                log "in add_osd(), succeeded to add keyring" 
                break
            fi

            retry=`expr $retry + 1`
            sleep 10 
        done

        if [ $retry -ge 5 ] ; then
            log "warn: in add_osd(), failed to add keyring" 
            #return 1   don't return error, because keyring is not necessary in our env (auth is disabled)
        fi
    fi

    log "add osd $osd_id to crushmap"
    log "ceph --connect-timeout 20 osd crush create-or-move --name=$osd $keyring_opt -- $osd_id 1 root=default rack=unknownrack host=$host"
    retry=0
    while [ $retry -lt 20 ] ; do
        execute_remote_command $host ceph --connect-timeout 20 osd crush create-or-move --name=$osd $keyring_opt -- $osd_id 1 root=default rack=unknownrack host=$host

        sleep 2

        is_osd_out_crush $osd
        if [ $? -eq 1 ] ; then
            log "in add_osd(), osd $osd_id is successfully added to crushmap"
            break
        fi

        retry=`expr $retry + 1`
        sleep 6
    done

    if [ $retry -ge 20 ] ; then
        log "in add_osd(), failed to add osd to crush map" 
        rm_osd_id $osd_id
        return 1
    fi

    execute_remote_command $host systemctl start ceph-osd@${osd_id}.service

    retry=0
    while [ $retry -lt 20 ] ; do
        is_osd_running $osd
        if [ $? -eq 0 ] ; then
            log "in add_osd(), $osd is added and it's running now"
            break
        fi

        retry=`expr $retry + 1`
        sleep 6
    done

    if [ $retry -ge 20 ] ; then
        log "in add_osd(), $osd is added but it failed to start up" 
    fi

    wait_health_ok
    if [ $? -eq 0 ] ; then
        log "$osd is added successfully"
    else
        log "$osd is added but cluster is NOT in HEALTH_OK state"
    fi

    return 0
}

function apply_conf_change()
{
    local rack_to_add=$1
    [ -z "$rack_to_add" ] && rack_to_add=0   #0 means not specified

    log "in function apply_conf_change()"

    check_conf $ORIGIN_CONF || return 1

    is_ceph_running 
    if [ $? -ne 0 ] ; then  #not running
        log "Cannot apply config changes because ceph cluster is not running currently. Please start ceph cluster with original config"
        return 0
    fi

    #overwrite_conf || return 1

    local conf_diff=`diff $ORIGIN_CONF $BACKUP_CONF`

    if [ -z "$conf_diff" ] ; then 
        log "no changes detected"
        return 0
    fi

    #1. apply "osd pool default size" change if necessary;
    local def_pool_size_old=`ceph-conf -c $BACKUP_CONF "osd pool default size"`
    local def_pool_size=`ceph-conf -c $ORIGIN_CONF "osd pool default size"`

    #2. add or/add remove monitors;
    local mons=`ceph-conf -c $ORIGIN_CONF -l mon | egrep -v '^mon$' | sort`
    local mons_old=`ceph-conf -c $BACKUP_CONF -l mon | egrep -v '^mon$' | sort`

    local mons_add=""
    local mons_rm=""
    local mons_mod=""

    for mon in $mons ; do
        local found=0
        for mon_old in $mons_old ; do
            if [ "$mon" == "$mon_old" ] ; then
                host=`ceph-conf -c $ORIGIN_CONF -n $mon "host"`
                addr=`ceph-conf -c $ORIGIN_CONF -n $mon "mon addr"`
                host_old=`ceph-conf -c $BACKUP_CONF -n $mon_old "host"`  
                addr_old=`ceph-conf -c $BACKUP_CONF -n $mon_old "mon addr"`
                if [[ "$host" != "$host_old" || "$addr" != "$addr_old" ]] ; then
                    mons_mod="$mon $mons_mod"
                fi

                found=1
                break;
            fi
        done
        [ $found -eq 0 ] && mons_add="$mon $mons_add"
    done

    for mon_old in $mons_old ; do
        local found=0
        for mon in $mons ; do
            if [ "$mon" == "$mon_old" ] ; then
                found=1
                break;
            fi
        done
        [ $found -eq 0 ] && mons_rm="$mon_old $mons_rm"
    done

    #3. add or/add remove osds;
    local osds=`ceph-conf -c $ORIGIN_CONF -l osd | egrep -v '^osd$' | sort`
    local osds_old=`ceph-conf -c $BACKUP_CONF -l osd | egrep -v '^osd$' | sort`

    local osds_add=""
    local osds_rm=""
    local osds_mod=""

    for osd in $osds ; do
        local found=0
        for osd_old in $osds_old ; do
            if [ "$osd" == "$osd_old" ] ; then
                host=`ceph-conf -c $ORIGIN_CONF -n $osd "host"`
                dev=`ceph-conf -c $ORIGIN_CONF -n $osd "devs"`
                host_old=`ceph-conf -c $BACKUP_CONF -n $osd_old "host"`  
                dev_old=`ceph-conf -c $BACKUP_CONF -n $osd_old "devs"`
                if [[ "$host" != "$host_old" || "$dev" != "$dev_old" ]] ; then
                    osds_mod="$osd $osds_mod"
                fi

                found=1
                break;
            fi
        done
        [ $found -eq 0 ] && osds_add="$osd $osds_add"
    done

    for osd_old in $osds_old ; do
        local found=0
        for osd in $osds ; do
            if [ "$osd" == "$osd_old" ] ; then
                found=1
                break;
            fi
        done
        [ $found -eq 0 ] && osds_rm="$osd_old $osds_rm"
    done


    local pool_size_changed=0
    local monitors_changed=0
    local osds_changed=0


    [ $def_pool_size_old -ne $def_pool_size ] && pool_size_changed=1

    [ -n "$mons_rm" ] && monitors_changed=1
    [ -n "$mons_mod" ] && monitors_changed=1
    [ -n "$mons_add" ] && monitors_changed=1

    [ -n "$osds_rm" ] && osds_changed=1
    [ -n "$osds_add" ] && osds_changed=1

    if [[ $pool_size_changed -eq 0 ]] && [[ $monitors_changed -eq 0 ]] && [[ $osds_changed -eq 0 ]] ; then
        log "no changes detected after parsing the conf file"
        return 0
    fi

    if [ -n "$osds_mod" ] ; then
        log "You are trying to modify $osds_mod; Note: osd modification is not supported!!!"
        return 1
    fi

    local num_mon_rm=`echo $mons_rm | wc -w`
    if [ $num_mon_rm -gt 1 ] ; then
        log "You are trying to remove $num_mon_rm monitors; only one monitor can be removed"
        return 1
    fi

    local sum=`expr $pool_size_changed + $monitors_changed + $osds_changed`
    if [ $sum -gt 1 ] ; then
        log "Only one kind of change can be made at a time: 1. default pool size; 2. monitors; 3. osds; but you changed more than one:"
        [ $pool_size_changed -eq 1 ] && log "You changed default pool size (from $def_pool_size_old to $def_pool_size)"
        [ $monitors_changed -eq 1 ]  && log "You changed monitors (add: [$mons_add], remove: [$mons_rm], modify: [$mons_mod])"
        [ $osds_changed -eq 1 ]      && log "You changed osds (add: [$osds_add], remove: [$osds_rm])"
        log "the changes is not applied"
        return 1
    fi

    log "start to apply the changes ..."

    wait_health_ok 
    if [ $? -ne 0 ] ; then
        log "cluster is not in HEALTH_OK state, cannot apply conf changes" 
        return 1
    fi 

    #TODO: stop monitor scripts which start up ceph processes automatically  
    local err_no=0

    local baktime=`date +%Y-%m-%d-%H-%M`
    dispatch_conf $baktime || return 1

    if [ $pool_size_changed -eq 1 ] ; then
        log "osd pool default size was changed (previously $def_pool_size_old, now $def_pool_size), apply the change by restarting all monitors"
        local monhosts=`get_allhosts $ORIGIN_CONF mon`
        monhosts=`echo $monhosts | tr -s ' ' | sed -e 's/ /,/g'`
        execute_remote_command_batch $monhosts systemctl start ceph-mon.target
        log "sleep for 30 seconds"
        sleep 30
        wait_health_ok
        if [ $? -ne 0 ] ; then
            log "warn: cluster fails to get into HEALTH_OK state after apply pool default size change"
        else
            log "successfully applied pool default size change. Please check it by command: ceph daemon mon.a config show | grep osd_pool_default_size"
        fi
    elif [ $monitors_changed -eq 1 ] ; then
        log "monitors to be removed: $mons_rm"
        for mon in $mons_rm ; do
            rm_mon $mon
        done

        log "monitors to be modified: $mons_mod"
        for mon in $mons_mod ; do 
            log "start to modify $mon, that is to remove it and then add it"
            rm_mon $mon
            add_mon $mon
        done

        log "monitors to be added: $mons_add"
        for mon in $mons_add ; do
            add_mon $mon
        done

        wait_health_ok
        if [ $? -ne 0 ] ; then
            log "warn: cluster fails to get into HEALTH_OK state after apply monitor changes"
        else
            log "successfully applied monitor changes"
        fi
    else # $osds_changed -eq 1

        #don't support modify now

        #log osds_modified: $osds_mod
        #for osd in $osds_mod ; do
        #    log "start to modify $osd, that is to remove it and then add it"
        #    rm_osd $osd
        #    if [ $? -eq 0 ] ; then
        #        add_osd $osd
        #    else
        #        log "will not add $osd due to removing operation failure"
        #    fi
        #done

        log "osds_removed: $osds_rm"
        if [ -n "$osds_rm" ] ; then
            rm_osds $osds_rm
            if [ $? -eq 1 ] ; then  #if return 1, removal not started or rolled back, so recover the config file
                recover_conf_file $baktime
                err_no=1
            fi
        fi

        if [ $err_no -eq 0 ] ; then
            log "osds added: $osds_add"
            if [ -n "$osds_add" ] ; then

                #if we have removed some osds, wait for health ok state
                if [ -n "$osds_rm" ] ; then
                    wait_health_ok 1800
                    if [ $? -ne 0 ] ; then
                        log "warn: cluster didn't get into HEALTH_OK state in 1800 seconds; continue to add osds"
                    fi
                fi

                add_osds $rack_to_add $osds_add  #rack_to_add: 0 means not specified
                if [ $? -eq 1 ] ; then  #if return 1, addition not started or rolled back, so recover the config file
                    recover_conf_file $baktime
                    err_no=1
                fi
            fi
        else
            log "skip add osds"
        fi

        wait_health_ok 180
    fi

    #we have applied the changes, back up the conf file, so we can get the diff when apply changes next time 
    [ $err_no -eq 0 ] && back_conf_file

    #TODO: start monitor scripts which start up ceph processes automatically  
    return $err_no 
}


log "Start to run $0 with arguments: $*"

arg=$1

if [ "$arg" == "deploy" ] ; then
    echo -e "\033[31m Ceph集群将要被重新部署，所有磁盘将会被格式化，现有数据将永久丢失，无法找回。请确认： \033[0m"
    echo -e "\033[31m      1. 操作正确 \033[0m"
    echo -e "\033[31m      2. 被操作集群正确 \033[0m"
    echo -e "\033[31m 正确请输入yes；否则请输入no: \033[0m"
    read answer
    if [ X"$answer" != "Xyes" ] ; then
        log "你的输入不是yes，退出。"
        exit 0
    else
        log "你的输入是yes，操作将继续……"
    fi

    log "re-deploy ceph cluster ..."
    re_deploy
    if [ $? -eq 0 ] ; then
        log "Succeeded"
        exit 0
    fi
    log "Failed"
    exit 1
elif [ "$arg" == "change" ] ; then
    echo -e "\033[31m Ceph集群将要被修改，若有新增OSD，则对应磁盘将会被格式化，现有数据将永久丢失，无法找回。请确认： \033[0m"
    echo -e "\033[31m      1. 操作正确 \033[0m"
    echo -e "\033[31m      2. 被操作集群正确 \033[0m"
    echo -e "\033[31m 正确请输入yes；否则请输入no: \033[0m"
    read answer
    if [ X"$answer" != "Xyes" ] ; then
        log "你的输入不是yes，退出。"
        exit 0
    else
        log "你的输入是yes，操作将继续……"
    fi

    log "apply configuration changes to ceph cluster ..."
    apply_conf_change
    if [ $? -eq 0 ] ; then
        log "Succeeded"
        exit 0
    fi
    log "Failed"
    exit 1
else
    log "Error: Argument is invalid"
    log "Usage: $0 deploy  <-- Re-deploy the ceph cluster"
    log "       $0 change  <-- Apply configuration changes"
    exit 1
fi
