#!/bin/bash

[ -z "$CEPH_HOME" ] && CEPH_HOME=/usr/local/ceph

LOG_FILE=$1
. $CEPH_HOME/log.sh

function dev_uuid()
{
    local dev=$1
    local uuid=`blkid $dev | sed -e 's/.*\<UUID\>="\([-0-9a-fA-F]*\)".*/\1/'`
    echo $uuid
}

#umount a device and remove it from fstab 
#params: $1, device
function umount_by_uuid()
{
    local dev=$1
    local uuid=`dev_uuid $dev`
    local fstab=/etc/fstab

    local info=`umount $dev`
    if [ $? -ne 0 ] ; then
        echo $info | grep "not mounted" > /dev/null 2>&1
        if [ $? -ne 0 ] ; then
            log "unable to umount $dev"
            return 1
        fi
    fi

    sed -i /$uuid/d $fstab

    return 0
}

#mount a device and add it to fstab 
#params: $1-device; $2-mountpoint; $3 fstype; $4-mount options
function mount_by_uuid()
{
    local fstab=/etc/fstab

    local dev=$1
    local mnt=$2
    local fst=$3
    local opt=$4
    [ -z "$opt" ] && opt=defaults

    local retry=0
    while [ $retry -lt 5 ] ; do
        log "mount filesystem: mount -t $fst $dev $mnt -o $opt"
        mount -t $fst $dev $mnt -o $opt 
        [ $? -eq 0 ] && break
        retry=`expr $retry + 1`
        sleep 1
    done

    if [ $retry -ge 5 ] ; then
        log "cannot mount device $dev on $mnt"
        return 1
    fi

    local uuid=`dev_uuid $dev`
    [ -z "$uuid" ] && log "Cannot find UUID for $dev" && return 1
    log "$dev has UUID $uuid"

    sed -i /$uuid/d $fstab

    local item="UUID=$uuid    $mnt    $fst    $opt    0    0"
    sed -i -e "$ a $item" $fstab

    return 0
}

#we use label to replacy UUID now. so I add the following 5 functions;

#get the device from the label
function label2dev()
{
    local label=$1
    for d in {a..z} ; do
        local tmp=`xfs_admin -l /dev/sd$d 2> /dev/null | cut -d "\"" -f 2`
        if [ "$label" == "$tmp" ] ; then
            echo sd$d
            return 0
        fi 
    done
    for d in {a..z} ; do
        local tmp=`xfs_admin -l /dev/xvd$d 2> /dev/null | cut -d "\"" -f 2`
        if [ "$label" == "$tmp" ] ; then
            echo xvd$d
            return 0
        fi 
    done
    return 1
}

#if a device who has the given label exists
function label_exists()
{
    local label=$1
    local dev=`label2dev $label`

    if [ -z "$dev" ] ; then
        log "label_exists false: cannot find device whose label is $label"
        return 1
    fi

    dev="/dev/$dev"

    if [ ! -b "$dev" ] ; then
        log "label_exists false: $dev whose label is $label is not a block device"
        return 1
    fi

    return 0
}

#remove all mounts which was mounted by UUID in fstab; it is out-dated;
function rm_legacy_uuid_mnt()
{
    local fstab=/etc/fstab
    sed -i /"UUID=.*\/data\/cache\/osd"/d $fstab
}

#umount a device by label and remove it from fstab 
function umount_by_label()
{
    local fstab=/etc/fstab
    local label=$1

    local dev=`label2dev $label`
    if [ -z "$dev" ] ; then
        log "cannot find device whose label is $label"
        return 1
    fi

    dev="/dev/$dev"

    if [ ! -b "$dev" ] ; then
        log "$dev whose label is $label is not a block device"
        return 1
    fi

    local info=`umount $dev`
    if [ $? -ne 0 ] ; then
        echo $info | grep "not mounted" > /dev/null 2>&1
        if [ $? -ne 0 ] ; then
            log "unable to umount $dev whose label is $label"
            return 1
        fi
    fi

    sed -i /"LABEL=$label[     ]"/d $fstab

    return 0
}

#mount a device by label and add it to fstab 
function mount_by_label()
{
    local fstab=/etc/fstab

    local label=$1
    local path=$2
    local fs_type=$3
    local opts=$4

    [ -z "$opts" ] && opts=defaults

    local retry=0
    while [ $retry -lt 5 ] ; do
        log "mount filesystem: mount -t $fs_type -o $opts LABEL=$label $path"
        mount -t $fs_type -o $opts LABEL=$label $path
        [ $? -eq 0 ] && break
        retry=`expr $retry + 1`
        sleep 1
    done

    if [ $retry -ge 5 ] ; then
        log "cannot mount device whose label is $label to $path"
        return 1
    fi


    sed -i /"LABEL=$label[     ]"/d $fstab
    local item="LABEL=$label    $path    $fs_type    $opts    0    0"
    sed -i -e "$ a $item" $fstab

    return 0
}

case $2 in     # $1 is the LOG_FILE
    umount_by_uuid)
        dev=$3
        umount_by_uuid $dev
        exit 0
        ;;
    mount_by_uuid)
        mount_by_uuid $3 $4 $5 $6 
        [ $? -ne 0 ] && log "failed to add device $3 to fstab by uuid" && exit 1 
        exit 0
        ;;
    dev_uuid)
        dev=$3
        dev_uuid $dev
        exit 0
        ;;
    rm_legacy_uuid_mnt)
        rm_legacy_uuid_mnt
        exit 0
        ;;
    umount_by_label)
        umount_by_label $3
        [ $? -ne 0 ] && log "failed to umount device by label" && exit 1 
        exit 0
        ;;
    mount_by_label)
        mount_by_label $3 $4 $5 $6
        [ $? -ne 0 ] && log "failed to mount device by label" && exit 1
        exit 0
        ;;
    label_exists)
        label_exists $3
        [ $? -ne 0 ] && log "device with label $3 does not exist" && exit 1
        exit 0
        ;;
    *)
        log "invalid command for mnt.sh"
        exit 0
        ;;
esac
