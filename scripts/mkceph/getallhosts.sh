#!/bin/bash

function get_allhosts()
{
    local conf=$1
    [ -z "$conf" ] && conf=/etc/ceph/ceph.conf

    local tmpf=/tmp/allhosts.`date +%s`
    touch $tmpf
    echo "" > $tmpf

    local mons=""
    local osds=""
    local mdss=""
    if [ -z "$2" ] ; then #no args, get hosts for all daemons
        mons=`ceph-conf -c $conf -l mon | egrep -v '^mon$' | sort`
        osds=`ceph-conf -c $conf -l osd | egrep -v '^osd$' | sort`
        mdss=`ceph-conf -c $conf -l mds | egrep -v '^mds$' | sort`
    else
        while [ -n "$2" ] ; do
            case $2 in
                mon)
                    mons=`ceph-conf -c $conf -l mon | egrep -v '^mon$' | sort`
                    ;;
                osd)
                    osds=`ceph-conf -c $conf -l osd | egrep -v '^osd$' | sort`
                    ;;
                mds)
                    mdss=`ceph-conf -c $conf -l mds | egrep -v '^mds$' | sort`
                    ;;
                *)
                    log "WARN: invalid arg ($2) for get_allhosts"
                    ;;
            esac
            shift
        done
    fi

    local alldaems="$mons $osds $mdss"

    for name in $mons $osds $mdss; do
        host=`ceph-conf -c $conf -n $name "host"`
        echo $host >> $tmpf
    done

    local allhosts=`sort $tmpf | uniq`
    rm -f $tmpf

    echo "$allhosts"
}
