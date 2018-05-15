#!/bin/bash

#ceph use IPADDR as hostname. the my_hostname actually returns the IPADDR of
#current host.

function my_hostname()
{
    local conf=$1
    [ -z "$conf" ] && conf=/etc/ceph/ceph.conf

    local osds=`ceph-conf -c $conf -l osd | egrep -v '^osd$' | sort`
    local mons=`ceph-conf -c $conf -l mon | egrep -v '^mon$' | sort`
    local mds=`ceph-conf -c $conf -l mds | egrep -v '^mds$' | sort`

    for entity in $osds $mons $mds; do
        host=`ceph-conf -c $conf -n $entity "host"`
        # use command "ifconfig"
        ifconfig > /dev/null 2>&1
        if [ $? -eq 0 ] ; then
            ifconfig | grep -w $host > /dev/null 2>&1
            if [ $? -eq 0 ] ; then
                echo $host
                return 0
            else
                continue
            fi
        fi

        # use command "ip addr"
        ip addr > /dev/null 2>&1
        if [ $? -eq 0 ] ; then
            ip addr | grep -w $host > /dev/null 2>&1
            if [ $? -eq 0 ] ; then
                echo $host
                return 0
            else
                continue
            fi
        fi
    done

    return 1
}
