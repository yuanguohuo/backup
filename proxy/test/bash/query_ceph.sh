#!/bin/bash

dir=`dirname $0`
if [[ -f $dir/s3cfg ]] ; then
. $dir/s3cfg
fi

from=
ceph_id=

function usage()
{
    echo "Usage: $0 -f {hbase|memory} [-i {id}]"
}

while getopts "i:f:h" opt ; do
    case $opt in
        f)
            from=$OPTARG
            ;;
        i)
            ceph_id=$OPTARG
            ;;
        h)
            usage
            exit 0
            ;;
        *)
            echo "Error: unrecognized option $opt"
            usage
            exit 1
            ;;
    esac
done

host=127.0.0.1:6080

method="GET"

date=$(date --utc -R)
header="${method}\n\ntext/plain\n${date}\n/admin/ceph/get"
sig=$(echo -en ${header} | openssl sha1 -hmac ${root_secret} -binary | base64)

args="from=$from&id=$ceph_id"

curl -v                                             \
    -H "Date: ${date}"                              \
    -H "Host: $admin_host"                          \
    -H "Expect:"                                    \
    -H "Content-Length: 0"                          \
    -H "Content-Type: text/plain"                   \
    -H "Authorization: AWS ${root_token}:${sig}"    \
    -L -X $method "http://$admin_host/admin/ceph/get?$args"
