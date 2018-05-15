#!/bin/bash

dir=`dirname $0`
if [[ -f $dir/s3cfg ]] ; then
. $dir/s3cfg
fi

ceph_id=
ceph_state=
ceph_weight=

function usage()
{
    echo "Usage: $0 -i {id} -s {state} -w {weight}"
}

while getopts "i:s:w:h" opt ; do
    case $opt in
        i)
            ceph_id=$OPTARG
            ;;
        s)
            ceph_state=$OPTARG
            ;;
        w)
            ceph_weight=$OPTARG
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

method="PUT"

date=$(date --utc -R)
header="${method}\n\ntext/plain\n${date}\n/admin/ceph/set"
sig=$(echo -en ${header} | openssl sha1 -hmac ${root_secret} -binary | base64)

args="id=$ceph_id&state=$ceph_state&weight=$ceph_weight"

curl -v                                             \
    -H "Date: ${date}"                              \
    -H "Host: $admin_host"                          \
    -H "Expect:"                                    \
    -H "Content-Length: 0"                          \
    -H "Content-Type: text/plain"                   \
    -H "Authorization: AWS ${root_token}:${sig}"    \
    -L -X $method "http://$admin_host/admin/ceph/set?$args"
