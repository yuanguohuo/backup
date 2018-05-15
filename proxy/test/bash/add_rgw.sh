#!/bin/bash

dir=`dirname $0`
if [[ -f $dir/s3cfg ]] ; then
. $dir/s3cfg
fi

ceph_id=
server=
port=

function usage()
{
    echo "Usage: $0 -i {ceph-id} -s {rgw-server} -p {rgw-port}"
}

while getopts "i:s:p:h" opt ; do
    case $opt in
        i)
            ceph_id=$OPTARG
            ;;
        s)
            server=$OPTARG
            ;;
        p)
            port=$OPTARG
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
header="${method}\n\ntext/plain\n${date}\n/admin/rgw/add"
sig=$(echo -en ${header} | openssl sha1 -hmac ${root_secret} -binary | base64)

args="cid=$ceph_id&server=$server&port=$port"

curl -v                                             \
    -H "Date: ${date}"                              \
    -H "Host: $admin_host"                          \
    -H "Expect:"                                    \
    -H "Content-Length: 0"                          \
    -H "Content-Type: text/plain"                   \
    -H "Authorization: AWS ${root_token}:${sig}"    \
    -L -X $method "http://$admin_host/admin/rgw/add?$args"
