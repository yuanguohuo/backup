#!/bin/bash

dir=`dirname $0`
if [[ -f $dir/s3cfg ]] ; then
. $dir/s3cfg
fi

uid=
method=
host=
#host to connect
function usage()
{
    echo "Usage: $0 -u {uid} [-s {host}]" 

}

while getopts "u:h" opt ; do
    case $opt in 
        u)
            uid=$OPTARG
            ;;
        s)
            host=$OPTARG
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

if [[ -z "$uid" ]] ; then
    echo "Error: argument uid is missing"
    usage
    exit 1
fi

if [[ -z "$accid" ]] || [[ -z "$seckey" ]] ; then
    accid=$root_token
    seckey=$root_secret
fi

if [[ -z "$method" ]] ; then
    method="PUT"
fi
if [[ -z "$host" ]] ; then
    host=127.0.0.1:6080
fi

date=$(date --utc -R)
id="JTLA6O1TF69Z0YB4I7O1"
key="cwoNbM7TZLxYeMcmQxfiwL7n4Pv0JhPRlNG6m1dq"
#header="${method}\n\ntext/plain\n${date}\n/$uid/"
header="${method}\n\ntext/plain\n${date}\n/admin/user"
sig=$(echo -en ${header} | openssl sha1 -hmac ${seckey} -binary | base64)
args="uid=$uid&accessid=$id&secretkey=$key"
curl -v                                       \
    -H "Date: ${date}"                        \
    -H "Host: $host"                          \
    -H "Expect:"                              \
    -H "Content-Length: 0"                    \
    -H "Content-Type: text/plain"             \
    -H "Authorization: AWS ${accid}:${sig}"  \
    -L -X $method "http://$host/admin/user?$args"
