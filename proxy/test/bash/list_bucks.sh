#!/bin/bash

dir=`dirname $0`
if [[ -f $dir/s3cfg ]] ; then
. $dir/s3cfg
fi

uaccid=
useckey=

function usage()
{
    echo "Usage: $0 [-i {user-access-id} -k {user-secret-key}]"
}

while getopts "i:k:h" optname ; do
    case $optname in
        i)
            uaccid=$OPTARG
            ;;
        k)
            useckey=$OPTARG
            ;;
        h)
            usage
            exit 0
            ;;
        *)
            echo "Error: unrecognized option $optname"
            usage
            exit 1
            ;;
    esac
done

if [[ -z "$uaccid" ]] || [[ -z "$useckey" ]] ; then
    uaccid=$token
    useckey=$secret
fi

host=127.0.0.1:6081
date=$(date -R --utc)
header="GET\n\n\n${date}\n/"

sig=$(echo -en ${header} | openssl sha1 -hmac ${useckey} -binary | base64)

curl -v                                     \
    -H "Host: $host"                        \
    -H "Date: ${date}"                      \
    -H "Connection: keep-alive"             \
    -H "Authorization: AWS ${uaccid}:${sig}" \
    -L -X GET "http://$host/"  
