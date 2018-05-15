#!/bin/bash

dir=`dirname $0`
if [[ -f $dir/s3cfg ]] ; then
. $dir/s3cfg
fi

uid=
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

if [[ -z "$host" ]] ; then
    host=127.0.0.1:6080
fi

date=$(date --utc -R)
header="${DELETE}\n\ntext/plain\n${date}\n/$uid/"
sig=$(echo -en ${header} | openssl sha1 -hmac ${seckey} -binary | base64)

curl -v                                       \
    -H "Date: ${date}"                        \
    -H "Host: $host"                          \
    -H "Expect:"                              \
    -H "Content-Length: 0"                    \
    -H "Content-Type: text/plain"             \
    -H "Authorization: AWS ${accid}:${sig}"  \
    -L -X DELETE "http://$host/admin/user?uid=$uid"
