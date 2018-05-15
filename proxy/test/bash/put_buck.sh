#!/bin/bash

dir=`dirname $0`
if [[ -f $dir/s3cfg ]] ; then
. $dir/s3cfg
fi

bucket=
uaccid=
useckey=
method=
#get/delete
host=
#host to connect
function usage()
{
    echo "Usage: $0 -b {bucket} [-s {host}] [-m {method}] [-i {user-access-id} -k {user-secret-key}]"

}

while getopts "s:m:b:i:k:h" opt ; do
    case $opt in 
        m)
            method=$OPTARG
            ;;
        b)
            bucket=$OPTARG
            ;;
        i)
            uaccid=$OPTARG
            ;;
        k)
            useckey=$OPTARG
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

if [[ -z "$bucket" ]] ; then
    echo "Error: argument bucket is missing"
    usage
    exit 1
fi

if [[ -z "$uaccid" ]] || [[ -z "$useckey" ]] ; then
    uaccid=$token
    useckey=$secret
fi

if [[ -z "$uaccid" ]] || [[ -z "$useckey" ]] ; then
    echo "Error: user-access-id or user-secret-key is missing, and there is no default value"
    usage
    exit 1
fi


if [[ -z "$method" ]] ; then
    method="PUT"
fi
if [[ -z "$host" ]] ; then
    host=127.0.0.1:6081
fi
#host=127.0.0.1:6081
date=$(date --utc -R)
header="${method}\n\ntext/plain\n${date}\n/$bucket/"

sig=$(echo -en ${header} | openssl sha1 -hmac ${useckey} -binary | base64)

curl -v                                       \
    -H "Date: ${date}"                        \
    -H "Host: $host"                          \
    -H "Expect:"                              \
    -H "Content-Length: 0"                    \
    -H "Content-Type: text/plain"             \
    -H "Authorization: AWS ${uaccid}:${sig}"  \
    -L -X $method "http://$host/$bucket/"
