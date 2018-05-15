#!/bin/bash

dir=`dirname $0`
. $dir/../../s3cfg

uaccid=
useckey=

bucket=
agent=

function usage()
{
    echo "Usage: $0 -b {bucket} [-i {user-access-id} -k {user-secret-key}] [-a {user-agent}] -h"
}

while getopts "b:o:f:a:i:k:h" opt ; do
    case $opt in
        b)
            bucket=$OPTARG
            ;;
        a)
            agent=$OPTARG
            ;;
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
            echo "Error: unrecognized option $opt"
            usage
            exit 1
            ;;
    esac
done

if [[ -z "$bucket" ]]  ; then
    echo "Error: argument bucket or object is missing"
    usage
    exit 1
fi

if [[ -z "$uaccid" ]] || [[ -z "$useckey" ]] ; then
    uaccid=$token
    useckey=$secret
fi

if [[ -z "$uaccid" ]] || [[ -z "$useckey" ]] ; then
    echo "Error: user-access-id or user-secret-key is missing ,and there is no default value"
    usage
    exit 1
fi


host=127.0.0.1:6081
date=$(date --utc -R)
header="DELETE\n\ntext/plain\n${date}\n/$bucket?lifecycle"


sig=$(echo -en ${header} | openssl sha1 -hmac ${useckey} -binary | base64)

curl -v -H "Date: ${date}" -H "Expect:" -H "Content-Type: text/plain" -H "Authorization: AWS ${uaccid}:${sig}" -L -X DELETE "http://$host/$bucket?lifecycle" -H "Host: $host"

