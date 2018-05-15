#!/bin/bash

dir=`dirname $0`
. $dir/s3cfg

bucket=
objname=
src_obj=
agent=

function usage()
{
    echo "Usage: put_obj.sh -b {bucket} -o {object} -s {src-obj} [-a {user-agent}] -h"
}

while getopts "b:o:s:a:h" opt ; do
    case $opt in
        b)
            bucket=$OPTARG
            ;;
        o)
            objname=$OPTARG
            ;;
        s)
            src_obj=$OPTARG
            ;;
        a)
            agent=$OPTARG
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

if [[ -z "$bucket" ]] || [[ -z "$objname" ]] || [[ -z "$src_obj" ]] ; then
    echo "Error: argument bucket or object is missing"
    usage
    exit 1
fi

host=127.0.0.1:6081
date=$(date --utc -R)

header="PUT\n\ntext/plain\n${date}\nx-amz-copy-source:${src_obj}\n/$bucket/$objname"
#StringToSign = HTTP-Verb + "\n" +   --> PUT \n
#    Content-MD5 + "\n" +            --> "" \n
#    Content-Type + "\n" +           --> "text/plain" \n
#    Date + "\n" +                   --> ${date} \n
#    CanonicalizedAmzHeaders +       --> "" 
#    CanonicalizedResource;          --> /$bucket/$objname

sig=$(echo -en ${header} | openssl sha1 -hmac ${secret} -binary | base64)

curl -v                \
    -H "Date: ${date}" \
    -H "Host: $host"   \
    -H "Expect:"       \
    -H "x-amz-copy-source: $src_obj"        \
    -H "Content-Type: text/plain"           \
    -H "Authorization: AWS ${token}:${sig}" \
    -L -X PUT "http://$host/$bucket/$objname"
