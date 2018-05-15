#!/bin/bash

dir=`dirname $0`
. $dir/s3cfg

bucket=$1
objname=$2

if [[ -z "$bucket" ]] || [[ -z "$objname" ]] ; then
    echo "Error: argument missing"
    echo "Usage: {bucket-name} {object-name}"
    exit 1
fi

host=127.0.0.1:6081
date=$(date --utc -R)
header="HEAD\n\n\n${date}\n/$bucket/$objname"

#StringToSign = HTTP-Verb + "\n" +   --> PUT \n
#    Content-MD5 + "\n" +            --> "" \n
#    Content-Type + "\n" +           --> "text/plain" \n
#    Date + "\n" +                   --> ${date} \n
#    CanonicalizedAmzHeaders +       --> "" 
#    CanonicalizedResource;          --> /$bucket/$objname

sig=$(echo -en ${header} | openssl sha1 -hmac ${secret} -binary | base64)

curl -v                                         \
    -H "Host: $host"                            \
    -H "Date: ${date}"                          \
    -H "Expect:"                                \
    -H "Authorization: AWS ${token}:${sig}"     \
    -L --head "http://$host/$bucket/$objname"
