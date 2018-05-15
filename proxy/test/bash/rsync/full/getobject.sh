#!/bin/bash

dir=`dirname $0`
. $dir/../../s3cfg

bucket="admin"
objname="log"
marker=$1
if [[ -z "$bucket" ]] || [[ -z "$objname" ]] ; then
    echo "Error: argument missing"
    echo "Usage: {bucket-name} {object-name} {local-file}"
    exit 1
fi

host=127.0.0.1:6080
#host=192.168.34.59:6080
#length=`ls -l $file | tr -s ' ' | cut -d ' ' -f 5`
date=$(date --utc -R)
header="GET\n\ntext/plain\n${date}\n/$bucket/$objname"

#StringToSign = HTTP-Verb + "\n" +   --> PUT \n
#    Content-MD5 + "\n" +            --> "" \n
#    Content-Type + "\n" +           --> "text/plain" \n
#    Date + "\n" +                   --> ${date} \n
#    CanonicalizedAmzHeaders +       --> "" 
#    CanonicalizedResource;          --> /$bucket/$objname

sig=$(echo -en ${header} | openssl sha1 -hmac ${secret} -binary | base64)

curl -v -H "Date: ${date}" -H "Expect:" -H "Content-Type: text/plain" -H "Authorization: AWS ${token}:${sig}" -L -X GET "http://$host/$bucket/$objname?type=metadata&id=objects&marker=$marker " -H "Host: $host"
