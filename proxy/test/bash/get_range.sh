#!/bin/bash

dir=`dirname $0`
. $dir/s3cfg

bucket=$1
objname=$2
begin=$3
end=$4
file=$5

if [[ -z "$bucket" ]] || [[ -z "$objname" ]] || [[ -z "$file" ]] ; then
    echo "Error: argument missing"
    echo "Usage: {bucket-name} {object-name} {begin} {end} {local-file}"
    exit 1
fi

host=127.0.0.1:6081
date=$(date -R --utc)
header="GET\n\ntext/plain\n${date}\n/$bucket/$objname"

#StringToSign = HTTP-Verb + "\n" +   --> PUT \n
#    Content-MD5 + "\n" +            --> "" \n
#    Content-Type + "\n" +           --> "text/plain" \n
#    Date + "\n" +                   --> ${date} \n
#    CanonicalizedAmzHeaders +       --> "" 
#    CanonicalizedResource;          --> /$bucket/$objname

sig=$(echo -en ${header} | openssl sha1 -hmac ${secret} -binary | base64)

curl -v -H "Date: ${date}" -H "Expect:" -H "Content-Type: text/plain" -H "Range: bytes=$begin-$end" -H "Authorization: AWS ${token}:${sig}" -L -X GET "http://$host/$bucket/$objname" -H "Host: $host"  > $file
