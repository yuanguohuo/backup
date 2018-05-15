#!/bin/bash

dir=`dirname $0`
. $dir/s3cfg

host=192.168.137.128:8000
date=$(date -R)
header="GET\n\n\n${date}\n/"

#StringToSign = HTTP-Verb + "\n" +   --> GET \n
#    Content-MD5 + "\n" +            --> "" \n
#    Content-Type + "\n" +           --> "" \n
#    Date + "\n" +                   --> ${date} \n
#    CanonicalizedAmzHeaders +       --> "" 
#    CanonicalizedResource;          --> /

sig=$(echo -en ${header} | openssl sha1 -hmac ${secret} -binary | base64)

curl -v -H "Date: ${date}" -H "Authorization: AWS ${token}:${sig}" -L -X GET "http://$host/" -H "Host: $host"
