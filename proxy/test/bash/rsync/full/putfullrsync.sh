#!/bin/bash

dir=`dirname $0`
. $dir/../../s3cfg

sync_type=
remote_ip=

function usage()
{
    echo " Uaget: -p remote_ip -t sync_type:
                  1 sync all user. but unsync bucket,data. 
                  2 sync user,bucket.but unsync data.
                  3 sync user,bucket,data.
                  4 sync specify user.but unsync bucket,data.                 
                  5 sync specify user,all bucket. but unsync data.                                          
                  6 sync specify user,all bucket data.                                                      
                  7 sync specify user,specify bucket. but unsync data.                                      
                  8 sync specify user,specify bucket all data."
}
while getopts "p:t:h" opt ; do
    case $opt in
        p)
            remote_ip=$OPTARG
            ;;
        t)
            sync_type=$OPTARG
            ;;
        h)
            usage
            exit 0
            ;;
        *)
            usage
            exit 1
            ;;
    esac
done

if [[ -z "$remote_ip" ]]  ; then
    echo " Error: argument remote_ip is missing"
    usage
    exit 1
fi

if [[ -z "$sync_type" ]]  ; then
    echo " Error: argument sync_type is missing"
    usage
    exit 1
fi

bucket="sync"
objname="full"

if [[ -z "$bucket" ]] || [[ -z "$objname" ]] ; then
    echo "Error: argument missing"
    echo "Usage: {bucket-name} {object-name} {local-file}"
    exit 1
fi

host=127.0.0.1:6080
#length=`ls -l $file | tr -s ' ' | cut -d ' ' -f 5`
date=$(date --utc -R)
header="POST\n\ntext/plain\n${date}\n/$bucket/$objname"

#StringToSign = HTTP-Verb + "\n" +   --> PUT \n
#    Content-MD5 + "\n" +            --> "" \n
#    Content-Type + "\n" +           --> "text/plain" \n
#    Date + "\n" +                   --> ${date} \n
#    CanonicalizedAmzHeaders +       --> "" 
#    CanonicalizedResource;          --> /$bucket/$objname

sig=$(echo -en ${header} | openssl sha1 -hmac ${root_secret} -binary | base64)
body="{\"remote_ip\":\"$remote_ip\",\"remote_port\":\"6080\",\"remote_data_port\":\"6081\",\"sync_type\":\"$sync_type\",\"force\":\"true\"}"
curl -v -H "Date: ${date}" -H "Expect:" -H "Content-Type: text/plain" -H "Authorization: AWS ${root_token}:${sig}" -L -X POST "http://$host/$bucket/$objname" -H "Host: $host" -d $body
