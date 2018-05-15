#!/bin/bash

dir=`dirname $0`
if [[ -f $dir/s3cfg ]] ; then
. $dir/s3cfg
fi

uaccid=
useckey=

bucket=
objname=
file=
agent=

function usage()
{
    echo "Usage: $0 -b {bucket} -o {object} [-f {src-file}] [-i {user-access-id} -k {user-secret-key}] [-a {user-agent}] -h"
}

while getopts "b:o:f:a:i:k:h" opt ; do
    case $opt in
        b)
            bucket=$OPTARG
            ;;
        o)
            objname=$OPTARG
            ;;
        f)
            file=$OPTARG
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

if [[ -z "$bucket" ]] || [[ -z "$objname" ]] ; then
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

opt_T=
length=0
if [ -n "$file" ] ; then
    length=`ls -l $file | tr -s ' ' | cut -d ' ' -f 5`
    opt_T="-T $file"
fi

header="PUT\n\ntext/plain\n${date}\n/$bucket/$objname"
#StringToSign = HTTP-Verb + "\n" +   --> PUT \n
#    Content-MD5 + "\n" +            --> "" \n
#    Content-Type + "\n" +           --> "text/plain" \n
#    Date + "\n" +                   --> ${date} \n
#    CanonicalizedAmzHeaders +       --> "" 
#    CanonicalizedResource;          --> /$bucket/$objname

sig=$(echo -en ${header} | openssl sha1 -hmac ${useckey} -binary | base64)

if [ -z "$agent" ] ; then
  curl -v                \
      -H "Date: ${date}" \
      -H "Host: $host"   \
      -H "Expect:"       \
      -H "Content-Length: ${length}"          \
      -H "Content-Type: text/plain"           \
      -H "Authorization: AWS ${uaccid}:${sig}" \
      -L -X PUT "http://$host/$bucket/$objname" $opt_T 
else
  curl -v                \
      -H "Date: ${date}" \
      -H "Host: $host"   \
      -H "Expect:"       \
      -H "Content-Length: ${length}"          \
      -H "Content-Type: text/plain"           \
      -H "Authorization: AWS ${uaccid}:${sig}" \
      -H "User-Agent: $agent"                 \
      -L -X PUT "http://$host/$bucket/$objname" $opt_T
fi
