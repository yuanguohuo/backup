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

function usage()
{
    echo "Usage: get_obj.sh -b {bucket} -o {object} -f {dest-file} [-i {user-access-id} -k {user-secret-key}]"
}

while getopts "b:o:f:i:k:h" opt ; do
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


if [[ -z "$bucket" ]] || [[ -z "$objname" ]] || [[ -z "$file" ]]; then
    echo "Error: argument bucket, object or dest-file is missing"
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
header="GET\n\ntext/plain\n${date}\n/$bucket/$objname"

#StringToSign = HTTP-Verb + "\n" +   --> PUT \n
#    Content-MD5 + "\n" +            --> "" \n
#    Content-Type + "\n" +           --> "text/plain" \n
#    Date + "\n" +                   --> ${date} \n
#    CanonicalizedAmzHeaders +       --> "" 
#    CanonicalizedResource;          --> /$bucket/$objname

sig=$(echo -en ${header} | openssl sha1 -hmac ${useckey} -binary | base64)

curl -v                                           \
    -H "Host: $host"                              \
    -H "Date: ${date}"                            \
    -H "Expect:"                                  \
    -H "Content-Type: text/plain"                 \
    -H "Authorization: AWS ${uaccid}:${sig}"      \
    -L -X GET "http://$host/$bucket/$objname" > $file
