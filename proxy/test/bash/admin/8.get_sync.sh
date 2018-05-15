#!/bin/bash

dir=`dirname $0`
if [[ -f $dir/s3cfg ]] ; then
. $dir/s3cfg
fi

uid=
bucket=
method=
host=
id=
#host to connect
function usage()
{
    echo "Usage: $0 -u {uid} [-b {bucket}] [-s {host}]" 

}

while getopts "u:b:h" opt ; do
    case $opt in 
        u)
            uid=$OPTARG
            ;;
        b)
            bucket=$OPTARG
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

if [[ -z "$uid" ]] ; then
    echo "Error: argument uid is missing"
    usage
    exit 1
fi

if [[ -z "$accid" ]] || [[ -z "$seckey" ]] ; then
    accid="1WYCCJZ9JRLWZU8JTDQJ"
    seckey="PXhbQDJVeF1PsXw5tsCuIaKY0N8s1BP2J3yCn9K3"
fi

if [[ -z "$method" ]] ; then
    method="GET"
fi
if [[ -z "$host" ]] ; then
    host=101.71.5.235:6080
fi

length=0
if [ -n "$file" ] ; then
    length=`ls -l $file | tr -s ' ' | cut -d ' ' -f 5`
fi

date=$(date --utc -R)
uri="/admin/sync"
stringtosign="${method}\n\ntext/plain\n${date}\n$uri"
echo -e "stringtosign is: $stringtosign"

sig=$(echo -en ${stringtosign} | openssl sha1 -hmac ${seckey} -binary | base64)
args="uid=$uid"
if [[ -n "$bucket" ]] ; then
    args=$args"&bucket=$bucket"
fi

echo -e "args is $args"

curl -v                                       \
    -H "Date: ${date}"                        \
    -H "Host: $host"                          \
    -H "Expect:"                              \
    -H "Content-Length: $length"                    \
    -H "Content-Type: text/plain"             \
    -H "Authorization: AWS ${accid}:${sig}"  \
    -X $method "http://$host$uri?$args"      \
