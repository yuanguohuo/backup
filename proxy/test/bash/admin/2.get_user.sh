#!/bin/bash

dir=`dirname $0`
if [[ -f $dir/s3cfg ]] ; then
. $dir/s3cfg
fi

uid=
method=
host=
id=
key=
#host to connect
function usage()
{
    echo "Usage: $0 -u {uid} [-s {host}]" 

}

while getopts "u:h" opt ; do
    case $opt in 
        u)
            uid=$OPTARG
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

date=$(date --utc -R)
stringtosign="${method}\n\ntext/plain\n${date}\n/admin/user"
echo $stringtosign
sig=$(echo -en ${stringtosign} | openssl sha1 -hmac ${seckey} -binary | base64)

curl -v                                       \
    -H "Date: ${date}"                        \
    -H "Host: $host"                          \
    -H "Expect:"                              \
    -H "Content-Length: 0"                    \
    -H "Content-Type: text/plain"             \
    -H "Authorization: AWS ${accid}:${sig}"  \
    -L -X $method "http://$host/admin/user?uid=$uid"
