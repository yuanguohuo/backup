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
pid=
#host to connect
function usage()
{
    echo "Usage: $0 -u {uid} -i {id} -k {key} [-p {pid}] [-q {quota}] [-m {mode}] [-s {host}]" 

}

while getopts "u:i:k:p:q:m:h" opt ; do
    case $opt in 
        u)
            uid=$OPTARG
            ;;
        i)
            id=$OPTARG
            ;;
        k)
            key=$OPTARG
            ;;
        p)
            pid=$OPTARG
            ;;
        q)
            quota=$OPTARG
            ;;
        m)
            mode=$OPTARG
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

if [[ -z "$id" ]] ; then
    echo "Error: argument id is missing"
    usage
    exit 1
fi

if [[ -z "$key" ]] ; then
    echo "Error: argument key is missing"
    usage
    exit 1
fi

if [[ -z "$accid" ]] || [[ -z "$seckey" ]] ; then
    accid="1WYCCJZ9JRLWZU8JTDQJ"
    seckey="PXhbQDJVeF1PsXw5tsCuIaKY0N8s1BP2J3yCn9K3"
fi

if [[ -z "$method" ]] ; then
    method="PUT"
fi
if [[ -z "$host" ]] ; then
    host=127.0.0.1:6080
fi

date=$(date --utc -R)
stringtosign="${method}\n\ntext/plain\n${date}\n/admin/user"
echo -e "stringtosign is: $stringtosign\n"

sig=$(echo -en ${stringtosign} | openssl sha1 -hmac ${seckey} -binary | base64)
args="uid=$uid&accessid=$id&secretkey=$key"
if [[ -n "$pid" ]] ; then
    args=${args}"&parent_uid=$pid"
fi
if [[ -n "$mode" ]] ; then
    args=${args}"&mode=$mode"
fi
if [[ -n "$quota" ]] ; then
    args=${args}"&max-objects=$quota"
fi
echo -e "args is $args\n"

curl -v                                       \
    -H "Date: ${date}"                        \
    -H "Host: $host"                          \
    -H "Expect:"                              \
    -H "Content-Length: 0"                    \
    -H "Content-Type: text/plain"             \
    -H "Authorization: AWS ${accid}:${sig}"  \
    -L -X $method "http://$host/admin/user?$args"
