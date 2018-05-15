#!/bin/bash

dir=`dirname $0`
. $dir/s3cfg


function usage()
{
    echo "Usage: $0 -b {bucket} [-p {prefix}] [-m {marker}] [-d {delimiter}] [-n {max-keys}] [-a {user-agent}]"
}

bucket=
args=
agent=
maxkeys=


while getopts "b:p:m:d:n:a:h" opt ; do
    case $opt in
        b)
            bucket=$OPTARG
            ;;
        p)
            if [ -z "$args" ] ; then
                args="?prefix=$OPTARG"
            else
                args="$args&prefix=$OPTARG"
            fi
            ;;
        m)
            if [ -z "$args" ] ; then
                args="?marker=$OPTARG"
            else
                args="$args&marker=$OPTARG"
            fi
            ;;
        d)
            if [ -z "$args" ] ; then
                args="?delimiter=$OPTARG"
            else
                args="$args&delimiter=$OPTARG"
            fi
            ;;
        n)
            if [ -z "$args" ] ; then
                args="?max-keys=$OPTARG"
            else
                args="$args&max-keys=$OPTARG"
            fi
            ;;
        a)
            agent=$OPTARG
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

if [[ -z "$bucket" ]] ; then
    echo "Error: bucket is mandatory"
    usage
    exit 1
fi

echo args:   $args
echo bucket: $bucket

host=127.0.0.1:6081
date=$(date --utc -R)
header="GET\n\ntext/plain\n${date}\n/$bucket/"

sig=$(echo -en ${header} | openssl sha1 -hmac ${secret} -binary | base64)


if [ -z "$agent" ] ; then
    curl -v                                     \
        -H "Date: ${date}"                      \
        -H "Host: $host"                        \
        -H "Expect:"                            \
        -H "Content-Type: text/plain"           \
        -H "Authorization: AWS ${token}:${sig}" \
        -L -X GET "http://$host/$bucket/$args"
else
    curl -v                                     \
        -H "Date: ${date}"                      \
        -H "Host: $host"                        \
        -H "Expect:"                            \
        -H "Content-Type: text/plain"           \
        -H "Authorization: AWS ${token}:${sig}" \
        -H "User-Agent: $agent"                 \
        -L -X GET "http://$host/$bucket/$args"
fi
