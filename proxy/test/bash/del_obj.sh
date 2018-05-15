#!/bin/bash

dir=`dirname $0`
. $dir/s3cfg

bucket=
objname=
agent=



function usage()
{
    echo "Usage delete_obj.sh -b {bucket} -o {object} [-a {user-agent}] -h"
}


while getopts "b:o:a:h" opt ; do
    case $opt in 
        b)
            bucket=$OPTARG
            ;;
        o)
            objname=$OPTARG
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

if [[ -z "$bucket" ]] || [[ -z "$objname" ]] ; then
    echo "Error: argument bucket or object is missing"
    usage
    exit 1
fi

host=127.0.0.1:6081
date=$(date --utc -R)
header="DELETE\n\ntext/plain\n${date}\n/$bucket/$objname"

sig=$(echo -en ${header} | openssl sha1 -hmac ${secret} -binary | base64)

if [ -z "$agent" ] ; then
  curl -v                                     \
      -H "Date: ${date}"                      \
      -H "Host: $host"                        \
      -H "Expect:"                            \
      -H "Content-Type: text/plain"           \
      -H "Authorization: AWS ${token}:${sig}" \
      -L -X DELETE "http://$host/$bucket/$objname"
else
  curl -v                                     \
      -H "Date: ${date}"                      \
      -H "Host: $host"                        \
      -H "Expect:"                            \
      -H "Content-Type: text/plain"           \
      -H "Authorization: AWS ${token}:${sig}" \
      -H "User-Agent: $agent"                 \
      -L -X DELETE "http://$host/$bucket/$objname"
fi
