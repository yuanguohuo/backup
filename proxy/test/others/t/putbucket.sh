#!/bin/bash
#set -x
token=O911PT5Z34WN8Q92C8YU ## USER_TOKEN
secret=l0fjx5mHz3KoOOiiYILc6JtpXqVJNOCpYJXUlOIZ ## USER_SECRET
query=
date=$(date -R)
#query3="&uid="
#query2=$1
#xmlon=$2
bucket=$1
#objname=$2
#file=$3
length=0
host=192.168.122.170:8000
#length=`ls -l $file | tr -s ' ' | cut -d ' ' -f 5`
#date=$(date -R)
#date=$(TZ=GMT date -u -R)
#date=$(date +"%a, %d %b %Y %H:%M:%S GMT")
#header="GET\n\n\n${date}\n/${query2}"
header="PUT\n\ntext/plain\n${date}\n/$bucket"
#header="GET\n\n\nThu, 28 Jul 2016 07:11:07 GMT\n/${query2}"
sig=$(echo -en ${header} | openssl sha1 -hmac ${secret} -binary | base64)
#curl -v -H "Date: ${date}" -H "Authorization: AWS ${token}:${sig}" -L -X GET "http://192.168.122.214:7480/${query2}?format=xml${query3}${query}" -H "Host: 192.168.122.214:7480"
#curl -v -H "Date: ${date}" -H "Authorization: AWS ${token}:${sig}" -L -X GET "http://192.168.122.24:8081/${query2}?format=${xmlon}${query3}${query}" -H "Host: 192.168.122.24:8081"
#curl -v -H "Date: Thu, 28 Jul 2016 07:11:07 GMT" -H "Authorization: AWS ${token}:${sig}" -L -X GET "http://192.168.122.214:7480/${query2}?format=json${query3}${query}" -H "Host: 192.168.122.214:7480"
#Change IPs with your own IPs
curl -v -H "Date: ${date}" -H "Expect:" -H "Content-Length: ${length}" -H "Content-Type: text/plain" -H "Authorization: AWS ${token}:${sig}" -L -X PUT "http://$host/$bucket/" -H "Host: $host"

