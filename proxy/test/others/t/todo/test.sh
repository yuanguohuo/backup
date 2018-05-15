#!/bin/bash
#set -x
token=O911PT5Z34WN8Q92C8YU ## USER_TOKEN
secret=l0fjx5mHz3KoOOiiYILc6JtpXqVJNOCpYJXUlOIZ ## USER_SECRET
query=
date=$(date -R)
#query3="&uid="
query2=$1
xmlon=$2
#date=$(date -R)
#date=$(TZ=GMT date -u -R)
#date= "五, 02 9月 2016 09:00:56 +0000"
##date=$(date +"%a, %d %b %Y %H:%M:%S GMT")
header="GET\n\n\n${date}\n/t/${query2}"
#header="GET\n\n\n${date}\n/${query2}"
#header="GET\n\n\nThu, 28 Jul 2016 07:11:07 GMT\n/${query2}"
sig=$(echo -en ${header} | openssl sha1 -hmac ${secret} -binary | base64)
#curl -v -H "Date: ${date}" -H "Authorization: AWS ${token}:${sig}" -L -X GET "http://192.168.122.214:7480/${query2}?format=xml${query3}${query}" -H "Host: 192.168.122.214:7480"
#curl -v -H "Date: ${date}" -H "Authorization: AWS ${token}:${sig}" -L -X GET "http://192.168.122.24:8081/${query2}?format=${xmlon}${query3}${query}" -H "Host: 192.168.122.24:8081"
curl -v -H "Date: ${date}" -H "Authorization: AWS ${token}:${sig}" -L -X GET "http://192.168.122.24:8081/t/${query2}?format=${xmlon}${query3}${query}" -H "Host: 192.168.122.24:8081"
#curl -v -H "Date: Thu, 28 Jul 2016 07:11:07 GMT" -H "Authorization: AWS ${token}:${sig}" -L -X GET "http://192.168.122.214:7480/${quer

