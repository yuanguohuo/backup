#!/bin/bash

#==GET===
#return content is in json
curl -v -H "Accept: application/json" -X GET "http://192.168.100.130:8080/user/O6BJRVFDYY088T3AIO40/"
#return content is in xml
curl -v -H "Accept: text/xml" -X GET "http://192.168.100.130:8080/user/O6BJRVFDYY088T3AIO40/"

#==PUT===


