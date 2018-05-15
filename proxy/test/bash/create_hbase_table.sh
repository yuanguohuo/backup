#!/bin/bash

##### user #####
#disable 'user'
#drop 'user'
#create 'user', 'info', 'quota', 'stats', 'ver', 'exattrs'
# now quota mean user quota,bucket_quota means per bucket.
create 'user', 'info', 'quota', 'stats', 'ver', 'exattrs', 'bucket_quota'

#root user
put 'user',  '1WYCCJZ9JRLWZU8JTDQJ', 'info:uid', 'root'
put 'user',  '1WYCCJZ9JRLWZU8JTDQJ', 'info:type', 'fs'
put 'user',  '1WYCCJZ9JRLWZU8JTDQJ', 'info:secretkey', 'PXhbQDJVeF1PsXw5tsCuIaKY0N8s1BP2J3yCn9K3' 
put 'user',  '1WYCCJZ9JRLWZU8JTDQJ', 'info:displayname', 'rootdnionstore'
put 'user',  '1WYCCJZ9JRLWZU8JTDQJ', 'info:mtime', `date --utc --rfc-2822 | xargs echo -n`
put 'user',  '1WYCCJZ9JRLWZU8JTDQJ', 'info:maxbuckets', 1024
put 'user',  '1WYCCJZ9JRLWZU8JTDQJ', 'quota:enabled', 'yes'
put 'user',  '1WYCCJZ9JRLWZU8JTDQJ', 'quota:objects', 20480
put 'user',  '1WYCCJZ9JRLWZU8JTDQJ', 'quota:size_kb', 20480000
incr 'user', '1WYCCJZ9JRLWZU8JTDQJ', 'stats:objects', 0
incr 'user', '1WYCCJZ9JRLWZU8JTDQJ', 'stats:size_bytes', 0
put 'user',  '1WYCCJZ9JRLWZU8JTDQJ', 'ver:tag', 'ROOT'
put 'user', '1WYCCJZ9JRLWZU8JTDQJ', 'ver:version', '0'
put 'user',  '1WYCCJZ9JRLWZU8JTDQJ', 'exattrs:company', 'dnion'

# userid,rowkey is userid/uid,for admin use
#disable 'userid'
#drop 'userid'
#create 'userid', 'info', 'quota', 'stats', 'ver', 'exattrs'
create 'userid', 'info', 'ver'
put 'userid',  'root', 'info:accessid', '1WYCCJZ9JRLWZU8JTDQJ'

##### bucket #####
#disable 'bucket'
#drop 'bucket'
create 'bucket', 'info', 'quota', 'stats', 'ver', 'exattrs'

##### object #####
#disable 'object'
#drop 'object'
create 'object', 'info', 'ver', 'mfest', 'sattrs', 'uattrs', 'hdata'

#### temp_object ####
#disable 'temp_object'
#drop 'temp_object'
create 'temp_object','info','ver','mfest','flag','sattrs','uattrs'

#### temp_part ####
#disable 'temp_part'
#drop 'temp_part'
create 'temp_part','info','ver'

##### delete_object #####
#disable 'delete_object'
#drop 'delete_object'
create 'delete_object', 'info', 'stats', 'ver', 'mfest', 'sattrs', 'uattrs', 'hdata'

##### lifecycle #####
#disable 'lifecycle'
#drop 'lifecycle'
create 'lifecycle', 'info', 'ver'

##### expire_object #####
#disable 'expire_object'
#drop 'expire_object'
create 'expire_object', 'info', 'ver'

##### datalog #####
#disable 'datalog'
#drop 'datalog'
create 'datalog', 'info', 'ver'

##### log_marker #####
#disable 'log_marker'
#drop 'log_marker'
create 'log_marker', 'info', 'ver'

##### placeholder #####
#disable 'placeholder'
#drop 'placeholder'
create 'placeholder', 'info', 'ver'

##### full_marker #####
#disable 'full_marker'
#drop 'full_marker'
create 'full_marker', 'info', 'ver'

put 'full_marker',  'fullsync', 'info:mtime', `date --utc --rfc-2822 | xargs echo -n`
put 'full_marker',  'fullsync', 'info:id', '11111111111111111111111111111111'
put 'full_marker',  'fullsync', 'info:status', 'complete'
##### delete_bucket #####
#disable 'delete_bucket'
#drop 'delete_bucket'
create 'delete_bucket', 'info', 'quota', 'stats', 'ver', 'exattrs'

##### metalog #####
#disable 'metalog'
#drop 'metalog'
create 'metalog', 'info'

##### ceph clusters #####
#disable 'ceph'
#drop 'ceph'
create 'ceph', 'info'
put 'ceph',  'ceph-1', 'info:state', 'OK'
put 'ceph',  'ceph-1', 'info:weight', '100'

##### rgw list of each ceph cluster #####
#disable 'rgw'
#drop 'rgw'
create 'rgw', 'info'
put 'rgw',  'ceph-1-127.0.0.1:8000', 'info:state', 'OK'

exit
