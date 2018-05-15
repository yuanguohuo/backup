#use lib '/home/deploy/dobjstor/proxy/test/';
use t::MYTEST 'no_plan';
no_long_string();
log_level 'debug';
#use Test::Nginx::Socket::Lua 'no_plan'; 
no_root_location();

no_shuffle();
run_tests();
# problem :only one value
#$UPLOADID='12345678900987654321';
__DATA__

=== TEST 1: complete upload http://ip close 
--- raw_request eval
use POSIX;
use lib './t';
use MY::access qw(makereq);
use URI::Escape;
my $req1;
print 'test_1';
print `hbase shell<<EFO
disable 'bucket'
drop 'bucket'
create 'bucket', 'info', 'quota', 'stats', 'ver', 'exattrs'

put 'bucket', 'UUUU1234_bucket1', 'info:ctime', 'Thu, 11 Aug 2016 02:14:45 +0000'
put 'bucket', 'UUUU1234_bucket1', 'info:mtime', 'Thu, 11 Aug 2016 02:14:45 +0000'

put 'bucket', 'UUUU1234_bucket1', 'quota:enabled', 'yes'
put 'bucket', 'UUUU1234_bucket1', 'quota:size_mb', '1024000'
put 'bucket', 'UUUU1234_bucket1', 'quota:objects', '1024'
 
put 'bucket', 'UUUU1234_bucket1', 'ver:tag', 'BBBB1234'
put 'bucket', 'UUUU1234_bucket1', 'ver:version', '0'

put 'bucket', 'UUUU1234_bucket1', 'exattrs:content', 'movies'

disable 'object'
drop 'object'
create 'object', 'info', 'ver', 'mfest', 'sattrs', 'uattrs', 'hdata'

disable 'temp_object'
drop 'temp_object'
create 'temp_object','info','flag','sattrs','uattrs'

put 'temp_object', 'BBBB1234_12345678900987654321', 'info:bucket', 'bucket1'
put 'temp_object', 'BBBB1234_12345678900987654321', 'flag:flag', '0'

disable 'temp_part'
drop 'temp_part'
create 'temp_part','info'

put 'temp_part', '12345678900987654321_1', 'info:size', '5242880'
put 'temp_part', '12345678900987654321_1', 'info:etag', '"1111111111111111"'
put 'temp_part', '12345678900987654321_1', 'info:ctime', 'Thu, 11 Aug 2016 02:14:45 +0000'
put 'temp_part', '12345678900987654321_1', 'info:mtime', 'Thu, 11 Aug 2016 02:14:45 +0000'

put 'temp_part', '12345678900987654321_2', 'info:size', '5242880'
put 'temp_part', '12345678900987654321_2', 'info:etag', '"2222222222222222"'
put 'temp_part', '12345678900987654321_2', 'info:ctime', 'Thu, 11 Aug 2016 02:14:45 +0000'
put 'temp_part', '12345678900987654321_2', 'info:mtime', 'Thu, 11 Aug 2016 02:14:45 +0000'
EFO
`;
my $UPLOADID='12345678900987654321';
my $mybody="<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"1111111111111111\"</ETag></Part><Part><PartNumber>2</PartNumber><ETag>\"2222222222222222\"</ETag></Part></CompleteMultipartUpload>";
my $size = length($mybody);
my @url1 = ("http://s3.dnion.com:8081/bucket1/object1?uploadId=$UPLOADID","-H","Host: 192.168.34.59:7480","-H", "Connection: close", "-H", "Content-Length: $size", "-X", "PUT");
$req1 = MY::access::makereq(\@url1, \$mybody,0,'');
#--- response_body:

--- response_body_like eval
#qr/([^N]*)oSuchBucket(\S+)/

--- error_code: 200
--- timeout: 50
--- no_error_log
[error]
#--- abort
#--- SKIP
--- ONLY

=== TEST 2: complete upload http://ip close 
--- raw_request eval
use POSIX;
use lib './t';
use MY::access qw(makereq);
use URI::Escape;
my $req1;
print 'test_2';
print `hbase shell<<EFO
disable 'bucket'
drop 'bucket'
create 'bucket', 'info', 'quota', 'stats', 'ver', 'exattrs'

put 'bucket', 'UUUU1234_bucket1', 'info:ctime', 'Thu, 11 Aug 2016 02:14:45 +0000'
put 'bucket', 'UUUU1234_bucket1', 'info:mtime', 'Thu, 11 Aug 2016 02:14:45 +0000'

put 'bucket', 'UUUU1234_bucket1', 'quota:enabled', 'yes'
put 'bucket', 'UUUU1234_bucket1', 'quota:size_mb', '1024000'
put 'bucket', 'UUUU1234_bucket1', 'quota:objects', '1024'
 
put 'bucket', 'UUUU1234_bucket1', 'ver:tag', 'BBBB1234'
put 'bucket', 'UUUU1234_bucket1', 'ver:version', '0'

put 'bucket', 'UUUU1234_bucket1', 'exattrs:content', 'movies'

disable 'object'
drop 'object'
create 'object', 'info', 'ver', 'mfest', 'sattrs', 'uattrs', 'hdata'

disable 'temp_object'
drop 'temp_object'
create 'temp_object','info','flag','sattrs','uattrs'

put 'temp_object', 'BBBB1234_12345678900987654321', 'info:bucket', 'bucket1'
put 'temp_object', 'BBBB1234_12345678900987654321', 'flag:flag', '0'

disable 'temp_part'
drop 'temp_part'
create 'temp_part','info'
put 'temp_part', '12345678900987654321_1', 'info:size', '524288'
put 'temp_part', '12345678900987654321_1', 'info:etag', '"1111111111111111"'
put 'temp_part', '12345678900987654321_1', 'info:ctime', 'Thu, 11 Aug 2016 02:14:45 +0000'
put 'temp_part', '12345678900987654321_1', 'info:mtime', 'Thu, 11 Aug 2016 02:14:45 +0000'

put 'temp_part', '12345678900987654321_2', 'info:size', '524288'
put 'temp_part', '12345678900987654321_2', 'info:etag', '"2222222222222222"'
put 'temp_part', '12345678900987654321_2', 'info:ctime', 'Thu, 11 Aug 2016 02:14:45 +0000'
put 'temp_part', '12345678900987654321_2', 'info:mtime', 'Thu, 11 Aug 2016 02:14:45 +0000'
EFO
`;
my $UPLOADID='12345678900987654321';
my $mybody="<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"1111111111111111\"</ETag></Part><Part><PartNumber>2</PartNumber><ETag>\"2222222222222222\"</ETag></Part></CompleteMultipartUpload>";
my $size = length($mybody);
my @url1 = ("http://s3.dnion.com:8081/bucket1/object1?uploadId=$UPLOADID","-H","Host: 192.168.34.59:7480","-H", "Connection: close", "-H", "Content-Length: $size", "-X", "PUT");
$req1 = MY::access::makereq(\@url1, \$mybody,0,'');
#--- response_body:

--- response_body_like eval
#qr/([^N]*)oSuchBucket(\S+)/

--- error_code: 400
--- timeout: 50
--- no_error_log
#[error]
#--- abort
#--- SKIP
#--- ONLY

=== TEST 3: complete upload http://ip close 
--- raw_request eval
use POSIX;
use lib './t';
use MY::access qw(makereq);
use URI::Escape;
my $req1;
print `hbase shell<<EFO
truncate 'bucket'

put 'bucket', 'UUUU1234_bucket1', 'info:ctime', 'Thu, 11 Aug 2016 02:14:45 +0000'
put 'bucket', 'UUUU1234_bucket1', 'info:mtime', 'Thu, 11 Aug 2016 02:14:45 +0000'

put 'bucket', 'UUUU1234_bucket1', 'quota:enabled', 'yes'
put 'bucket', 'UUUU1234_bucket1', 'quota:size_mb', '1024000'
put 'bucket', 'UUUU1234_bucket1', 'quota:objects', '1024'
 
put 'bucket', 'UUUU1234_bucket1', 'ver:tag', 'BBBB1234'
put 'bucket', 'UUUU1234_bucket1', 'ver:version', '0'

put 'bucket', 'UUUU1234_bucket1', 'exattrs:content', 'movies'

truncate 'temp_object'

put 'temp_object', 'BBBB1234_12345678900987654321', 'info:bucket', 'bucket1'
put 'temp_object', 'BBBB1234_12345678900987654321', 'flag:flag', '0'

truncate 'temp_part'

put 'temp_part', '12345678900987654321_1', 'info:size', '5242880'
put 'temp_part', '12345678900987654321_1', 'info:etag', '"1111111111111111"'
put 'temp_part', '12345678900987654321_1', 'info:ctime', 'Thu, 11 Aug 2016 02:14:45 +0000'
put 'temp_part', '12345678900987654321_1', 'info:mtime', 'Thu, 11 Aug 2016 02:14:45 +0000'

put 'temp_part', '12345678900987654321_2', 'info:size', '5242880'
put 'temp_part', '12345678900987654321_2', 'info:etag', '"2222222222222222"'
put 'temp_part', '12345678900987654321_2', 'info:ctime', 'Thu, 11 Aug 2016 02:14:45 +0000'
put 'temp_part', '12345678900987654321_2', 'info:mtime', 'Thu, 11 Aug 2016 02:14:45 +0000'
EFO
`;
my $UPLOADID='12345678900987654321';
my $mybody="<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"111111111111111\"</ETag></Part><Part><PartNumber>2</PartNumber><ETag>\"2222222222222222\"</ETag></Part></CompleteMultipartUpload>";
my $size = length($mybody);
my @url1 = ("http://s3.dnion.com:8081/bucket1/object1?uploadId=$UPLOADID","-H","Host: 192.168.34.59:7480","-H", "Connection: close", "-H", "Content-Length: $size", "-X", "PUT");
$req1 = MY::access::makereq(\@url1, \$mybody,0,'');
#--- response_body:

--- response_body_like eval
#qr/([^N]*)oSuchBucket(\S+)/

--- error_code: 400
--- timeout: 50
--- no_error_log
#[error]
#--- abort
--- SKIP
#--- ONLY

=== TEST 4: complete upload http://ip close 
--- raw_request eval
use POSIX;
use lib './t';
use MY::access qw(makereq);
use URI::Escape;
my $req1;
print `hbase shell<<EFO
truncate 'bucket'

put 'bucket', 'UUUU1234_bucket1', 'info:ctime', 'Thu, 11 Aug 2016 02:14:45 +0000'
put 'bucket', 'UUUU1234_bucket1', 'info:mtime', 'Thu, 11 Aug 2016 02:14:45 +0000'

put 'bucket', 'UUUU1234_bucket1', 'quota:enabled', 'yes'
put 'bucket', 'UUUU1234_bucket1', 'quota:size_mb', '1024000'
put 'bucket', 'UUUU1234_bucket1', 'quota:objects', '1024'
 
put 'bucket', 'UUUU1234_bucket1', 'ver:tag', 'BBBB1234'
put 'bucket', 'UUUU1234_bucket1', 'ver:version', '0'

put 'bucket', 'UUUU1234_bucket1', 'exattrs:content', 'movies'

truncate 'temp_object'

put 'temp_object', 'BBBB1234_12345678900987654321', 'info:bucket', 'bucket1'
put 'temp_object', 'BBBB1234_12345678900987654321', 'flag:flag', '0'

truncate 'temp_part'

put 'temp_part', '12345678900987654321_1', 'info:size', '5242880'
put 'temp_part', '12345678900987654321_1', 'info:etag', '"1111111111111111"'
put 'temp_part', '12345678900987654321_1', 'info:ctime', 'Thu, 11 Aug 2016 02:14:45 +0000'
put 'temp_part', '12345678900987654321_1', 'info:mtime', 'Thu, 11 Aug 2016 02:14:45 +0000'

put 'temp_part', '12345678900987654321_2', 'info:size', '5242880'
put 'temp_part', '12345678900987654321_2', 'info:etag', '"2222222222222222"'
put 'temp_part', '12345678900987654321_2', 'info:ctime', 'Thu, 11 Aug 2016 02:14:45 +0000'
put 'temp_part', '12345678900987654321_2', 'info:mtime', 'Thu, 11 Aug 2016 02:14:45 +0000'
EFO
`;
my $UPLOADID='1234567890098765421';
my $mybody="<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"1111111111111111\"</ETag></Part><Part><PartNumber>2</PartNumber><ETag>\"2222222222222222\"</ETag></Part></CompleteMultipartUpload>";
my $size = length($mybody);
my @url1 = ("http://s3.dnion.com:8081/bucket1/object1?uploadId=$UPLOADID","-H","Host: 192.168.34.59:7480","-H", "Connection: close", "-H", "Content-Length: $size", "-X", "PUT");
$req1 = MY::access::makereq(\@url1, \$mybody,0,'');
#--- response_body:

--- response_body_like eval
#qr/([^N]*)oSuchBucket(\S+)/

--- error_code: 404
--- timeout: 50
--- no_error_log
#[error]
#--- abort
--- SKIP
#--- ONLY
