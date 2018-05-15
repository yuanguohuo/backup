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

=== TEST 1: multipart http://ip close 
--- raw_request eval
use POSIX;
use lib './t';
use MY::access qw(makereq);
use URI::Escape;
my $req1;
my $bodyy = '';
my $file = '/usr/local/openresty/nginx/svn/proxy/test/t/part_1';
print STDERR "test_1";
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
EFO
`;
my $UPLOADID='12345678900987654321';
my $size = -s $file;
my @url1 = ("http://s3.dnion.com:8081/bucket1/object1?partNumber=1&uploadId=$UPLOADID","-H","Host: 192.168.34.59:7480","-H", "Connection: close", "-H", "Content-Length: $size", "-X", "PUT");
$req1 = MY::access::makereq(\@url1, '',0,'',\$file);
#--- response_body:

--- response_body_like eval
#qr/([^N]*)oSuchBucket(\S+)/

--- error_code: 200
--- timeout: 50
--- no_error_log
[error]
#--- abort
#--- SKIP
#--- ONLY

=== TEST 2: upload part http://ip close 
--- raw_request eval
use POSIX;
use lib './t';
use MY::access qw(makereq);
use URI::Escape;
my $req1;
my $bodyy = '';
my $file = '/usr/local/openresty/nginx/svn/proxy/test/t/part_1';
print STDERR "test_2";
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
EFO
`;
my $UPLOADID='12345678900987654321';
my $size = -s $file;
my @url1 = ("http://s3.dnion.com:8081/bucket1/object1?partNumber=0&uploadId=$UPLOADID","-H","Host: 192.168.34.59:7480","-H", "Connection: close", "-H", "Content-Length: $size", "-X", "PUT");
$req1 = MY::access::makereq(\@url1, '',0,'',\$file);
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

=== TEST 3: upload part http://ip close 
--- raw_request eval
use POSIX;
use lib './t';
use MY::access qw(makereq);
use URI::Escape;
my $req1;
my $bodyy = '';
my $file = '/usr/local/openresty/nginx/svn/proxy/test/t/part_1';
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
EFO
`;
my $UPLOADID='12345678900987654321';
my $size = -s $file;
my @url1 = ("http://s3.dnion.com:8081/bucket1/object1?partNumber=10001&uploadId=$UPLOADID","-H","Host: 192.168.34.59:7480","-H", "Connection: close", "-H", "Content-Length: $size", "-X", "PUT");
$req1 = MY::access::makereq(\@url1, '',0,'',\$file);
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

=== TEST 4: upload part http://ip close 
--- raw_request eval
use POSIX;
use lib './t';
use MY::access qw(makereq);
use URI::Escape;
my $req1;
my $bodyy = '';
my $file = '/usr/local/openresty/nginx/svn/proxy/test/t/part_1';
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
EFO
`;
my $UPLOADID='12345678900987654321';
my $size = -s $file;
my @url1 = ("http://s3.dnion.com:8081/bucket1/object1?partNumber&uploadId=$UPLOADID","-H","Host: 192.168.34.59:7480","-H", "Connection: close", "-H", "Content-Length: $size", "-X", "PUT");
$req1 = MY::access::makereq(\@url1, '',0,'',\$file);
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


=== TEST 5: multipart http://ip close 
--- raw_request eval
use POSIX;
use lib './t';
use MY::access qw(makereq);
use URI::Escape;
my $req1;
my $bodyy = '';
my $file = '/usr/local/openresty/nginx/svn/proxy/test/t/part_1';
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
EFO
`;
my $UPLOADID='1234567890098765421';
my $size = -s $file;
my @url1 = ("http://s3.dnion.com:8081/bucket1/object1?partNumber=1&uploadId=$UPLOADID","-H","Host: 192.168.34.59:7480","-H", "Connection: close", "-H", "Content-Length: $size", "-X", "PUT");
$req1 = MY::access::makereq(\@url1, '',0,'',\$file);
#--- response_body:

--- response_body_like eval
qr/([^N]*)oSuchUpload(\S+)/

--- error_code: 404
--- timeout: 50
--- no_error_log
#[error]
#--- abort
#--- SKIP
#--- ONLY

