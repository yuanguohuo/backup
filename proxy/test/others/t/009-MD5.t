#use lib '/root/atest/openresty/proxy/test/';
use t::MYTEST 'no_plan';
no_long_string();
log_level 'debug';
#use Test::Nginx::Socket::Lua 'no_plan'; 

use Getopt::Long qw(GetOptions);

our  $bucket;
our  $object;
GetOptions(
    'bucket=s' => \$bucket,
    'object=s' => \$object,
);
print "bucket is $bucket \n";
print "object is $object \n";
our $file = '/home/deploy/rnpatch';
no_root_location();
no_shuffle();
workers(1);
#worker_connections(102400);
worker_connections(1024);
run_tests();
# problem :only one value


__DATA__

=== TEST 1: single put: incorrect md5 and using file ."Connection: close", "-H", "Expect: ", "-H" , "Content-Length: 1000",100<len?
--- raw_request eval
use POSIX;
use lib '/root/atest/openresty/proxy/test/t';
use MY::access qw(makereq);
use URI::Escape;
my @url1 = ("http://s3.dnion.com:8081/009$::bucket/01$::object","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-H", "Expect: ", "-H" , "Content-Length: 1000", "-X", "PUT");
my $req1;
my $bodyy = 'objcontent' x 100;
#my $file = '/home/deploy/rnpatch';
$req1 = MY::access::makereq(\@url1, \$bodyy, 0,'2222',\$::file);
--- error_code: 422
--- error_log
[error]
--- timeout: 60
--- abort

=== TEST 2: single put: correct md5 and using string ,normal len,no null.

--- raw_request eval
use POSIX;
use lib '/root/atest/openresty/proxy/test/t';
use MY::access qw(makereq);
use URI::Escape;
my @url1 = ("http://s3.dnion.com:8081/009$::bucket/02$::object","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-H", "Expect: ", "-H" , "Content-Length: 10", "-X", "PUT");
#my @url1 = ("http://s3.dnion.com:8081/$::bucket/02$::object","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-H", "Expect: ", "-H" , "Content-Length: 10", "-X", "PUT");
my $req1;
my $bodyy = 'objcontent';
my $len =  length('objcontent' x 100);
print STDERR "$len  len is";
my $file = '/home/deploy/rnpatch';
#$req1 = MY::access::makereq(\@url1, \$bodyy, 1,'',\$file);
$req1 = MY::access::makereq(\@url1, \$bodyy, 1,'');
--- error_code: 200
--- response_headers
ETag: "2cbbfe139a744d6abbe695e17f3c1991" 
--- no_error_log
[error]
--- timeout: 60
--- abort

=== TEST 3: single put: correct md5 and using string ,normal len,keepalive

--- raw_request eval
use POSIX;
use lib '/root/atest/openresty/proxy/test/t';
use MY::access qw(makereq);
use URI::Escape;
my @url1 = ("http://s3.dnion.com:8081/009$::bucket/03$::object","-H","Host: s3.dnion.com:8081", "-H", "Expect: ", "-H" , "Content-Length: 10", "-X", "PUT");
my $req1;
my $bodyy = 'objcontent';
my $len =  length('objcontent' x 100);
print STDERR "$len  len is";
my $file = '/home/deploy/rnpatch';
#$req1 = MY::access::makereq(\@url1, \$bodyy, 1,'',\$file);
$req1 = MY::access::makereq(\@url1, \$bodyy, 1,'');
--- error_code: 200
--- no_error_log
[error]
--- timeout: 3
--- abort



=== TEST 4: put:incorrect len+1

--- raw_request eval
use POSIX;
use lib '/root/atest/openresty/proxy/test/t';
use MY::access qw(makereq);
use URI::Escape;
my @url1 = ("http://s3.dnion.com:8081/009$::bucket/04$::object","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-H", "Expect: ", "-H" , "Content-Length: 11", "-X", "PUT");
my $req1;
my $bodyy = 'objcontent';
my $len =  length('objcontent' x 100);
print STDERR "$len  len is";
my $file = '/home/deploy/rnpatch';
#$req1 = MY::access::makereq(\@url1, \$bodyy, 1,'',\$file);
$req1 = MY::access::makereq(\@url1, \$bodyy, 1,'');
--- error_code: 
--- error_log
failed to read head from client body
--- timeout: 6
--- abort


=== TEST 5: provided len<file len 2.2k fornow,only by content-len
--- raw_request eval
use POSIX;
use lib '/root/atest/openresty/proxy/test/t';
use MY::access qw(makereq);
use URI::Escape;
my @url1 = ("http://s3.dnion.com:8081/009$::bucket/05$::object","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-H", "Expect: ", "-H" , "Content-Length: 1000", "-X", "PUT");
my $req1;
my $file = '/home/deploy/rnpatch';
$req1 = MY::access::makereq(\@url1, '', 0,'',\$file);
--- error_code: 200
--- no_error_log
[error]
--- timeout: 60
--- abort

=== TEST 5: provided len<file len 2.2k given md5 incorrect with the computed. fornow,only by content-len
--- raw_request eval
use POSIX;
use lib '/root/atest/openresty/proxy/test/t';
use MY::access qw(makereq);
use URI::Escape;
my @url1 = ("http://s3.dnion.com:8081/009$::bucket/05$::object","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-H", "Expect: ", "-H" , "Content-Length: 1000", "-X", "PUT");
my $req1;
my $file = '/home/deploy/rnpatch';
$req1 = MY::access::makereq(\@url1, '', 1,'',\$file);
--- error_code: 422
--- error_log
[error]
--- timeout: 60
--- abort



=== TEST 7: single put: chunk no "md5,length" signv2 need len; have "Connection: close", "-H", "Expect: ",
--- raw_request eval
use POSIX;
use lib '/root/atest/openresty/proxy/test/t';
use MY::access qw(makereq);
use URI::Escape;
my @url2 = ("http://s3.dnion.com:8081/009$::bucket/07$::object","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-H", "Expect: ", "-X", "PUT");
my $req2;
my $bodyd = 'objcontent' x 100;
$req2 = MY::access::makereq(\@url2, \$bodyd, 0, '','');
--- error_code: 411
--- error_log
[error]
--- timeout: 60
--- abort

=== TEST 8: single put:correct md5 ,"Connection: close", "-H", "Expect: ", "-H" , "Content-Length: 1000",100<len?
--- raw_request eval
use POSIX;
use lib '/root/atest/openresty/proxy/test/t';
use MY::access;
use URI::Escape;
my @url1 = ("http://s3.dnion.com:8081/009$::bucket/08$::object","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-H", "Expect: ", "-H" , "Content-Length: 1000", "-X", "PUT");
my $req1;
my $bodyy = 'objcontent' x 100;
$req1 = MY::access::makereq(\@url1, \$bodyy, 1,'');

--- error_code: 200
--- no_error_log
[error]
--- timeout: 60
--- abort
--- SKIP

=== TEST 9: single put req ,incorrect md5 ,"Connection: close", "-H", "Expect: ", "-H" , "Content-Length: 1000",100<len?
--- raw_request eval
use POSIX;
use lib '/root/atest/openresty/proxy/test/t';
use MY::access;
use URI::Escape;
my @url1 = ("http://s3.dnion.com:8081/009$::bucket/09$::object","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-H", "Expect: ", "-H" , "Content-Length: 1000", "-X", "PUT");
my $req1;
my $bodyy = 'objcontent' x 100;
$req1 = MY::access::makereq(\@url1, \$bodyy, 0,'11');
--- error_code: 400
--- error_log
[error]
--- timeout: 60
--- abort
--- SKIP

=== TEST 10: single put: 400MB correct md5 and using big file ."Connection: close", "-H", "Expect: ", "-H" , "Content-Length: 1000",100<len?
--- raw_request eval
use POSIX;
use lib '/root/atest/openresty/proxy/test/t';
use MY::access qw(makereq);
use URI::Escape;
my $req1;
my $bodyy = 'objcontent' x 100;
my $file1 = '/root/atest/openresty/proxy/test/testbig';
my $size = -s $file1;
my @url1 = ("http://s3.dnion.com:8081/009$::bucket/0010$::object","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-H", "Expect: ", "-H" , "Content-Length: $size", "-X", "PUT");
#my @url1 = ("http://s3.dnion.com:8081/$::bucket/0010$::object","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-H", "Expect: ", "-H" , "Content-Length: $size", "-X", "PUT");
$req1 = MY::access::makereq(\@url1, '', 1,'',\$file1);
--- error_code: 200
--- response_headers
ETag: "61eabaf2bf278703738b433ff884c91f" 
--- no_error_log
[error]
--- timeout: 60
--- abort
#--- SKIP

#--- response_body_like chomp
#\b\b.*?\bETag: 61eabaf2bf278703738b433ff884c91f\b.*?\b\b
=== TEST 10-1: single put: 400MB correct md5 and using big file ."Connection: close", "-H", "Expect: ", "-H" , "Content-Length: 1000",100<len?
--- raw_request eval
use POSIX;
use lib '/root/atest/openresty/proxy/test/t';
use MY::access qw(makereq);
use URI::Escape;
my $req1;
my $bodyy = 'objcontent' x 100;
my $file1 = '/root/atest/openresty/proxy/test/testbig';
my $size = -s $file1;
my @url1 = ("http://s3.dnion.com:8081/009$::bucket/00101$::object","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-H", "Expect: 100-Continue", "-H" , "Content-Length: $size", "-X", "PUT");
$req1 = MY::access::makereq(\@url1, '', 1,'',\$file1);
--- curl
--- error_code: 100
--- response_body eval
qr/\S*Etag: "61eabaf2bf278703738b433ff884c91f"\S*/s
--- no_error_log
[error]
--- timeout: 60
--- abort
=== TEST 11: single put: 80MB correct md5 and using big file ."Connection: close", "-H", "Expect: ", "-H" , "Content-Length: 1000",100<len?
--- raw_request eval
use POSIX;
use lib '/root/atest/openresty/proxy/test/t';
use MY::access qw(makereq);
use URI::Escape;
my $req1;
my $bodyy = 'objcontent' x 100;
my $file = '/root/atest/openresty/proxy/test/80MB';
my $size = -s $file;
my @url1 = ("http://s3.dnion.com:8081/009$::bucket/11$::object","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-H", "Expect: ", "-H" , "Content-Length: $size", "-X", "PUT");
#$req1 = MY::access::makereq(\@url1, \$bodyy, 1,'',\$file);
$req1 = MY::access::makereq(\@url1, '', 1,'',\$file);
--- error_code: 200
--- no_error_log
[error]
--- timeout: 60
--- abort
--- SKIP

=== TEST 12: single put:10MB correct md5 and using big file ."Connection: close", "-H", "Expect: ", "-H" , "Content-Length: 1000",100<len?
--- raw_request eval
use POSIX;
use lib '/root/atest/openresty/proxy/test/t';
use MY::access qw(makereq);
use URI::Escape;
my $req1;
my $bodyy = 'objcontent' x 100;
my $file = '/root/atest/openresty/proxy/test/10MB';
my $size = -s $file;
my @url1 = ("http://s3.dnion.com:8081/bucket777/obj7","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-H", "Expect: ", "-H" , "Content-Length: $size", "-X", "PUT");
#$req1 = MY::access::makereq(\@url1, \$bodyy, 1,'',\$file);
$req1 = MY::access::makereq(\@url1, '', 1,'',\$file);
--- error_code: 200
--- no_error_log
[error]
--- timeout: 60
--- abort
--- SKIP

=== TEST 13: single put: 2mB."Connection: close", "-H", "Expect: ", "-H" , "Content-Length: 1000",100<len?
--- raw_request eval
use POSIX;
use lib '/root/atest/openresty/proxy/test/t';
use MY::access qw(makereq);
use URI::Escape;
my $req1;
my $bodyy = 'objcontent' x 100;
my $file = '/root/atest/openresty/proxy/test/2MB';
my $size = -s $file;
my @url1 = ("http://s3.dnion.com:8081/bucket777/obj7","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-H", "Expect: ", "-H" , "Content-Length: $size", "-X", "PUT");
$req1 = MY::access::makereq(\@url1, \$bodyy, 1,'',\$file);
#$req1 = MY::access::makereq(\@url1, '', 1,'',\$file);
--- error_code: 200
--- no_error_log
[error]
--- timeout: 60
--- abort
--- SKIP
=== TEST 14: single put: correct md5 and using small file ."Connection: close", "-H", "Expect: ", "-H" , "Content-Length: 1000",100<len?
--- raw_request eval
use POSIX;
use lib '/root/atest/openresty/proxy/test/t';
use MY::access qw(makereq);
use URI::Escape;
my @url1 = ("http://s3.dnion.com:8081/bucket777/obj6","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-H", "Expect: ", "-H" , "Content-Length: 1000", "-X", "PUT");
my $req1;
my $bodyy = 'objcontent' x 100;
my $len =  length('objcontent' x 100);
print "$len len is \n";
my $file = '/home/deploy/rnpatch';
$req1 = MY::access::makereq(\@url1, \$bodyy, 1,'',\$file);
#$req1 = MY::access::makereq(\@url1, \$bodyy, 1,'');
--- error_code: 422
--- error_log
[error]
--- timeout: 60
--- abort
--- SKIP



=== TEST 15: single put: correct md5 and using small file correct len string len+1,coparawithstring ."Connection: close", "-H", "Expect: ", "-H" , "Content-Length: 1000",100<len?
--- raw_request eval
use POSIX;
use lib '/root/atest/openresty/proxy/test/t';
use MY::access qw(makereq);
use URI::Escape;
my @url1 = ("http://s3.dnion.com:8081/bucket777/obj6","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-H", "Expect: ", "-H" , "Content-Length: 11", "-X", "PUT");
my $req1;
my $bodyy = 'objcontent' x 100;
my $len =  length('objcontent' x 100);
my $file = '/home/deploy/testfile';
$req1 = MY::access::makereq(\@url1, \$bodyy, 1,'',\$file);
#$req1 = MY::access::makereq(\@url1, \$bodyy, 1,'');
--- error_code: 200
--- no_error_log
[error]
--- timeout: 60
--- abort
--- SKIP



=== TEST 16: single put: correct md5 and using small file len-1 string len+1  ."Connection: close", "-H", "Expect: ", "-H" , "Content-Length: 1000",100<len?
--- raw_request eval
use POSIX;
use lib '/root/atest/openresty/proxy/test/t';
use MY::access qw(makereq);
use URI::Escape;
my @url1 = ("http://s3.dnion.com:8081/bucket777/obj6","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-H", "Expect: ", "-H" , "Content-Length: 10", "-X", "PUT");
my $req1;
my $bodyy = 'objcontent' x 100;
my $len =  length('objcontent' x 100);
my $file = '/home/deploy/testfile';
$req1 = MY::access::makereq(\@url1, \$bodyy, 1,'',\$file);
#$req1 = MY::access::makereq(\@url1, \$bodyy, 1,'');
--- error_code: 422
--- error_log
[error]
--- timeout: 60
--- abort
--- SKIP
=== TEST 17: single put: correct md5, small file,correct len string len
--- raw_request eval
use POSIX;
use lib '/root/atest/openresty/proxy/test/t';
use MY::access qw(makereq);
use URI::Escape;
my $req1;
my $bodyy = 'objcontent' x 100;
my $len =  length('objcontent' x 100);
my $file = '/home/deploy/testfile';
my $size = -s $file;
my @url1 = ("http://s3.dnion.com:8081/bucket777/obj6","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-H", "Expect: ", "-H" , "Content-Length: $size", "-X", "PUT");
#$req1 = MY::access::makereq(\@url1, \$bodyy, 1,'',\$file);
$req1 = MY::access::makereq(\@url1, '', 1,'',\$file);
--- error_code: 200
--- no_error_log
[error]
--- timeout: 60
--- abort
--- SKIP
=== TEST 18: single put: incorrect len string len+1correct md5,small file,
--- raw_request eval
use POSIX;
use lib '/root/atest/openresty/proxy/test/t';
use MY::access qw(makereq);
use URI::Escape;
my $req1;
my $bodyy = 'objcontent' x 100;
my $len =  length('objcontent' x 100);
my $file = '/home/deploy/testfile';
my $size = -s $file;
$size = $size + 1;
my @url1 = ("http://s3.dnion.com:8081/bucket777/obj6","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-H", "Expect: ", "-H" , "Content-Length: $size", "-X", "PUT");
$req1 = MY::access::makereq(\@url1, \$bodyy, 1,'',\$file);
#$req1 = MY::access::makereq(\@url1, \$bodyy, 1,'');
--- error_code: 400
--- error_log
[error]
--- timeout: 5
--- abort`
--- SKIP
=== TEST 19: single put:10MB correct md5 and using big file ."Connection: close", "-H", "Expect: ", "-H" , "Content-Length: 1000",100<len?
--- raw_request eval
use POSIX;
use lib '/root/atest/openresty/proxy/test/t';
use MY::access qw(makereq);
use URI::Escape;
my $req1;
my $bodyy = 'objcontent' x 100;
my $file = '/root/atest/openresty/proxy/test/10MB';
my $size = -s $file;
my @url1 = ("http://s3.dnion.com:8081/$::bucket777/$::object","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-H", "Expect: ", "-H" , "Content-Length: $size", "-X", "PUT");
print STDERR "url1[0]";
#$req1 = MY::access::makereq(\@url1, \$bodyy, 1,'',\$file);
$req1 = MY::access::makereq(\@url1, '', 1,'',\$file);
--- error_code: 200
--- no_error_log
[error]
--- timeout: 60
--- abort
--- SKIP

=== TEST 20: single put: correct md5 and using string ,normal len,no null.

--- raw_request eval
use POSIX;
use lib '/root/atest/openresty/proxy/test/t';
use MY::access qw(makereq);
use URI::Escape;
my @url1 = ("http://s3.dnion.com:8081/$::bucket/$::object","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-H", "Expect: ", "-H" , "Content-Length: 10", "-X", "PUT");
my $req1;
my $bodyy = 'objcontent';
$req1 = MY::access::makereq(\@url1, \$bodyy, 1,'');
--- error_code: 200
--- response_headers
ETag: "2cbbfe139a744d6abbe695e17f3c1991" 
--- no_error_log
[error]
--- timeout: 60
--- abort
--- ONLY
