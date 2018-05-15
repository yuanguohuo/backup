use t::MYTEST 'no_plan';
log_level 'debug';
#use Test::Nginx::Socket::Lua 'no_plan'; 

use Getopt::Long qw(GetOptions);

$ENV{TEST_NGINX_RESOLVER} ||= '8.8.8.8';
our  $bucket;
our  $object;
GetOptions(
    'bucket=s' => \$bucket,
    'object=s' => \$object,
);
print "bucket is $bucket \n";
print "object is $object \n";
#no_diff;
no_long_string();
no_shuffle();
no_root_location();
#use Test::Nginx::Socket::Lua 'no_plan'; 
run_tests();

__DATA__


=== TEST 1: creat ojb
--- raw_request eval
use POSIX;
use lib '/root/atest/openresty/proxy/test/t';
use MY::access qw(makereq);
use URI::Escape;
my $req1;
my $bodyy = 'objcontent' x 100;
my $file = '/home/deploy/dobjstor/proxy/test/mmmsh';
my $size = -s $file;
my @url1 = ("http://s3.dnion.com:8081/$::bucket/$::object","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-H" , "Content-Length: $size", "-X", "PUT");
$req1 = MY::access::makereq(\@url1, '', 0,'',\$file);
--- error_code: 200
--- no_error_log
[error]
--- timeout: 5
--- abort
#--- SKIP
#--- ONLY
=== TEST 2: readobj 
--- raw_request eval
use POSIX;
use lib '/root/atest/openresty/proxy/test/t';
use MY::access qw(makereq);
use URI::Escape;
my $req1;
my $bodyy = '';
my $len =  length('objcontent' x 100);
my $file = '';
my $size = -s $file;
$size = $size + 1;
my @url1 = ("http://s3.dnion.com:8081/$::bucket/$::object","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-X", "GET");
#$req1 = MY::access::makereq(\@url1, \$bodyy, 0,'',\$file);
# CAN $req1 = MY::access::makereq(\@url1,\$bodyy,0,'','');
# can$req1 = MY::access::makereq(\@url1, '', 0,'','');
$req1 = MY::access::makereq(\@url1);
#--- response_body_lik: 400 Bad
--- response_body_like chomp
baojianguo
--- error_code: 200
--- timeout: 5000ms
--- abort
#--- ONLY
=== TEST 3: delete object
--- raw_request eval
use POSIX;
use lib '/root/atest/openresty/proxy/test/t';
use MY::access qw(makereq);
use URI::Escape;
my $req1;
my $bodyy = 'objcontent' x 100;
my $file = '/home/deploy/dobjstor/proxy/test/10MB';
my $size = -s $file;
my @url1 = ("http://s3.dnion.com:8081/$::bucket/$::object","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-H", "Expect: ", "-X", "DELETE");
#print STDERR "url1[0]";
#$req1 = MY::access::makereq(\@url1, \$bodyy, 1,'',\$file);
$req1 = MY::access::makereq(\@url1);
--- error_code: 204
--- no_error_log
[error]
--- timeout: 60
--- abort
#--- SKIP
#--- ONLY
=== TEST 4: list this bucket
--- raw_request eval
use POSIX;
use lib '/root/atest/openresty/proxy/test/t';
use MY::access qw(makereq);
use URI::Escape;
my $req1;
my $bodyy = 'objcontent' x 100;
my $file = '/home/deploy/dobjstor/proxy/test/10MB';
my $size = -s $file;
my @url1 = ("http://s3.dnion.com:8081/$::bucket","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-H", "Expect: ", "-X", "GET");
#print STDERR "url1[0]";
#$req1 = MY::access::makereq(\@url1, \$bodyy, 1,'',\$file);
$req1 = MY::access::makereq(\@url1);
--- response_body_like chomp
"$::object"
--- error_code: 200
--- no_error_log
[error]
--- timeout: 60
--- abort
--- SKIP
#--- ONLY
=== TEST 5: read deleted obj 
--- raw_request eval
use POSIX;
use lib '/root/atest/openresty/proxy/test/t';
use MY::access qw(makereq);
use URI::Escape;
my $req1;
my $bodyy = '';
my $len =  length('objcontent' x 100);
my $file = '';
my $size = -s $file;
$size = $size + 1;
my @url1 = ("http://s3.dnion.com:8081/$::bucket/$::object","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-X", "GET");
#$req1 = MY::access::makereq(\@url1, \$bodyy, 0,'',\$file);
# CAN $req1 = MY::access::makereq(\@url1,\$bodyy,0,'','');
# can$req1 = MY::access::makereq(\@url1, '', 0,'','');
$req1 = MY::access::makereq(\@url1);
--- error_code: 404
--- timeout: 5000ms
--- abort
#--- SKIP
=== TEST 6: creat ojb2
--- raw_request eval
use POSIX;
use lib '/root/atest/openresty/proxy/test/t';
use MY::access qw(makereq);
use URI::Escape;
my $req1;
my $bodyy = 'objcontent' x 100;
my $file = '/home/deploy/dobjstor/proxy/test/mmmsh';
my $size = -s $file;
my @url1 = ("http://s3.dnion.com:8081/$::bucket/2$::object","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-H" , "Content-Length: $size", "-X", "PUT");
$req1 = MY::access::makereq(\@url1, '', 0,'',\$file);
--- error_code: 200
--- no_error_log
[error]
--- timeout: 5
--- abort
#--- SKIP
#--- ONLY
=== TEST 7: creat ojb7
--- raw_request eval
use POSIX;
use lib '/root/atest/openresty/proxy/test/t';
use MY::access qw(makereq);
use URI::Escape;
my $req1;
my $bodyy = 'objcontent' x 100;
my $file = '/home/deploy/dobjstor/proxy/test/49MB';
my $size = -s $file;
my @url1 = ("http://s3.dnion.com:8081/$::bucket/7$::object","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-H" , "Content-Length: $size", "-X", "PUT");
$req1 = MY::access::makereq(\@url1, '', 0,'',\$file);
--- error_code: 200
--- no_error_log
[error]
--- timeout: 5
--- abort
#--- SKIP
#--- ONLY
=== TEST 8: readobj 8
--- raw_request eval
use POSIX;
use lib '/root/atest/openresty/proxy/test/t';
use MY::access qw(makereq);
use URI::Escape;
my $req1;
my $bodyy = '';
my $len =  length('objcontent' x 100);
my $file = '';
my $size = -s $file;
$size = $size + 1;
my @url1 = ("http://s3.dnion.com:8081/$::bucket/7$::object","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-X", "GET");
#$req1 = MY::access::makereq(\@url1, \$bodyy, 0,'',\$file);
# CAN $req1 = MY::access::makereq(\@url1,\$bodyy,0,'','');
# can$req1 = MY::access::makereq(\@url1, '', 0,'','');
$req1 = MY::access::makereq(\@url1);
#--- response_body_lik: 400 Bad
--- response_body_like chomp
huayuanguo
--- error_code: 200
--- timeout: 5000ms
--- SKIP
--- abort
#--- ONLY
=== TEST 9: md5_hex
--- raw_request eval
use POSIX;
use lib '/root/atest/openresty/proxy/test/t';
use MY::access qw(makereq);
use URI::Escape;
my $req1;
my $bodyy = '';
my $len =  length('objcontent' x 100);
my $file = '';
my $size = -s $file;
$size = $size + 1;
my @url1 = ("http://s3.dnion.com:8081/$::bucket/7$::object","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-X", "GET");
#$req1 = MY::access::makereq(\@url1, \$bodyy, 0,'',\$file);
# CAN $req1 = MY::access::makereq(\@url1,\$bodyy,0,'','');
# can$req1 = MY::access::makereq(\@url1, '', 0,'','');
$req1 = MY::access::makereq(\@url1);

--- response_body_filters
md5_hex
--- response_body chop
ad0549c89dcbe3af0ee47c862630126e
=== TEST 10: range
--- raw_request eval
use POSIX;
use lib '/root/atest/openresty/proxy/test/t';
use MY::access qw(makereq);
use URI::Escape;
my $req1;
my $bodyy = '';
my $len =  length('objcontent' x 100);
my $file = '';
my $size = -s $file;
$size = $size + 1;
my @url1 = ("http://s3.dnion.com:8081/$::bucket/7$::object","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "Range","bytes=50536500-50536556","-X", "GET");
#$req1 = MY::access::makereq(\@url1, \$bodyy, 0,'',\$file);
# CAN $req1 = MY::access::makereq(\@url1,\$bodyy,0,'','');
# can$req1 = MY::access::makereq(\@url1, '', 0,'','');
$req1 = MY::access::makereq(\@url1);
--- error_code: 200
--- response_body_like chomp
huoyuanguo
--- SKIP
