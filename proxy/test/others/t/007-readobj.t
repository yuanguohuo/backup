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

no_root_location();
no_root_location();
no_shuffle();
run_tests();
# problem :only one value


__DATA__


=== TEST 1: readobj 
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
--- ONLY

=== TEST 2: using text/plain
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
my @url1 = ("http://s3.dnion.com:8081/1bkt","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-H", "Content-Type: text/plain", "-X", "GET");
#$req1 = MY::access::makereq(\@url1, \$bodyy, 0,'',\$file);
# CAN $req1 = MY::access::makereq(\@url1,\$bodyy,0,'','');
# can$req1 = MY::access::makereq(\@url1, '', 0,'','');
$req1 = MY::access::makereq(\@url1);
#--- response_body_lik: 400 Bad
--- response_body_like chomp
@
--- error_code: 200
--- timeout: 5000ms
--- abort
#--- ONLY
=== TEST 3: no args

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
my @url1 = ("http://s3.dnion.com:8081/1bkt","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-X", "GET");
#$req1 = MY::access::makereq(\@url1, \$bodyy, 0,'',\$file);
# CAN $req1 = MY::access::makereq(\@url1,\$bodyy,0,'','');
# can$req1 = MY::access::makereq(\@url1, '', 0,'','');
$req1 = MY::access::makereq(\@url1);

#--- response_body_lik: 400 Bad
--- response_body_like chomp
ListBucketResult
--- error_code: 200
--- timeout: 5000ms
--- abort
#--- ONLY

=== TEST 4: args

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
my @url1 = ("http://s3.dnion.com:8081/1bkt?prefix=N&marker=Ned&max-keys=40","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-X", "GET");
#$req1 = MY::access::makereq(\@url1, \$bodyy, 0,'',\$file);
# CAN $req1 = MY::access::makereq(\@url1,\$bodyy,0,'','');
# can$req1 = MY::access::makereq(\@url1, '', 0,'','');
$req1 = MY::access::makereq(\@url1);

#--- response_body_lik: 400 Bad
--- response_body_like chomp
prefix
--- error_code: 400
--- timeout: 5000ms
--- abort
#--- ONLY


=== TEST 5: usr args test,using format=xml keepalive
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
my @url1 = ("http://s3.dnion.com:8081/bucket777?format=xml","-H","Host: s3.dnion.com:8081", "-X", "GET");
#$req1 = MY::access::makereq(\@url1, \$bodyy, 0,'',\$file);
# CAN $req1 = MY::access::makereq(\@url1,\$bodyy,0,'','');
# can$req1 = MY::access::makereq(\@url1, '', 0,'','');
$req1 = MY::access::makereq(\@url1);
#--- response_body_lik: 400 Bad
--- response_body_like chomp
ListBucketResult
--- error_code: 200
--- timeout: 5000ms
--- abort
--- SKIP

=== TEST 6: format=json
--- raw_request eval
use POSIX;
use lib '/root/atest/openresty/proxy/test/t/';
use MY::access qw(req);
use MY::access qw(sign);
#curl para
my @url = ("http://192.168.122.214:7480/1bkt?format=json","-H","Host: 192.168.122.214:7481", "-X", "GET");
my $req1;
$req1 = MY::access::makereq(\@url1);

#--- response_body_lik: 400 Bad
--- response_body_like chomp
ListBucketResult
--- error_code: 200
--- timeout: 5000ms
--- abort
--- SKIP


