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


=== TEST 1: create bucket
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
my @url = ("http://192.168.122.214:8081/1$::bucket","-H","Host: 192.168.122.214:8081", "-H", "Connection: close", "-H", "Content-Length: 0", "-H", "Content-Type: text/plain", "-X", "PUT");
$req1 = MY::access::makereq(\@url);
--- error_code: 200
--- timeout: 5000ms
--- abort

=== TEST 2: delet this real empty bucket
--- raw_request eval
use POSIX;
use lib '/root/atest/openresty/proxy/test/t';
use MY::access qw(makereq);
use URI::Escape;
my $bodyy = '';
my $len =  length('objcontent' x 100);
my $file = '';
my $size = -s $file;
$size = $size + 1;
my @url = ("http://192.168.122.214:7480/1$::bucket","-H","Host: 192.168.122.214:7481", "-X", "DELETE");
my $req1;
$req1 = MY::access::makereq(\@url);

--- error_code: 204
--- timeout: 3000ms
--- abort
#--- ONLY

=== TEST 3: test bucket is delete

--- raw_request eval
use POSIX;
use lib '/root/atest/openresty/proxy/test/t';
use MY::access qw(makereq);
use URI::Escape;
my $bodyy = '';
my $len =  length('objcontent' x 100);
my $file = '';
my $size = -s $file;
$size = $size + 1;
my @url = ("http://192.168.122.214:7480/1$::bucket","-H","Host: 192.168.122.214:7481", "-X", "GET");
my $req1;
$req1 = MY::access::makereq(\@url);

--- response_body_like eval
qr/NoSuchBucket/

--- error_code: 404
--- timeout: 5000ms
--- abort
#--- ONLY
=== TEST 4: delet this real bucket have obj2 create in the 001delobj.t
--- raw_request eval
use POSIX;
use lib '/root/atest/openresty/proxy/test/t';
use MY::access qw(makereq);
use URI::Escape;
my $bodyy = '';
my $len =  length('objcontent' x 100);
my $file = '';
my $size = -s $file;
$size = $size + 1;
my @url = ("http://192.168.122.214:7480/$::bucket","-H","Host: 192.168.122.214:7481", "-X", "DELETE");
my $req1;
$req1 = MY::access::makereq(\@url);

--- error_code: 409
--- timeout: 3000ms
--- abort
#--- ONLY
#--- SKIP
