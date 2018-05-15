use t::MYTEST 'no_plan';
no_long_string();
log_level 'debug';

use Getopt::Long qw(GetOptions);

our  $bucket;
GetOptions(
    'bucket=s' => \$bucket,
);
print "bucket is $bucket \n";
no_root_location();
no_shuffle();
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
my @url = ("http://192.168.122.214:8081/$::bucket","-H","Host: 192.168.122.214:8081", "-H", "Connection: close", "-H", "Content-Length: 0", "-H", "Content-Type: text/plain", "-X", "PUT");
$req1 = MY::access::makereq(\@url);
--- error_code: 200
--- timeout: 5000ms
--- abort
--- ONLY

=== TEST 2: create same bucket
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
my @url = ("http://192.168.122.214:8081/$::bucket","-H","Host: 192.168.122.214:8081", "-H", "Connection: close", "-H", "Content-Length: 0", "-H", "Content-Type: text/plain", "-X", "PUT");
$req1 = MY::access::makereq(\@url);
--- error_code: 409
--- timeout: 5000ms
--- abort
=== TEST 3: listthisbucket
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
my @url = ("http://192.168.122.214:8081/$::bucket","-H","Host: 192.168.122.214:8081", "-H", "Content-Type: text/plain","-H", "Connection: close", "-X", "GET");
$req1 = MY::access::makereq(\@url);


#--- response_body_like eval
#qr/ListBucketResult*bucket777*/
--- response_body_like eval
qr/$::bucket/
--- error_code: 200
--- timeout: 5000ms
--- abort
#--- ONLY
--- curl
=== TEST 3: create bucket for 009md5
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
my @url = ("http://192.168.122.214:8081/009$::bucket","-H","Host: 192.168.122.214:8081", "-H", "Connection: close", "-H", "Content-Length: 0", "-H", "Content-Type: text/plain", "-X", "PUT");
$req1 = MY::access::makereq(\@url);
--- error_code: 200
--- timeout: 5000ms
--- abort
