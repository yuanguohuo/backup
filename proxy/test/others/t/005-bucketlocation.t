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
no_shuffle();
run_tests();


__DATA__
=== TEST 1: get bucket "locaiton"
--- raw_request eval
use POSIX;
use lib '/root/atest/openresty/proxy/test/t';
use MY::access qw(makereq);
use URI::Escape;
my $req1;
my $bodyy = 'objcontent' x 100;
#my $file = '/home/deploy/dobjstor/proxy/test/10MB';
#my $file = '/home/deploy/dobjstor/proxy/test/closelog';
#my $file = '/home/deploy/dobjstor/proxy/test/closelogold';
my $file = '/home/deploy/dobjstor/proxy/test/mmmsh';
my $size = -s $file;
#my @url1 = ("http://s3.dnion.com:8081/$::bucket/?location=a","-H","Host: s3.dnion.com:8081", "-H", "Connection: close","-X", "GET");
my @url1 = ("http://s3.dnion.com:8081/$::bucket/?location","-H","Host: s3.dnion.com:8081", "-H", "Connection: close","-X", "GET");
#my @url1 = ("http://s3.dnion.com:8081/$::bucket/?location=","-H","Host: s3.dnion.com:8081", "-H", "Connection: close","-X", "GET");
#my @url1 = ("http://s3.dnion.com:8081/$::bucket/$::object","-H","Host: s3.dnion.com:8081", "-H" , "Content-Length: $size", "-X", "PUT");
#$req1 = MY::access::makereq(\@url1, \$bodyy, 1,'',\$file);
$req1 = MY::access::makereq(\@url1)
--- error_code: 200
--- no_error_log
[error]
--- timeout: 5
--- abort
#--- SKIP
--- ONLY
=== TEST 2: get bucket "locaiton="
--- raw_request eval
use POSIX;
use lib '/root/atest/openresty/proxy/test/t';
use MY::access qw(makereq);
use URI::Escape;
my $req1;
my $bodyy = 'objcontent' x 100;
#my $file = '/home/deploy/dobjstor/proxy/test/10MB';
#my $file = '/home/deploy/dobjstor/proxy/test/closelog';
#my $file = '/home/deploy/dobjstor/proxy/test/closelogold';
my $file = '/home/deploy/dobjstor/proxy/test/mmmsh';
my $size = -s $file;
my @url1 = ("http://s3.dnion.com:8081/$::bucket/?location=","-H","Host: s3.dnion.com:8081", "-H", "Connection: close","-X", "GET");
#$req1 = MY::access::makereq(\@url1, \$bodyy, 1,'',\$file);
$req1 = MY::access::makereq(\@url1)
--- error_code: 200
--- no_error_log
[error]
--- timeout: 5
--- abort
--- SKIP
#--- ONLY
=== TEST 3: get bucket "locaiton="""
--- raw_request eval
use POSIX;
use lib '/root/atest/openresty/proxy/test/t';
use MY::access qw(makereq);
use URI::Escape;
my $req1;
my $bodyy = 'objcontent' x 100;
#my $file = '/home/deploy/dobjstor/proxy/test/10MB';
#my $file = '/home/deploy/dobjstor/proxy/test/closelog';
#my $file = '/home/deploy/dobjstor/proxy/test/closelogold';
my $file = '/home/deploy/dobjstor/proxy/test/mmmsh';
my $size = -s $file;
my @url1 = ("http://s3.dnion.com:8081/$::bucket/?location=a","-H","Host: s3.dnion.com:8081", "-H", "Connection: close","-X", "GET");
#my @url1 = ("http://s3.dnion.com:8081/$::bucket/?location","-H","Host: s3.dnion.com:8081", "-H", "Connection: close","-X", "GET");
#my @url1 = ("http://s3.dnion.com:8081/$::bucket/?location=","-H","Host: s3.dnion.com:8081", "-H", "Connection: close","-X", "GET");
#my @url1 = ("http://s3.dnion.com:8081/$::bucket/$::object","-H","Host: s3.dnion.com:8081", "-H" , "Content-Length: $size", "-X", "PUT");
#$req1 = MY::access::makereq(\@url1, \$bodyy, 1,'',\$file);
$req1 = MY::access::makereq(\@url1)
--- error_code: 200
--- no_error_log
[error]
--- timeout: 5
--- abort
--- SKIP
