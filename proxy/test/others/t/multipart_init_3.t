#use lib '/home/deploy/dobjstor/proxy/test/';
use t::MYTEST 'no_plan';
no_long_string();
log_level 'debug';
#use Test::Nginx::Socket::Lua 'no_plan'; 
no_root_location();

no_shuffle();
run_tests();
# problem :only one value

__DATA__


=== TEST 1_1: put bucket1  http://ip close 
--- raw_request eval
use POSIX;
use lib './t';
use MY::access qw(makereq);
use URI::Escape;
my $req1;
my $bodyy = '';
my $len =  length('objcontent' x 100);
my $file = '';
print STDERR "xxxxx\n";
print `hbase shell<<EFO
truncate 'bucket'
EFO
`;
my $size = -s $file;
$size = $size + 1;
my @create_bucket1=("http://s3.dnion.com:8081/bucket1/","-H","Host: s3.dnion.com:8081",     "-H", "Connection: close", "-X", "PUT");
$req1 = MY::access::makereq(\@create_bucket1);
#--- response_body:
#--- response_body_like eval
#--- error_code: 200
#--- timeout: 50
#--- no_error_log
#[error]
#--- abort
#--- SKIP
#--- ONLY

=== TEST 1: multipart init http://ip close 
--- raw_request eval
use POSIX;
use lib './t';
use MY::access qw(makereq);
use URI::Escape;
my $req1;
my $bodyy = '';
my $len =  length('objcontent' x 100);
my $file = '';
print STDERR "xxxxx\n";
my $size = -s $file;
$size = $size + 1;
my @url1 = ("http://s3.dnion.com:8081/bucket1/object1?uploads","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-X", "GET");
$req1 = MY::access::makereq(\@url1);
#--- response_body:

--- response_body_like eval
qr/([^I]*)nitiateMultipartUploadResult(\S+)/

--- error_code: 200
--- timeout: 50
--- no_error_log
[error]
#--- abort
#--- SKIP
#--- ONLY

=== TEST 2: multipart init http://ip close 
--- raw_request eval
use POSIX;
use lib './t';
use MY::access qw(makereq);
use URI::Escape;
my $req1;
my $bodyy = '';
my $len =  length('objcontent' x 100);
my $file = '';
print STDERR "xxxxx\n";
my $size = -s $file;
$size = $size + 1;
my @url1 = ("http://s3.dnion.com:8081/bucket1/object1?uploads","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-X", "GET");
$req1 = MY::access::makereq(\@url1);
$req1 = MY::access::makereq(\@url1);
#--- response_body:

--- response_body_like eval
qr/([^I]*)nitiateMultipartUploadResult(\S+)/

--- error_code: 200
--- timeout: 50
--- no_error_log
[error]
#--- abort
#--- SKIP
#--- ONLY

=== TEST 3: multipart http://ip close 
--- raw_request eval
use POSIX;
use lib './t';
use MY::access qw(makereq);
use URI::Escape;
my $req1;
my $bodyy = '';
my $len =  length('objcontent' x 100);
my $file = '';
print `hbase shell<<EFO
truncate 'bucket'
EFO
`;
my $size = -s $file;
$size = $size + 1;
my @url1 = ("http://s3.dnion.com:8081/bucket1/object1?uploads","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-X", "GET");
$req1 = MY::access::makereq(\@url1);
#--- response_body:

--- response_body_like eval
qr/([^N]*)oSuchBucket(\S+)/

--- error_code: 404
--- timeout: 50
--- no_error_log
#[error]
#--- abort
#--- SKIP
--- ONLY

