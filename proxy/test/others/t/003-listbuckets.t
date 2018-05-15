#use lib '/home/deploy/dobjstor/proxy/test/';
use t::MYTEST 'no_plan';
no_long_string();
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

run_tests();
# problem :only one value


__DATA__




=== TEST 1: listbuckets http://ip close 
#todo process content
--- raw_request eval
use POSIX;
#use lib '/home/deploy/dobjstor/proxy/test/t';
use lib '/root/atest/openresty/proxy/test/t';
use MY::access qw(makereq);
use URI::Escape;
my $req1;
my $bodyy = '';
my $len =  length('objcontent' x 100);
my $file = '';
my $size = -s $file;
$size = $size + 1;
# this only connect to local nginx
my @url1 = ("http://s3.dnion.com:8081","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-X", "GET");
#my @url1 = ("http://183.131.9.155:6081","-H","Host: 183.131.9.155:6081", "-H", "Connection: close", "-X", "GET");
#$req1 = MY::access::makereq(\@url1, \$bodyy, 0,'',\$file);
# CAN $req1 = MY::access::makereq(\@url1,\$bodyy,0,'','');
#$req1 = MY::access::makereq(\@url1, '', 1,'','');
$req1 = MY::access::makereq(\@url1);
#--- response_body:
#<?xml version="1.0" encoding="UTF-8"?>
#<ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01">

#qr/([^L]*)istAllMyBucketsResult(\S+)/
#--- response_body_like eval
#qr/([^L]*)istAllMyBucketsResult(\S+)/

--- error_code: 200
--- timeout: 50
--- no_error_log
[error]
#--- abort
--- ONLY


=== TEST 2: listbuckets http://ip/ close 
--- raw_request eval
use POSIX;
use lib '/home/deploy/dobjstor/proxy/test/t';
use MY::access qw(makereq);
use URI::Escape;
my $req1;
my $bodyy = '';
my $len =  length('objcontent' x 100);
my $file = '';
my $size = -s $file;
$size = $size + 1;
my @url1 = ("http://s3.dnion.com:8081/","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-X", "GET");
#$req1 = MY::access::makereq(\@url1, \$bodyy, 0,'',\$file);
# CAN $req1 = MY::access::makereq(\@url1,\$bodyy,0,'','');
#$req1 = MY::access::makereq(\@url1, '', 1,'','');
$req1 = MY::access::makereq(\@url1);
#--- response_body:
#<?xml version="1.0" encoding="UTF-8"?>
#<ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01">

#qr/([^L]*)istAllMyBucketsResult(\S+)/
#--- response_body_like eval
#qr/([^L]*)istAllMyBucketsResult(\S+)/

--- error_code: 200
--- timeout: 50
--- no_error_log
[error]
#--- abort
#--- SKIP
#--- ONLY

=== TEST 3: listbuckets http://ip/ keepalive
--- raw_request eval
use POSIX;
use lib '/home/deploy/dobjstor/proxy/test/t';
use MY::access qw(makereq);
use URI::Escape;
my @url1 = ("http://192.168.122.24:8081/","-H","Host: 192.168.122.24:8081","-H","Connection: close", "-X", "GET");
my $req1 = MY::access::makereq(\@url1);

#--- response_body_like eval
#qr/([^L]*)istAllMyBucketsResult(\S+)/

--- error_code: 200
--- timeout: 50
--- abort
--- no_error_log
[error]
#--- curl
