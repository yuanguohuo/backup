#use lib '/home/deploy/dobjstor/proxy/test/';
use t::MYTEST 'no_plan';
no_long_string();
log_level 'debug';
#log_level 'info';
#use Test::Nginx::Socket::Lua 'no_plan'; 
no_shuffle();

run_tests();


__DATA__

=== TEST 5: single put req NOT  includeing length,s3 must have length!. "-H", "Connection: close", "-H", "Expect: ",
#ailed to get client body reader: err=nil maybe not support non length.
--- raw_request eval
use POSIX;
use lib '/home/deploy/dobjstor/proxy/test/t';
use MY::access qw(gsign);
use MY::access qw(req);
use URI::Escape;
my @url2 = ("http://192.168.122.214:7480/bucket777/obj5","-H","Host: 192.168.122.214:7480", "-H", "Connection: close", "-H", "Expect: ", "-X", "PUT");

my @arry2 = MY::access::gsign(@url2);
my $req2;
my $bodyd = 'objcontent' x 100;
$req2 = MY::access::req(\@arry2,  \$bodyd);
--- error_code: 500
--- timeout: 60
--- abort
#--- ONLY

=== TEST 4: single put req "Connection: close", "-H", "Expect: ", "-H" , "Content-Length: 1000"
--- raw_request eval
use POSIX;
use lib '/home/deploy/dobjstor/proxy/test/t';
use MY::access qw(req);
use MY::access qw(gsign);
use URI::Escape;
my @url1 = ("http://192.168.122.214:7480/bucket777/obj6","-H","Host: 192.168.122.214:7480", "-H", "Connection: close", "-H", "Expect: ", "-H" , "Content-Length: 1000", "-X", "PUT");

my @arry1 = MY::access::gsign(@url1);
my $req1;
my $bodyy = 'objcontent' x 100;
$req1 = MY::access::req(\@arry1,  \$bodyy);
--- error_code: 200
--- timeout: 60
--- abort
--- no_error_log
[error]
#--- SKIP
#--- ONLY
=== TEST 6: single put req "Connection: close", "-H", "Expect: 100-continue", "-H" , "Content-Length: 1000"
--- raw_request eval
use POSIX;
use lib '/home/deploy/dobjstor/proxy/test/t';
use MY::access qw(req);
use MY::access qw(gsign);
use URI::Escape;
my @url1 = ("http://192.168.122.214:7480/bucket777/obj6","-H","Host: 192.168.122.214:7480", "-H", "Connection: close", "-H", "Expect: 100-continue", "-H" , "Content-Length: 1000", "-X", "PUT");

my @arry1 = MY::access::gsign(@url1);
my $req1;
my $bodyy = 'objcontent' x 100;
$req1 = MY::access::req(\@arry1,  \$bodyy);
--- error_code: 100
--- timeout: 60
--- abort
--- no_error_log
[error]

#--- SKIP
#--- ONLY
