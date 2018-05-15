#use lib '/home/deploy/dobjstor/proxy/test/';
use t::MYTEST 'no_plan';
no_long_string();
log_level 'debug';
#use Test::Nginx::Socket::Lua 'no_plan'; 

no_shuffle();
run_tests();
# problem :only one value


__DATA__




=== TEST 5: single put req NOT  includeing length "-H", "Connection: close", "-H", "Expect: ",
--- raw_request eval
use POSIX;
use lib '/home/deploy/dobjstor/proxy/test/t';
use MY::access qw(gsign);
use MY::access qw(req);
use URI::Escape;
my @url2 = ("http://s3.dnion.com:8081/bucket777/obj5","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-H", "Expect: ", "-X", "PUT");

my @arry2 = MY::access::gsign(@url2);
my $req2;
my $bodyd = 'objcontent' x 100;
$req2 = MY::access::req(\@arry2,  \$bodyd);
--- error_code: 500
--- timeout: 60
--- abort
#--- ONLY

=== TEST 4: single put req "Connection: close", "-H", "Expect: ", "-H" , "Content-Length: 1000",100<len?
--- raw_request eval
use POSIX;
#use lib '/home/deploy/dobjstor/proxy/test/';
use lib '/home/deploy/dobjstor/proxy/test/t';
use MY::access qw(req);
use MY::access qw(gsign);
use URI::Escape;
#my @url1 = ("http://192.168.122.24:8081/bucket777/obj6","-H","Host: 192.168.122.24:8081", "-H", "Connection: close", "-H", "Expect: ", "-H" , "Content-Length: 1000", "-X", "PUT");
my @url1 = ("http://s3.dnion.com:8081/bucket777/obj6","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-H", "Expect: ", "-H" , "Content-Length: 1000", "-X", "PUT");

my @arry1 = MY::access::gsign(@url1);
my $req1;
my $bodyy = 'objcontent' x 100;
$req1 = MY::access::req(\@arry1,  \$bodyy);
--- error_code: 200
--- timeout: 60
--- abort
#--- SKIP
#--- ONLY
