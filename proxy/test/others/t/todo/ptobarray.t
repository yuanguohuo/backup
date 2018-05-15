use Test::Nginx::Socket 'no_plan';
use lib '/home/deploy/dobjstor/proxy/test/';
use t::MYTEST 'no_plan';
#use t::MYTEST qw(no_long_string);
#t::MYTEST::no_long_string();
no_long_string();
log_level 'info';
#use Test::Nginx::Socket::Lua 'no_plan'; 


use POSIX;
use lib '/home/deploy/dobjstor/proxy/test/';
#use MY::access qw(req);
#use MY::access qw(gsign);
use t::ACCESS qw(req);
use t::ACCESS qw(gsign);
use URI::Escape;

my @url1 = ("http://192.168.122.214:7480/bucket777/obj6","-H","Host: 192.168.122.214:7480", "-H", "Connection: close", "-H", "Expect: ", "-H" , "Content-Length: 1000", "-X", "PUT");
my @url2 = ("http://192.168.122.214:7480/bucket777/obj5","-H","Host: 192.168.122.214:7480", "-H", "Connection: close", "-H", "Expect: ", "-X", "PUT");
#my @url = ("http://192.168.122.214:7480/bucket777/obj1","-H","Host: 192.168.122.214:7480", "-H", "Content-Type: text/plain","-X", "PUT");
#my @url = ("http://192.168.122.214:7480/bucket777/obj1","-H","Host: 192.168.122.214:7480", "-X", "PUT");

my @arry1 = t::ACCESS::gsign(@url1);
my @arry2 = t::ACCESS::gsign(@url2);
our $req1;
our $req2;
my $body = 'objcontent' x 100;
$req1 = t::ACCESS::req(\@arry1,  \$body);
$req2 = t::ACCESS::req(\@arry2,  \$body);
# or my @req = ("$req1" , "$req2");
run_tests();

__DATA__

=== TEST 1: put req array

--- raw_request eval
use POSIX;
use lib '/home/deploy/dobjstor/proxy/test/';
#use MY::access qw(req);
#use MY::access qw(gsign);
use t::ACCESS qw(req);
use t::ACCESS qw(gsign);
use URI::Escape;

my @url1 = ("http://192.168.122.214:7480/bucket777/obj6","-H","Host: 192.168.122.214:7480", "-H", "Connection: close", "-H", "Expect: ", "-H" , "Content-Length: 1000", "-X", "PUT");
my @url2 = ("http://192.168.122.214:7480/bucket777/obj5","-H","Host: 192.168.122.214:7480", "-H", "Connection: close", "-H", "Expect: ", "-X", "PUT");
#my @url = ("http://192.168.122.214:7480/bucket777/obj1","-H","Host: 192.168.122.214:7480", "-H", "Content-Type: text/plain","-X", "PUT");
#my @url = ("http://192.168.122.214:7480/bucket777/obj1","-H","Host: 192.168.122.214:7480", "-X", "PUT");

my @arry1 = t::ACCESS::gsign(@url1);
my @arry2 = t::ACCESS::gsign(@url2);
my $req1;
my $req2;
my $body = 'objcontent' x 100;
$req1 = t::ACCESS::req(\@arry1,  \$body);
$req2 = t::ACCESS::req(\@arry2,  \$body);
# or my @req = ("$req1" , "$req2");
[$req1,$req2]

--- error_code: [200, 200]
--- timeout: 60
--- abort

=== TEST 2: put req improve

--- raw_request eval
[$::req1, $::req2]

--- error_code: [200, 200]
--- timeout: 60
--- abort
--- ONLY
