#use lib '/home/deploy/dobjstor/proxy/test/';
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
# problem :only one value


__DATA__

=== TEST 1: single put: incorrect md5 and using file ."Connection: close", "-H", "Expect: ", "-H" , "Content-Length: 1000",100<len?
--- raw_request eval
use POSIX;
use lib '/home/deploy/dobjstor/proxy/test/t';
use MY::access qw(makereq);
use URI::Escape;
my @url1 = ("http://s3.dnion.com:8081/bucket777/obj6","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-H", "Expect: ", "-H" , "Content-Length: 1000", "-X", "PUT");
my $req1;
my $bodyy = 'objcontent' x 100;
my $file = '/home/deploy/rnpatch';
$req1 = MY::access::makereq(\@url1, \$bodyy, 0,'2222',\$file);
--- error_code: 400
--- error_log
[error]
--- timeout: 60
--- abort
#--- SKIP
#--- ONLY

=== TEST 2: single put: correct md5 and using string ,normal len,no null.

--- raw_request eval
use POSIX;
use lib '/home/deploy/dobjstor/proxy/test/t';
use MY::access qw(makereq);
use URI::Escape;
my @url1 = ("http://s3.dnion.com:8081/bucket777/obj6","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-H", "Expect: ", "-H" , "Content-Length: 10", "-X", "PUT");
my $req1;
my $bodyy = 'objcontent';
my $len =  length('objcontent' x 100);
print STDERR "$len  len is";
my $file = '/home/deploy/rnpatch';
#$req1 = MY::access::makereq(\@url1, \$bodyy, 1,'',\$file);
$req1 = MY::access::makereq(\@url1, \$bodyy, 1,'');
--- error_code: 200
--- no_error_log
[error]
--- timeout: 60
--- abort
#--- SKIP
#--- ONLY

=== TEST 3: single put: correct md5 and using string ,normal len,no null.keepalive

--- raw_request eval
use POSIX;
use lib '/home/deploy/dobjstor/proxy/test/t';
use MY::access qw(makereq);
use URI::Escape;
my @url1 = ("http://s3.dnion.com:8081/bucket777/obj6","-H","Host: s3.dnion.com:8081", "-H", "Expect: ", "-H" , "Content-Length: 10", "-X", "PUT");
my $req1;
my $bodyy = 'objcontent';
my $len =  length('objcontent' x 100);
print STDERR "$len  len is";
my $file = '/home/deploy/rnpatch';
#$req1 = MY::access::makereq(\@url1, \$bodyy, 1,'',\$file);
$req1 = MY::access::makereq(\@url1, \$bodyy, 1,'');
--- error_code: 200
--- no_error_log
[error]
--- timeout: 5
--- abort
#--- SKIP
#--- ONLY



=== TEST 4: single put: correct md5 and using string ,incorrect len+1

--- raw_request eval
use POSIX;
use lib '/home/deploy/dobjstor/proxy/test/t';
use MY::access qw(makereq);
use URI::Escape;
my @url1 = ("http://s3.dnion.com:8081/bucket777/obj6","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-H", "Expect: ", "-H" , "Content-Length: 11", "-X", "PUT");
my $req1;
my $bodyy = 'objcontent';
my $len =  length('objcontent' x 100);
print STDERR "$len  len is";
my $file = '/home/deploy/rnpatch';
#$req1 = MY::access::makereq(\@url1, \$bodyy, 1,'',\$file);
$req1 = MY::access::makereq(\@url1, \$bodyy, 1,'');
--- error_code: 422
--- error_log
[error]
--- timeout: 6
--- abort
#--- SKIP
#--- ONLY



=== TEST 5: single put: no md5 using file ."Connection: close", "-H", "Expect: ", "-H" , "Content-Length: 1000",100<len?
--- raw_request eval
use POSIX;
use lib '/home/deploy/dobjstor/proxy/test/t';
use MY::access qw(makereq);
use URI::Escape;
my @url1 = ("http://s3.dnion.com:8081/bucket777/obj6","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-H", "Expect: ", "-H" , "Content-Length: 1000", "-X", "PUT");
my $req1;
my $bodyy = 'objcontent' x 100;
my $file = '/home/deploy/rnpatch';
$req1 = MY::access::makereq(\@url1, \$bodyy, 0,'',\$file);
#$req1 = MY::access::makereq(\@url1, \$bodyy, 0,'');
--- error_code: 200
--- no_error_log
[error]
--- timeout: 60
--- abort
#--- SKIP
#--- ONLY

=== TEST 6: single put: no md5 ."Connection: close", "-H", "Expect: ", "-H" , "Content-Length: 1000",100<len?
--- raw_request eval
use POSIX;
use lib '/home/deploy/dobjstor/proxy/test/t';
use MY::access qw(makereq);
use URI::Escape;
my @url1 = ("http://s3.dnion.com:8081/bucket777/obj6","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-H", "Expect: ", "-H" , "Content-Length: 1000", "-X", "PUT");
my $req1;
my $bodyy = 'objcontent' x 100;
$req1 = MY::access::makereq(\@url1, \$bodyy, 0,'');
--- error_code: 200
--- no_error_log
[error]
--- timeout: 60
--- abort
#--- SKIP
#--- ONLY



=== TEST 7: single put: chunk ?no "md5,length"  have "Connection: close", "-H", "Expect: ",
--- raw_request eval
use POSIX;
use lib '/home/deploy/dobjstor/proxy/test/t';
use MY::access qw(makereq);
use URI::Escape;
my @url2 = ("http://s3.dnion.com:8081/bucket777/obj5","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-H", "Expect: ", "-X", "PUT");
my $req2;
my $bodyd = 'objcontent' x 100;
$req2 = MY::access::makereq(\@url2, \$bodyd), 0, '');
--- error_code: 411
--- error_log
[error]
--- timeout: 60
--- abort
#--- ONLY

=== TEST 8: single put:correct md5 ,"Connection: close", "-H", "Expect: ", "-H" , "Content-Length: 1000",100<len?
--- raw_request eval
use POSIX;
use lib '/home/deploy/dobjstor/proxy/test/t';
use MY::access;
use URI::Escape;
my @url1 = ("http://s3.dnion.com:8081/bucket777/obj6","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-H", "Expect: ", "-H" , "Content-Length: 1000", "-X", "PUT");
my $req1;
my $bodyy = 'objcontent' x 100;
$req1 = MY::access::makereq(\@url1, \$bodyy, 1,'');

--- error_code: 200
--- no_error_log
[error]
--- timeout: 60
--- abort
#--- SKIP
#--- ONLY

=== TEST 9: single put req ,incorrect md5 ,"Connection: close", "-H", "Expect: ", "-H" , "Content-Length: 1000",100<len?
--- raw_request eval
use POSIX;
use lib '/home/deploy/dobjstor/proxy/test/t';
use MY::access;
use URI::Escape;
my @url1 = ("http://s3.dnion.com:8081/bucket777/obj6","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-H", "Expect: ", "-H" , "Content-Length: 1000", "-X", "PUT");
my $req1;
my $bodyy = 'objcontent' x 100;
$req1 = MY::access::makereq(\@url1, \$bodyy, 0,'11');
--- error_code: 400
--- error_log
[error]
--- timeout: 60
--- abort
#--- SKIP
#--- ONLY

=== TEST 10: single put: 400MB correct md5 and using big file ."Connection: close", "-H", "Expect: ", "-H" , "Content-Length: 1000",100<len?
--- raw_request eval
use POSIX;
use lib '/home/deploy/dobjstor/proxy/test/t';
use MY::access qw(makereq);
use URI::Escape;
my $req1;
my $bodyy = 'objcontent' x 100;
my $file = '/home/deploy/dobjstor/proxy/test/testbig';
my $size = -s $file;
my @url1 = ("http://s3.dnion.com:8081/bucket777/obj80","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-H", "Expect: ", "-H" , "Content-Length: $size", "-X", "PUT");
#$req1 = MY::access::makereq(\@url1, \$bodyy, 1,'',\$file);
$req1 = MY::access::makereq(\@url1, '', 1,'',\$file);
--- error_code: 200
--- no_error_log
[error]
--- timeout: 60
--- abort
#--- SKIP
#--- ONLY

=== TEST 11: single put: 80MB correct md5 and using big file ."Connection: close", "-H", "Expect: ", "-H" , "Content-Length: 1000",100<len?
--- raw_request eval
use POSIX;
use lib '/home/deploy/dobjstor/proxy/test/t';
use MY::access qw(makereq);
use URI::Escape;
my $req1;
my $bodyy = 'objcontent' x 100;
my $file = '/home/deploy/dobjstor/proxy/test/80MB';
my $size = -s $file;
my @url1 = ("http://s3.dnion.com:8081/bucket777/obj7","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-H", "Expect: ", "-H" , "Content-Length: $size", "-X", "PUT");
#$req1 = MY::access::makereq(\@url1, \$bodyy, 1,'',\$file);
$req1 = MY::access::makereq(\@url1, '', 1,'',\$file);
--- error_code: 200
--- no_error_log
[error]
--- timeout: 60
--- abort
#--- SKIP
#--- ONLY

=== TEST 12: single put:10MB correct md5 and using big file ."Connection: close", "-H", "Expect: ", "-H" , "Content-Length: 1000",100<len?
--- raw_request eval
use POSIX;
use lib '/home/deploy/dobjstor/proxy/test/t';
use MY::access qw(makereq);
use URI::Escape;
my $req1;
my $bodyy = 'objcontent' x 100;
my $file = '/home/deploy/dobjstor/proxy/test/10MB';
my $size = -s $file;
my @url1 = ("http://s3.dnion.com:8081/bucket777/obj7","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-H", "Expect: ", "-H" , "Content-Length: $size", "-X", "PUT");
#$req1 = MY::access::makereq(\@url1, \$bodyy, 1,'',\$file);
$req1 = MY::access::makereq(\@url1, '', 1,'',\$file);
--- error_code: 200
--- no_error_log
[error]
--- timeout: 60
--- abort
#--- SKIP
#--- ONLY

=== TEST 13: single put: 2mB."Connection: close", "-H", "Expect: ", "-H" , "Content-Length: 1000",100<len?
--- raw_request eval
use POSIX;
use lib '/home/deploy/dobjstor/proxy/test/t';
use MY::access qw(makereq);
use URI::Escape;
my $req1;
my $bodyy = 'objcontent' x 100;
my $file = '/home/deploy/dobjstor/proxy/test/2MB';
my $size = -s $file;
my @url1 = ("http://s3.dnion.com:8081/bucket777/obj7","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-H", "Expect: ", "-H" , "Content-Length: $size", "-X", "PUT");
$req1 = MY::access::makereq(\@url1, \$bodyy, 1,'',\$file);
#$req1 = MY::access::makereq(\@url1, '', 1,'',\$file);
--- error_code: 200
--- no_error_log
[error]
--- timeout: 60
--- abort
#--- SKIP
#--- ONLY
=== TEST 14: single put: correct md5 and using small file ."Connection: close", "-H", "Expect: ", "-H" , "Content-Length: 1000",100<len?
--- raw_request eval
use POSIX;
use lib '/home/deploy/dobjstor/proxy/test/t';
use MY::access qw(makereq);
use URI::Escape;
my @url1 = ("http://s3.dnion.com:8081/bucket777/obj6","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-H", "Expect: ", "-H" , "Content-Length: 1000", "-X", "PUT");
my $req1;
my $bodyy = 'objcontent' x 100;
my $len =  length('objcontent' x 100);
print "$len len is \n";
my $file = '/home/deploy/rnpatch';
$req1 = MY::access::makereq(\@url1, \$bodyy, 1,'',\$file);
#$req1 = MY::access::makereq(\@url1, \$bodyy, 1,'');
--- error_code: 422
--- error_log
[error]
--- timeout: 60
--- abort
#--- SKIP
#--- ONLY



=== TEST 15: single put: correct md5 and using small file correct len string len+1,coparawithstring ."Connection: close", "-H", "Expect: ", "-H" , "Content-Length: 1000",100<len?
--- raw_request eval
use POSIX;
use lib '/home/deploy/dobjstor/proxy/test/t';
use MY::access qw(makereq);
use URI::Escape;
my @url1 = ("http://s3.dnion.com:8081/bucket777/obj6","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-H", "Expect: ", "-H" , "Content-Length: 11", "-X", "PUT");
my $req1;
my $bodyy = 'objcontent' x 100;
my $len =  length('objcontent' x 100);
my $file = '/home/deploy/testfile';
$req1 = MY::access::makereq(\@url1, \$bodyy, 1,'',\$file);
#$req1 = MY::access::makereq(\@url1, \$bodyy, 1,'');
--- error_code: 200
--- no_error_log
[error]
--- timeout: 60
--- abort
#--- SKIP
#--- ONLY



=== TEST 16: single put: correct md5 and using small file len-1 string len+1  ."Connection: close", "-H", "Expect: ", "-H" , "Content-Length: 1000",100<len?
--- raw_request eval
use POSIX;
use lib '/home/deploy/dobjstor/proxy/test/t';
use MY::access qw(makereq);
use URI::Escape;
my @url1 = ("http://s3.dnion.com:8081/bucket777/obj6","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-H", "Expect: ", "-H" , "Content-Length: 10", "-X", "PUT");
my $req1;
my $bodyy = 'objcontent' x 100;
my $len =  length('objcontent' x 100);
my $file = '/home/deploy/testfile';
$req1 = MY::access::makereq(\@url1, \$bodyy, 1,'',\$file);
#$req1 = MY::access::makereq(\@url1, \$bodyy, 1,'');
--- error_code: 422
--- error_log
[error]
--- timeout: 60
--- abort
#--- SKIP
#--- ONLY
=== TEST 17: single put: correct md5, small file,correct len string len
--- raw_request eval
use POSIX;
use lib '/home/deploy/dobjstor/proxy/test/t';
use MY::access qw(makereq);
use URI::Escape;
my $req1;
my $bodyy = 'objcontent' x 100;
my $len =  length('objcontent' x 100);
my $file = '/home/deploy/testfile';
my $size = -s $file;
my @url1 = ("http://s3.dnion.com:8081/bucket777/obj6","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-H", "Expect: ", "-H" , "Content-Length: $size", "-X", "PUT");
#$req1 = MY::access::makereq(\@url1, \$bodyy, 1,'',\$file);
$req1 = MY::access::makereq(\@url1, '', 1,'',\$file);
--- error_code: 200
--- no_error_log
[error]
--- timeout: 60
--- abort
#--- SKIP
#--- ONLY
=== TEST 18: single put: incorrect len string len+1correct md5,small file,
--- raw_request eval
use POSIX;
use lib '/home/deploy/dobjstor/proxy/test/t';
use MY::access qw(makereq);
use URI::Escape;
my $req1;
my $bodyy = 'objcontent' x 100;
my $len =  length('objcontent' x 100);
my $file = '/home/deploy/testfile';
my $size = -s $file;
$size = $size + 1;
my @url1 = ("http://s3.dnion.com:8081/bucket777/obj6","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-H", "Expect: ", "-H" , "Content-Length: $size", "-X", "PUT");
$req1 = MY::access::makereq(\@url1, \$bodyy, 1,'',\$file);
#$req1 = MY::access::makereq(\@url1, \$bodyy, 1,'');
--- error_code: 400
--- error_log
[error]
--- timeout: 5
--- abort`
#--- SKIP
#--- ONLY
=== TEST 19: single put:10MB correct md5 and using big file ."Connection: close", "-H", "Expect: ", "-H" , "Content-Length: 1000",100<len?
--- raw_request eval
use POSIX;
use lib '/home/deploy/dobjstor/proxy/test/t';
use MY::access qw(makereq);
use URI::Escape;
my $req1;
my $bodyy = 'objcontent' x 100;
my $file = '/home/deploy/dobjstor/proxy/test/10MB';
my $size = -s $file;
my @url1 = ("http://s3.dnion.com:8081/$::bucket777/$::object","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-H", "Expect: ", "-H" , "Content-Length: $size", "-X", "PUT");
print STDERR "url1[0]";
#$req1 = MY::access::makereq(\@url1, \$bodyy, 1,'',\$file);
$req1 = MY::access::makereq(\@url1, '', 1,'',\$file);
--- error_code: 200
--- no_error_log
[error]
--- timeout: 60
--- abort
#--- SKIP
--- ONLY

