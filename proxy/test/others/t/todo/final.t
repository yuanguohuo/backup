use t::MYTEST 'no_plan';
no_long_string();
log_level 'debug';

no_shuffle();
run_tests();


__DATA__


=== TEST 10: single put: correct md5 and using small file correct len string len+1 ."Connection: close", "-H", "Expect: ", "-H" , "Content-Length: 1000",100<len?
--- raw_request eval
use POSIX;
use lib '/home/deploy/dobjstor/proxy/test/t';
use MY::access qw(makereq);
use URI::Escape;
my @url1 = ("http://s3.dnion.com:8081/bucket777/obj6","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-H" , "Content-Length: 11", "-X", "PUT");
my $req1;
my $bodyy = 'objcontent' x 100;
my $len =  length('objcontent' x 100);
my $file = '/home/deploy/testfile';
$req1 = MY::access::makereq(\@url1, \$bodyy, 1,'',\$file);
--- error_code: 200
--- no_error_log
[error]
--- timeout: 60
--- abort
#--- SKIP
--- ONLY
