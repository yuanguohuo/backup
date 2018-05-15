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



=== TEST 1: reg exp parse auth header
--- request
GET /
--- more_headers
Authorization: AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/s3/aws4_request,SignedHeaders=content-type;host;x-amz-date,Signature=5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7
--- error_code: 200
--- timeout: 5000ms
--- abort

