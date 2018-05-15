use t::MYTEST 'no_plan';
use strict;
use warnings;
use Data::Dumper;
use XML::Simple ':strict';
no_long_string();
log_level 'debug';

use Getopt::Long qw(GetOptions);
#state  $upid;
#package share;
#our $upid;
#package main;

#our $upid;


our  $bucket;
our  $object;
GetOptions(
    'bucket=s' => \$bucket,
    'object=s' => \$object,
);
print "bucket is $bucket \n";
print "object is $object \n";

print "package is in abcd: " . __PACKAGE__ . "\n";

#$ENV{'UPID'} = '';

sub getupid  {

    my ($body) = @_;
    #str=${result#*<UploadId>};
    #uploadid=${str%</UploadId>*};
    #return uploadid;
    #$_ =~ /\s+<UploadId>(\s+)</UploadId>/;
    #return $1;
    my $xs = XML::Simple->new( KeepRoot => 1, KeyAttr => 1, ForceArray => 0 );
    #my $xs = XML::Simple->new( KeepRoot => 1, KeyAttr => 1, ForceArray => 1 );
    my $ref = $xs->XMLin($body);
    print STDERR Data::Dumper->Dump( [ $ref ], [ '$ref' ] );
    #my $upid = $ref->{'InitiateMultipartUploadResult'}->{$UploadId};
    #$share::upid = $ref->{'InitiateMultipartUploadResult'}->{'UploadId'};
    my $upid = $ref->{'InitiateMultipartUploadResult'}->{'UploadId'};
    #print STDERR "$share::upid \n";
    # print STDERR "$upid \n"; also right
    print STDERR "to print packaganme\n";
    print "package is in getupid: " . __PACKAGE__ . "\n";
    # $ENV{'UPID'} = $ref->{'InitiateMultipartUploadResult'}->{'UploadId'};
    my $outfile = './t/upidfile';
    open (FILE, "> $outfile") || die "problem opening $outfile\n";
    print FILE $upid;
    close FILE;
    return $ref->{'InitiateMultipartUploadResult'}->{'UploadId'};
    
}
sub startupid {
    my  $desired_line_number = 1;
    my $filename = './t/upidfile';
    
    # use perl open function to open the file (or die trying)
    open(FILE, $filename) or die "Could not read from $filename, program halting.";
    
    # loop through the file with a perl while loop, stopping when you get to the desired record
    my $count = 1;
    while (<FILE>)
    {
      if ($count == $desired_line_number)
      {
        # print line, then get out of here
	#
	my $upidd = $_; 
	print "upidd is $_";
        close FILE;
	# exit;
        return $upidd;
      }
      $count++;
    }
    close FILE;
}
sub get_record
{
    # the filename should be passed in as a parameter
    # my $filename = shift;
    my $filename = './t/upidfile';
    open FILE, $filename or die "Could not read from $filename, program halting.";
    # read the record, and chomp off the newline
    chomp(my $record = <FILE>);
    close FILE;
    return $record;
}
#our  $bucket;
#GetOptions(
#    'bucket=s' => \$bucket,
#);
#print "bucket is $bucket \n";
no_root_location();
no_shuffle();
no_diff();
run_tests();


__DATA__



=== TEST 1: create bucket
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
my @url = ("http://192.168.122.214:8081/$::bucket","-H","Host: 192.168.122.214:8081", "-H", "Connection: close", "-H", "Content-Length: 0", "-H", "Content-Type: text/plain", "-X", "PUT");
$req1 = MY::access::makereq(\@url);
--- error_code: 200
--- timeout: 5000ms
--- abort
#--- ONLY
--- SKIP

=== TEST 2: mp init http://ip close 
--- raw_request eval
use POSIX;
use lib './t';
use MY::access qw(makereq);
use URI::Escape;
my $req1;
my $bodyy = '';
my $len =  length('objcontent' x 100);
my $file = '';
my @url1 = ("http://s3.dnion.com:8081/$::bucket/$::object?uploads","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-X", "POST");
$req1 = MY::access::makereq(\@url1);

--- response_body_filters eval
#print "package is in bodyfilters eval: " . __PACKAGE__ . "\n";
\&::getupid
--- error_code: 200
--- timeout: 50
--- no_error_log
[error]
#--- abort
--- SKIP
#--- ONLY

=== TEST 3: mp upload part,no md5，shoudl use http or capture or echo asyn send http://ip close 
--- raw_request eval
use POSIX;
use lib './t';
use MY::access qw(makereq);
use URI::Escape;
my $req1;
my $bodyy = '';
my $len =  length('objcontent' x 100);
my $file = '';
my $mybody="<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"1111111111111111\"</ETag></Part><Part><PartNumber>2</PartNumber><ETag>\"2222222222222222\"</ETag></Part></CompleteMultipartUpload>";
my $size = length($mybody);
#print "package is in rawreq eval: " . __PACKAGE__ . "\n";
#my $upid = ::startupid();
my $upid = &::get_record();
#print STDERR "upid is in test3 $upid\n";
#my @url1 = ("http://s3.dnion.com:8081/$::bucket/$::object?partNumber=1&uploadId=$ENV{'UPID'}","-H","Host: 192.168.34.59:7480","-H", "Connection: close", "-H", "Content-Length: $size", "-X", "PUT");
#my @url1 = ("http://s3.dnion.com:8081/$::bucket/$::object?partNumber=1&uploadId=$::upid","-H","Host: 192.168.34.59:7480","-H", "Connection: close", "-H", "Content-Length: $size", "-X", "PUT");
#my @url1 = ("http://s3.dnion.com:8081/$::bucket/$::object?partNumber=1&uploadId=$share::upid","-H","Host: 192.168.34.59:7480","-H", "Connection: close", "-H", "Content-Length: $size", "-X", "PUT");
my @url1 = ("http://s3.dnion.com:8081/$::bucket/$::object?partNumber=1&uploadId=$upid","-H","Host: 192.168.34.59:7480","-H", "Connection: close", "-H", "Content-Length: $size", "-X", "PUT");
$req1 = MY::access::makereq(\@url1, \$mybody,0,'');

--- error_code: 200
--- timeout: 50
--- no_error_log
[error]
#--- ONLY

=== TEST 4: multipart init http://ip close 
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
my @url1 = ("http://s3.dnion.com:8081/bucket1/object1?uploads","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-X", "POST");
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
--- SKIP
#--- ONLY

=== TEST 5: multipart http://ip close 
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
my @url1 = ("http://s3.dnion.com:8081/bucket1/object1?uploads","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-X", "POST");
$req1 = MY::access::makereq(\@url1);
#--- response_body:

--- response_body_like eval
qr/([^N]*)oSuchBucket(\S+)/

--- error_code: 404
--- timeout: 50
--- no_error_log
#[error]
#--- abort
--- SKIP
#--- ONLY

=== TEST 6: mp upload part,no md5，shoudl use http or capture or echo asyn send http://ip close 
--- raw_request eval
use POSIX;
use lib './t';
use MY::access qw(makereq);
use URI::Escape;
my $req1;
my $file = '/home/deploy/dobjstor/proxy/test/10MB';
my $size = -s $file;
my $upid = &::get_record();
my @url1 = ("http://s3.dnion.com:8081/$::bucket/$::object?partNumber=2&uploadId=$upid","-H","Host: 192.168.34.59:7480","-H", "Connection: close", "-H", "Content-Length: $size", "-X", "PUT");
$req1 = MY::access::makereq(\@url1, '',0,'',\$file);

#--- response_headers
#ETag
--- error_code: 200
--- timeout: 50
--- no_error_log
[error]
#--- abort
#--- SKIP
#--- ONLY
=== TEST 7: mp upload part,no md5，shoudl use http or capture or echo asyn send http://ip close 
--- raw_request eval
use POSIX;
use lib './t';
use MY::access qw(makereq);
use URI::Escape;
my $req1;
my $file = '/home/deploy/dobjstor/proxy/test/10MB';
my $size = -s $file;
my $upid = &::get_record();
my @url1 = ("http://s3.dnion.com:8081/$::bucket/$::object?partNumber=3&uploadId=$upid","-H","Host: 192.168.34.59:7480","-H", "Connection: close", "-H", "Content-Length: $size", "-X", "PUT");
$req1 = MY::access::makereq(\@url1, '',0,'',\$file);

--- error_code: 200
--- timeout: 50
--- no_error_log
[error]
#--- abort
#--- SKIP
#--- ONLY
=== TEST 8: mp upload part,no md5，shoudl use http or capture or echo asyn send http://ip close 
--- raw_request eval
use POSIX;
use lib './t';
use MY::access qw(makereq);
use URI::Escape;
my $req1;
my $file = '/home/deploy/dobjstor/proxy/test/10MB';
my $size = -s $file;
my $upid = &::get_record();
my @url1 = ("http://s3.dnion.com:8081/$::bucket/$::object?partNumber=4&uploadId=$upid","-H","Host: 192.168.34.59:7480","-H", "Connection: close", "-H", "Content-Length: $size", "-X", "PUT");
$req1 = MY::access::makereq(\@url1, '',0,'',\$file);

--- error_code: 200
--- timeout: 50
--- no_error_log
[error]
#--- abort
#--- SKIP
#--- ONLY



=== TEST 9: list multi part,not support http://ip close 
--- raw_request eval
use POSIX;
use lib './t';
use MY::access qw(makereq);
use URI::Escape;
my $req1;
my @url1 = ("http://s3.dnion.com:8081/$::bucket?uploads","-H","Host: s3.dnion.com:8081", "-H", "Connection: close", "-X", "GET");
$req1 = MY::access::makereq(\@url1);

#--- response_body_like eval
#qr/([^I]*)nitiateMultipartUploadresult(\S+)/
--- error_code: 405
--- timeout: 50
--- error_log
[error]
