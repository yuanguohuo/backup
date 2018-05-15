package t::ACCESS;
use POSIX;
use Digest::HMAC_SHA1;
use FindBin;
use MIME::Base64 qw(encode_base64);
use URI::Escape;

my @endpoints = ( 's3.amazonaws.com',
                  's3-us-west-1.amazonaws.com',
                  's3-us-west-2.amazonaws.com',
                  's3-us-gov-west-1.amazonaws.com',
                  's3-eu-west-1.amazonaws.com',
                  's3-ap-southeast-1.amazonaws.com',
                  's3-ap-northeast-1.amazonaws.com',
                  's3-sa-east-1.amazonaws.com', 
);

#my $CURL = "curl";
my $keyId = "O911PT5Z34WN8Q92C8YU";
my $secretKey = "l0fjx5mHz3KoOOiiYILc6JtpXqVJNOCpYJXUlOIZ";
my $cmdLineSecretKey;
my %awsSecretAccessKeys = ();
my $keyFriendlyName;
#my $keyId;
#my $secretKey;
#my $contentType = "";
my $contentType;
my $acl;
my $contentMD5 = "";
my $fileToPut;
my $createBucket;
my $doDelete;
my $doHead;
my $help;
my $debug = 0;
my $copySourceObject;
my $copySourceRange;
my $postBody;
my $calculateContentMD5 = 0;
my $host;
my $resource;
my $port;
sub calculateStringContentMD5 {
    my ($string) = @_;
    my $md5 = Digest::MD5->new;
    $md5->add($string);
    my $b64 = encode_base64($md5->digest);
    chomp($b64);
    return $b64;
}

sub debug {
    my ($str) = @_;
    $str =~ s/\n/\\n/g;
    print STDERR "s3curl: $str\n" if ($debug);
}

sub getResourceToSign {
    my ($host, $resourceToSignRef) = @_;
    for my $ep (@endpoints) {
        if ($host =~ /(.*)\.$ep/) { # vanity subdomain case
            my $vanityBucket = $1;
            $$resourceToSignRef = "/$vanityBucket".$$resourceToSignRef;
            debug("vanity endpoint signing case");
            return;
        }
        elsif ($host eq $ep) {
            debug("ordinary endpoint signing case");
            return;
        }
    }
    # cname case
    $$resourceToSignRef = "/$host".$$resourceToSignRef;
    debug("cname endpoint signing case");
}
my %normHeaders;
my %xamzHeaders;
#my @normHeaders;
#push @args, ("-H", "x-amz-acl: $acl") if (defined $acl);
#push @args, ("-L");
#push @args, ("-H", "content-type: $contentType") if (defined $contentType);
#push @args, ("-H", "Content-MD5: $contentMD5") if (length $contentMD5);
#from getoption for perl
sub gsign {
    #url is curl args and option.
    my @url = @_; 
    #my @url = shift; only index
    my $uriandargs;
    my $method;
    $xamzHeaders{'x-amz-acl'}=$acl if (defined $acl);
    $xamzHeaders{'x-amz-copy-source'}=$copySourceObject if (defined $copySourceObject);
    $xamzHeaders{'x-amz-copy-source-range'}="bytes=$copySourceRange" if (defined $copySourceRange);
    # args is after --
    # try to understand curl args
    for (my $i=0; $i<@url; $i++) {
        my $arg = $url[$i];
        # resource name
        if ($arg =~ /https?:\/\/([^\/:?]+)(?::(\d+))?([^?]*)(?:\?(\S+))?/) {
    #               ?0/1 s :// 
    #               ([^\/:?]+) match host
    #               (?::(\d+)) mathc ipv6
    #               ?
    #               ([^?]*) util pars
    #               (?:\?(\S+))?  match ?parshu
    #
  #          $host = $1 if !$host;
  #          $port = defined $2 ? $2 : "";
  #          print "$host host\n";
  #          print "$port port\n";
            my $requestURI = $3;
            #my $requestURI = $3; need process strip the /t..
           # my $requestURI =  substr $3,2;
            print "$requestURI stripURI\n"; 
            my $query = defined $4 ? $4 : "";
            #$uriandargs = $requestURI .. $query;    
            if ($query eq "") {
                 $uriandargs = $requestURI;
            } else {
                 $uriandargs = $requestURI . "?" . $query;
            }    
            print "$uriandargs uriandargs\n";
            print "$requestURI requestURI\n";
            print "$query queryargs\n"; 
            #debug("Found the url: host=$host; port=$port; uri=$requestURI; query=$query;");
            if (length $requestURI) {
		    #$resource = "/t" . $requestURI;
                $resource = $requestURI;
            #may be change to location var
            } else {
		    #$resource = "/t" . "/";
                $resource = "/";
            }
            my @attributes = ();
            for my $attribute ("acl", "delete", "location", "logging", "notification",
                "partNumber", "policy", "requestPayment", "response-cache-control",
                "response-content-disposition", "response-content-encoding", "response-content-language",
                "response-content-type", "response-expires", "torrent",
                "uploadId", "uploads", "versionId", "versioning", "versions", "website", "lifecycle") {
                if ($query =~ /(?:^|&)($attribute(?:=[^&]*)?)(?:&|$)/) {
                    push @attributes, uri_unescape($1);
                }
            }
            if (@attributes) {
                $resource .= "?" . join("&", @attributes);
            }
            # handle virtual hosted requests for now dialbed 
#            getResourceToSign($host, \$resource);
        }
        elsif ($arg =~ /\-X/) {
            # mainly for DELETE
        print "entering in to the -x";
        print "$method  method1111\n"; 
        $method = $url[++$i];
        }
        elsif ($arg =~ /\-H/) {
        my $header = $url[++$i];
            #check for host: and x-amz*
            if ($header =~ /^[Hh][Oo][Ss][Tt]:(.+)$/) {
                $host = $1;
                print "$1 is hostH\n";
            }
            elsif ($header =~ /^([Xx]-[Aa][Mm][Zz]-.+): *(.+)$/) {
                my $name = lc $1;
                my $value = $2;
                # merge with existing values for now diable
		#    if (exists $xamzHeaders{$name}) {
                #        $value = $xamzHeaders{$name} . "," . $value;
                #    }
                $xamzHeaders{$name} = $value;
            }
            elsif ($header =~ /^([Cc][Oo][Nn][Tt][Ee][Nn][Tt]-[Tt][Yy][Pp][Ee]): *(.+)$/){# strip space
		    # my $name = lc $1;
                        my $value = $2;
			$contentType = $value;
			print "got contentype\n $contentTyep\n";
	    }
	    #elsif ($header =~ /(.+): *(.+)$/){# util:.
	    elsif ($header =~ /([^:]*): *(.+)$/){
		my $name = lc $1;
		my $value = $2;
#                if (exists $normHeaders{$name}) {
#                    $value = $normHeaders{$name} . "," . $value;
#                }
		$normHeaders{$name} = $value;#note the blanck such as conetent-type? not including host
		print " normheader is $name and $value\n";
           
		#if ($name =~ /^([Cc][Oo][Nn][Tt][Ee][Nn][Tt]-[Tt][Yy][Pp][Ee]): *(.+)$/){
	            
	    }   
	       
        }
    }
    
    die "Couldn't find resource by digging through your curl command line args!"
        unless defined $resource;
    
    my $xamzHeadersToSign = "";
    foreach (sort (keys %xamzHeaders)) {
        my $headerValue = $xamzHeaders{$_};
        $xamzHeadersToSign .= "$_:$headerValue\n";
    }
    print "$xamzHeadersToSign  xmhead\n";
    print "resource is \n$resource\nresource\n";   
    POSIX::setlocale(LC_TIME,"en_US.utf8");
    my $httpDate = POSIX::strftime("%a, %d %b %Y %H:%M:%S +0000", gmtime );

    # todo if compute md5,and  	$normHeaders{$name} = $value;
    my $stringToSign = "$method\n$contentMD5\n$contentType\n$httpDate\n$xamzHeadersToSign$resource";
    print "\nstrtosign\n$stringToSign\nstrtosign\n\n";    
    #debug("StringToSign='" . $stringToSign . "'");
    my $hmac = Digest::HMAC_SHA1->new($secretKey);
    $hmac->add($stringToSign);
    my $signature = encode_base64($hmac->digest, "");
    return ($method, $uriandargs, $signature, $httpDate);
}

sub req{ 
    ($i, $j) = @_;
    my $method = shift$i;
# where method?
    my $uriandargs = shift$i;
    my $signature = shift$i;
    my $httpDate = shift$i;
    #  my $body = shift$j;
    my $body = $$j;
    #my $body = $j[0];
    my $normHstring = "";
    foreach (sort (keys %normHeaders)) {
        my $headerValue = $normHeaders{$_};
        print "-- $_ \n";
        print "---- $headerValue \n";
	#$normHstring .= "$headerValue\n";
	if(!$headerValue){
 
        $normHstring .= "$_: \r\n";
        }
        else {
        $normHstring .= "$_: $headerValue\r\n";
        }
        print "aaLLLL\n$normHstring\n\n";
    }
#   for ($index =0; $index < @normHeaders; $index++){
#        my $headerValue = $normHeaders[$index];
#        $normHstring .= "$headerValue\n";
#       
#        print "aaLLLL$normHstring";
#    }
    print "normheadstring\n$normHstring";
    #todo xamz headers string
    my $xamzHstring = "";
    foreach (sort (keys %xamzHeaders)) {
        my $headerValue = $xamzHeaders{$_};
        $xamzHstring .= "$_: $headerValue\r\n";
    }
#    my $rawreq = "$method /t$uriandargs HTTP/1.1\nAuthorization: AWS $keyId:$signature\nDate: $httpDate\nHost: $host\n";
##host/port from tow src.
##    $rawreq = $rawreq . "x-amz-acl: $acl\n" if (defined $acl);
#     $rawreq = $rawreq . "$normHstring";
#     $rawreq = $rawreq . "$xamzHstring";
#     $rawreq = $rawreq . "content-type: $contentType\n" if (defined $contentType);
#     $rawreq = $rawreq . "Content-MD5: $contentMD5\n" if (length $contentMD5);
#     $rawreq = $rawreq . "\r\n\r\n";
#     $rawreq = $rawreq . "$body\n";
#    print "in req is \n$rawreq";
#    # other head? need put in this

    # beforea add " would be in the req,the test data section cannot stripe
    #ok my $rawreq = "$method /t$uriandargs HTTP/1.1\r\nAuthorization: AWS $keyId:$signature\r\nDate: $httpDate\r\nHost: $host\r\n";
   my $rawreq = "$method /t$uriandargs HTTP/1.1\r\nAuthorization: AWS $keyId:$signature\r\nDate: $httpDate\r\nHost: $host\r\n";
#host/port from tow src.
#    $rawreq = $rawreq . "x-amz-acl: $acl\n" if (defined $acl);
     $rawreq = $rawreq . "$normHstring";
     $rawreq = $rawreq . "$xamzHstring";
     #print STDERR "contacnt string before\n$rawreq";
     $rawreq = $rawreq . "Content-type: $contentType\r\n" if (defined $contentType);
     $rawreq = $rawreq . "Content-MD5: $contentMD5\r\n" if (length $contentMD5);
     #not relate the content-type and md5,maybe relate with the . and test:ngnxi parse.
     # ok$rawreq = $rawreq . "\r\n\r\n" . $body;
#     $rawreq = $rawreq . "\r\n\r\n";
     #$rawreq = join("\r\n\r\n",$rawreq, $body);
     $rawreq = join("\r\n",$rawreq, $body);
#     $rawreq = $rawreq . "\r";
     #muset /r/n on all heaer,or so nginx heaer not correct
     # now join and . alse functioning
#     if body is empley, no exe,but not empety,should the split req.
     #$rawreq = $rawreq . "\r\n\r\n" . uri_escape($body) . "\"";
# ok    $rawreq = $rawreq . "\r\n\r\n" . "$body";# double quote/r/rn escape .,''only literal
#     my $rawreq1 = substr $rawreq 1;

     # my $char1 = chop($rawreq);
     #print "chopchar1 is $char1";is t if the body "content"
     
#     $rawreq = $rawreq . "$body";
     # print STDERR "\n\n\n after contanct in req is \n$rawreq\n\n"; #why can print whyrawreq below cannot print?
    # other head? need put in this
    # my $ref_type = \$rawreq;
    # print STDERR "rawreq type is $ref_type";
    print STDERR "\n\nrawreq  is\n$rawreq\n";
    return $rawreq;
   
#    return "$method /t/$uriandargs HTTP/1.1\nAuthorization: AWS $keyId:$signature\nDate: $httpDate\nHost: 192.168.122.24\nx-amz-acl: $acl if (defined $acl);\ncontent-type: $contentType if (defined $contentType);\nContent-MD5: $contentMD5 if (length $contentMD5);\n\n";
}

#     #my @url = ("http://192.168.122.214:7480/bucket777/obj1","-H","Host: 192.168.122.214:7480", "-H", "Expect: ", "-H", "Content-Type: text/plain","-X", "PUT");
#     ##my @arry = ($method, $uriandargs, $signature, $httpDate) = &MY::access::gsign(@url);
#     #my @arry = &MY::access::gsign(@url);
#     #my $req1;
#     #my $body = 'objcontent';
#     #my @arry1 = ($body,);
#     #$req1 = MY::access::req(\@arry,  \@arry1);
#     ##$req1 = req($method, $uriandargs, $signature, $httpDate, $body);
#     ##$req1 = req($method, $uriandargs, $signature, $httpDate);
#     #print STDERR "req is \n$req1";
1;
