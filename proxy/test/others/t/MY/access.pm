package MY::access;
use strict;
use POSIX;
use Digest::HMAC_SHA1;
use FindBin;
use MIME::Base64 qw(encode_base64);
use URI::Escape;
use File::Slurper qw(read_text);
use File::Slurper qw(read_binary);
use Unicode::UTF8 'decode_utf8';
use Digest::MD5;
my @endpoints = ( 's3.amazonaws.com',
                  's3-us-west-1.amazonaws.com',
                  's3-us-west-2.amazonaws.com',
                  's3-us-gov-west-1.amazonaws.com',
                  's3-eu-west-1.amazonaws.com',
                  's3-ap-southeast-1.amazonaws.com',
                  's3-ap-northeast-1.amazonaws.com',
                  's3-sa-east-1.amazonaws.com', 
);
use constant STAT_MODE => 2;
use constant STAT_UID => 4;

#my $keyId = "O911PT5Z34WN8Q92C8YU";
my $keyId;
#my $secretKey = "l0fjx5mHz3KoOOiiYILc6JtpXqVJNOCpYJXUlOIZ";
my $secretKey;
my $cmdLineSecretKey;
my %awsSecretAccessKeys = ();
my $keyFriendlyName;
my $doDelete;
my $doHead;
my $help;
my $debug = 0;
my $copySourceObject;
my $copySourceRange;
# fow now we have not impletn the post body op
my $DOTFILENAME="s3curl";
my $EXECFILE=$FindBin::Bin;
my $LOCALDOTFILE = $EXECFILE . "/" . $DOTFILENAME;
print "Localdotfile is\n", $LOCALDOTFILE,"\n";
my $HOMEDOTFILE = $ENV{HOME} . "/" . $DOTFILENAME;
my $DOTFILE = -f $LOCALDOTFILE? $LOCALDOTFILE : $HOMEDOTFILE;

if (-f $DOTFILE) {
    open(CONFIG, $DOTFILE) || die "can't open $DOTFILE: $!";

    my @stats = stat(*CONFIG);

    if (($stats[STAT_UID] != $<) || $stats[STAT_MODE] & 066) {
        die "I refuse to read your credentials from $DOTFILE as this file is " .
            "readable by, writable by or owned by someone else. Try " .
            "chmod 600 $DOTFILE";
    }

    my @lines = <CONFIG>;
    close CONFIG;
    eval("@lines");
    die "Failed to eval() file $DOTFILE:\n$@\n" if ($@);
}

if ($cmdLineSecretKey) {
    printCmdlineSecretWarning();
    sleep 5;

    $secretKey = $cmdLineSecretKey;
} else {
    my $keyinfo = $awsSecretAccessKeys{personal};
    #my $keyinfo = $awsSecretAccessKeys{$keyId};
    die "I don't know about key with friendly name $keyId. " .
        "Do you need to set it up in $DOTFILE?"
        unless defined $keyinfo;

    $keyId = $keyinfo->{id};
    $secretKey = $keyinfo->{key};
}
sub readfile {
     my	($filename) = @_;
       	open my $fh, '<', $filename or die $!; 
	return POSIX::read(fileno $fh, my $content, -s $fh);
}
sub uri_unescape {
  my ($input) = @_;
  $input =~ s/\%([A-Fa-f0-9]{2})/pack('C', hex($1))/seg;
  debug("replaced string: " . $input);
  return ($input);
}

# generate the XML for bucket creation.
sub getCreateBucketData {
    my ($createBucket) = @_;

    my $data = "";
    if (length($createBucket) > 0) {
        $data = "<CreateBucketConfiguration><LocationConstraint>$createBucket</LocationConstraint></CreateBucketConfiguration>";
    }
    return $data;
}

# calculates the MD5 header for a string.
sub calculateStringContentMD5 {
    my ($string) = @_;
    my $md5 = Digest::MD5->new;
    $md5->add($string);
    my $b64 = encode_base64($md5->digest);
    chomp($b64);
    return $b64;
}

# calculates the MD5 header for a file.
sub calculateFileContentMD5 {
    my ($file_name) = @_;
    open(FILE, "<$file_name") || die "could not open file $file_name for MD5 calculation";
    binmode(FILE) || die "could not set file reading to binary mode: $!";
    my $md5 = Digest::MD5->new;
    $md5->addfile(*FILE);
    close(FILE) || die "could not close $file_name";

    #print "fileMD5 before base64 is" $md5->digest;
    print "fileMD5 hexdigest before base64 is\n", $md5->clone->hexdigest,"\n";

    my $b64 = encode_base64($md5->digest);
    print "fileMD5 base64 is\n", $b64,"\n";
    chomp($b64);
    print "fileMD5 base64 chomp is\n", $b64,"\n";
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
sub makereq {
    #url is curl args and option.
    #my @url = @_; 
    #my @url = shift; only index

    my $contentMD5 = "";
    my $calculateContentMD5 = 0;
    my $uriandargs;
    my $method;
    my $host;
    my $resource;
    my %normHeaders;
    my %xamzHeaders;
    my $port;
    my $contentType;
    my $acl;
    my $fileToPut;
    # location par for create bucket
    my $createBucket;
    my $postBody;
    # deprected ,becase same as put
    my ($i, $j, $k, $l, $m) = @_;
    #my ($i, $j) = @_;
    my @url= @$i; 
    my $body = $$j if $j;
    # my $body = $$j;
    my $calculateContentMD5 = $k if $k;
    my $sMD5 = $l if $l;

#    $fileToPut = $$m if defined $m;
    $fileToPut = $$m if $m;

    print "put file is $fileToPut, $$m\n" if $m; 
    print "$calculateContentMD5 need md5 calcu is\n"; 
    if ($calculateContentMD5) {
        if ($fileToPut) {
            $contentMD5 = calculateFileContentMD5($fileToPut);
            print "fileMD5 after compu is\n$contentMD5\n";
        }elsif ($body) {
            # for put 
            $contentMD5 = calculateStringContentMD5($body);
            print "fileMD5 after compu string is\n$contentMD5\n";
        } elsif ($createBucket) {
            $contentMD5 = calculateStringContentMD5(getCreateBucketData($createBucket));
            print "fileMD5 after compu locationcreatebucket is\n$contentMD5\n";
        } elsif ($postBody) {
            $contentMD5 = calculateFileContentMD5($postBody);
        } else {
            $contentMD5 = calculateStringContentMD5('');
            print "fileMD5 empty string is\n$contentMD5\n";
        }
    }
    else {
	    if ($l){
            $contentMD5 = $l;
            print "fileMD5 incorretct md5  is\n$contentMD5\n";
            }
#            else {
#            $contentMD5 = calculateStringContentMD5('');
#            print "fileMD5 empty last string is\n$contentMD5\n";
#	    }

             print "stringContentMD5 md5 is\n$l\n"; 
    }

    print "fileMD5 finally is  \n$contentMD5\n";
# all pass from test case below 
#    $xamzHeaders{'x-amz-acl'}=$acl if (defined $acl);
#    $xamzHeaders{'x-amz-copy-source'}=$copySourceObject if (defined $copySourceObject);
#    $xamzHeaders{'x-amz-copy-source-range'}="bytes=$copySourceRange" if (defined $copySourceRange);
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
#            print "$requestURI stripURI\n"; 
            my $query = defined $4 ? $4 : "";
            #$uriandargs = $requestURI .. $query;    
            if ($query eq "") {
                 $uriandargs = $requestURI;
            } else {
                 $uriandargs = $requestURI . "?" . $query;
            }    
	    print "$arg url\n";
	    print "$uriandargs uriandargs\n";
	    print "$requestURI requestURI\n";
#            print "$query queryargs\n"; 
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
#        print "entering in to the -x";
#        print "$method  method1111\n"; 
        $method = $url[++$i];
        }
        elsif ($arg =~ /\-H/) {
        my $header = $url[++$i];
            #check for host: and x-amz*
            if ($header =~ /^[Hh][Oo][Ss][Tt]:(.+)$/) {
                $host = $1;
 #              print "$1 is hostH\n";
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
#			print "got contentype\n $contentType\n";
	    }
	    #elsif ($header =~ /(.+): *(.+)$/){# util:.
	    elsif ($header =~ /([^:]*): *(.+)$/){
		my $name = lc $1;
		my $value = $2;
#                if (exists $normHeaders{$name}) {
#                    $value = $normHeaders{$name} . "," . $value;
#                }
		$normHeaders{$name} = $value;#note the blanck such as conetent-type? not including host
#		print " normheader is $name and $value\n";
           
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
    $contentType = "text/plain";

    # todo if compute md5,and  	$normHeaders{$name} = $value;
    my $stringToSign = "$method\n$contentMD5\n$contentType\n$httpDate\n$xamzHeadersToSign$resource";
    # print "\nstrtosign\n$stringToSign\nstrtosign\n\n";    
    #debug("StringToSign='" . $stringToSign . "'");
    my $hmac = Digest::HMAC_SHA1->new($secretKey);
    $hmac->add($stringToSign);
    my $signature = encode_base64($hmac->digest, "");
#    return ($method, $uriandargs, $signature, $httpDate);

#    print STDERR "body is $body[0]\n";
#    print STDERR "body is $j[0]\n";
#    print STDERR "body is $j\n";
#    print STDERR "body is $$j\n";
#    print STDERR "uriandargs is $uriandargs\n";
#    print STDERR "signa is $signature\n";
#    print STDERR "httpdate is $httpDate\n\n";
    my $normHstring = "";
    foreach (sort (keys %normHeaders)) {
        my $headerValue = $normHeaders{$_};
#        print "-- $_ \n";
#        print "---- $headerValue \n";
	#$normHstring .= "$headerValue\n";
	if (defined $headerValue) {

        $normHstring .= "$_: $headerValue\r\n";

        }
	else {

        $normHstring .= "$_: \r\n";

        }
#	if(!$headerValue){
# 
#        $normHstring .= "$_: \r\n";
#        }
#        else {
#        $normHstring .= "$_: $headerValue\r\n";
#        }
#        print "aaLLLL\n$normHstring\n\n";
    }
#    print "normheadstring\n$normHstring";
    #todo xamz headers string
    my $xamzHstring = "";
    foreach (sort (keys %xamzHeaders)) {
        my $headerValue = $xamzHeaders{$_};
        $xamzHstring .= "$_: $headerValue\r\n";
    }
    # beforea add " would be in the req,the test data section cannot stripe
    #ok my $rawreq = "$method /t$uriandargs HTTP/1.1\r\nAuthorization: AWS $keyId:$signature\r\nDate: $httpDate\r\nHost: $host\r\n";
    #my $rawreq = "$method /t$uriandargs HTTP/1.1\r\nAuthorization: AWS $keyId:$signature\r\nDate: $httpDate\r\nHost: $host\r\n";
    my $rawreq;
    if (length $uriandargs) {
	    #$resource = "/t" . $requestURI;
	    #$rawreq = "$method $uriandargs HTTP/1.1\r\nAuthorization: AWS $keyId:$signature\r\nDate: $httpDate\r\nHost: $host\r\n";
       $rawreq = "$method $uriandargs HTTP/1.1\r\nUser-Agent: test\r\nAccept: */*\r\nAuthorization: AWS $keyId:$signature\r\nDate: $httpDate\r\nHost: $host\r\n";
      # for one req
      # $rawreq = "$method $uriandargs\r\nUser-Agent: test\r\nAccept: */*\r\nAuthorization: AWS $keyId:$signature\r\nDate: $httpDate\r\nHost: $host\r\n";
        # for benchmarking why remove http1.1 ,the v is fast?
    #may be change to location var
    } else {
	    #$resource = "/t" . "/";
	    #$rawreq = "$method /$uriandargs HTTP/1.1\r\nAuthorization: AWS $keyId:$signature\r\nDate: $httpDate\r\nHost: $host\r\n";
        $rawreq = "$method /$uriandargs HTTP/1.1\r\nUser-Agent: test\r\nAccept: */*\r\nAuthorization: AWS $keyId:$signature\r\nDate: $httpDate\r\nHost: $host\r\n";
    }
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
    if ($fileToPut) {
         #print STDERR " filetoput is $fileToPut\n";
         # print STDERR " filetoput is read_text($fileToPut)\n";
         # my fcontent = read_text($fileToPut);
	    #   open(FILE, "<$fileToPut") || die "could not open file $fileToPut";
	    ## binmode(FILE) || die "could not set file reading to binary mode: $!";
	    #    $rawreq = join("\r\n",$rawreq, read_text($fileToPut));
	    #     my $textcont = read_text($fileToPut);
            #     my $len = length $textcont;
	    #      print STDERR  " $len \n";
	    #     my $textcont = read_text($fileToPut,'UTF-8');
            #     my $len = length $textcont;
	    #      print STDERR  " $len \n";
#	      $rawreq = join("\r\n",$rawreq, read_text($fileToPut,'UTF-8','off'));
#	     my $binarycont = read_binary($fileToPut);
#             my $len1 = length $binarycont;
#	      print STDERR " binary len is $len1 \n";
#              my $size = -s $fileToPut;
#	      print STDERR " -s size len is $size \n";

#	     my $textcont = read_text($fileToPut) ;
#             my $len1 = length $textcont;
#	      print STDERR " text len is  $len1 \n";
#
	      # my $posixread = readfile($fileToPut);	      
	      $rawreq = join("\r\n", $rawreq, read_binary($fileToPut));
#	      $rawreq = join("\r\n", $rawreq, utf8::decode($binarycont));
	      #$rawreq = join("\r\n", $rawreq, $posixread);
	      #chomp($rawreq);
	      # $rawreq = join("\r\n",$rawreq, fcontent);
     } else {
	     #   $rawreq = join("\r\n",$rawreq, $body) if (defined $body);
	      $rawreq = join("\r\n",$rawreq, $body)
       #if body is empty,we should place an rn in the end ,so donnot check the body.
     }
     #  $rawreq = $rawreq . "\r\n"; #dosent matter?
     # $rawreq = $rawreq . "\r"; #dosent matter?
    
     # print STDERR "\n\nrawreq  is\n$rawreq\nnewlineend";
    return $rawreq;
}
1;
#my $stringToSign = "GET\n\n\n$httpDate\n/t/1bkt";
    #
    #print STDERR "tosign:\n$stringToSign";
    
    #debug("StringToSign='" . $stringToSign . "'");
#    my $hmac = Digest::HMAC_SHA1->new($secretKey);
#    $hmac->add($stringToSign);
#    my $signature = encode_base64($hmac->digest, "");
#my @rawreq = ();
#push @rawreq, ($method);
#push @rawreq, ("/t/$uriandargs"); 
#push @args, ("-H", "Date: $httpDate");
#push @args, ("-H", "Authorization: AWS $keyId:$signature");
#push @args, ("-H", "x-amz-acl: $acl") if (defined $acl);
#push @args, ("-L");
#push @args, ("-H", "content-type: $contentType") if (defined $contentType);
#push @args, ("-H", "Content-MD5: $contentMD5") if (length $contentMD5);
#push @args, ("-T", $fileToPut) if (defined $fileToPut);
#push @args, ("-X", "DELETE") if (defined $doDelete);
#push @args, ("-X", "POST") if(defined $postBody);
#push @args, ("-I") if (defined $doHead);
#
#   for ($index =0; $index < @normHeaders; $index++){
#        my $headerValue = $normHeaders[$index];
#        $normHstring .= "$headerValue\n";
#       
#        print "aaLLLL$normHstring";
#    }
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
   
#    return "$method /t/$uriandargs HTTP/1.1\nAuthorization: AWS $keyId:$signature\nDate: $httpDate\nHost: 192.168.122.24\nx-amz-acl: $acl if (defined $acl);\ncontent-type: $contentType if (defined $contentType);\nContent-MD5: $contentMD5 if (length $contentMD5);\n\n";
