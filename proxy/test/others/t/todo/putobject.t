use Test::Nginx::Socket 'no_plan';
no_long_string();
log_level 'info';
#use Test::Nginx::Socket::Lua 'no_plan'; 
run_tests();

__DATA__



=== TEST 1: raw request eval including length put object on real bucket
--- main_config
error_log logs/error.log info;

--- http_config
    lua_package_path    "/home/deploy/dobjstor/proxy/?.lua;/home/deploy/dobjstor/proxy/thirdparty/?.lua;/home/deploy/dobjstor/proxy/ostore_api/librados/?.lua;/home/deploy/dobjstor/proxy/common/?.lua;;";
    lua_package_cpath    "/home/deploy/dobjstor/proxy/?.so;/home/deploy/dobjstor/proxy/thirdparty/?.so;;";
    lua_shared_dict     cephvars    1M;
    init_by_lua_file "/home/deploy/dobjstor/proxy/radosload.lua";

--- config
    location /t {
        client_max_body_size 10000000k;
        client_body_buffer_size 100000k;
        default_type text/plain;
        content_by_lua_file "/home/deploy/dobjstor/proxy/storageproxy.lua";
    }

#--- raw_request eval chomp
#--- raw_request eval
--- raw_request eval
use POSIX;
use lib '/home/deploy/dobjstor/proxy/test/t/';
use MY::access qw(req);
use MY::access qw(gsign);
use URI::Escape;
#curl para
##my @url = ("http://192.168.122.214:7480/bucket777/obj2","-H","Host: 192.168.122.214:7481", "-H", "Expect: ", "-H", "Content-Length: ${length}", "-H", "Content-Type: text/plain","-X", "PUT");
#my @url = ("http://192.168.122.214:7480/bucket777/obj1","-H","Host: 192.168.122.214:7480", "-H", "Expect:", "-H", "Content-Type: text/plain","-X", "PUT");
#my ($method, $uriandargs, $signature, $httpDate) = &MY::access::gsign(@url);
#my $req1;
#$req1 = &MY::access::req($method, $uriandargs, $signature, $httpDate);
#my $body = 'objcontent';
##$req1 = $req1 ."\r\n";
##$req1 = $req1 . "\n\n" . "value=$body" if (defined $body);
##$req1 = $req1 . "\n" . "value=$body \n" if (defined $body);
##$req1 = $req1 . "\r\n\r\n" . "value=" . uri_escape($body) if (defined $body);
##$req1 = $req1 . "\r\n"    . "value=" . uri_escape($body) if (defined $body);
#$req1 = $req1 . "\r\n"    . "value=" . uri_escape($body) if (defined $body);
#$req1 = $req1 . "\n";
#print "$req1\n";
#$req1;

# after err
# my @url = ("http://192.168.122.214:7480/bucket777/obj1","-H","Host: 192.168.122.214:7480", "-H" , "Content-Length: 10", "-H", "Expect: ", "-H", "Content-Type: text/plain", "-X", "PUT");
my @url = ("http://192.168.122.214:7480/bucket777/obj6","-H","Host: 192.168.122.214:7480", "-H", "Connection: close", "-H", "Expect: ", "-H" , "Content-Length: 1000", "-X", "PUT");
#my @url = ("http://192.168.122.214:7480/bucket777/obj5","-H","Host: 192.168.122.214:7480", "-H", "Connection: close", "-H", "Expect: ", "-X", "PUT");
#my @url = ("http://192.168.122.214:7480/bucket777/obj1","-H","Host: 192.168.122.214:7480", "-H", "Content-Type: text/plain","-X", "PUT");
#my @url = ("http://192.168.122.214:7480/bucket777/obj1","-H","Host: 192.168.122.214:7480", "-X", "PUT");
#my @arry = ($method, $uriandargs, $signature, $httpDate) = &MY::access::gsign(@url);
my @arry = &MY::access::gsign(@url);
my $req1;
my $body = 'objcontent' x 100;
my @arry1 = ("$body",);
#two req,becasue ""
#my @arry1 = ($body,);
#$req1 = MY::access::req(\@arry,  \@arry1);
$req1 = MY::access::req(\@arry,  \$body);
#qq{$req1}
#$req1 = req($method, $uriandargs, $signature, $httpDate, $body);
#$req1 = req($method, $uriandargs, $signature, $httpDate);
#print STDERR "req is \n$req1";

#my $ref_type = \$req1;
#    print STDERR "rawreq type is $ref_type";


#--- response_body_lik: 400 Bad
#--- response_body_like chomp
#ListBucketResult
--- error_code: 200
--- timeout: 60
--- abort


=== TEST 2: raw_request post object on real bucket
--- main_config
error_log logs/error.log info;

--- http_config
    lua_package_path    "/home/deploy/dobjstor/proxy/?.lua;/home/deploy/dobjstor/proxy/thirdparty/?.lua;/home/deploy/dobjstor/proxy/ostore_api/librados/?.lua;/home/deploy/dobjstor/proxy/common/?.lua;;";
    lua_package_cpath    "/home/deploy/dobjstor/proxy/?.so;/home/deploy/dobjstor/proxy/thirdparty/?.so;;";
    lua_shared_dict     cephvars    1M;
    init_by_lua_file "/home/deploy/dobjstor/proxy/radosload.lua";

--- config
    location /t {
        client_max_body_size 10000000k;
        client_body_buffer_size 100000k;
        default_type text/plain;
        content_by_lua_file "/home/deploy/dobjstor/proxy/storageproxy.lua";
    }

#--- raw_request eval chomp
#--- raw_request eval
--- raw_request eval
use POSIX;
use lib '/home/deploy/dobjstor/proxy/test/t/';
use MY::access qw(req);
use MY::access qw(gsign);
use URI::Escape;
#curl para
##my @url = ("http://192.168.122.214:7480/bucket777/obj2","-H","Host: 192.168.122.214:7481", "-H", "Expect: ", "-H", "Content-Length: ${length}", "-H", "Content-Type: text/plain","-X", "PUT");
#my @url = ("http://192.168.122.214:7480/bucket777/obj1","-H","Host: 192.168.122.214:7480", "-H", "Expect:", "-H", "Content-Type: text/plain","-X", "PUT");
#my ($method, $uriandargs, $signature, $httpDate) = &MY::access::gsign(@url);
#my $req1;
#$req1 = &MY::access::req($method, $uriandargs, $signature, $httpDate);
#my $body = 'objcontent';
##$req1 = $req1 ."\r\n";
##$req1 = $req1 . "\n\n" . "value=$body" if (defined $body);
##$req1 = $req1 . "\n" . "value=$body \n" if (defined $body);
##$req1 = $req1 . "\r\n\r\n" . "value=" . uri_escape($body) if (defined $body);
##$req1 = $req1 . "\r\n"    . "value=" . uri_escape($body) if (defined $body);
#$req1 = $req1 . "\r\n"    . "value=" . uri_escape($body) if (defined $body);
#$req1 = $req1 . "\n";
#print "$req1\n";
#$req1;

#my @url = ("http://192.168.122.214:7480/bucket777/obj1","-H","Host: 192.168.122.214:7480", "-H" , "Content-Length: 10", "-H", "Expect: ", "-H", "Content-Type: text/plain", "-X", "PUT");
#my @url = ("http://192.168.122.214:7480/bucket777/obj1","-H","Host: 192.168.122.214:7480", "-H", "Content-Type: text/plain","-X", "PUT");
#my @url = ("http://192.168.122.214:7480/bucket777/obj1","-H","Host: 192.168.122.214:7480", "-X", "PUT");
my @url = ("http://192.168.122.214:7480/bucket777/obj1","-H","Host: 192.168.122.214:7480", "-X", "POST");
#my @arry = ($method, $uriandargs, $signature, $httpDate) = &MY::access::gsign(@url);
my @arry = &MY::access::gsign(@url);
my $req1;
my $body = 'objcontent';
my @arry1 = ("$body",);
#two req,becasue ""
#my @arry1 = ($body,);
req1 = MY::access::req(\@arry,  \@arry1);
#$req1 = MY::access::req(\@arry,  \$body);
#qq{$req1}
#$req1 = req($method, $uriandargs, $signature, $httpDate, $body);
#$req1 = req($method, $uriandargs, $signature, $httpDate);
#print STDERR "req is \n$req1";

#my $ref_type = \$req1;
#    print STDERR "rawreq type is $ref_type";


#--- response_body_lik: 400 Bad
#--- response_body_like chomp
#ListBucketResult
--- error_code: 200
--- timeout: 5000ms
--- abort

=== TEST 3: raw req constructt
--- main_config
error_log logs/error.log info;

--- http_config
    lua_package_path    "/home/deploy/dobjstor/proxy/?.lua;/home/deploy/dobjstor/proxy/thirdparty/?.lua;/home/deploy/dobjstor/proxy/ostore_api/librados/?.lua;/home/deploy/dobjstor/proxy/common/?.lua;;";
    lua_package_cpath    "/home/deploy/dobjstor/proxy/?.so;/home/deploy/dobjstor/proxy/thirdparty/?.so;;";
    lua_shared_dict     cephvars    1M;
    init_by_lua_file "/home/deploy/dobjstor/proxy/radosload.lua";

--- config
    location /t {
        client_max_body_size 10000000k;
        client_body_buffer_size 100000k;
        default_type text/plain;
        content_by_lua_file "/home/deploy/dobjstor/proxy/storageproxy.lua";
    }


--- raw_request eval
qq{PUT /t/bucket777/obj1 HTTP/1.1\r
Authorization: AWS O911PT5Z34WN8Q92C8YU:mXOHOLzqQk0iOWNj60FqLyKLbE4=\r
Date: Wed, 07 Sep 2016 09:59:51 +0000\r
Host:  192.168.122.214:7480\r
content-type: text/plain\r
\r
\r
nobjcontent
}

#/r or oneline /r/n all tw req
--- error_code: 200
--- timeout: 5000ms
--- abort

=== TEST 3: put request eval 
--- main_config
error_log logs/error.log info;

--- http_config
    lua_package_path    "/home/deploy/dobjstor/proxy/?.lua;/home/deploy/dobjstor/proxy/thirdparty/?.lua;/home/deploy/dobjstor/proxy/ostore_api/librados/?.lua;/home/deploy/dobjstor/proxy/common/?.lua;;";
    lua_package_cpath    "/home/deploy/dobjstor/proxy/?.so;/home/deploy/dobjstor/proxy/thirdparty/?.so;;";
    lua_shared_dict     cephvars    1M;
    init_by_lua_file "/home/deploy/dobjstor/proxy/radosload.lua";

--- config
    location /t {
        client_max_body_size 10000000k;
        client_body_buffer_size 100000k;
        default_type text/plain;
        content_by_lua_file "/home/deploy/dobjstor/proxy/storageproxy.lua";
    }

--- request eval
"PUT /t/bucket777/obj1\r
Authorization: AWS O911PT5Z34WN8Q92C8YU:mXOHOLzqQk0iOWNj60FqLyKLbE4=\r
Date: Wed, 07 Sep 2016 09:59:51 +0000\r
Host:  192.168.122.214:7480\r
content-type: text/plain\r
\r
objcontent\r"


#/r or oneline /r/n all tw req
--- error_code: 200
--- timeout: 5000ms
--- abort

=== TEST 3: put request not using eval 
--- main_config
error_log logs/error.log info;

--- http_config
    lua_package_path    "/home/deploy/dobjstor/proxy/?.lua;/home/deploy/dobjstor/proxy/thirdparty/?.lua;/home/deploy/dobjstor/proxy/ostore_api/librados/?.lua;/home/deploy/dobjstor/proxy/common/?.lua;;";
    lua_package_cpath    "/home/deploy/dobjstor/proxy/?.so;/home/deploy/dobjstor/proxy/thirdparty/?.so;;";
    lua_shared_dict     cephvars    1M;
    init_by_lua_file "/home/deploy/dobjstor/proxy/radosload.lua";

--- config
    location /t {
        client_max_body_size 10000000k;
        client_body_buffer_size 100000k;
        default_type text/plain;
        content_by_lua_file "/home/deploy/dobjstor/proxy/storageproxy.lua";
    }

--- request
PUT /t/bucket777/obj1
Date: Wed, 07 Sep 2016 09:59:51 +0000
Host:  192.168.122.214:7480
Content-type: text/plain
content-length: 10
Authorization: AWS O911PT5Z34WN8Q92C8YU:mXOHOLzqQk0iOWNj60FqLyKLbE4=

objcontent
--- error_code: 200
--- timeout: 5000ms
--- abort

=== TEST 4: put,no length

--- main_config
error_log logs/error.log info;

--- http_config
    lua_package_path    "/home/deploy/dobjstor/proxy/?.lua;/home/deploy/dobjstor/proxy/thirdparty/?.lua;/home/deploy/dobjstor/proxy/ostore_api/librados/?.lua;/home/deploy/dobjstor/proxy/common/?.lua;;";
    lua_package_cpath    "/home/deploy/dobjstor/proxy/?.so;/home/deploy/dobjstor/proxy/thirdparty/?.so;;";
    lua_shared_dict     cephvars    1M;
    init_by_lua_file "/home/deploy/dobjstor/proxy/radosload.lua";

--- config
    location /t {
        client_max_body_size 10000000k;
        client_body_buffer_size 100000k;
        default_type text/plain;
        content_by_lua_file "/home/deploy/dobjstor/proxy/storageproxy.lua";
    }

--- raw_request eval
use POSIX;
use lib '/home/deploy/dobjstor/proxy/test/t/';
use MY::access qw(req);
use MY::access qw(gsign);
use URI::Escape;
#my @url = ("http://192.168.122.214:7480/bucket777/obj6","-H","Host: 192.168.122.214:7480", "-H", "Connection: close", "-H", "Expect: ", "-H" , "Content-Length: 1000", "-X", "PUT");
my @url = ("http://192.168.122.214:7480/bucket777/obj5","-H","Host: 192.168.122.214:7480", "-H", "Connection: close", "-H", "Expect: ", "-X", "PUT");
#my @url = ("http://192.168.122.214:7480/bucket777/obj1","-H","Host: 192.168.122.214:7480", "-H", "Content-Type: text/plain","-X", "PUT");
#my @url = ("http://192.168.122.214:7480/bucket777/obj1","-H","Host: 192.168.122.214:7480", "-X", "PUT");
my @arry = &MY::access::gsign(@url);
my $req1;
my $body = 'objcontent' x 100;
my @arry1 = ("$body",);
#two req,becasue ""
#my @arry1 = ($body,);
#$req1 = MY::access::req(\@arry,  \@arry1);
$req1 = MY::access::req(\@arry,  \$body);


--- error_code: 200
--- timeout: 60
--- abort
=== TEST 5: put req array

--- main_config
error_log logs/error.log info;

--- http_config
    lua_package_path    "/home/deploy/dobjstor/proxy/?.lua;/home/deploy/dobjstor/proxy/thirdparty/?.lua;/home/deploy/dobjstor/proxy/ostore_api/librados/?.lua;/home/deploy/dobjstor/proxy/common/?.lua;;";
    lua_package_cpath    "/home/deploy/dobjstor/proxy/?.so;/home/deploy/dobjstor/proxy/thirdparty/?.so;;";
    lua_shared_dict     cephvars    1M;
    init_by_lua_file "/home/deploy/dobjstor/proxy/radosload.lua";

--- config
    location /t {
        client_max_body_size 10000000k;
        client_body_buffer_size 100000k;
        default_type text/plain;
        content_by_lua_file "/home/deploy/dobjstor/proxy/storageproxy.lua";
    }

--- raw_request eval
use POSIX;
use lib '/home/deploy/dobjstor/proxy/test/t/';
use MY::access qw(req);
use MY::access qw(gsign);
use URI::Escape;
my @url1 = ("http://192.168.122.214:7480/bucket777/obj6","-H","Host: 192.168.122.214:7480", "-H", "Connection: close", "-H", "Expect: ", "-H" , "Content-Length: 1000", "-X", "PUT");
my @url2 = ("http://192.168.122.214:7480/bucket777/obj5","-H","Host: 192.168.122.214:7480", "-H", "Connection: close", "-H", "Expect: ", "-X", "PUT");
#my @url = ("http://192.168.122.214:7480/bucket777/obj1","-H","Host: 192.168.122.214:7480", "-H", "Content-Type: text/plain","-X", "PUT");
#my @url = ("http://192.168.122.214:7480/bucket777/obj1","-H","Host: 192.168.122.214:7480", "-X", "PUT");
my @arry1 = &MY::access::gsign(@url1);
my @arry2 = &MY::access::gsign(@url2);
my $req1;
my $req2;
my $body = 'objcontent' x 100;
#$req1 = MY::access::req(\@arry,  \@arry1);
$req1 = MY::access::req(\@arry1,  \$body);
$req2 = MY::access::req(\@arry2,  \$body);
#my @req = ("$req1" , "$req2");
[$req1,$req2]

--- error_code: [200, 200]
--- timeout: 60
--- abort
--- ONLY
