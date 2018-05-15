use Test::Nginx::Socket 'no_plan';
#no_long_string();

#use Test::Nginx::Socket::Lua 'no_plan'; 
run_tests();

__DATA__


=== TEST 1: listonebucket
--- main_config
#env MY_ENVIRONMENT;
#user deploy;
#worker_processes 2;
#daemon off;
#master_process on
#error_log /tmp/nginx_error.log info;
#error_log logs/error.log debug;
error_log logs/error.log info;

--- http_config
    lua_package_path    "/home/deploy/dobjstor/proxy/?.lua;/home/deploy/dobjstor/proxy/thirdparty/?.lua;/home/deploy/dobjstor/proxy/ostore_api/librados/?.lua;/home/deploy/dobjstor/proxy/common/?.lua;;";
    lua_package_cpath    "/home/deploy/dobjstor/proxy/?.so;/home/deploy/dobjstor/proxy/thirdparty/?.so;;";
    lua_shared_dict     cephvars    1M;
    init_by_lua_file "/home/deploy/dobjstor/proxy/radosload.lua";

--- config
#    listen 8081;
    location /t {
        client_max_body_size 10000000k;
        client_body_buffer_size 100000k;
	# lua_code_cache off;
       # lua_need_request_body on;
        default_type text/plain;
        content_by_lua_file "/home/deploy/dobjstor/proxy/storageproxy.lua";
    }

#--- raw_request eval nobody!
#use POSIX;
#use lib '/home/deploy/dobjstor/proxy/test/t/';
#use MY::access qw(req);
#use MY::access qw(sign);
##curl para
#my @url = ("http://192.168.122.214:7480/bucket777?format=xml","-H","Host: 192.168.122.214:7481", "-X", "GET");
#my ($method, $uriandargs, $signature, $httpDate) = &MY::access::gsign(@url);
#my $req1;
#$req1 = &MY::access::req($method, $uriandargs, $signature, $httpDate);


# have body para,but empty
--- raw_request eval
use POSIX;
use lib '/home/deploy/dobjstor/proxy/test/t/';
use MY::access qw(req);
use MY::access qw(sign);
#curl para
my @url = ("http://192.168.122.214:7480/bucket777?format=xml","-H","Host: 192.168.122.214:7481", "-X", "GET");
#my @url = ("http://192.168.122.214:7480/bucket777?format=xml","-H","Host: 192.168.122.214:7481", "-H", "Content-Type: text/plain", "-X", "GET");
    #     #my @url = ("http://192.168.122.214:7480/bucket777/obj1","-H","Host: 192.168.122.214:7480", "-H", "Expect: ", "-H", "Content-Type: text/plain","-X", "PUT");
    #     ##my @arry = ($method, $uriandargs, $signature, $httpDate) = &MY::access::gsign(@url);
my @arry = &MY::access::gsign(@url);
my $req1;
#  my $body = 'objcontent';
    #my @arry1 = ($body,);
    my @arry1 = ();
    $req1 = MY::access::req(\@arry,  \@arry1);
#     ##$req1 = req($method, $uriandargs, $signature, $httpDate, $body);
#     ##$req1 = req($method, $uriandargs, $signature, $httpDate);
#     #print STDERR "req is \n$req1";



#--- response_body_lik: 400 Bad
--- response_body_like chomp
ListBucketResult
--- error_code: 200
--- timeout: 500ms
--- abort
=== TEST 2: listonebucket incluee contenttype
--- main_config
#env MY_ENVIRONMENT;
#user deploy;
#worker_processes 2;
#daemon off;
#master_process on
#error_log /tmp/nginx_error.log info;
#error_log logs/error.log debug;
error_log logs/error.log info;

--- http_config
    lua_package_path    "/home/deploy/dobjstor/proxy/?.lua;/home/deploy/dobjstor/proxy/thirdparty/?.lua;/home/deploy/dobjstor/proxy/ostore_api/librados/?.lua;/home/deploy/dobjstor/proxy/common/?.lua;;";
    lua_package_cpath    "/home/deploy/dobjstor/proxy/?.so;/home/deploy/dobjstor/proxy/thirdparty/?.so;;";
    lua_shared_dict     cephvars    1M;
    init_by_lua_file "/home/deploy/dobjstor/proxy/radosload.lua";

--- config
#    listen 8081;
    location /t {
        client_max_body_size 10000000k;
        client_body_buffer_size 100000k;
	# lua_code_cache off;
       # lua_need_request_body on;
        default_type text/plain;
        content_by_lua_file "/home/deploy/dobjstor/proxy/storageproxy.lua";
    }

#--- raw_request eval nobody!
#use POSIX;
#use lib '/home/deploy/dobjstor/proxy/test/t/';
#use MY::access qw(req);
#use MY::access qw(sign);
##curl para
#my @url = ("http://192.168.122.214:7480/bucket777?format=xml","-H","Host: 192.168.122.214:7481", "-X", "GET");
#my ($method, $uriandargs, $signature, $httpDate) = &MY::access::gsign(@url);
#my $req1;
#$req1 = &MY::access::req($method, $uriandargs, $signature, $httpDate);


# have body para,but empty
--- raw_request eval
use POSIX;
use lib '/home/deploy/dobjstor/proxy/test/t/';
use MY::access qw(req);
use MY::access qw(sign);
#curl para
#my @url = ("http://192.168.122.214:7480/bucket777?format=xml","-H","Host: 192.168.122.214:7481", "-X", "GET");
my @url = ("http://192.168.122.214:7480/bucket777?format=xml","-H","Host: 192.168.122.214:7481", "-H", "Content-Type: text/plain", "-X", "GET");
    #     #my @url = ("http://192.168.122.214:7480/bucket777/obj1","-H","Host: 192.168.122.214:7480", "-H", "Expect: ", "-H", "Content-Type: text/plain","-X", "PUT");
    #     ##my @arry = ($method, $uriandargs, $signature, $httpDate) = &MY::access::gsign(@url);
my @arry = &MY::access::gsign(@url);
my $req1;
#  my $body = 'objcontent';
    #my @arry1 = ($body,);
    my @arry1 = ();
    $req1 = MY::access::req(\@arry,  \@arry1);
#     ##$req1 = req($method, $uriandargs, $signature, $httpDate, $body);
#     ##$req1 = req($method, $uriandargs, $signature, $httpDate);
#     #print STDERR "req is \n$req1";



#--- response_body_lik: 400 Bad
--- response_body_like chomp
ListBucketResult
--- error_code: 200
--- timeout: 500ms
--- abort
=== TEST 3: listonebucket incluee contenttype using $ not array
--- main_config
#env MY_ENVIRONMENT;
#user deploy;
#worker_processes 2;
#daemon off;
#master_process on
#error_log /tmp/nginx_error.log info;
#error_log logs/error.log debug;
error_log logs/error.log info;

--- http_config
    lua_package_path    "/home/deploy/dobjstor/proxy/?.lua;/home/deploy/dobjstor/proxy/thirdparty/?.lua;/home/deploy/dobjstor/proxy/ostore_api/librados/?.lua;/home/deploy/dobjstor/proxy/common/?.lua;;";
    lua_package_cpath    "/home/deploy/dobjstor/proxy/?.so;/home/deploy/dobjstor/proxy/thirdparty/?.so;;";
    lua_shared_dict     cephvars    1M;
    init_by_lua_file "/home/deploy/dobjstor/proxy/radosload.lua";

--- config
#    listen 8081;
    location /t {
        client_max_body_size 10000000k;
        client_body_buffer_size 100000k;
	# lua_code_cache off;
       # lua_need_request_body on;
        default_type text/plain;
        content_by_lua_file "/home/deploy/dobjstor/proxy/storageproxy.lua";
    }



# have body para,but empty
--- raw_request eval
use POSIX;
use lib '/home/deploy/dobjstor/proxy/test/t/';
use MY::access qw(req);
use MY::access qw(sign);
#curl para
#my @url = ("http://192.168.122.214:7480/bucket777?format=xml","-H","Host: 192.168.122.214:7481", "-X", "GET");
my @url = ("http://192.168.122.214:7480/bucket777?format=xml","-H","Host: 192.168.122.214:7481", "-H", "Content-Type: text/plain", "-X", "GET");
    #     #my @url = ("http://192.168.122.214:7480/bucket777/obj1","-H","Host: 192.168.122.214:7480", "-H", "Expect: ", "-H", "Content-Type: text/plain","-X", "PUT");
    #     ##my @arry = ($method, $uriandargs, $signature, $httpDate) = &MY::access::gsign(@url);
my @arry = &MY::access::gsign(@url);
my $req1;
  my $body = '';
    #my @arry1 = ($body,);
    my @arry1 = ();
    #$req1 = MY::access::req(\@arry,  \@arry1);
    $req1 = MY::access::req(\@arry,  \$body);
#     ##$req1 = req($method, $uriandargs, $signature, $httpDate, $body);
#     ##$req1 = req($method, $uriandargs, $signature, $httpDate);
#     #print STDERR "req is \n$req1";



#--- response_body_lik: 400 Bad
--- response_body_like chomp
ListBucketResult
--- error_code: 200
--- timeout: 500ms
--- abort
--- ONLY
