## vim:set ft= ts=4 sw=4 et fdm=marker:
##use lib 'lib';
#use Test::Nginx::Socket::Lua 'no_plan';
##use Test::Nginx::Socket::Lua;
#use Cwd qw(cwd);
#
#log_level 'debug';

##master_process_enabled(1);
##log_level('warn');
#
##repeat_each(1);
#
##plan tests => repeat_each() * (blocks() * 4) - 1;
#
#my $pwd = cwd();
#
## try to read the nameservers used by the system resolver:
#my @nameservers;
#if (open my $in, "/etc/resolv.conf") {
#    while (<$in>) {
#        if (/^\s*nameserver\s+(\d+(?:\.\d+){3})(?:\s+|$)/) {
#            push @nameservers, $1;
#            if (@nameservers > 10) {
#                last;
#            }
#        }
#    }
#    close $in;
#}
#
#if (!@nameservers) {
#    # default to Google's open DNS servers
#    push @nameservers, "8.8.8.8", "8.8.4.4";
#}
#
#
#warn "Using nameservers: \n@nameservers\n";
#
##our $HttpConfig = <<_EOC_;
##    lua_package_path 'src/lua/?.lua;;';
##    lua_package_cpath 'src/lua/?.so;;';
##    init_by_lua '
##        local v = require "jit.v"
##        v.on("$Test::Nginx::Util::ErrLogFile")
##        require "resty.core"
##    ';
##    resolver @nameservers;
##    client_body_temp_path /tmp/;
##    proxy_temp_path /tmp/;
##    fastcgi_temp_path /tmp/;
##_EOC_
#
#our $HttpConfig = qq{
#    lua_package_path "$pwd/../cache_api/?.lua;$pwd/../thirdparty/?.lua;$pwd/../?.lua;$pwd/t/clientlib/?.lua;;";
#    lua_package_cpath "/usr/local/openresty-debug/lualib/?.so;;";
#};
#no_diff();
#no_long_string();
#run_tests();

use t::MYTESTv4 'no_plan';
no_long_string();
log_level 'debug';
worker_connections(102400);
#workers(24);
#master_on();
# use ENV pass
$ENV{TEST_NGINX_BUCKET} = ''; 
use Getopt::Long qw(GetOptions);

our  $bucket;
our  $object;
GetOptions(
    'bucket=s' => \$bucket,
    'object=s' => \$object,
);
no_root_location();
no_shuffle();
run_tests();
__DATA__

=== TEST 1: test v4 to /
--- config


#client_max_body_size 40g;
    client_body_buffer_size 512m;

    lua_socket_read_timeout 240s;
    lua_socket_send_timeout 240s;
    lua_socket_connect_timeout 240s;

#        listen 8081 so_keepalive=2s:2s:8;
#        listen       80 reuseport  backlog=65533;
        keepalive_timeout 120;
        location /v4 {

            content_by_lua_block {
                local AWSV4S = require "api-gateway.aws.AwsV4Signature"
                local AWSV4Service = require "api-gateway.aws.AwsService"
                local s3config = {
                  aws_region = "dnion-beijing",     
                  aws_service = "s3",   
                  -- o.aws_credentials                 An object defining the credentials provider.
                  --          i.e. for IAM Credentials
                  --            aws_credentials = {
                  --                provider = "api-gateway.aws.AWSIAMCredentials",
                  --                shared_cache_dict = "my_dict",
                  --                security_credentials_host = "169.254.169.254",
                  --                security_credentials_port = "80"
                  --            }
                  --         i.e. for STS Credentials
                  --            aws_credentials = {
                  --                provider = "api-gateway.aws.AWSSTSCredentials",
                  --                role_ARN = "roleA",
                  --                role_session_name = "sessionB",
                  --                shared_cache_dict = "my_dict",
                  --                iam_security_credentials_host = "169.254.169.254",
                  --                iam_security_credentials_port = "80"
                  --         }
                  --        i.e. for Basic Credentials with access_key and secret:
                  aws_credentials = {
                  provider = "api-gateway.aws.AWSBasicCredentials",
                  -- access_key = ngx.var.aws_access_key,
                  -- access_key = "JQWE7H60CYOI3QZNLS3K",
                  access_key = "JQWE7H60CYOI3QZNLS3K",
                  -- secret_key = ngx.var.aws_secret_key
                  --secret_key = "l0fjx5mHz3KoOOiiYILc6JtpXqVJNOCpYJXUlOIZ"
                  secret_key = "SVRnr3L7pPShRyYbodtmHKNjBg9wpXbFScSkwlEL"
                  }
              -- o.aws_secret_key  - deprecated. Use AWSBasicCredentials instead.
              -- o.aws_access_key  - deprecated. Use AWSBasicCredentials instead.
              -- o.security_credentials_host - optional. the AWS URL to read security credentials from and figure out the iam_user
              -- o.security_credentials_port - optional. the port used when connecting to security_credentials_host
              -- o.shared_cache_dict - optional. AWSIAMCredentials uses it to store IAM Credentials.
              -- o.doubleUrlEncode - optional. Whether to double url-encode the resource path
              --                                when constructing the canonical request for AWSV4 signature.
              --
               }
                local inspect = require "inspect"
--                local awsAuth =  AWSV4S:new()
                local s3service = AWSV4Service:new(s3config)
                
                s3service:constructor(s3service)
--                local host = s3service:getAWSHost()
--                local credential  = s3service:getCredentials()

                local path = "/"
                local extra_headers = {
--                    ["X-Amz-Client-Context"] = "clientContext",
--                    ["X-Amz-Invocation-Type"] = "invocationType",
--                    ["X-Amz-Log-Type"] = "logType"
--                      ["X-AMZ-Content-Sha256"] = "UNSIGNED-PAYLOAD"
                }

              -- actionName, arguments, path, http_method, useSSL, timeout, contentType
               local actionname = nil
               local uriargs = ngx.req.get_uri_args()                  
               -- local ok, code, headers, status, body = s3service:performAction(actionname, uriargs, path, "GET", false, 60000, "application/x-amz-json-1.1", extra_headers)
               local ok, code, headers, status, body = s3service:performAction(actionname, uriargs, path, "GET", false, 60000, "text/plain", extra_headers, "")
--               local authheader = s3service:getAuthorizationHeader(http_method, path, uri_args, body)

             --  ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", "uriargs is ", inspect(ngx.req.get_uri_args()))
             --  ngx.print(awsAuth:formatQueryString(ngx.req.get_uri_args()))
               ngx.say("http code: ",code)
               ngx.say(body)


         } 
     }    
    location / {
	 lua_check_client_abort on;
#        client_max_body_size 10000000k;
#        client_body_buffer_size 100000k;
               default_type text/plain;
        #       default_type application/octet-stream;
     	lua_code_cache on;
         set $rangestr '';
         rewrite_by_lua_file /root/atest/openresty/proxy/storageproxy_rewrite.lua;
      #content_by_lua_file "/home/deploy/dobjstor/proxy/storageproxy.lua";
        content_by_lua_file "/root/atest/openresty/proxy/storageproxy.lua";
        #  body_filter_by_lua_file "/home/deploy/dobjstor/proxy/bodyfilter.lua";
        # body_filter_by_lua_file "/root/atest/openresty/proxy/bodyfilter.lua";
     } 

--- more_headers
X-Test: test
--- request
GET /v4
--- response_body_like eval
qr/http code: 200\s<\?xml version="1.0" encoding="UTF-8"\?>\s<ListAllMyBucketsResult.*<\/Buckets><\/ListAllMyBucketsResult>/s
--- no_error_log
[error]
--- error_code: 200
=== TEST 2: test v4 to put bucket
--- config
        listen 8081 so_keepalive=2s:2s:8;
        keepalive_timeout 120;
        location /v4 {

            content_by_lua_block {
                local AWSV4S = require "api-gateway.aws.AwsV4Signature"
                local AWSV4Service = require "api-gateway.aws.AwsService"
                local s3config = {
                  aws_region = "dnion-beijing",     
                  aws_service = "s3",   
                  aws_credentials = {
                  provider = "api-gateway.aws.AWSBasicCredentials",
                  access_key = "JQWE7H60CYOI3QZNLS3K",
                  secret_key = "SVRnr3L7pPShRyYbodtmHKNjBg9wpXbFScSkwlEL"
                  }
               }
                local inspect = require "inspect"
                local s3service = AWSV4Service:new(s3config)
                
                s3service:constructor(s3service)

                local path = "/bucketshapanshangi1"
               -- local path = "/v4bucket" --dead
                local extra_headers = {
                }

              -- actionName, arguments, path, http_method, useSSL, timeout, contentType
               local actionname = nil
               local uriargs = ngx.req.get_uri_args()                  
               local requestbody = ""
               local ok, code, headers, status, body = s3service:performAction(actionname, uriargs, path, "PUT", false, 60000, "text/plain", extra_headers, requestbody)

               ngx.say("http code: ",code)
               ngx.say(body)


         } 
     }    
    location / {
	 lua_check_client_abort on;
        client_max_body_size 10000000k;
        client_body_buffer_size 100000k;
        default_type text/plain;
     	lua_code_cache on;
         set $rangestr '';
         rewrite_by_lua_file /root/atest/openresty/proxy/storageproxy_rewrite.lua;
        content_by_lua_file "/root/atest/openresty/proxy/storageproxy.lua";
     } 

--- request
GET /v4
--- no_error_log
[error]
--- error_code: 200
=== TEST 3: test v4 to  list bucket  /bauldnion2-w6ue3q92sc4ix4uqd6-72
--- config
        listen 8081 so_keepalive=2s:2s:8;
        keepalive_timeout 120;
        location /v4 {

            content_by_lua_block {
                local AWSV4S = require "api-gateway.aws.AwsV4Signature"
                local AWSV4Service = require "api-gateway.aws.AwsService"
                local s3config = {
                  aws_region = "dnion-beijing",     
                  aws_service = "s3",   
                  aws_credentials = {
                  provider = "api-gateway.aws.AWSBasicCredentials",
                  -- access_key = ngx.var.aws_access_key,
                  access_key = "JQWE7H60CYOI3QZNLS3K",
                  -- secret_key = ngx.var.aws_secret_key
                  secret_key = "SVRnr3L7pPShRyYbodtmHKNjBg9wpXbFScSkwlEL"
                  }
               }
                local inspect = require "inspect"
                local s3service = AWSV4Service:new(s3config)
                
                s3service:constructor(s3service)

                -- local path = "/$::bucket"
        --        local path = "/bucketshapanshangi1"
                -- local path = "/bucketshapanshangi1"
                local path = "/1158abc9"
                local extra_headers = {
 --                     ["X-AMZ-Content-Sha256"] = "UNSIGNED-PAYLOAD" -- in the lib change it

                }

              -- actionName, arguments, path, http_method, useSSL, timeout, contentType
               local actionname = nil
               local uriargs = ngx.req.get_uri_args()                  
             -- local uriargs = { "maxkeys" = 10 }
	      -- local uriargs = {}
	      -- uriargs["max-keys"] = 10
               -- local ok, code, headers, status, body = s3service:performAction(actionname, uriargs, path, "GET", false, 60000, "application/x-amz-json-1.1", extra_headers)
               local ok, code, headers, status, body = s3service:performAction(actionname, uriargs, path, "GET", false, 60000, "text/plain", extra_headers, "")
--               local authheader = s3service:getAuthorizationHeader(http_method, path, uri_args, body)

             --  ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", "uriargs is ", inspect(ngx.req.get_uri_args()))
             --  ngx.print(awsAuth:formatQueryString(ngx.req.get_uri_args()))
               ngx.say("http code: ",code)
               ngx.say(body)


         } 
     }    
    location / {
	 #set $portal 1;
	 lua_check_client_abort on;
        client_max_body_size 10000000k;
        client_body_buffer_size 100000k;
        default_type text/plain;
     	lua_code_cache on;
         set $rangestr '';
         rewrite_by_lua_file /root/atest/openresty/proxy/storageproxy_rewrite.lua;
        content_by_lua_file "/root/atest/openresty/proxy/storageproxy.lua";
#        body_filter_by_lua_file "/root/atest/openresty/proxy/bodyfilter.lua";
#qr/http code: 200\s<\?xml version="1.0" encoding="UTF-8"\?>.*<Name>bucketshapanshangi1<\/Name>.*<\/ListBucketResult>/s
     } 

--- more_headers
X-Test: test
--- request
GET /v4?max-keys=1000&Subject=nginx:test!@$
--- response_body_like eval
qr/http code: 200\s<\?xml version="1.0" encoding="UTF-8"\?>.*<Name>1158abc9<\/Name>.*<\/ListBucketResult>/s
--- no_error_log
[error]
--- error_code: 200
--- abort
--- timeout: 300s
=== TEST 4: test v4 to  list bucket , UNSIGNED-PAYLOAD /bauldnion2-w6ue3q92sc4ix4uqd6-72
--- config
        listen 8081 so_keepalive=2s:2s:8;
        keepalive_timeout 120;
        location /v4 {

            content_by_lua_block {
                local AWSV4S = require "api-gateway.aws.AwsV4Signature"
                local AWSV4Service = require "api-gateway.aws.AwsService"
                local s3config = {
                  aws_region = "dnion-beijing",     
                  aws_service = "s3",   
                  aws_credentials = {
                  provider = "api-gateway.aws.AWSBasicCredentials",
                  -- access_key = ngx.var.aws_access_key,
                  access_key = "JQWE7H60CYOI3QZNLS3K",
                  -- secret_key = ngx.var.aws_secret_key
                  secret_key = "SVRnr3L7pPShRyYbodtmHKNjBg9wpXbFScSkwlEL"
                  }
               }
                local inspect = require "inspect"
                local s3service = AWSV4Service:new(s3config)
                
                s3service:constructor(s3service)

                local path = "/bucketshapanshangi1"
                -- local path = "/$::bucket"
                local extra_headers = {
                      ["X-AMZ-Content-Sha256"] = "UNSIGNED-PAYLOAD" -- in the lib change it

                }

              -- actionName, arguments, path, http_method, useSSL, timeout, contentType
               local actionname = nil
               local uriargs = ngx.req.get_uri_args()                  
               -- local ok, code, headers, status, body = s3service:performAction(actionname, uriargs, path, "GET", false, 60000, "application/x-amz-json-1.1", extra_headers)
               local ok, code, headers, status, body = s3service:performAction(actionname, uriargs, path, "GET", false, 60000, "text/plain", extra_headers, "")
--               local authheader = s3service:getAuthorizationHeader(http_method, path, uri_args, body)

             --  ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", "uriargs is ", inspect(ngx.req.get_uri_args()))
             --  ngx.print(awsAuth:formatQueryString(ngx.req.get_uri_args()))
               ngx.say("http code: ",code)
               ngx.say(body)


         } 
     }    
    location / {
	 #set $portal 1;
	 lua_check_client_abort on;
        client_max_body_size 10000000k;
        client_body_buffer_size 100000k;
        default_type text/plain;
     	lua_code_cache on;
         set $rangestr '';
         rewrite_by_lua_file /root/atest/openresty/proxy/storageproxy_rewrite.lua;
        content_by_lua_file "/root/atest/openresty/proxy/storageproxy.lua";
#        body_filter_by_lua_file "/root/atest/openresty/proxy/bodyfilter.lua";
     } 

--- more_headers
X-Test: test
--- request
GET /v4
--- response_body_like eval
qr/http code: 200\s<\?xml version="1.0" encoding="UTF-8"\?>.*<Name>bucketshapanshangi1<\/Name>.*<\/ListBucketResult>/s
--- no_error_log
[error]
--- error_code: 200
=== TEST 5: test v4 to put object
--- config
        listen 8081 so_keepalive=2s:2s:8;
        keepalive_timeout 120;
        location /v4 {

            content_by_lua_block {
                local AWSV4S = require "api-gateway.aws.AwsV4Signature"
                local AWSV4Service = require "api-gateway.aws.AwsService"
                local s3config = {
                  aws_region = "dnion-beijing",     
                  aws_service = "s3",   
                  aws_credentials = {
                  provider = "api-gateway.aws.AWSBasicCredentials",
                  access_key = "JQWE7H60CYOI3QZNLS3K",
                  secret_key = "SVRnr3L7pPShRyYbodtmHKNjBg9wpXbFScSkwlEL"
                  }
               }
                local inspect = require "inspect"
                local s3service = AWSV4Service:new(s3config)
                
                s3service:constructor(s3service)

                local path = "/bucketshapanshangi1/zuux0"
                local extra_headers = {
                }

              -- actionName, arguments, path, http_method, useSSL, timeout, contentType
               local actionname = nil
               local uriargs = ngx.req.get_uri_args()                  
               local requestbody = "aaa"
               local ok, code, headers, status, body = s3service:performAction(actionname, uriargs, path, "PUT", false, 60000, "text/plain", extra_headers, requestbody)

               ngx.say("http code: ",code)
               ngx.say(body)


         } 
     }    
    location / {
	 lua_check_client_abort on;
        client_max_body_size 10000000k;
        client_body_buffer_size 100000k;
        default_type text/plain;
     	lua_code_cache on;
         set $rangestr '';
         rewrite_by_lua_file /root/atest/openresty/proxy/storageproxy_rewrite.lua;
        content_by_lua_file "/root/atest/openresty/proxy/storageproxy.lua";
        #    body_filter_by_lua_file "/root/atest/openresty/proxy/bodyfilter.lua";
     } 

--- request
GET /v4
--- no_error_log
[error]
--- error_code: 200
=== TEST 6: read object
--- config
        listen 8081 so_keepalive=2s:2s:8;
        keepalive_timeout 120;
        location /v4 {

            content_by_lua_block {
                local AWSV4S = require "api-gateway.aws.AwsV4Signature"
                local AWSV4Service = require "api-gateway.aws.AwsService"
                local s3config = {
                  aws_region = "dnion-beijing",     
                  aws_service = "s3",   
                  aws_credentials = {
                  provider = "api-gateway.aws.AWSBasicCredentials",
                  access_key = "JQWE7H60CYOI3QZNLS3K",
                  secret_key = "SVRnr3L7pPShRyYbodtmHKNjBg9wpXbFScSkwlEL"
                  }
               }
                local inspect = require "inspect"
                local s3service = AWSV4Service:new(s3config)
                
                s3service:constructor(s3service)

                local path = "/bucketshapanshangi1/zuux0"
               -- local path = "/$::bucket/$::object"
               -- local path = "/v4bucket" --dead
                local extra_headers = {
                }

              -- actionName, arguments, path, http_method, useSSL, timeout, contentType
               local actionname = nil
               local uriargs = ngx.req.get_uri_args()                  
               local requestbody = ""
               local ok, code, headers, status, body = s3service:performAction(actionname, uriargs, path, "GET", false, 60000, "text/plain", extra_headers, requestbody)

               ngx.say("http code: ",code)
               ngx.say("body: ",body)


         } 
     }    
    location / {
	 lua_check_client_abort on;
        client_max_body_size 10000000k;
        client_body_buffer_size 100000k;
        default_type text/plain;
     	lua_code_cache on;
         set $rangestr '';
         rewrite_by_lua_file /root/atest/openresty/proxy/storageproxy_rewrite.lua;
        content_by_lua_file "/root/atest/openresty/proxy/storageproxy.lua";
#        body_filter_by_lua_file "/root/atest/openresty/proxy/bodyfilter.lua";
     } 

--- request
GET /v4
--- response_body
http code: 200
body: aaa
--- no_error_log
[error]
--- error_code: 200
=== TEST 7: delet object 
--- config
        listen 8081 so_keepalive=2s:2s:8;
        keepalive_timeout 120;
        location /v4 {

            content_by_lua_block {
                local AWSV4S = require "api-gateway.aws.AwsV4Signature"
                local AWSV4Service = require "api-gateway.aws.AwsService"
                local s3config = {
                  aws_region = "dnion-beijing",     
                  aws_service = "s3",   
                  aws_credentials = {
                  provider = "api-gateway.aws.AWSBasicCredentials",
                  access_key = "JQWE7H60CYOI3QZNLS3K",
                  secret_key = "SVRnr3L7pPShRyYbodtmHKNjBg9wpXbFScSkwlEL"
                  }
               }
                local inspect = require "inspect"
                local s3service = AWSV4Service:new(s3config)
                
                s3service:constructor(s3service)

                 -- local path = "/$::bucket/$::object"
                -- local path = "/bucketshapanshang/objetct1"
               -- local path = "/v4bucket" --dead
                local path = "/bucketshapanshangi1/zuux0"
                local extra_headers = {
                }

              -- actionName, arguments, path, http_method, useSSL, timeout, contentType
               local actionname = nil
               local uriargs = ngx.req.get_uri_args()                  
               local requestbody = ""
               local ok, code, headers, status, body = s3service:performAction(actionname, uriargs, path, "DELETE", false, 60000, "text/plain", extra_headers, requestbody)

               ngx.say("http code: ",code)
--               ngx.say("body: ",body)


         } 
     }    
    location / {
	 lua_check_client_abort on;
        client_max_body_size 10000000k;
        client_body_buffer_size 100000k;
        default_type text/plain;
     	lua_code_cache on;
         set $rangestr '';
         rewrite_by_lua_file /root/atest/openresty/proxy/storageproxy_rewrite.lua;
        content_by_lua_file "/root/atest/openresty/proxy/storageproxy.lua";
#        body_filter_by_lua_file "/root/atest/openresty/proxy/bodyfilter.lua";
     } 

--- request
GET /v4
--- response_body
http code: 204
--- no_error_log
[error]
--- error_code: 200
=== TEST 8: read non-exist object
--- config
        listen 8081 so_keepalive=2s:2s:8;
        keepalive_timeout 120;
        location /v4 {

            content_by_lua_block {
                local AWSV4S = require "api-gateway.aws.AwsV4Signature"
                local AWSV4Service = require "api-gateway.aws.AwsService"
                local s3config = {
                  aws_region = "dnion-beijing",     
                  aws_service = "s3",   
                  aws_credentials = {
                  provider = "api-gateway.aws.AWSBasicCredentials",
                  access_key = "JQWE7H60CYOI3QZNLS3K",
                  secret_key = "SVRnr3L7pPShRyYbodtmHKNjBg9wpXbFScSkwlEL"
                  }
               }
                local inspect = require "inspect"
                local s3service = AWSV4Service:new(s3config)
                
                s3service:constructor(s3service)

                --local path = "/bucketshapanshang/objetct1"
                local path = "/bucketshapanshangi1/zuux0"
               -- local path = "/$::bucket/$::object"
               -- local path = "/v4bucket" --dead
                local extra_headers = {
                }

              -- actionName, arguments, path, http_method, useSSL, timeout, contentType
               local actionname = nil
               local uriargs = ngx.req.get_uri_args()                  
               local requestbody = ""
               local ok, code, headers, status, body = s3service:performAction(actionname, uriargs, path, "GET", false, 60000, "text/plain", extra_headers, requestbody)

               ngx.say("http code: ",code)


         } 
     }    
    location / {
	 lua_check_client_abort on;
        client_max_body_size 10000000k;
        client_body_buffer_size 100000k;
        default_type text/plain;
     	lua_code_cache on;
         set $rangestr '';
         rewrite_by_lua_file /root/atest/openresty/proxy/storageproxy_rewrite.lua;
        content_by_lua_file "/root/atest/openresty/proxy/storageproxy.lua";
#        body_filter_by_lua_file "/root/atest/openresty/proxy/bodyfilter.lua";
     } 

--- request
GET /v4
--- response_body
http code: 404
--- error_log
[error]
--- error_code: 200
=== TEST 8: delete this bucket
--- config
        listen 8081 so_keepalive=2s:2s:8;
        keepalive_timeout 120;
        location /v4 {

            content_by_lua_block {
                local AWSV4S = require "api-gateway.aws.AwsV4Signature"
                local AWSV4Service = require "api-gateway.aws.AwsService"
                local s3config = {
                  aws_region = "dnion-beijing",     
                  aws_service = "s3",   
                  aws_credentials = {
                  provider = "api-gateway.aws.AWSBasicCredentials",
                  access_key = "JQWE7H60CYOI3QZNLS3K",
                  secret_key = "SVRnr3L7pPShRyYbodtmHKNjBg9wpXbFScSkwlEL"
                  }
               }
                local inspect = require "inspect"
                local s3service = AWSV4Service:new(s3config)
                
                s3service:constructor(s3service)

              --  local path = "/bucketshapanshang"
                local path = "/bucketshapanshangi1"
               --  local path = "/$::bucket/$::object"
               -- local path = "/v4bucket" --dead
                local extra_headers = {
                }

              -- actionName, arguments, path, http_method, useSSL, timeout, contentType
               local actionname = nil
               local uriargs = ngx.req.get_uri_args()                  
               local requestbody = ""
               local ok, code, headers, status, body = s3service:performAction(actionname, uriargs, path, "DELETE", false, 60000, "text/plain", extra_headers, requestbody)

               ngx.say("http code: ",code)
         --      ngx.say("body: ",body)


         } 
     }    
    location / {
	 lua_check_client_abort on;
        client_max_body_size 10000000k;
        client_body_buffer_size 100000k;
        default_type text/plain;
     	lua_code_cache on;
         set $rangestr '';
         rewrite_by_lua_file /root/atest/openresty/proxy/storageproxy_rewrite.lua;
        content_by_lua_file "/root/atest/openresty/proxy/storageproxy.lua";
#        body_filter_by_lua_file "/root/atest/openresty/proxy/bodyfilter.lua";
     } 

--- request
GET /v4
#--- response_body_like eval
--- response_body
http code: 204
--- no_error_log
[error]
--- error_code: 200
=== TEST 9: list non-exist bucket  /bauldnion2-w6ue3q92sc4ix4uqd6-72
--- config
        listen 8081 so_keepalive=2s:2s:8;
        keepalive_timeout 120;
        location /v4 {

            content_by_lua_block {
                local AWSV4S = require "api-gateway.aws.AwsV4Signature"
                local AWSV4Service = require "api-gateway.aws.AwsService"
                local s3config = {
                  aws_region = "dnion-beijing",     
                  aws_service = "s3",   
                  aws_credentials = {
                  provider = "api-gateway.aws.AWSBasicCredentials",
                  -- access_key = ngx.var.aws_access_key,
                  access_key = "JQWE7H60CYOI3QZNLS3K",
                  -- secret_key = ngx.var.aws_secret_key
                  secret_key = "SVRnr3L7pPShRyYbodtmHKNjBg9wpXbFScSkwlEL"
                  }
               }
                local inspect = require "inspect"
                local s3service = AWSV4Service:new(s3config)
                
                s3service:constructor(s3service)

                -- local path = "/$::bucket"
                local path = "/bucketshapanshangi1"
                local extra_headers = {
 --                     ["X-AMZ-Content-Sha256"] = "UNSIGNED-PAYLOAD" -- in the lib change it

                }

              -- actionName, arguments, path, http_method, useSSL, timeout, contentType
               local actionname = nil
               local uriargs = ngx.req.get_uri_args()                  
               -- local ok, code, headers, status, body = s3service:performAction(actionname, uriargs, path, "GET", false, 60000, "application/x-amz-json-1.1", extra_headers)
               local ok, code, headers, status, body = s3service:performAction(actionname, uriargs, path, "GET", false, 60000, "text/plain", extra_headers, "")
--               local authheader = s3service:getAuthorizationHeader(http_method, path, uri_args, body)

             --  ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", "uriargs is ", inspect(ngx.req.get_uri_args()))
             --  ngx.print(awsAuth:formatQueryString(ngx.req.get_uri_args()))
               ngx.say("http code: ",code)
    --           ngx.say(body)


         } 
     }    
    location / {
	 #set $portal 1;
	 lua_check_client_abort on;
        client_max_body_size 10000000k;
        client_body_buffer_size 100000k;
        default_type text/plain;
     	lua_code_cache on;
         set $rangestr '';
         rewrite_by_lua_file /root/atest/openresty/proxy/storageproxy_rewrite.lua;
        content_by_lua_file "/root/atest/openresty/proxy/storageproxy.lua";
#        body_filter_by_lua_file "/root/atest/openresty/proxy/bodyfilter.lua";
     } 

--- more_headers
X-Test: test
--- request
GET /v4
--- response_body
http code: 404
--- error_log
[error]
--- error_code: 200
=== TEST 11: test delimiter
--- config
        listen 8081 so_keepalive=2s:2s:8;
        keepalive_timeout 120;
        location /v4 {

            content_by_lua_block {
                local AWSV4S = require "api-gateway.aws.AwsV4Signature"
                local AWSV4Service = require "api-gateway.aws.AwsService"
                local s3config = {
                  aws_region = "dnion-beijing",     
                  aws_service = "s3",   
                  aws_credentials = {
                  provider = "api-gateway.aws.AWSBasicCredentials",
                  access_key = "JQWE7H60CYOI3QZNLS3K",
                  secret_key = "SVRnr3L7pPShRyYbodtmHKNjBg9wpXbFScSkwlEL"
                  }
               }
                local inspect = require "inspect"
                local s3service = AWSV4Service:new(s3config)
                
                s3service:constructor(s3service)

                local path1 = "/bucketshapanshangi1/sasdf"
                local path2 = "/bucketshapanshangi1/sfoo/bar"
                local path3 = "/bucketshapanshangi1/sfoo/baz/xyzzy"
                local path4 = "/bucketshapanshangi1/zuux/thud"
                local path5 = "/bucketshapanshangi1"
                local extra_headers = {
                }

              -- actionName, arguments, path, http_method, useSSL, timeout, contentType
               local actionname = nil
               local uriargs = ngx.req.get_uri_args()                  
               local requestbody = "aaa"
               local ok, code, headers, status, body = s3service:performAction(actionname, uriargs, path1, "PUT", false, 60000, "text/plain", extra_headers, requestbody)
               ngx.say("http code: ",code)
               local ok, code, headers, status, body = s3service:performAction(actionname, uriargs, path2, "PUT", false, 60000, "text/plain", extra_headers, requestbody)
               ngx.say("http code: ",code)
               local ok, code, headers, status, body = s3service:performAction(actionname, uriargs, path3, "PUT", false, 60000, "text/plain", extra_headers, requestbody)
               ngx.say("http code: ",code)
               local ok, code, headers, status, body = s3service:performAction(actionname, uriargs, path4, "PUT", false, 60000, "text/plain", extra_headers, requestbody)
               ngx.say("http code: ",code)

              -- ngx.say("http code: ",code)
              --  ngx.say(body)

              local ok, code, headers, status, body = s3service:performAction(actionname, uriargs, path5, "GET", false, 60000, "text/plain", extra_headers, "")

               ngx.say("http code: ",code)
               ngx.say(body)


         } 
     }    
    location / {
	 lua_check_client_abort on;
        client_max_body_size 10000000k;
        client_body_buffer_size 100000k;
        default_type text/plain;
     	lua_code_cache on;
         set $rangestr '';
         rewrite_by_lua_file /root/atest/openresty/proxy/storageproxy_rewrite.lua;
        content_by_lua_file "/root/atest/openresty/proxy/storageproxy.lua";
        body_filter_by_lua_file "/root/atest/openresty/proxy/bodyfilter.lua";
     } 

--- request
GET /v4
--- no_error_log
[error]
--- error_code: 200
--- response_body
http code: 200
http code: 200
http code: 200
http code: 200
http code: 200

=== TEST 12: test v4 delimiter
--- config
        listen 8081 so_keepalive=2s:2s:8;
        keepalive_timeout 120;
        location /v4 {

            content_by_lua_block {
                local AWSV4S = require "api-gateway.aws.AwsV4Signature"
                local AWSV4Service = require "api-gateway.aws.AwsService"
                local s3config = {
                  aws_region = "dnion-beijing",     
                  aws_service = "s3",   
                  aws_credentials = {
                  provider = "api-gateway.aws.AWSBasicCredentials",
                  -- access_key = ngx.var.aws_access_key,
                  access_key = "JQWE7H60CYOI3QZNLS3K",
                  -- secret_key = ngx.var.aws_secret_key
                  secret_key = "SVRnr3L7pPShRyYbodtmHKNjBg9wpXbFScSkwlEL"
                  }
               }
                local inspect = require "inspect"
                local s3service = AWSV4Service:new(s3config)
                
                s3service:constructor(s3service)

                -- local path = "/$::bucket"
                local path = "/bucketshapanshangi1"
                local extra_headers = {
 --                     ["X-AMZ-Content-Sha256"] = "UNSIGNED-PAYLOAD" -- in the lib change it

                }

               local actionname = nil
               local uriargs = ngx.req.get_uri_args()                  
               local ok, code, headers, status, body = s3service:performAction(actionname, uriargs, path, "GET", false, 60000, "text/plain", extra_headers, "")

               ngx.say("http code: ",code)
               ngx.say(body)


         } 
     }    
    location / {
	 #set $portal 1;
	 lua_check_client_abort on;
        client_max_body_size 10000000k;
        client_body_buffer_size 100000k;
        default_type text/plain;
     	lua_code_cache on;
         set $rangestr '';
         rewrite_by_lua_file /root/atest/openresty/proxy/storageproxy_rewrite.lua;
        content_by_lua_file "/root/atest/openresty/proxy/storageproxy.lua";
        body_filter_by_lua_file "/root/atest/openresty/proxy/bodyfilter.lua";
     } 

--- more_headers
X-Test: test
--- request
GET /v4?delimiter=/
--- response_body_like eval
qr/http code: 200\s<\?xml version="1.0" encoding="UTF-8"\?>.*<Name>bucketshapanshangi<\/Name>.*<\/ListBucketResult>/s
--- no_error_log
[error]
--- error_code: 200

=== TEST 13: test v4 to /
--- config
    client_body_buffer_size 512m;
    lua_socket_read_timeout 240s;
    lua_socket_send_timeout 240s;
    lua_socket_connect_timeout 240s;
    keepalive_timeout 120;
    location /v4 {

            content_by_lua_block {
                local AWSV4S = require "api-gateway.aws.AwsV4Signature"
                local AWSV4Service = require "api-gateway.aws.AwsService"
                local s3config = {
                  aws_region = "dnion-beijing",     
                  aws_service = "s3",   
                  aws_credentials = {
                  provider = "api-gateway.aws.AWSBasicCredentials",
                  access_key = "OC09OUB15D4MX05MFDXS",
                  secret_key = "D65GFZGTBNUAe84PV4GMpW9GuKz3CXFKtzHsJjGz"
                  }
               }
                local inspect = require "inspect"
                local s3service = AWSV4Service:new(s3config)
                
                s3service:constructor(s3service)

                local path = "/"
                local extra_headers = {
                }

              -- actionName, arguments, path, http_method, useSSL, timeout, contentType
               local actionname = nil
               local uriargs = ngx.req.get_uri_args()                  
               local ok, code, headers, status, body = s3service:performAction(actionname, uriargs, path, "GET", false, 60000, "text/plain", extra_headers, "")

               ngx.say("http code: ",code)
               ngx.say(body)
         } 
     }    
--- more_headers
X-Test: test
--- request
GET /v4
--- response_body_like eval
qr/http code: 200\s<\?xml version="1.0" encoding="UTF-8"\?>\s<ListAllMyBucketsResult.*<\/Buckets><\/ListAllMyBucketsResult>/s
--- no_error_log
[error]
--- error_code: 200
=== TEST 14: put many bucket
--- config
        listen 8081 so_keepalive=2s:2s:8;
        keepalive_timeout 120;
        location /v4 {

            content_by_lua_block {
                local AWSV4S = require "api-gateway.aws.AwsV4Signature"
                local AWSV4Service = require "api-gateway.aws.AwsService"
                local s3config = {
                  aws_region = "dnion-beijing",     
                  aws_service = "s3",   
                  aws_credentials = {
                  provider = "api-gateway.aws.AWSBasicCredentials",
                  access_key = "JQWE7H60CYOI3QZNLS3K",
                  secret_key = "SVRnr3L7pPShRyYbodtmHKNjBg9wpXbFScSkwlEL"
                  }
               }
                local inspect = require "inspect"
                local s3service = AWSV4Service:new(s3config)
                
                s3service:constructor(s3service)
                   
                local path = "abc9"
                local actionname = nil
                local uriargs = ngx.req.get_uri_args()                  
                local requestbody = ""

                local extra_headers = {
                }
                local n=50000
                for i = 1, n do
                    -- actionName, arguments, path, http_method, useSSL, timeout, contentType
                    local ok, code, headers, status, body = s3service:performAction(actionname, uriargs, "/" .. i .. path, "PUT", false, 60000, "text/plain", extra_headers, requestbody)

                    ngx.say("http code: ",code)
                    ngx.say(body)
                    -- if code ~= "200" then

                --    ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ", i)
                end

                   -- ngx.say("http code: ",code)


         } 
     }    
    location / {
	 lua_check_client_abort on;
        client_max_body_size 10000000k;
        client_body_buffer_size 100000k;
        default_type text/plain;
     	lua_code_cache on;
         set $rangestr '';
         rewrite_by_lua_file /root/atest/openresty/proxy/storageproxy_rewrite.lua;
        content_by_lua_file "/root/atest/openresty/proxy/storageproxy.lua";
     } 

--- request
GET /v4
--- no_error_log
[error]
--- error_code: 200
--- abort
--- timeout: 60
=== TEST 14: put order many objects
--- config
        listen 8081 so_keepalive=2s:2s:8;
        keepalive_timeout 120;
        location /v4 {

            content_by_lua_block {
                local AWSV4S = require "api-gateway.aws.AwsV4Signature"
                local AWSV4Service = require "api-gateway.aws.AwsService"
                local s3config = {
                  aws_region = "dnion-beijing",     
                  aws_service = "s3",   
                  aws_credentials = {
                  provider = "api-gateway.aws.AWSBasicCredentials",
                  access_key = "JQWE7H60CYOI3QZNLS3K",
                  secret_key = "SVRnr3L7pPShRyYbodtmHKNjBg9wpXbFScSkwlEL"
                  }
               }
                local inspect = require "inspect"
                local s3service = AWSV4Service:new(s3config)
                
                s3service:constructor(s3service)
                   
                local actionname = nil
                local uriargs = ngx.req.get_uri_args()                  
                local path = "/1158abc9/zuuxac"

               local requestbody = "aaabbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbaaaaaaaaaaaaaaaaaaabbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbaabbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbaabbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbaabbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbaabbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbaabbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbaabbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbaabbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbaabbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbaabbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbaabbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbaabbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbaabbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbaabbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbaabbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbaabbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbaabbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
               


                local extra_headers = {
                }
                local n=20000
                for i = 1, n do
                    -- actionName, arguments, path, http_method, useSSL, timeout, contentType
                    local ok, code, headers, status, body = s3service:performAction(actionname, uriargs, path .. i, "PUT", false, 60000, "text/plain", extra_headers, requestbody)

                    ngx.say("http code: ",code)
                    ngx.say(body)

                end



         } 
     }    
    location / {
	 lua_check_client_abort on;
        client_max_body_size 10000000k;
        client_body_buffer_size 100000k;
        default_type text/plain;
     	lua_code_cache on;
         set $rangestr '';
         rewrite_by_lua_file /root/atest/openresty/proxy/storageproxy_rewrite.lua;
        content_by_lua_file "/root/atest/openresty/proxy/storageproxy.lua";
     } 

--- request
GET /v4
--- no_error_log
[error]
--- error_code: 200
--- abort
--- timeout: 60
=== TEST 15: test v4 to  list bucket
--- config
        listen 8081 so_keepalive=2s:2s:8;
        keepalive_timeout 120;
        location /v4 {

            content_by_lua_block {
                local AWSV4S = require "api-gateway.aws.AwsV4Signature"
                local AWSV4Service = require "api-gateway.aws.AwsService"
                local s3config = {
                  aws_region = "dnion-beijing",     
                  aws_service = "s3",   
                  aws_credentials = {
                  provider = "api-gateway.aws.AWSBasicCredentials",
                  -- access_key = ngx.var.aws_access_key,
                  access_key = "JQWE7H60CYOI3QZNLS3K",
                  -- secret_key = ngx.var.aws_secret_key
                  secret_key = "SVRnr3L7pPShRyYbodtmHKNjBg9wpXbFScSkwlEL"
                  }
               }
                local inspect = require "inspect"
                local s3service = AWSV4Service:new(s3config)
                
                s3service:constructor(s3service)

                -- local path = "/$::bucket"
                local path = "/1158abc9"
	             local args = {}
	             args["marker"] = "fbed31e5fda0e9855b4bc3f41221b8453794"
                local extra_headers = {
 --                     ["X-AMZ-Content-Sha256"] = "UNSIGNED-PAYLOAD" -- in the lib change it

                }

              -- actionName, arguments, path, http_method, useSSL, timeout, contentType
               local actionname = nil
               local uriargs = ngx.req.get_uri_args()                  
               local ok, code, headers, status, body = s3service:performAction(actionname, uriargs, path, "GET", false, 60000, "text/plain", extra_headers, "")
--               local ok, code, headers, status, body = s3service:performAction(actionname, args, path, "GET", false, 60000, "text/plain", extra_headers, "")

               ngx.say("http code: ",code)
               ngx.say(body)


         } 
     }    
    location / {
	 #set $portal 1;
	 lua_check_client_abort on;
        client_max_body_size 10000000k;
        client_body_buffer_size 100000k;
        default_type text/plain;
     	lua_code_cache on;
         set $rangestr '';
         rewrite_by_lua_file /root/atest/openresty/proxy/storageproxy_rewrite.lua;
        content_by_lua_file "/root/atest/openresty/proxy/storageproxy.lua";
#        body_filter_by_lua_file "/root/atest/openresty/proxy/bodyfilter.lua";
     } 

--- more_headers
X-Test: test
--- request
GET /v4
--- response_body_like eval
qr/http code: 200\s<\?xml version="1.0" encoding="UTF-8"\?>.*<Name>1158abc9<\/Name>.*<\/ListBucketResult>/s
--- no_error_log
[error]
--- error_code: 200
--- abort
--- timeout: 60
=== TEST 16: put random many objects
--- config
        listen 8081 so_keepalive=2s:2s:8;
        keepalive_timeout 120;
        location /v4 {

            content_by_lua_block {
                local AWSV4S = require "api-gateway.aws.AwsV4Signature"
                local AWSV4Service = require "api-gateway.aws.AwsService"
                local s3config = {
                  aws_region = "dnion-beijing",     
                  aws_service = "s3",   
                  aws_credentials = {
                  provider = "api-gateway.aws.AWSBasicCredentials",
                  access_key = "JQWE7H60CYOI3QZNLS3K",
                  secret_key = "SVRnr3L7pPShRyYbodtmHKNjBg9wpXbFScSkwlEL"
                  }
               }
                local inspect = require "inspect"
                local s3service = AWSV4Service:new(s3config)
                
                s3service:constructor(s3service)
                   
                local actionname = nil
                local uriargs = ngx.req.get_uri_args()                  
                local path = "/1158abc9/"

                local requestbody = "aaabbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbaaaaaaaaaaaaaaaaaaabbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbaabbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbaabbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbaabbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbaabbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbaabbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbaabbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbaabbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbaabbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbaabbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbaabbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbaabbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbaabbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbaabbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbaabbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbaabbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbaabbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
               

                local resty_random = require "resty.random"
                local str = require "resty.string"

                local extra_headers = {
                }
                local n=20000
                for i = 1, n do
                    local random = resty_random.bytes(16)
                     -- generate 16 bytes of pseudo-random data
                    -- ngx.say("pseudo-random: ", str.to_hex(random))
                    local randomstr = str.to_hex(random)
                    -- actionName, arguments, path, http_method, useSSL, timeout, contentType
                    local ok, code, headers, status, body = s3service:performAction(actionname, uriargs, path .. randomstr .. i, "PUT", false, 60000, "text/plain", extra_headers, requestbody)

                    ngx.say("http code: ",code)
                    ngx.say(body)

                end



         } 
     }    
    location / {
	 lua_check_client_abort on;
        client_max_body_size 10000000k;
        client_body_buffer_size 100000k;
        default_type text/plain;
     	lua_code_cache on;
         set $rangestr '';
         rewrite_by_lua_file /root/atest/openresty/proxy/storageproxy_rewrite.lua;
        content_by_lua_file "/root/atest/openresty/proxy/storageproxy.lua";
     } 

--- request
GET /v4
--- no_error_log
[error]
--- error_code: 200
--- abort
--- timeout: 60
=== TEST 17: read many random object
--- config
        listen 8081 so_keepalive=2s:2s:8;
        keepalive_timeout 120;
        location /v4 {

            content_by_lua_block {
                local AWSV4S = require "api-gateway.aws.AwsV4Signature"
                local AWSV4Service = require "api-gateway.aws.AwsService"
                local s3config = {
                  aws_region = "dnion-beijing",     
                  aws_service = "s3",   
                  aws_credentials = {
                  provider = "api-gateway.aws.AWSBasicCredentials",
                  access_key = "JQWE7H60CYOI3QZNLS3K",
                  secret_key = "SVRnr3L7pPShRyYbodtmHKNjBg9wpXbFScSkwlEL"
                  }
               }
                local inspect = require "inspect"
                local s3service = AWSV4Service:new(s3config)
                
                s3service:constructor(s3service)

                local actionname = nil
                local uriargs = ngx.req.get_uri_args()                  
                local path = "/1158abc9"
                local extra_headers = {
                }

              -- actionName, arguments, path, http_method, useSSL, timeout, contentType
               local actionname = nil
               local uriargs = ngx.req.get_uri_args()                  
               local requestbody = ""

               local xml = require("xmlSimple").newParser()

             local IsTruncated = "true"
             local marker, maxkeys, nextmarker
             while IsTruncated == "true"  do
              local ok,code,headers,status,body
              if nextmarker == nil then -- parse the first
               ok, code, headers, status, body = s3service:performAction(actionname, uriargs, path, "GET", false, 60000, "text/plain", extra_headers, requestbody)
              else 
               -- in truncated = true case   
         --      ok, code, headers, status, body = s3service:performAction(actionname, "marker=" .. nextmarker, path, "GET", false, 60000, "text/plain", extra_headers, requestbody)
	             local args = { "marker" = nextmarker }
               ok, code, headers, status, body = s3service:performAction(actionname, args, path, "GET", false, 60000, "text/plain", extra_headers, requestbody)
              end

--             ngx.say("http code: ",code)
--             ngx.say(body)

--             code type is  not string
                if code == 200 then
                        local m, err = ngx.re.match(body, "<Marker>\\S+<\\/Contents>")
                        if m then
                            local parsedXml = xml:ParseXmlText(m[0])
                            -- if parsedXml.Marker == nil then error("Node not created") end
                            if parsedXml.Marker ~= nil then
                                    --marker = parsedXml.Marker:value() or ""
                                    marker = parsedXml.Marker:value()
                            end
                            maxkeys = parsedXml.MaxKeys:value()
                            local i=1        
                            repeat 
                                  if parsedXml.Contents[i] == nil then
                                     break
                                  end 
                                  local key = parsedXml.Contents[i].key:value() 
                                  if key then
                                          -- read key
                                         local keypath = path .. "/" .. key
                                         local requestbody = ""
                                         local ok, code, headers, status, body = s3service:performAction(actionname, uriargs, keypath, "GET", false, 60000, "text/plain", extra_headers, requestbody)
                                  end
                                     
                                  i = i + 1
                                  local contents = parsedXml.Contents[i]
                            until contents == nil
                            if parsedXml.IsTruncated:value() ~= "false" then
                                 IsTruncated = "true"
                                 nextmarker = parsedXml.NextMarker:value()
                            else
                                 nextmarker = nil
                            end
                       else
                           if err then
                               ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "error: ", err)
                               break
                           end
                           -- ngx.say("list object match not found")
                           IsTruncated = "false"
                       end                 
                else
                      IsTruncated = "false"
                end
            end       
                       

         } 
     }    
    location / {
	 lua_check_client_abort on;
        client_max_body_size 10000000k;
        client_body_buffer_size 100000k;
        default_type text/plain;
     	lua_code_cache on;
         set $rangestr '';
         rewrite_by_lua_file /root/atest/openresty/proxy/storageproxy_rewrite.lua;
        content_by_lua_file "/root/atest/openresty/proxy/storageproxy.lua";
#        body_filter_by_lua_file "/root/atest/openresty/proxy/bodyfilter.lua";
     } 

--- request
GET /v4
--- no_error_log
[error]
--- response_body_like eval
qr/<Marker>.*<\/Contents>/s
--- error_code: 200
--- abort
--- timeout: 60
=== TEST 18: read many random object lua by file
--- config
# listen 8081 so_keepalive=2s:2s:8;
        keepalive_timeout 1200;
        keepalive_requests 1000000;
        location /v4 {
	      lua_check_client_abort on;
        client_max_body_size 10000000k;
        client_body_buffer_size 100000k;
        default_type text/plain;
       	lua_code_cache on;

        content_by_lua_file "/root/atest/openresty/proxy/test/t/s3vroot.lua";
     }    
    location / {
	 lua_check_client_abort on;
        client_max_body_size 10000000k;
        client_body_buffer_size 100000k;
        default_type text/plain;
     	lua_code_cache on;
         set $rangestr '';
         rewrite_by_lua_file /root/atest/openresty/proxy/storageproxy_rewrite.lua;
        content_by_lua_file "/root/atest/openresty/proxy/storageproxy.lua";
#        body_filter_by_lua_file "/root/atest/openresty/proxy/bodyfilter.lua";
     } 
     location /gc {
         content_by_lua_block {
                 ngx.say(string.format("GC: %dKB", collectgarbage("count")))     
               }
     } 

--- request
GET /v4
--- no_error_log
[error]
--- error_code: 200
--- abort
--- timeout: 60000s
=== TEST 19:  check memleak for /v4 only small req
--- config
# listen 8081 so_keepalive=2s:2s:8;
        keepalive_timeout 1200;
        keepalive_requests 1000000;
        location /v4 {
	      lua_check_client_abort on;
        client_max_body_size 10000000k;
        client_body_buffer_size 100000k;
        default_type text/plain;
       	lua_code_cache on;

        content_by_lua_file "/root/atest/openresty/proxy/test/t/s3vroot.lua";
     }    
    location / {
	 lua_check_client_abort on;
        client_max_body_size 10000000k;
        client_body_buffer_size 100000k;
        default_type text/plain;
     	lua_code_cache on;
       content_by_lua_block {
           collectgarbage()
           ngx.say("ok")
            }
          }       
     location /gc {
         content_by_lua_block {
                 ngx.say(string.format("GC: %dKB", collectgarbage("count")))     
               }
     } 

--- request
GET /v4
--- no_error_log
[error]
--- error_code: 200
--- abort
--- timeout: 60000s
=== TEST 20: check memleak list buckets
--- config
        listen 8081 so_keepalive=2s:2s:8;
        keepalive_timeout 120;
        location /v4 {

            content_by_lua_block {
                collectgarbage()
                local AWSV4S = require "api-gateway.aws.AwsV4Signature"
                local AWSV4Service = require "api-gateway.aws.AwsService"
                local s3config = {
                  aws_region = "dnion-beijing",     
                  aws_service = "s3",   
                  aws_credentials = {
                  provider = "api-gateway.aws.AWSBasicCredentials",
                  -- access_key = ngx.var.aws_access_key,
                  access_key = "JQWE7H60CYOI3QZNLS3K",
                  -- secret_key = ngx.var.aws_secret_key
                  secret_key = "SVRnr3L7pPShRyYbodtmHKNjBg9wpXbFScSkwlEL"
                  }
               }
                local inspect = require "inspect"
                local s3service = AWSV4Service:new(s3config)
                
                s3service:constructor(s3service)

                -- local path = "/$::bucket"
                local path = "/1158abc9"
	             local args = {}
	             args["marker"] = "fbed31e5fda0e9855b4bc3f41221b8453794"
                local extra_headers = {
 --                     ["X-AMZ-Content-Sha256"] = "UNSIGNED-PAYLOAD" -- in the lib change it

                }

              -- actionName, arguments, path, http_method, useSSL, timeout, contentType
               local actionname = nil
               local uriargs = ngx.req.get_uri_args()                  
               local ok, code, headers, status, body = s3service:performAction(actionname, uriargs, path, "GET", false, 60000, "text/plain", extra_headers, "")
--               local ok, code, headers, status, body = s3service:performAction(actionname, args, path, "GET", false, 60000, "text/plain", extra_headers, "")

               ngx.say("http code: ",code)
               ngx.say(body)


         } 
     }    
    location / {
	 #set $portal 1;
	 lua_check_client_abort on;
        client_max_body_size 10000000k;
        client_body_buffer_size 100000k;
        default_type text/plain;
     	lua_code_cache on;
         set $rangestr '';
         rewrite_by_lua_file /root/atest/openresty/proxy/storageproxy_rewrite.lua;
        content_by_lua_file "/root/atest/openresty/proxy/storageproxy.lua";
#        body_filter_by_lua_file "/root/atest/openresty/proxy/bodyfilter.lua";
     } 
     location /gc {
         content_by_lua_block {
                 ngx.say(string.format("GC: %dKB", collectgarbage("count")))     
               }
     } 

--- more_headers
X-Test: test
--- request
GET /v4
--- response_body_like eval
qr/http code: 200\s<\?xml version="1.0" encoding="UTF-8"\?>.*<Name>1158abc9<\/Name>.*<\/ListBucketResult>/s
--- no_error_log
[error]
--- error_code: 200
--- abort
--- timeout: 60000s
=== TEST 21: test xml node:tostr
--- config
        listen 8081 so_keepalive=2s:2s:8;
        keepalive_timeout 120;
        location /v4 {

            local xmlsimple = require("xmlSimple")
	          local node = xmlsimple.node
            local xml_res = node:new({___name = "Error"})
            local s3code = 1
            local message = 2
            local Resource = 3
            local RequestId = 4
            ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " ",  " this is ", xml_res:name())
            local code = node:new({___name ="Code", ___value = s3code})
            local mesg = node:new({___name ="Message", ___value = message})
            local resce = node:new({___name ="Resource", ___value = Resource})
            local reqid = node:new({___name ="RequestId", ___value = RequestId})

--	          xml_res:addChild(code)
--            ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " ",  " this child is1 ", xml_res:numChildren())
--            
--	          xml_res:addChild(mesg)
--            ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " ",  " this child is mesg1 ", xml_res:numChildren())
--	          xml_res:addChild(resce)
--	          xml_res:addChild(reqid)
--            ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " ",  " this child is reqid4 ", xml_res:numChildren())
--            ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " ", xml_res:tostr())
              local xmlstr = "<Error>" .. code:tostr() .. mesg:tostr() .. resce:tostr() .. reqid:tostr() .."</Eroor>"
              ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " ", xmlstr)

            }
     } 

--- request
GET /v4
--- no_error_log
[error]
--- error_code: 200
--- abort
--- timeout: 60000s
=== TEST 22: test inspect
--- config
        listen 8081 so_keepalive=2s:2s:8;
        keepalive_timeout 120;
        location /v4 {

            content_by_lua_block {
                    local function mytestfunc()
	                       ngx.say("HAHAHAHAHAHA")
                         ngx.flush()
                         return "1"
                    end
                    
	

                    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  mytestfunc())
--                    ngx.exit()
            }
     } 

--- request
GET /v4
--- no_error_log
[error]
--- error_code: 200
--- response_body_like eval
qr/HAHAHAHAHAHA/s
--- abort
--- timeout: 60000s
=== TEST 23: test split
--- config
        listen 8081 so_keepalive=2s:2s:8;
        keepalive_timeout 120;
        location /v4 {

            content_by_lua_block {
                local inspect = require "inspect"
                local ngx_re = require "ngx.re"

                local res, err = ngx_re.split("a,b,c,d", ",", nil, {pos = 5})
                local one = res[1]
	              ngx.say(table.concat(res))
                ngx.flush()
                    
	

                -- ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  res.1)
            }
     } 

--- request
GET /v4
--- no_error_log
[error]
--- error_code: 200
--- response_body_like eval
qr/cd/s
--- abort
--- timeout: 60000s
=== TEST 24: test split
--- config
        listen 8081 so_keepalive=2s:2s:8;
        keepalive_timeout 120;
        location /v4 {

            content_by_lua_block {
                ngx.print(collectgarbage("setpause", 90))

                local inspect = require "inspect"
                local ngx_re = require "ngx.re"

                --local res, err = ngx_re.split("a,b,c,d", "\\s", nil, {pos = 5})
                local res, err = ngx_re.split("a,b,c,d", "\\s", nil, {pos = 1,nil,2})
            --    local one = res[1]
                ngx.print(collectgarbage("setpause"))
	              ngx.say(table.concat(res))
                ngx.flush()
                    
	

                -- ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  res.1)
            }
     } 

--- request
GET /v4
--- no_error_log
[error]
--- error_code: 200
--- response_body_like eval
qr/c,d1/s
--- abort
--- timeout: 60000s
=== TEST 25: test tablepool
--- config
        listen 8081 so_keepalive=2s:2s:8;
        keepalive_timeout 120;
        location /v4 {

            content_by_lua_block {
            local tablepool = require "tablepool"

            local pool_name = "some_tag"

            local my_tb = tablepool.fetch(pool_name, 0, 10)

            -- using my_tb for some purposes...

            tablepool.release(pool_name, my_tb)
            }
     } 

--- request
GET /v4
--- no_error_log
[error]
--- error_code: 200
--- response_body_like eval
qr/c,d1/s
--- abort
--- timeout: 60000s
=== TEST 26: test split /
--- config
        listen 8081 so_keepalive=2s:2s:8;
        keepalive_timeout 120;
        location /v4 {

            content_by_lua_block {
    --            ngx.print(collectgarbage("setpause", 90))

                local inspect = require "inspect"
                local ngx_re = require "ngx.re"

--                 local res, err = ngx_re.split("a/b/c/d/e", "/")
--              for i,r in pairs(res) do
--                   ngx.log(ngx.INFO, r)
--                   ngx.say("111" .. r)
--              end
-- 
                local res, err = ngx_re.split("/a/b/c/d/e", "/")
             for i,r in pairs(res) do
                  ngx.log(ngx.INFO, r)
      --            ngx.say("111" .. (r or ''))
                  ngx.say("111" .. r)
             end
                     
              ngx.say(inspect(res))
                local res, err = ngx_re.split("/a/b/c/d/e/", "/")
             for i,r in pairs(res) do
                  ngx.log(ngx.INFO, r)
      --            ngx.say("111" .. (r or ''))
                  ngx.say("111" .. r)
             end
                     
              ngx.say(inspect(res))
	
                local res, err = ngx_re.split("/1158abc9?max-keys=1000","\\?")
                -- ngx.log(ngx.INFO, res.1)
              ngx.say(inspect(res))
            }
     } 

--- request
GET /v4
--- no_error_log
[error]
--- error_code: 200
--- response_body 
111
111a
111b
111c
111d
111e
{ "", "a", "b", "c", "d", "e" }
111
111a
111b
111c
111d
111e
111
{ "", "a", "b", "c", "d", "e", "" }
{ "/1158abc9", "max-keys=1000" }

--- abort
--- timeout: 60000s




=== TEST 27: test adminportal
--- config
        listen 8081 so_keepalive=2s:2s:8;
        keepalive_timeout 120;
        location /admin {

            content_by_lua_block { 
            local admin = require "admin"
            local bucketadm = admin.bucket
            local statsadm = admin.stats

    ngx.log(ngx.INFO,"##### ngx.var.uri is ", ngx.var.uri)
    local uri = ngx.var.uri
    local ngx_re = require "ngx.re"
    local splittab, err = ngx_re.split(uri, "/")
    local first = "" 
    local second = ""

    for i,v in pairs(splittab) do
        if "" == first then
            first = v
        elseif "" == second then
            second = v
        else
            second = second .. "/" .. v
        end
    end
   ngx.log(ngx.INFO,"##### second: " .. second)
   local args = ngx.req.get_uri_args()
   for key, val in pairs(args) do
       if type(val) == "table" then
           -- do nothing
           -- ngx.say(key, ": ", table.concat(val, ", "))
       else
           -- ngx.say(key, ": ", val)
       end
   end
   local bucket
   if second == "bucket" then
          if args["bucket"] then
              bucket = bucketadm:new(args)
              bucket:getcap()
          else   
              getcap()
          end
       
   elseif second == "stats" then
          getstats()
   else 

   end

            }
     } 

--- request
GET /admin/bucket?format=json
--- no_error_log
[error]
--- error_code: 200
--- response_body 
--- abort
--- timeout: 60000s
--- ONLY
