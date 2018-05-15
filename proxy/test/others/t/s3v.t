# vim:set ft= ts=4 sw=4 et fdm=marker:
#use lib 'lib';
use Test::Nginx::Socket::Lua 'no_plan';
#use Test::Nginx::Socket::Lua;
use Cwd qw(cwd);

log_level 'debug';
#worker_connections(1014);
#master_process_enabled(1);
#log_level('warn');

#repeat_each(1);

#plan tests => repeat_each() * (blocks() * 4) - 1;

my $pwd = cwd();

# try to read the nameservers used by the system resolver:
my @nameservers;
if (open my $in, "/etc/resolv.conf") {
    while (<$in>) {
        if (/^\s*nameserver\s+(\d+(?:\.\d+){3})(?:\s+|$)/) {
            push @nameservers, $1;
            if (@nameservers > 10) {
                last;
            }
        }
    }
    close $in;
}

if (!@nameservers) {
    # default to Google's open DNS servers
    push @nameservers, "8.8.8.8", "8.8.4.4";
}


warn "Using nameservers: \n@nameservers\n";

#our $HttpConfig = <<_EOC_;
#    lua_package_path 'src/lua/?.lua;;';
#    lua_package_cpath 'src/lua/?.so;;';
#    init_by_lua '
#        local v = require "jit.v"
#        v.on("$Test::Nginx::Util::ErrLogFile")
#        require "resty.core"
#    ';
#    resolver @nameservers;
#    client_body_temp_path /tmp/;
#    proxy_temp_path /tmp/;
#    fastcgi_temp_path /tmp/;
#_EOC_

our $HttpConfig = qq{
    lua_package_path "$pwd/../cache_api/?.lua;$pwd/../thirdparty/?.lua;$pwd/../?.lua;$pwd/t/clientlib/?.lua;;";
    lua_package_cpath "/usr/local/openresty-debug/lualib/?.so;;";
};
#no_diff();
no_long_string();
run_tests();

__DATA__

=== TEST 1: test request args with same first character
--- http_config eval: $::HttpConfig
--- config
        location /test-signature {

            content_by_lua '
                local AWSV4S = require "awsc.AwsV4Signature"
                local awsAuth =  AWSV4S:new()
                ngx.print(awsAuth:formatQueryString(ngx.req.get_uri_args()))
            ';
        }

--- more_headers
X-Test: test
--- request
POST /test-signature?Subject=nginx:test!@$&TopicArn=arn:aws:sns:us-east-1:492299007544:apiplatform-dev-ue1-topic-analytics&Message=hello_from_nginx,with_comma!&Action=Publish&Subject1=nginx:test
--- response_body eval
["Action=Publish&Message=hello_from_nginx%2Cwith_comma%21&Subject=nginx%3Atest%21%40%24&Subject1=nginx%3Atest&TopicArn=arn%3Aaws%3Asns%3Aus-east-1%3A492299007544%3Aapiplatform-dev-ue1-topic-analytics"]
--- no_error_log
[error]
--- error_code: 200

=== TEST 2: test v4
--- http_config eval: $::HttpConfig
--- config
        location /signature {

            content_by_lua_block {
                local AWSV4S = require "awsc.AwsV4Signature"
                local awsAuth =  AWSV4S:new()
                ngx.print(awsAuth:formatQueryString(ngx.req.get_uri_args()))
            }
        }

--- more_headers
X-Test: test
--- request
GET /test-signature?Subject=nginx:test!@$&TopicArn=arn:aws:sns:us-east-1:492299007544:apiplatform-dev-ue1-topic-analytics&Message=hello_from_nginx,with_comma!&Action=Publish&Subject1=nginx:test
--- response_body eval
["Action=Publish&Message=hello_from_nginx%2Cwith_comma%21&Subject=nginx%3Atest%21%40%24&Subject1=nginx%3Atest&TopicArn=arn%3Aaws%3Asns%3Aus-east-1%3A492299007544%3Aapiplatform-dev-ue1-topic-analytics"]
--- no_error_log
[error]
--- error_code: 200
=== TEST 3: test v4 ,Querystring
--- http_config eval: $::HttpConfig
--- config
        location /signature {

            content_by_lua_block {
                local AWSV4S = require "api-gateway.aws.AwsV4Signature"
                local inspect = require "inspect"
                local awsAuth =  AWSV4S:new()
                ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", "uriargs is ", inspect(ngx.req.get_uri_args()))
                ngx.print(awsAuth:formatQueryString(ngx.req.get_uri_args()))
            }
        }

--- more_headers
X-Test: test
--- request
GET /signature?Subject=nginx:test!@$&TopicArn=arn:aws:sns:us-east-1:492299007544:apiplatform-dev-ue1-topic-analytics&Message=hello_from_nginx,with_comma!&Action=Publish&Subject1=nginx:test
--- response_body eval
["Action=Publish&Message=hello_from_nginx%2Cwith_comma%21&Subject=nginx%3Atest%21%40%24&Subject1=nginx%3Atest&TopicArn=arn%3Aaws%3Asns%3Aus-east-1%3A492299007544%3Aapiplatform-dev-ue1-topic-analytics"]
--- no_error_log
[error]
--- error_code: 200
=== TEST 4: test v4 ,Querystring
--- http_config eval: $::HttpConfig
--- config
        location /signature {

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
                  access_key = "O911PT5Z34WN8Q92C8YU",
                  -- secret_key = ngx.var.aws_secret_key
                  secret_key = "l0fjx5mHz3KoOOiiYILc6JtpXqVJNOCpYJXUlOIZ"
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
                local awsAuth =  AWSV4S:new()
                local s3service = AWSV4Service:new(s3config)
                
                s3service:constructor(s3service)
--                local host = s3service:getAWSHost()
--                local credential  = s3service:getCredentials()

                local path = "/"
                local extra_headers = {
                    ["X-Amz-Client-Context"] = "clientContext",
                    ["X-Amz-Invocation-Type"] = "invocationType",
                    ["X-Amz-Log-Type"] = "logType"
                }

              -- actionName, arguments, path, http_method, useSSL, timeout, contentType
               local actionname = nil
               local urigargs = ngx.req.get_uri_args()                  
               local ok, code, headers, status, body = s3service:performAction(actionname, uriargs, path, "POST", false, 60000, "application/x-amz-json-1.1", extra_headers)




--               local authheader = s3service:getAuthorizationHeader(http_method, path, uri_args, body)

             --  ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", "uriargs is ", inspect(ngx.req.get_uri_args()))
             --  ngx.print(awsAuth:formatQueryString(ngx.req.get_uri_args()))
         } 
        }

--- more_headers
X-Test: test
--- request
GET /signature?Subject=nginx:test!@$&TopicArn=arn:aws:sns:us-east-1:492299007544:apiplatform-dev-ue1-topic-analytics&Message=hello_from_nginx,with_comma!&Subject1=nginx:test
--- response_body eval
["Action=Publish&Message=hello_from_nginx%2Cwith_comma%21&Subject=nginx%3Atest%21%40%24&Subject1=nginx%3Atest&TopicArn=arn%3Aaws%3Asns%3Aus-east-1%3A492299007544%3Aapiplatform-dev-ue1-topic-analytics"]
--- no_error_log
[error]
--- error_code: 200
=== TEST 5: test v4 /
--- http_config eval: $::HttpConfig
--- config
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
                  access_key = "O911PT5Z34WN8Q92C8YU",
                  -- secret_key = ngx.var.aws_secret_key
                  secret_key = "l0fjx5mHz3KoOOiiYILc6JtpXqVJNOCpYJXUlOIZ"
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
                    ["X-Amz-Client-Context"] = "clientContext",
                    ["X-Amz-Invocation-Type"] = "invocationType",
                    ["X-Amz-Log-Type"] = "logType"
                }

              -- actionName, arguments, path, http_method, useSSL, timeout, contentType
               local actionname = nil
               local urigargs = ngx.req.get_uri_args()                  
               local ok, code, headers, status, body = s3service:performAction(actionname, uriargs, path, "GET", false, 60000, "application/x-amz-json-1.1", extra_headers)
--               local authheader = s3service:getAuthorizationHeader(http_method, path, uri_args, body)

             --  ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", "uriargs is ", inspect(ngx.req.get_uri_args()))
             --  ngx.print(awsAuth:formatQueryString(ngx.req.get_uri_args()))
         } 
        }

--- more_headers
X-Test: test
--- request
GET /v4
#--- response_body eval
--- no_error_log
[error]
--- error_code: 200
--- ONLY
