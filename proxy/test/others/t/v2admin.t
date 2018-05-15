#use t::MYTEST 'no_plan';
#use Test::Nginx::Socket;
use Test::Nginx::Socket 'no_plan';
use Cwd qw(cwd);
no_long_string();
log_level 'debug';

use Getopt::Long qw(GetOptions);

our  $bucket;
GetOptions(
    'bucket=s' => \$bucket,
);
our  $pre;
GetOptions(
    'pre=s' => \$pre,
);
#our $svip1;
#GetOptions(
#    'svip1' => \$svip1,
#);
#our $serverip; how to pass to lua(ngx.var or?)
#
#GetOptions(
#    'serverip=s' => \$serverip,
#);
# how import into the lua code?
print "bucket is $bucket \n";
no_root_location();
no_shuffle();


#plan tests => repeat_each() * (blocks() * 4) + 1;

my $pwd = cwd();

$ENV{TEST_NGINX_RESOLVER} = '8.8.8.8';
$ENV{TEST_COVERAGE} ||= 0;
$ENV{SVIP} = '127.0.0.1';
env_to_nginx("SVIP");
our $HttpConfig = qq{
    lua_package_path "$pwd/lib/?.lua;/usr/local/share/lua/5.1/?.lua;$pwd/../thirdparty/?.lua;/usr/local/openresty/lualib/?.lua;;";
    error_log logs/error.log debug;
    init_by_lua_block {
        if $ENV{TEST_COVERAGE} == 1 then
            jit.off()
            require("luacov.runner").init()
        end
    }
};

#no_diff();

run_tests();

__DATA__




=== TEST 3: Simple default get.
--- http_config eval: $::HttpConfig
--- config
    location = /a {
        content_by_lua '
            local http = require "resty.http"
            local httpc = http.new()
            httpc:connect("127.0.0.1", ngx.var.server_port)
            local res, err = httpc:request{
                path = "/b"
            }
            ngx.status = res.status
            ngx.print(res:read_body())
            httpc:close()
        ';
    }
    location = /b {
        echo "OK";
    }
--- request
GET /a
--- response_body
OK
--- no_error_log
[error]
[warn]
--- SKIP

=== TEST 4: Simple default get.
--- http_config eval: $::HttpConfig
--- config
    location = /a {
        content_by_lua '
            local http = require "resty.http"
            local httpc = http.new()
            -- httpc:connect("127.0.0.1", ngx.var.server_port)
            httpc:connect("127.0.0.1", 6080)
            local res, err = httpc:request{
                path = "/admin"
            }
            ngx.status = res.status
            ngx.print(res:read_body())
            httpc:close()
        ';
    }
--- request
GET /a
--- response_body
--- no_error_log
[error]
[warn]
--- SKIP

=== TEST 5: /admin/user
--- http_config eval: $::HttpConfig
--- config
    location = /a {
        content_by_lua '
            local http = require "resty.http"
            local httpc = http.new()
            -- httpc:connect("127.0.0.1", ngx.var.server_port)
            httpc:connect("127.0.0.1", 6080)

--            local res, err = httpc:request{
--                path = "/admin/user",
--                method = "PUT",
--              --  query = "uid=testeeee&maxbuckets=21&isindex=true&company=xiaomi" 
--                query = "?uid=testeeee" 
--            }
--            ngx.status = res.status
--            ngx.print(res:read_body())

            local res, err = httpc:request{
                path = "/admin/user"
            }
--            ngx.status = res.status
            ngx.say(res:read_body())

            local res, err = httpc:request{
                path = "/admin/user?abc"
            }
            --ngx.print(res:read_body())
            ngx.say(res:read_body())




            local res, err = httpc:request{
                path = "/admin/bucket"
            }
            --ngx.print(res:read_body())
            ngx.say(res:read_body())
            httpc:close()
        ';
    }
--- request
GET /a
--- response_body_like
maybe you should supply the uid when operate on  user 
maybe you should supply the uid when operate on  user
[{"owner":"root","buckets":[{"bucket":"aaaaa3","mtime":"1488269825.426","usage":{"size_bytes":"0","objects":"0","mb_rounded":"0"}},{"bucket":"aaaaa4","mtime":"1488270923.650","usage":{"size_bytes":"0","objects":"0","mb_rounded":"0"}},{"bucket":"aaaaa5","mtime":"1488271140.999","usage":{"size_bytes":"0","objects":"0","mb_rounded":"0"}},{"bucket":"aaaaa6","mtime":"1488272231.070","usage":{"size_bytes":"0","objects":"0","mb_rounded":"0"}},{"bucket":"aaaaa7","mtime":"1488272319.990","usage":{"size_bytes":"0","objects":"0","mb_rounded":"0"}}]}]
--- no_error_log
[error]
[warn]
--- timeout: 60
--- abort
--- SKIP


=== TEST 6: create user testeeee3
--- http_config eval: $::HttpConfig
--- config
    location = /a {
        content_by_lua '
            local http = require "resty.http"
            local httpc = http.new()
            -- httpc:connect("127.0.0.1", ngx.var.server_port)
            httpc:connect("127.0.0.1", 6080)
            -- httpc:connect("183.131.9.155", 6080)
            --httpc:connect(ngx.var.svip, 6080)
            -- httpc:connect($SVIP, 6080)

            local res, err = httpc:request({
                path = "/admin/user",
                method = "PUT",
                query = "?uid=teste1eee3" 
            })
--            ngx.status = res.status
            ngx.print(res:read_body())

            httpc:close()
        ';
    }
--- request
GET /a
--- response_body_like eval
qr/\[{"keys":{"user":"teste1eee3"(\S+)/s
--- no_error_log
[error]
[warn]
--- timeout: 60
--- abort

=== TEST 7: get all bucket stats
--- http_config eval: $::HttpConfig
--- config
    location = /a {
        content_by_lua '
            local http = require "resty.http"
            local httpc = http.new()
            -- httpc:connect("127.0.0.1", ngx.var.server_port)
             httpc:connect("127.0.0.1", 6080)
            -- httpc:connect(ngx.var.svip, 6080)
--            httpc:connect($SVIP, 6080)
           -- httpc:connect("183.131.9.155", 6080)
            local res, err = httpc:request{
                path = "/admin/bucket"
            }
--            ngx.status = res.status
            ngx.print(res:read_body())



            httpc:close()
        ';
    }
--- request
GET /a
--- response_body_like eval
qr/\[{"owner":(\S+)/s
--- no_error_log
[error]
[warn]
--- timeout: 60
--- abort

=== TEST 8: /admin/user;/admin/user?abc;/admin/user?uid=noexixt;/admin/bucket?uid=noexist
--- http_config eval: $::HttpConfig
--- config
    location = /a {
        content_by_lua '
            local http = require "resty.http"
            local httpc = http.new()
            -- httpc:connect("127.0.0.1", ngx.var.server_port)
            httpc:connect("127.0.0.1", 6080)


            local res, err = httpc:request{
                path = "/admin/user"
            }
--            ngx.status = res.status
-- todo: improve the resopnse code
            ngx.print(res:read_body())

            local res, err = httpc:request{
                path = "/admin/user?abc"
            }
            ngx.print(res:read_body())

            local res, err = httpc:request{
                path = "/admin/user?uid=nonexist"
            }
            ngx.print(res:read_body())

            local res, err = httpc:request{
                path = "/admin/bucket?uid=nonexist"
            }
            ngx.print(res:read_body())
            local res, err = httpc:request{ -- empty 
                path = "/admin/user/abc"
            }
            ngx.print(res:read_body())

            local res, err = httpc:request{ -- empty 
                path = "/admin/bucket?uid"
            }
            ngx.print(res:read_body())

            local res, err = httpc:request{
                path = "/admin/user?uid"
            }
            ngx.print(res:read_body())

            httpc:close()
        ';
    }
--- request
GET /a
--- error_code: 200
--- response_body

maybe you should supply the uid when operate on user
maybe you should supply the uid when operate on user
user do not exist when get userinfo!
1004:no exist user
please input uid string
you should supply the uid name when operate on user
--- no_error_log
[error]
[warn]
--- timeout: 60
--- abort
--- SKIP

# delete all user
# create user 
# test
# delete user
=== TEST 9: /admin/user
--- http_config eval: $::HttpConfig
--- config
    location = /a {
        content_by_lua '
            local http = require "resty.http"
            local httpc = http.new()
            -- httpc:connect("127.0.0.1", ngx.var.server_port)
             httpc:connect("127.0.0.1", 6080)
           -- httpc:connect($SVIP, 6080)
            -- httpc:connect("183.131.9.155", 6080)

            local res, err = httpc:request({
                path = "/admin/user",
                method = "DELETE",
                query = "?uid=teste1eee3" 
            })
--            ngx.status = res.status
            ngx.print(res:read_body())

            httpc:close()
        ';
    }
--- request
GET /a
--- response_body_like chomp
successful
--- no_error_log
[error]
[warn]
--- timeout: 60
--- abort

=== TEST 10: /admin/user;/admin/user?abc;/admin/user?uid=noexixt;/admin/bucket?uid=noexist
--- http_config eval: $::HttpConfig
--- config
    location = /a {
        content_by_lua '
            local http = require "resty.http"
            local httpc = http.new()
            -- httpc:connect("127.0.0.1", ngx.var.server_port)
            httpc:connect("127.0.0.1", 6080)


            local res, err = httpc:request{
                path = "/admin/user"
            }
--            ngx.status = res.status
-- todo: improve the resopnse code
            ngx.print(res:read_body())

--            local res, err = httpc:request{
--                path = "/admin/user?abc"
--            }
--            ngx.print(res:read_body())
--
--            local res, err = httpc:request{
--                path = "/admin/user?uid=nonexist"
--            }
--            ngx.print(res:read_body())
--
--            local res, err = httpc:request{
--                path = "/admin/bucket?uid=nonexist"
--            }
--            ngx.print(res:read_body())
--            local res, err = httpc:request{ -- empty 
--                path = "/admin/user/abc"
--            }
--            ngx.print(res:read_body())
--
--            local res, err = httpc:request{ -- empty 
--                path = "/admin/bucket?uid"
--            }
--            ngx.print(res:read_body())
--
--            local res, err = httpc:request{
--                path = "/admin/user?uid"
--            }
--            ngx.print(res:read_body())
--
            httpc:close()
        ';
    }
--- request
GET /a
--- error_code: 200
--- response_body chomp

<?xml version="1.0" encoding="UTF-8"?>
<Error><Code>MissingUid</Code><Message>Your request is missing uid arg</Message><Resource></Resource><RequestId></RequestId></Error>
--- no_error_log
[error]
[warn]
--- timeout: 60
--- abort
--- SKIP

=== TEST 11: /admin/user;/admin/user?abc;/admin/user?uid=noexixt;/admin/bucket?uid=noexist err test
--- http_config eval: $::HttpConfig
--- config
    location = /a {
        content_by_lua '
            local http = require "resty.http"
            local httpc = http.new()
            -- httpc:connect("127.0.0.1", ngx.var.server_port)
              --httpc:connect(ngx.var.svip, 6080)
              httpc:connect("127.0.0.1", 6080)
          --  httpc:connect($SVIP, 6080)
          --  httpc:connect("183.131.9.155", 6080)
            local str= ""

            local res, err = httpc:request{
                path = "/admin/user"
            }
           local m, err = ngx.re.match(res:read_body(), "MissingUid")
           if m then
                str = str .. m[0]  
           else
               if err then
                   ngx.log(ngx.ERR, "error: ", err)
                   return
               end
               ngx.say("match not found")
           end

            local res, err = httpc:request{
                path = "/admin/user?abc"
            }
           local m, err = ngx.re.match(res:read_body(), "MissingUid")
           if m then
                str = str .. m[0]  
           else
               if err then
                   ngx.log(ngx.ERR, "error: ", err)
                   return
               end
           end 
            local res, err = httpc:request{
                path = "/admin/user?uid=nonexist"
            }
           
           local m, err = ngx.re.match(res:read_body(), "NoSuchUid")
           if m then
                str = str .. m[0]  
           else
               if err then
                   ngx.log(ngx.ERR, "error: ", err)
                   return
               end
           end 
            local res, err = httpc:request{
                path = "/admin/bucket?uid=nonexist"
            }
           local m, err = ngx.re.match(res:read_body(), "NoSuchUid")
           if m then
                str = str .. m[0]  
           else
               if err then
                   ngx.log(ngx.ERR, "error: ", err)
                   return
               end
           end 
            local res, err = httpc:request{ -- empty 
                path = "/admin/user/abc"
            }
           local m, err = ngx.re.match(res:read_body(), "MethodNotAllowed")
           if m then
                str = str .. m[0]  
           else
               if err then
                   ngx.log(ngx.ERR, "error: ", err)
                   return
               end
           end 
            local res, err = httpc:request{ -- empty 
                path = "/admin/bucket?uid"
            }
           local m, err = ngx.re.match(res:read_body(), "InvalidUidType")
           if m then
                 ngx.log(ngx.INFO, "m0: ", m[0])
                str = str .. m[0]  
           else
               if err then
                   ngx.log(ngx.ERR, "error: ", err)
                   return
               end
           end 
            local res, err = httpc:request{
                path = "/admin/user?uid"
            }
           local m, err = ngx.re.match(res:read_body(), "InvalidUidType")
           if m then
                 ngx.log(ngx.INFO, "m0: ", m[0])
                str = str .. m[0]  
           else
               if err then
                   ngx.log(ngx.ERR, "error: ", err)
                   return
               end
           end 
            ngx.print(str)
            httpc:close()
        ';
    }
--- request
GET /a
--- error_code: 200
--- response_body chomp
MissingUidMissingUidNoSuchUidNoSuchUidMethodNotAllowedInvalidUidTypeInvalidUidType
--- no_error_log
[error]
[warn]
--- timeout: 60
--- abort
