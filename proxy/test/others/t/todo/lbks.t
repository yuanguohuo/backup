#use t::MYTEST 'no_plan';
use Test::Nginx::Socket 'no_plan';
no_long_string();
log_level 'debug';
#use Test::Nginx::Socket::Lua 'no_plan';
no_root_location();

no_shuffle();
run_tests();
# problem :only one value


__DATA__

=== TEST 1:  http://ip/ keepalive chunk
--- config 
     location /test {
     content_by_lua_block {
       local head = "ListAllMyBucketsResult"
       local tail =     "</Buckets>" .. "</ListAllMyBucketsResult>"

       ngx.header["Content-Type"] = "application/xml"

      ngx.say(head)
      ngx.say(tail)
      }
     }

--- request
GET /test
--- more_headers
connection: keep-alive
--- response_body_like eval
qr/([^L]*)istAllMyBucketsResult(\S+)/

--- error_code: 200
--- timeout: 10
--- no_error_log
[error]
--- curl
#--- ONLY

=== TEST 2:  http://ip/ keepalive length
--- config 
     location /test {
     content_by_lua_block {
       local head = "ListAllMyBucketsResult"
       local tail =     "</Buckets>" .. "</ListAllMyBucketsResult>"

       ngx.header["Content-Type"] = "application/xml"
       ngx.header["Content-length"] = string.len(head)

      ngx.say(head)
--      ngx.say(tail)
      }
     }

--- request
GET /test
--- more_headers
connection: keep-alive
--- response_body_like eval
qr/([^L]*)istAllMyBucketsResult(\S+)/

--- error_code: 200
--- timeout: 10
--- no_error_log
[error]
--- curl
--- ONLY

