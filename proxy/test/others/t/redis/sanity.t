# vim:set ft= ts=4 sw=4 et:

use Test::Nginx::Socket::Lua 'no_plan';
use Cwd qw(cwd);

# repeat_each(2);

#plan tests => repeat_each() * (3 * blocks());

my $pwd = cwd();

our $HttpConfig = qq{
    #lua_package_path "$pwd/lib/?.lua;;";
   # lua_package_cpath "/usr/local/openresty-debug/lualib/?.so;/usr/local/openresty/lualib/?.so;;";
    lua_package_path "$pwd/../cache_api/?.lua;$pwd/../thirdparty/?.lua;$pwd/../?.lua;;";
    #lua_package_cpath "/usr/local/openresty-debug/lualib/?.so;/usr/local/openresty/lualib/?.so;;";
    lua_package_cpath "/usr/local/openresty-debug/lualib/?.so;;";
};

$ENV{TEST_NGINX_RESOLVER} = '8.8.8.8';
$ENV{TEST_NGINX_REDIS_PORT} ||= 30001;
#no_shuffle();
log_level 'debug';
no_long_string();
#no_diff();

run_tests();

__DATA__

=== TEST 1: set and get
--- http_config eval: $::HttpConfig
--- config
    location /t {
    content_by_lua_block {
    local inspect = require "inspect" 
    
    local ok,cluster = pcall(require, "redis.cluster")
    if not ok or not cluster then
        ngx.say("failed to load redis.cluster. err="..(cluster or "nil"))
        return
    end
    
    local c=cluster:new("192.168.122.33:30001","192.168.122.33:30002","192.168.122.33:30003",
                        "192.168.122.33:30004","192.168.122.33:30005","192.168.122.33:30006")
    --1. wrong number argument 
    local res,err = c:do_cmd_quick("set", "foobar")
    -- if not res then
    if err then
        ngx.say("ERROR: do_cmd_quick_Test_SetGetExistDelete 1")
    end

    --2. wrong number of arguments, should fail
    local res,err = c:do_cmd_quick("set", "foobar", 1, 2)
    if not res then
        ngx.say("ERROR: do_cmd_quick_Test_SetGetExistDelete 2")
    end

    --3. positive 
    local res,err = c:do_cmd_quick("set", "foobar", 100)
    if not res then
        ngx.say("ERROR: do_cmd_quick_Test_SetGetExistDelete 3")
        return false
    end

    ngx.say("set foobar: ", res)

    --4.test
    local res,err = c:do_cmd_quick("get", "foobar")
    if tonumber(res) ~= 100 then
        ngx.say("ERROR: do_cmd_quick_Test_SetGetExistDelete 4", " res=", inspect(res))
        return false
    end

    ngx.say("get foobar: ", res)
    --5. exists
    local res,err = c:do_cmd_quick("exists", "foobar")
    if tonumber(res) ~= 1 then
        ngx.say("ERROR: do_cmd_quick_Test_SetGetExistDelete 5", " res=", inspect(res))
        return false
    end

    ngx.say("exists foobar: ", res)
    --6. delete
    local res,err = c:do_cmd_quick("del", "foobar")
    if not res then
        ngx.say("ERROR: do_cmd_quick_Test_SetGetExistDelete 6", " res=", inspect(res))
        return false
    end

    ngx.say("delete foobar: ", res)
    --7. exists
    local res,err = c:do_cmd_quick("exists", "foobar")
    if tonumber(res) ~= 0 then
        ngx.say("ERROR: do_cmd_quick_Test_SetGetExistDelete 7", " res=", inspect(res))
        return false
    end

    ngx.say("exists foobar: ", res)
       }
    }
--- request
GET /t
--- response_body
ERROR: do_cmd_quick_Test_SetGetExistDelete 1
ERROR: do_cmd_quick_Test_SetGetExistDelete 2
set foobar: OK
get foobar: 100
exists foobar: 1
delete foobar: 1
exists foobar: 0
--- error_log
[error]


=== TEST 2: flushall
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local redis = require "resty.redis"
            local red = redis:new()

            red:set_timeout(1000) -- 1 sec

            local ok, err = red:connect("127.0.0.1", $TEST_NGINX_REDIS_PORT)
            if not ok then
                ngx.say("failed to connect: ", err)
                return
            end

            local res, err = red:flushall()
            if not res then
                ngx.say("failed to flushall: ", err)
                return
            end
            ngx.say("flushall: ", res)

            red:close()
        ';
    }
--- request
GET /t
--- response_body
flushall: OK
--- no_error_log
[error]
--- SKIP



=== TEST 3: get nil bulk value
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
      local inspect = require "inspect" 
    
    local ok,cluster = pcall(require, "redis.cluster")
    if not ok or not cluster then
        ngx.say("failed to load redis.cluster. err="..(cluster or "nil"))
        return
    end
    
    local c=cluster:new("192.168.122.33:30001","192.168.122.33:30002","192.168.122.33:30003",
                        "192.168.122.33:30004","192.168.122.33:30005","192.168.122.33:30006")
    local res
    local err
            for i = 1, 2 do
                res, err = c:do_cmd_quick("get","not_found")
                if err then
                    ngx.say("failed to get: ", err)
                    return
                end

                if res == ngx.null then
                    ngx.say("not_found not found.")
                    return
                end

                ngx.say("get not_found: ", res)
            end

       --     red:close()
        }
    }
--- request
GET /t
--- response_body
not_found not found.
--- no_error_log
[error]



=== TEST 4: get nil list
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local redis = require "resty.redis"
            local red = redis:new()

            red:set_timeout(1000) -- 1 sec

            local ok, err = red:connect("127.0.0.1", $TEST_NGINX_REDIS_PORT)
            if not ok then
                ngx.say("failed to connect: ", err)
                return
            end

            local res, err = red:flushall()
            if not res then
                ngx.say("failed to flushall: ", err)
                return
            end

            ngx.say("flushall: ", res)

            for i = 1, 2 do
                res, err = red:lrange("nokey", 0, 1)
                if err then
                    ngx.say("failed to get: ", err)
                    return
                end

                if res == ngx.null then
                    ngx.say("nokey not found.")
                    return
                end

                ngx.say("get nokey: ", #res, " (", type(res), ")")
            end

            red:close()
        ';
    }
--- request
GET /t
--- response_body
flushall: OK
get nokey: 0 (table)
get nokey: 0 (table)
--- no_error_log
[error]
--- SKIP





=== TEST 5: incr and decr
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local redis = require "resty.redis"
            local red = redis:new()

            red:set_timeout(1000) -- 1 sec

            local ok, err = red:connect("127.0.0.1", $TEST_NGINX_REDIS_PORT)
            if not ok then
                ngx.say("failed to connect: ", err)
                return
            end

            local res, err = red:set("connections", 10)
            if not res then
                ngx.say("failed to set connections: ", err)
                return
            end

            ngx.say("set connections: ", res)

            res, err = red:incr("connections")
            if not res then
                ngx.say("failed to set connections: ", err)
                return
            end

            ngx.say("incr connections: ", res)

            local res, err = red:get("connections")
            if err then
                ngx.say("failed to get connections: ", err)
                return
            end

            res, err = red:incr("connections")
            if not res then
                ngx.say("failed to incr connections: ", err)
                return
            end

            ngx.say("incr connections: ", res)

            res, err = red:decr("connections")
            if not res then
                ngx.say("failed to decr connections: ", err)
                return
            end

            ngx.say("decr connections: ", res)

            res, err = red:get("connections")
            if not res then
                ngx.say("connections not found.")
                return
            end

            ngx.say("connections: ", res)

            res, err = red:del("connections")
            if not res then
                ngx.say("failed to del connections: ", err)
                return
            end

            ngx.say("del connections: ", res)

            res, err = red:incr("connections")
            if not res then
                ngx.say("failed to set connections: ", err)
                return
            end

            ngx.say("incr connections: ", res)

            res, err = red:get("connections")
            if not res then
                ngx.say("connections not found.")
                return
            end

            ngx.say("connections: ", res)

            red:close()
        ';
    }
--- request
GET /t
--- response_body
set connections: OK
incr connections: 11
incr connections: 12
decr connections: 11
connections: 11
del connections: 1
incr connections: 1
connections: 1
--- no_error_log
[error]
--- SKIP



=== TEST 6: bad incr command format
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local redis = require "resty.redis"
            local red = redis:new()

            red:set_timeout(1000) -- 1 sec

            local ok, err = red:connect("127.0.0.1", $TEST_NGINX_REDIS_PORT)
            if not ok then
                ngx.say("failed to connect: ", err)
                return
            end

            local res, err = red:incr("connections", 12)
            if not res then
                ngx.say("failed to set connections: ", res, ": ", err)
                return
            end

            ngx.say("incr connections: ", res)

            red:close()
        ';
    }
--- request
GET /t
--- response_body
failed to set connections: false: ERR wrong number of arguments for 'incr' command
--- no_error_log
[error]
--- SKIP



=== TEST 7: lpush and lrange
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local redis = require "resty.redis"
            local red = redis:new()

            red:set_timeout(1000) -- 1 sec

            local ok, err = red:connect("127.0.0.1", $TEST_NGINX_REDIS_PORT)
            if not ok then
                ngx.say("failed to connect: ", err)
                return
            end

            local res, err = red:flushall()
            if not res then
                ngx.say("failed to flushall: ", err)
                return
            end
            ngx.say("flushall: ", res)

            local res, err = red:lpush("mylist", "world")
            if not res then
                ngx.say("failed to lpush: ", err)
                return
            end
            ngx.say("lpush result: ", res)

            res, err = red:lpush("mylist", "hello")
            if not res then
                ngx.say("failed to lpush: ", err)
                return
            end
            ngx.say("lpush result: ", res)

            res, err = red:lrange("mylist", 0, -1)
            if not res then
                ngx.say("failed to lrange: ", err)
                return
            end
            local cjson = require "cjson"
            ngx.say("lrange result: ", cjson.encode(res))

            red:close()
        ';
    }
--- request
GET /t
--- response_body
flushall: OK
lpush result: 1
lpush result: 2
lrange result: ["hello","world"]
--- no_error_log
[error]
--- SKIP



=== TEST 8: blpop expires its own timeout
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local redis = require "resty.redis"
            local red = redis:new()

            red:set_timeout(2500) -- 2.5 sec

            local ok, err = red:connect("127.0.0.1", $TEST_NGINX_REDIS_PORT)
            if not ok then
                ngx.say("failed to connect: ", err)
                return
            end

            local res, err = red:flushall()
            if not res then
                ngx.say("failed to flushall: ", err)
                return
            end
            ngx.say("flushall: ", res)

            local res, err = red:blpop("key", 1)
            if err then
                ngx.say("failed to blpop: ", err)
                return
            end

            if res == ngx.null then
                ngx.say("no element popped.")
                return
            end

            local cjson = require "cjson"
            ngx.say("blpop result: ", cjson.encode(res))

            red:close()
        ';
    }
--- request
GET /t
--- response_body
flushall: OK
no element popped.
--- no_error_log
[error]
--- timeout: 3
--- SKIP



=== TEST 9: blpop expires cosocket timeout
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local redis = require "resty.redis"
            local red = redis:new()

            local ok, err = red:connect("127.0.0.1", $TEST_NGINX_REDIS_PORT)
            if not ok then
                ngx.say("failed to connect: ", err)
                return
            end

            local res, err = red:flushall()
            if not res then
                ngx.say("failed to flushall: ", err)
                return
            end
            ngx.say("flushall: ", res)

            red:set_timeout(200) -- 200 ms

            local res, err = red:blpop("key", 1)
            if err then
                ngx.say("failed to blpop: ", err)
                return
            end

            if not res then
                ngx.say("no element popped.")
                return
            end

            local cjson = require "cjson"
            ngx.say("blpop result: ", cjson.encode(res))

            red:close()
        ';
    }
--- request
GET /t
--- response_body
flushall: OK
failed to blpop: timeout
--- error_log
lua tcp socket read timed out
--- SKIP


=== TEST 10: set keepalive and get reused times
--- http_config eval: $::HttpConfig
--- config
    resolver $TEST_NGINX_RESOLVER;
    location /t {
        content_by_lua '
            local redis = require "resty.redis"
            local red = redis:new()

            red:set_timeout(1000) -- 1 sec

            local ok, err = red:connect("127.0.0.1", $TEST_NGINX_REDIS_PORT)
            if not ok then
                ngx.say("failed to connect: ", err)
                return
            end

            local times = red:get_reused_times()
            ngx.say("reused times: ", times)

            local ok, err = red:set_keepalive()
            if not ok then
                ngx.say("failed to set keepalive: ", err)
                return
            end

            ok, err = red:connect("127.0.0.1", $TEST_NGINX_REDIS_PORT)
            if not ok then
                ngx.say("failed to connect: ", err)
                return
            end

            times = red:get_reused_times()
            ngx.say("reused times: ", times)
        ';
    }
--- request
GET /t
--- response_body
reused times: 0
reused times: 1
--- no_error_log
[error]
--- SKIP





=== TEST 11: mget
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local redis = require "resty.redis"
            local red = redis:new()

            red:set_timeout(1000) -- 1 sec

            local ok, err = red:connect("127.0.0.1", $TEST_NGINX_REDIS_PORT)
            if not ok then
                ngx.say("failed to connect: ", err)
                return
            end

            ok, err = red:flushall()
            if not ok then
                ngx.say("failed to flush all: ", err)
                return
            end

            local res, err = red:set("dog", "an animal")
            if not res then
                ngx.say("failed to set dog: ", err)
                return
            end

            ngx.say("set dog: ", res)

            for i = 1, 2 do
                local res, err = red:mget("dog", "cat", "dog")
                if err then
                    ngx.say("failed to get dog: ", err)
                    return
                end

                if not res then
                    ngx.say("dog not found.")
                    return
                end

                local cjson = require "cjson"
                ngx.say("res: ", cjson.encode(res))
            end

            red:close()
        ';
    }
--- request
GET /t
--- response_body
set dog: OK
res: ["an animal",null,"an animal"]
res: ["an animal",null,"an animal"]
--- no_error_log
[error]
--- SKIP


=== TEST 12: hmget array_to_hash
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local redis = require "resty.redis"
            local red = redis:new()

            red:set_timeout(1000) -- 1 sec

            local ok, err = red:connect("127.0.0.1", $TEST_NGINX_REDIS_PORT)
            if not ok then
                ngx.say("failed to connect: ", err)
                return
            end

            ok, err = red:flushall()
            if not ok then
                ngx.say("failed to flush all: ", err)
                return
            end

            local res, err = red:hmset("animals", { dog = "bark", cat = "meow", cow = "moo" })
            if not res then
                ngx.say("failed to set animals: ", err)
                return
            end

            ngx.say("hmset animals: ", res)

            local res, err = red:hmget("animals", "dog", "cat", "cow")
            if not res then
                ngx.say("failed to get animals: ", err)
                return
            end

            ngx.say("hmget animals: ", res)

            local res, err = red:hgetall("animals")
            if err then
                ngx.say("failed to get animals: ", err)
                return
            end

            if not res then
                ngx.say("animals not found.")
                return
            end

            local h = red:array_to_hash(res)

            ngx.say("dog: ", h.dog)
            ngx.say("cat: ", h.cat)
            ngx.say("cow: ", h.cow)

            red:close()
        ';
    }
--- request
GET /t
--- response_body
hmset animals: OK
hmget animals: barkmeowmoo
dog: bark
cat: meow
cow: moo
--- no_error_log
[error]
--- SKIP



=== TEST 13: boolean args
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local redis = require "resty.redis"
            local red = redis:new()

            red:set_timeout(1000) -- 1 sec

            local ok, err = red:connect("127.0.0.1", $TEST_NGINX_REDIS_PORT)
            if not ok then
                ngx.say("failed to connect: ", err)
                return
            end

            ok, err = red:set("foo", true)
            if not ok then
                ngx.say("failed to set: ", err)
                return
            end

            local res, err = red:get("foo")
            if not res then
                ngx.say("failed to get: ", err)
                return
            end

            ngx.say("foo: ", res, ", type: ", type(res))

            ok, err = red:set("foo", false)
            if not ok then
                ngx.say("failed to set: ", err)
                return
            end

            local res, err = red:get("foo")
            if not res then
                ngx.say("failed to get: ", err)
                return
            end

            ngx.say("foo: ", res, ", type: ", type(res))

            ok, err = red:set("foo", nil)
            if not ok then
                ngx.say("failed to set: ", err)
            end

            local res, err = red:get("foo")
            if not res then
                ngx.say("failed to get: ", err)
                return
            end

            ngx.say("foo: ", res, ", type: ", type(res))

            local ok, err = red:set_keepalive(10, 10)
            if not ok then
                ngx.say("failed to set_keepalive: ", err)
            end
        ';
    }
--- request
GET /t
--- response_body
foo: true, type: string
foo: false, type: string
failed to set: ERR wrong number of arguments for 'set' command
foo: false, type: string
--- no_error_log
[error]
--- SKIP


=== TEST 14: set and get (key with underscores)
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local redis = require "resty.redis"
            local red = redis:new()

            red:set_timeout(1000) -- 1 sec

            local ok, err = red:connect("127.0.0.1", $TEST_NGINX_REDIS_PORT)
            if not ok then
                ngx.say("failed to connect: ", err)
                return
            end

            local res, err = red:set("a_dog", "an animal")
            if not res then
                ngx.say("failed to set a_dog: ", err)
                return
            end

            ngx.say("set a_dog: ", res)

            for i = 1, 2 do
                local res, err = red:get("a_dog")
                if err then
                    ngx.say("failed to get a_dog: ", err)
                    return
                end

                if not res then
                    ngx.say("a_dog not found.")
                    return
                end

                ngx.say("a_dog: ", res)
            end

            red:close()
        ';
    }
--- request
GET /t
--- response_body
set a_dog: OK
a_dog: an animal
a_dog: an animal
--- no_error_log
[error]
--- SKIP
=== TEST 15: increase key
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
      local inspect = require "inspect" 
    
    local ok,cluster = pcall(require, "redis.cluster")
    if not ok or not cluster then
        ngx.say("failed to load redis.cluster. err="..(cluster or "nil"))
        return
    end
    
    local c=cluster:new("192.168.122.33:30001","192.168.122.33:30002","192.168.122.33:30003",
                        "192.168.122.33:30004","192.168.122.33:30005","192.168.122.33:30006")
    local res, err = c:do_cmd_quick("del", "foobar")
    if not res then
        ngx.say("ERROR: do_cmd_quick_Test_Incr 1")
        return false
    end

    local steps = 12345
    for i = 1, steps do
        local res, err = c:do_cmd_quick("incr", "foobar")
        if not res then
            ngx.say("ERROR: failed to incr foobar. err="..(err or "nil"))
            return false
        end
    end

    local res, err = c:do_cmd_quick("get", "foobar")
    if not res then
        ngx.say("ERROR: failed to get foobar after incr. err="..(err or "nil"))
        return false
    else
        if not res == steps then
            ngx.say("ERROR: incr failed, expected="..(steps or "nil")..", but actual="..(res or "nil"))
            return false
        end
    end
    ngx.say("get foobar: ", res)
    local res, err = c:do_cmd_quick("del", "foobar")
    if not res then
        ngx.say("ERROR: do_cmd_quick_Test_Incr 2")
        return false
    end
        }
    }
--- request
GET /t
--- response_body
get foobar: 12345
--- no_error_log
[error]
--- timeout: 6s
