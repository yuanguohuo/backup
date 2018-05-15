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
