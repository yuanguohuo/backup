use Test::Nginx::Socket 'no_plan'; 
no_long_string();
log_level 'debug';
no_root_location();

no_shuffle();
run_tests();
__DATA__

=== TEST 1: listbuckets http://ip close 
--- main_config
--- http_config

--- config 
    location  /t {
         content_by_lua_block {
	 ngx.header.content_length = 12
	 ngx.say("hello")
	 ngx.say("world")
	 }
--- request
GET /t
--- response_body_like
world
--- error_code: 200
--- timeout: 6
--- error_log
[error]
--- abort
#--- SKIP
#--- ONLY

