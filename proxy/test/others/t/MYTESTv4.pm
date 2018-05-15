package t::MYTESTv4;
#package Test::Nginx::Socket::MYTEST;

use Test::Nginx::Socket -Base;

add_block_preprocessor(sub {
    my $block = shift;

#    if (!defined $block->config) {
#        $block->set_value("config", <<'_END_');
#    listen 8081 so_keepalive=2s:2s:8;
#    keepalive_timeout 120;
#    location / {
#	 #set $portal 1;
#	 lua_check_client_abort on;
#        client_max_body_size 10000000k;
#        client_body_buffer_size 100000k;
#        default_type text/plain;
#	lua_code_cache on;
##        content_by_lua_block {
##	ngx.print(ngx.req.raw_header())
##	ngx.req.read_body()
##	ngx.print(ngx.req.get_body_data() or "")
##	}
#        content_by_lua_file "/home/deploy/dobjstor/proxy/storageproxy.lua";
#        body_filter_by_lua_file "/home/deploy/dobjstor/proxy/bodyfilter.lua";
#    }
#_END_
#    }
    if (!defined $block->main_config) {
        $block->set_value("main_config", <<'_END_');
        worker_rlimit_nofile 102400;
        user root;
        worker_rlimit_core  5000M;
        working_directory   /root/atest/;
_END_
   }
    if (!defined $block->http_config) {
        $block->set_value("http_config", <<'_END_');
#        include mime.types;
#        default_type application/octet-stream;
        sendfile on;
#   lua_code_cache off;  
 #keepalive_timeout 65;

	 lua_check_client_abort on;
    lua_package_path    "/root/atest/openresty/proxy/?.lua;/root/atest/openresty/proxy/test/t/clientlib/?.lua;/root/atest/openresty/proxy/thirdparty/?.lua;/root/atest/openresty/proxy/thirdparty/thrift/lualib/?.lua;/root/atest/openresty/proxy/thirdparty/thrift/gen-lua/?.lua;/root/atest/openresty/proxy/ostore_api/librados/?.lua;/root/atest/openresty/proxy/?.lua;/root/atest/openresty/proxy/ostore_api/rgw/?.lua;/root/atest/openresty/proxy/common/?.lua;/root/atest/openresty/proxy/metadb_api/hbase/?.lua;/root/atest/openresty/proxy/metadb_api/?.lua;/root/atest/openresty/proxy/cache_api/?.lua;;";
    # lua_package_cpath    "/root/atest/openresty/proxy/?.so;/root/atest/openresty/proxy/thirdparty/?.so;/root/atest/openresty/proxy/thirdparty/thrift/lib/?.so;/root/atest/openresty/proxy/thirdparty/thrift/lib/?.so.0;;";
    lua_package_cpath    "/root/atest/openresty/proxy/?.so;/root/atest/openresty/proxy/thirdparty/?.so;/usr/local/thrift10lua51open/lib/?.so;/usr/local/thrift10lua51open/lib/?.so.0;;";
     lua_shared_dict     cephvars    1M;


    init_by_lua '
--        local v = require "jit.v"
         require("jit.opt").start("minstitch=10000000")
--        jit.off() 

--       v.on("$Test::Nginx::Util::ErrLogFile")
        require "resty.core"
               collectgarbage("setpause", 100)
            --    collectgarbage("setpause", 50)
            --    collectgarbage("setstepmul", 90)
    ';

#    init_by_lua_file "/root/atest/openresty/proxy/radosload.lua";
   init_worker_by_lua_block {
        local uuid = require 'resty.jit-uuid'
        uuid.seed() -- very important!
    }
_END_
   }
});
1;
