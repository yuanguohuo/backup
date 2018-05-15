use lib '/root/atest/openresty/proxy/test/';
package t::MYTEST;
#package Test::Nginx::Socket::MYTEST;

use Test::Nginx::Socket -Base;

add_block_preprocessor(sub {
    my $block = shift;

    if (!defined $block->config) {
        $block->set_value("config", <<'_END_');
     server_name  _;  
 
     client_body_buffer_size 512m;
 
     lua_socket_read_timeout 240s;
     lua_socket_send_timeout 240s;
     lua_socket_connect_timeout 240s;
     set $httph 'http://';
           
     set $urll "${httph}${server_addr}:${server_port}${request_uri}";

    access_log  logs/access.log  s3;
 
     location / { 
         default_type 'text/html';  
         lua_code_cache on; 
         client_max_body_size 40g;
         set $rangestr '';
         rewrite_by_lua_file /root/atest/openresty/proxy/storageproxy_rewrite.lua;
         set $userid '-';
 #        content_by_lua_file /root/atest/openresty/proxy/storageproxy.lua;
         content_by_lua_file /root/atest/openresty/proxy/proxyportal.lua;
     }   
_END_
    }
    if (!defined $block->main_config) {
        $block->set_value("main_config", <<'_END_');
_END_
   }
    if (!defined $block->http_config) {
        $block->set_value("http_config", <<'_END_');
    log_format s3  '$remote_addr $msec $server_protocol - $status - ' 
                   '$body_bytes_sent $request_method $urll - '
                   '- - - $server_addr $content_type "$http_referer" '
                   '"$http_user_agent" $request_time $userid - - - $cookie_COOKIE - - - - - '
                   '$remote_port - - - - - - -';
    sendfile        on;

    lua_shared_dict statics_dict    10M; 
     #listen 80 backlog=16384;
     client_max_body_size 40g;
     client_body_buffer_size 512m;
    lua_package_path    "/root/atest/openresty/proxy/?.lua;/root/atest/openresty/proxy/stats/?.lua;/root/atest/openresty/proxy/test/t/clientlib/?.lua;/root/atest/openresty/proxy/thirdparty/?.lua;/root/atest/openresty/proxy/thirdparty/thrift/lualib/?.lua;/root/atest/openresty/proxy/thirdparty/thrift/gen-lua/?.lua;/root/atest/openresty/proxy/ostore_api/librados/?.lua;/root/atest/openresty/proxy/?.lua;/root/atest/openresty/proxy/ostore_api/rgw/?.lua;/root/atest/openresty/proxy/common/?.lua;/root/atest/openresty/proxy/metadb_api/hbase/?.lua;/root/atest/openresty/proxy/metadb_api/?.lua;/root/atest/openresty/proxy/cache_api/?.lua;;";
    lua_package_cpath    "/root/atest/openresty/proxy/?.so;/root/atest/openresty/proxy/thirdparty/?.so;/usr/local/thrift10lua51open/lib/?.so;/usr/local/thrift10lua51open/lib/?.so.0;/usr/local/openresty/nginx/proxy/thirdparty/thrift/lib/?.so;;";
     lua_shared_dict     cephvars    1M;


    init_by_lua '
         require("jit.opt").start("minstitch=10000000")

        require "resty.core"
               collectgarbage("setpause", 100)
    ';
   init_worker_by_lua_block {
        local uuid = require 'resty.jit-uuid'
        uuid.seed() -- very important!
    }
    log_by_lua_file "/root/atest/openresty/proxy/stats/hook.lua";
    include /root/atest/openresty/proxy/test/t/proxy.conf;

_END_
   }
});
1;
