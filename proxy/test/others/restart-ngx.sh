NGX_HOME=/usr/local/openresty-1.9.15.1/nginx

$NGX_HOME/sbin/nginx -s stop

usleep 500000

ps -ef | grep nginx | grep -v grep | grep -v error.log 

echo -------------------- 

$NGX_HOME/sbin/nginx

usleep 500000

ps -ef | grep nginx | grep -v grep | grep -v error.log
