local ok,c1 = pcall(require, "redis.cluster-instance")
if not ok or not c1 then
    ngx.say("failed to load redis.cluster-instance. err="..(c1 or "nil"))
    return
end

local ok,err = c1:refresh()
if not ok then
    ngx.say("failed to refresh cluster. err="..(err or "nil"))
    return
end

local count = ngx.var.arg_c or 12345
for i = 1, count do
    local res, err = c1:do_cmd_quick("incr", "foobar")
    if not res then
        ngx.say("ERROR: failed to incr foobar. err="..(err or "nil"))
    end
end
