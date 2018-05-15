require("jit.opt").start("minstitch=10000000")
require "resty.core"
collectgarbage("setpause", 100)

local sp_conf = require("storageproxy_conf")
if sp_conf.config["luacov"] then
    local ok, runner = pcall(require, "luacov.runner")
    if not ok or not runner then
        error("failed to load luaconv:")
    end
    runner.init("/usr/local/stor-openresty/nginx/proxy/.luacov")
    jit.off() 
end

