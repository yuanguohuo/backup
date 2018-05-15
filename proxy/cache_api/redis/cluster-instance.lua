--Yuanguo: 12/4/2017

local ok,cluster = pcall(require, "redis.cluster")
if not ok or not cluster then
    error("failed to load redis.cluster: "..(cluster or "nil"))
end

local ok,stor_conf = pcall(require, "storageproxy_conf")
if not ok or not stor_conf then
    error("failed to load storageproxy_conf: "..(stor_conf or "nil"))
end

local instance = cluster:new(unpack(stor_conf.config.redis_config.servers))

return instance
