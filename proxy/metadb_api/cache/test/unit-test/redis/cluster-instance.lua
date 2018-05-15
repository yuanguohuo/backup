--Yuanguo: 12/4/2017

local ok,cluster = pcall(require, "redis.cluster")
if not ok or not cluster then
    error("failed to load redis.cluster: "..(cluster or "nil"))
end

local instance = cluster:new("127.0.0.1:7000")

return instance
