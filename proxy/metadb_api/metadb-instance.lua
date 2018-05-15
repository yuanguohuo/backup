local ok, metadb = pcall(require, "metadb_api.cache.lcache")
if not ok or not metadb then
    error("failed to load metadb_api.cache.lcache:" .. (metadb or "nil"))
end

local instance = metadb:new()

return instance
