local ok,hbase = pcall(require, "hbase.hbase")
if not ok or not hbase then
    error("failed to load hbase: "..(hbase or "nil"))
end

local instance = hbase:new("127.0.0.1", "8000", "binary", "framed")
print("Create hbase: ", "127.0.0.1", "8000", "binary", "framed")

return instance
