local ok, hbase = pcall(require, "metadb_api.hbase.thrift.hbase")
if not ok or not hbase then
    error("failed to load metadb_api.hbase.thrift.hbase:" .. (hbase or "nil"))
end

local ok, stor_conf = pcall(require, "storageproxy_conf")
if not ok or not stor_conf then
    error("failed to load storageproxy_conf:" .. (stor_conf or "nil"))
end

local instance = hbase:new(stor_conf.config["hbase_config"]["server"], stor_conf.config["hbase_config"]["port"])

return instance
