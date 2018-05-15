local ok, new_tab = pcall(require, "table.new")
if not ok or type(new_tab) ~= "function" then
    new_tab = function (narr, nrec) return {} end 
end

local ok,spconf = pcall(require, "storageproxy_conf")
if not ok or not spconf then
    error("failed to load storageproxy_conf:" .. (spconf or "nil"))
end

local _M = new_tab(0,155)
_M._VERSION = '0.1'

local sync_cfg = spconf.config["sync_config"]
local DATALOG_NUM = sync_cfg["datalog"]["num"]
local datalog_arry = {}
function _M.get_datalog_md5(id)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", "get datalog md5 id:", id)
    if next(datalog_arry) == nil then
        for i=1, DATALOG_NUM do
            datalog_arry[i] = string.sub(ngx.md5("datalog_"..i),1,10)
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", "get datalog md5 i:", i, " value:", datalog_arry[i])
        end
    end
    
    if id >= 1 and id <= DATALOG_NUM then
        return datalog_arry[id]   
    else
        assert(false)
    end

    return nil
   -- return string.sub(ngx.md5("datalog_"..id),1,10)
end

return _M

