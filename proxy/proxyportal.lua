local ok, new_tab = pcall(require, "table.new")
if not ok or type(new_tab) ~= "function" then
    new_tab = function (narr, nrec) return {} end
end

local ok,handler = pcall(require, "storageproxy_handle")
if not ok or not handler then
    error("failed to load storageproxy_handle:" .. (tostring(handler) or "nil"))
end

local ok,sp_conf = pcall(require, "storageproxy_conf")
if not ok or not sp_conf then
    error("failed to load storageproxy_conf:" .. (sp_conf or "nil"))
end

local trace_time = sp_conf.config["trace_time"]
if trace_time then
    ngx.ctx.trace_events = new_tab(128, 0)
end

if nil == ngx.var.uri or "" == ngx.var.uri then
    handler.handleStoreProxy("/")
else
    handler.handleStoreProxy(ngx.var.uri)
end
