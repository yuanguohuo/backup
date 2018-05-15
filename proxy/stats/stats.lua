local ok, new_tab = pcall(require, "table.new")
if not ok or type(new_tab) ~= "function" then
    new_tab = function (narr, nrec) return {} end 
end

local ok, spconf = pcall(require, "storageproxy_conf")
if not ok or not spconf then
    error("failed to load storageproxy_conf:" .. (spconf or "nil"))
end

local _M = new_tab(0,155)
_M._VERSION = '0.1'

function _M.update(self, key)
    if spconf.config["STATS"] then
        if not ngx.ctx[key] then
            ngx.ctx[key] = 1
        else
            ngx.ctx[key] = ngx.ctx[key] + 1
        end
    end
end

return _M
