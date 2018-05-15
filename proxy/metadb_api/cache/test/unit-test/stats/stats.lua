local ok, new_tab = pcall(require, "table.new")
if not ok or type(new_tab) ~= "function" then
    new_tab = function (narr, nrec) return {} end 
end

local _M = new_tab(0,5)
_M._VERSION = '0.1'

function _M.update(self, key)
    print("Miss/Hit: " .. key)
end

return _M
