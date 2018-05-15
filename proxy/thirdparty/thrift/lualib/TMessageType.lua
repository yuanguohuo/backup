--By Yuanguo
--06/09/2016

local ok, new_tab = pcall(require, "table.new")
if not ok or type(new_tab) ~= "function" then
    new_tab = function (narr, nrec) return {} end
end

local _M = new_tab(0, 75)
_M._VERSION = '0.1'

_M.CALL      = 1
_M.REPLY     = 2
_M.EXCEPTION = 3
_M.ONEWAY    = 4

return _M
