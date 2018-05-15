--By Yuanguo
--06/09/2016

local ok, new_tab = pcall(require, "table.new")
if not ok or type(new_tab) ~= "function" then
    new_tab = function (narr, nrec) return {} end
end

local _M = new_tab(0, 155)
_M._VERSION = '0.1'

_M.STOP   = 0
_M.VOID   = 1
_M.BOOL   = 2
_M.BYTE   = 3
_M.I08    = 3
_M.DOUBLE = 4
_M.I16    = 6
_M.I32    = 8
_M.I64    = 10
_M.STRING = 11
_M.UTF7   = 11
_M.STRUCT = 12
_M.MAP    = 13
_M.SET    = 14
_M.LIST   = 15
_M.UTF8   = 16
_M.UTF16  = 17

return _M
