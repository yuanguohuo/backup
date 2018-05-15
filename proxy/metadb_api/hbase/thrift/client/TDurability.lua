--By Yuanguo
--07/09/2016

local ok, new_tab = pcall(require, "table.new")
if not ok or type(new_tab) ~= "function" then
    new_tab = function (narr, nrec) return {} end
end

local _M = new_tab(0, 155)
_M._VERSION = '0.1'

_M.SKIP_WAL  = 1
_M.ASYNC_WAL = 2
_M.SYNC_WAL  = 3
_M.FSYNC_WAL = 4

return _M
