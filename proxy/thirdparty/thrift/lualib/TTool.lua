--By Yuanguo
--06/09/2016

local ok, new_tab = pcall(require, "table.new")
if not ok or type(new_tab) ~= "function" then
    new_tab = function (narr, nrec) return {} end
end

local _M = new_tab(0, 155)
_M._VERSION = '0.1'

function _M.terror(e)
    if e and e.__tostring then 
        error(e:__tostring())
    else
        error(e)
    end
end

function _M.ttype(obj)
    if type(obj) == 'table' and obj.__type and type(obj.__type) == 'string' then
        return obj.__type
    end
    return type(obj)
end

function _M.thrift_print_r(t)
    local ret = ''
    local ltype = type(t)
    if (ltype == 'table') then
        ret = ret .. '{ '
        for key,value in pairs(t) do
            ret = ret .. tostring(key) .. '=' .. _M.thrift_print_r(value) .. ' '
        end
        ret = ret .. '}'
    elseif ltype == 'string' then
        ret = ret .. "'" .. tostring(t) .. "'"
    else
        ret = ret .. tostring(t)
    end
    return ret
end

return _M
