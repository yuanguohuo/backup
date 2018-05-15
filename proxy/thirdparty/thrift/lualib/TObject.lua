--By Yuanguo
--06/09/2016

local ok, new_tab = pcall(require, "table.new")
if not ok or type(new_tab) ~= "function" then
    new_tab = function (narr, nrec) return {} end
end

local ok, Tool = pcall(require, "thrift.lualib.TTool")
if not ok or not Tool then
    error("failed to load thrift.lualib.TTool:" .. (Tool or "nil"))
end

local ttype = Tool.ttype
local terror = Tool.terror
local thrift_print_r = Tool.thrift_print_r

local _M = new_tab(0, 155)
_M._VERSION = '0.1'
_M.__type = 'TObject'
_M.__parent = nil

local function __tobj_index(self, key)
    local v = rawget(self, key)
    if nil ~= v then 
        return v 
    end
    local parent = rawget(self, "__parent")
    if parent then
        return __tobj_index(parent, key)
    end
    return nil
end

function _M.new(self, init_obj)
    local obj = {}
    if type(init_obj) == 'table' then
        obj = init_obj
    end

    obj.__parent = self
    setmetatable(obj, {__index=__tobj_index})

    return obj
end

return _M
