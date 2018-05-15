local ok, new_tab = pcall(require, "table.new")
if not ok or type(new_tab) ~= "function" then
    new_tab = function (narr, nrec) return {} end
end

local _M = new_tab(0, 9)
_M._VERSION = '0.1'

local stub_cache = {
    ["user_123"] = 
    {
        ["info:info1"] = "LCache value for info:info1",
    },
}

function _M.new(items)
    print("Create resty.lrucache, items="..tostring(items))
    local newobj = new_tab(0,7)
    setmetatable(newobj, {__index=_M})
    return newobj
end

function _M.get(self, key)
    return stub_cache[key]
end

function _M.set(self, key, tab, exp)
    local tt = stub_cache[key]
    if nil == tt then
        tt = new_tab(0,6)
        stub_cache[key] = tt
    end

    for k,v in pairs(tab) do
        print("set row "..key.." "..k.."="..v)
        tt[k] = v
    end
end


function _M.delete(self, key)
    stub_cache[key] = nil
    return true
end

return _M
