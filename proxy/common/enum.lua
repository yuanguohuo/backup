local _M = {}
function _M.CreatEnumTable(tbl, index) 
    assert("table" == type(tbl)) 
    local enumtbl = {} 
    local enumindex = index or 0 
    for i, v in ipairs(tbl) do 
        enumtbl[v] = enumindex + i 
    end 
    return enumtbl 
end
return _M
