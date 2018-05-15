--By Yuanguo: 16/8/2016

local ok, new_tab = pcall(require, "table.new")
if not ok or type(new_tab) ~= "function" then
    new_tab = function (narr, nrec) return {} end
end

local _M = new_tab(0,155)
_M._VERSION = '0.1'


local function val2str(val, result, indent, skipLongStr, threshold, newline)
    local i = #result
    local t = type(val)

    if "nil" == t or "boolean" == t or "number" == t then
        result[i+1] = tostring(val)
    elseif "function" == t or "thread" == t or "userdata" == t then
        result[i+1] = tostring(val) 
    elseif "string" == t then
        local len = #val
        if skipLongStr and len > threshold then
            result[i+1] = "[a string of " .. tostring(len) .. " bytes, too long to print ...]"
        else
            result[i+1] = val
        end
    elseif "table" == t then
        result[i+1] = "{\n"
        i = #result
        local newindent = indent .. "    "

        --for beauty, we save the table members and print them 
        --at last
        local table_members = {}
        for k,v in pairs(val) do
            if "table" == type(v) then
                table_members[k] = v
            else
                result[i+1] = newindent .. "[" .. tostring(k) .. "]="
                val2str(v, result, newindent, skipLongStr, threshold, true) -- value
                i = #result
            end
        end

        --print the table members
        for k,v in pairs(table_members) do
            result[i+1] = newindent .. "[" .. tostring(k) .. "]="
            val2str(v, result, newindent, skipLongStr, threshold, true) -- value
            i = #result
        end

        result[i+1] = indent .. "}"
    else 
        assert(false)
    end

    if newline then  -- in a table, we need a '\n' for every key
        result[i+2] = "\n"
    end
end

function _M.stringify(value, skip_long_string, long_threshold)
    local skipLongStr = true -- skip long string by default
    local threshold = 300    -- a string is deemed as "long" if it's longer than 400 by default

    if skip_long_string == false then
        skipLongStr = false
    end
    if long_threshold then
        threshold = tonumber(long_threshold)
    end

    local result = new_tab(256,0)
    val2str(value, result, "", skipLongStr, threshold)
    return table.concat(result)
end

return _M
