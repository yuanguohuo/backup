-- By Yuanguo, 21/7/2016

local ok, new_tab = pcall(require, "table.new")
if not ok or type(new_tab) ~= "function" then
    new_tab = function (narr, nrec) return {} end
end

local ok, utils = pcall(require, "common.utils")
if not ok or not utils then
    error("failed to load common.utils:" .. (utils or "nil"))
end

local NULL = "NULL"

local _M = new_tab(0, 9)
_M._VERSION = '0.1'

function _M.new(self, ...)
    return setmetatable(
               {cluster_id = "Redis-Cluster", nodemap = "nodemap", nodenum = 12, slotmap = "slotmap", needrefresh = false, errnum2refresh = 300, errnum = 3},
               {__index = self}
           )
end

function _M.refresh(self)
    return true, "SUCCESS"
end

function _M.set_errnum2refresh(self, num)
    print("set_errnum2refresh:", num)
end

function _M.create_terminal(self,key)
    return nil
end

function _M.do_cmd(self, term, ...)
    print("do_cmd:", ...)
end

function _M.do_cmd_quick(self, ...)
    print("do_cmd_quick:", ...)

    local args = {...}
    local cmd = args[1]

    if cmd == "get" then
        local key = args[2]

        local ret = nil
        ret = 200000000

        print(ret)
        return ret 
    elseif cmd == "hmget" then
        local key = args[2]

        local fields = {}
        for  i=3,#args do
            table.insert(fields,args[i])
        end

        print("fields:",utils.stringify(fields)) 

        local result = {}

        for i=1,#fields-1 do   --hit
            local val = "Redis Value of hash " .. key .. " field " .. fields[i]
            table.insert(result, val)
        end

        table.insert(result, NULL)  --last one, missed

        print("result:",utils.stringify(result)) 
        return result,nil
    elseif cmd == "hmset" then
        local key = args[2]

        local vals = {}
        for  i=3,#args do
            table.insert(vals,args[i])
        end

        print("hmset vals:",utils.stringify(vals)) 
        return "OK", nil
    elseif cmd == "mget" then
        local keys = {}
        for  i=2,#args do
            table.insert(keys,args[i])
        end

        print("keys:",utils.stringify(keys)) 

        local result = {
            NULL,
            NULL,

            3000,
            NULL,

            NULL,
            40,

            3000,
            456,
        }

        print("result:",utils.stringify(result)) 

        return result,nil
    elseif cmd == "mset" then
        local vals = {}
        for  i=2,#args do
            table.insert(vals,args[i])
        end

        print("mset vals:",utils.stringify(vals)) 
        return "OK", nil
    elseif cmd == "expire" then
        return "OK", nil
    elseif cmd == "incrby" then
        local key = args[2]
        local val = args[3]

        local current = val * 20
        print("current", current)
        return current,nil
    elseif cmd == "getset" then
        local key = args[2]
        local val = args[3]

        local current = 234567  
        print("current", current)
        return current,nil
    elseif cmd == "del" then
        local keys_to_del = {}
        for  i=2,#args do
            table.insert(keys_to_del,args[i])
        end

        print("Delete keys from redis: " .. utils.stringify(keys_to_del))
        return 0, nil 
    elseif cmd == "hdel" then
        local hkey = args[2]
        local fields_to_del = {}
        for  i=3,#args do
            table.insert(fields_to_del,args[i])
        end

        print("Delete fields of " .. hkey .. " from redis: " .. utils.stringify(fields_to_del))
        return 0,nil
    end

    return nil, nil
end

function _M.eval(self, script, num, ...)
    print("eval:", script, num, ...)
end

function _M.evalsha(self, script, sha, num, ...)
    print("eval:", script, sha, num, ...)
end

return _M
