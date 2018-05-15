-- chen tao


local ok, new_tab = pcall(require, "table.new")
if not ok or type(new_tab) ~= "function" then
    new_tab = function (narr, nrec) return {} end 
end
local dict = ngx.shared.statics_dict

local _M = new_tab(0,155)
_M._VERSION = '0.1'

local function incr(dict, key, increment)
    increment = increment or 1
    local newval, err = dict:incr(key, increment)
    if err then
        dict:set(key, increment)
        newval = increment
    end
    return newval
end

local function format(key, value)
    return string.format("%-28s:\t%d", key, value) 
end

function _M.init_dicts(self)
    local dict = ngx.shared.statics_dict
    dict:set("total_time", 0)
    dict:set("op_count", 0)
    dict:set("op_count_per_5", 0)
    dict:set("op_count_per_1", 0)
    dict:set("op_total_per_5", 0)
    dict:set("op_total_per_1", 0)
end

function _M.update_ops(dict, time)
   local start_time = dict:get("start_time")
   if not start_time then
      dict:set("start_time", ngx.now())
   end

   incr(dict, "total_time", time)
   incr(dict, "op_count")
   incr(dict, "op_count_per_5")
   incr(dict, "op_count_per_1")
end

function _M.analyse_ops(self, dict)
    local elapsed_time = 0
    local avg = 0

    local start_time = dict:get("start_time")
    if start_time then
        elapsed_time = ngx.now() - start_time
    end
    
    ngx.say("----------------ops stats start---------------")
    local count = dict:get("op_count") or 0
    local sum = dict:get("total_time") or 0

    if count > 0 then
        avg = sum / count
    end

    local qps = 0 
    if sum > 0 then
        qps = count*1000 / sum
    end
    
    ngx.say(format("Seconds Since Last", elapsed_time))
    ngx.say(format("Request Count", count))
    ngx.say(format("Total Req Time ms ", sum))
    ngx.say(format("Average Req Time ms", avg))
    ngx.say(format("Requests Per Secs", qps))
    ngx.say("----------------ops stats end-----------------\n")
end

function _M.update_code(dict, code)
   local items = 
   {
       ["code-200"] = true, 
       ["code-204"] = true, 
       ["code-206"] = true, 
       ["code-400"] = true, 
       ["code-403"] = true, 
       ["code-404"] = true, 
       ["code-500"] = true,
   }
   local code_key = "code-"..code
   if items[code_key] then
       incr(dict, code_key)
   else
       ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "no statistical error code=", code)
   end
end

function _M.analyse_code(self, dict)
    local items = {"op_count", "code-200", "code-204", "code-206", "code-400", "code-403", "code-404", "code-500",}

    ngx.say("----------------code stats start--------------")
    local value
    for i, k in pairs(items) do
        value = dict:get(k) or 0
        ngx.say(format(k, value))
    end
    ngx.say("----------------code stats end----------------\n")
end

function _M.update_hitrate(dict)
    local items = {
        "lcache_miss", 
        "lcache_hit", 
        "lcache_back_increment", 
        "lcache_cache_increment", 
        "lcache_back_put", 
        "lcache_cache_put", 
        "lcache_back_checkAndPut", 
        "lcache_cache_checkAndPut", 
        "lcache_back_delete", 
        "lcache_cache_delete", 
        "lcache_back_checkAndDelete", 
        "lcache_cache_checkAndDelete",
        "rcache_miss", 
        "rcache_hit", 
        "rcache_back_increment", 
        "rcache_cache_increment", 
        "rcache_back_put", 
        "rcache_cache_put", 
        "rcache_back_checkAndPut", 
        "rcache_cache_checkAndPut", 
        "rcache_back_delete", 
        "rcache_cache_delete", 
        "rcache_back_checkAndDelete", 
        "rcache_cache_checkAndDelete",
    }
    
    for i, k in pairs(items) do
        incr(dict, k, ngx.ctx[k] or 0)
    end
end

function _M.analyse_hitrate(self, dict)

    local items = {
        "op_count",
        "lcache_miss", 
        "lcache_hit", 
        "lcache_back_increment", 
        "lcache_cache_increment", 
        "lcache_back_put", 
        "lcache_cache_put", 
        "lcache_back_checkAndPut", 
        "lcache_cache_checkAndPut", 
        "lcache_back_delete", 
        "lcache_cache_delete", 
        "lcache_back_checkAndDelete", 
        "lcache_cache_checkAndDelete",
        "rcache_miss", 
        "rcache_hit", 
        "rcache_back_increment", 
        "rcache_cache_increment", 
        "rcache_back_put", 
        "rcache_cache_put", 
        "rcache_back_checkAndPut", 
        "rcache_cache_checkAndPut", 
        "rcache_back_delete", 
        "rcache_cache_delete", 
        "rcache_back_checkAndDelete", 
        "rcache_cache_checkAndDelete",
    }

    ngx.say("----------------hitrate stats start-----------")
    local value
    for i, k in pairs(items) do
        value = dict:get(k) or 0
        ngx.say(format(k, value))
    end
    ngx.say("----------------hitrate stats end------------\n")
end

function _M.update_cycle_workload(self)
    local now = ngx.now()
    --stat for every 5 minutes
    local begin_time_5 = dict:get("begin_time_5")
    if not begin_time_5 then
        dict:set("begin_time_5", now)
        dict:set("op_count_per_5", 0)
    elseif now - begin_time_5 >= 300 then
        local op_count_per_5 = dict:get("op_count_per_5")
        dict:set("op_total_per_5", op_count_per_5)
        dict:set("begin_time_5", ngx.now())
        dict:set("op_count_per_5", 0)
    end

    --stat for every 1 minutes
    local begin_time_1 = dict:get("begin_time_1")
    if not begin_time_1 then
        dict:set("begin_time_1", ngx.now())
        dict:set("op_count_per_1", 0)
    elseif ngx.now() - begin_time_1 >= 60 then
        local op_count_per_1 = dict:get("op_count_per_1")
        dict:set("op_total_per_1", op_count_per_1)
        dict:set("begin_time_1", ngx.now())
        dict:set("op_count_per_1", 0)
    end
end

function _M.reset_cycle_workload(self)
    dict:set("begin_time_5", ngx.now())
    dict:set("op_count_per_5", 0)
    dict:set("op_total_per_5", 0)

    dict:set("begin_time_1", ngx.now())
    dict:set("op_count_per_1", 0)
    dict:set("op_total_per_1", 0)
end

function _M.check_system_busy(self)
    local dict = ngx.shared.statics_dict 
    local op_total_per_5 = dict:get("op_total_per_5")
    local op_total_per_1 = dict:get("op_total_per_1")

    ngx.log(ngx.INFO, "op_total_per_5="..op_total_per_5..", op_total_per_1="..op_total_per_1)
    return (op_total_per_5 > 1000 or op_total_per_1 > 240)
end

function _M.clear(self, dict)
   local ops_items = {"total_time", "op_count", "start_time"}
   for i, k in pairs(ops_items) do
       dict:delete(k)
   end

   local code_items = {"code-200", "code-204", "code-206", "code-400", "code-403", "code-404", "code-500"}

   for i, k in pairs(code_items) do
       dict:delete(k)
   end
   

    local hit_items = {
        "lcache_miss", 
        "lcache_hit", 
        "lcache_back_increment", 
        "lcache_cache_increment", 
        "lcache_back_put", 
        "lcache_cache_put", 
        "lcache_back_checkAndPut", 
        "lcache_cache_checkAndPut", 
        "lcache_back_delete", 
        "lcache_cache_delete", 
        "lcache_back_checkAndDelete", 
        "lcache_cache_checkAndDelete",
        "rcache_miss", 
        "rcache_hit", 
        "rcache_back_increment", 
        "rcache_cache_increment", 
        "rcache_back_put", 
        "rcache_cache_put", 
        "rcache_back_checkAndPut", 
        "rcache_cache_checkAndPut", 
        "rcache_back_delete", 
        "rcache_cache_delete", 
        "rcache_back_checkAndDelete", 
        "rcache_cache_checkAndDelete",
    }

   for i, k in pairs(hit_items) do
       dict:delete(k)
   end

end

function _M.analyse(self, dict, key)
   if not key or "ops" == key then
       self:analyse_ops(dict)
   end

   if not key or "code" == key then
       self:analyse_code(dict)
   end
   
   if not key or "hitrate" == key then
       self:analyse_hitrate(dict)
   end

   self:clear(dict)
   return
end

return _M
