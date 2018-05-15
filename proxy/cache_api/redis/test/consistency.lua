--Yuanguo: 26/7/2016
--You can crash a master node when you are running this test case. When you
--crash the master node, error num will increase. However, after the cluster
--refreshes the slotmap, the error num should stop increasing.

local ok, new_tab = pcall(require, "table.new")
if not ok or type(new_tab) ~= "function" then
    new_tab = function (narr, nrec) return {} end
end

local ok,c1 = pcall(require, "redis.cluster-instance")
if not ok or not c1 then
    ngx.say("failed to load redis.cluster-instance. err="..(c1 or "nil"))
    return
end

c1:set_errnum2refresh(400)

local KEY_SPACE = 500

local reads = 0
local writes = 0
local rfails = 0
local wfails = 0
local wlosts = 0
local wnoack = 0

local cache = new_tab(0, KEY_SPACE)
local stamp = os.time()
local prefix = "key-"..stamp.."-"

local round = ngx.var.arg_round or 200
--local delay = ngx.var.arg_delay or 1000
local batch = ngx.var.arg_batch or 5000

local function genkey()
    math.randomseed(tostring(os.time()):reverse():sub(1,6))
    local r = math.random(1,KEY_SPACE)
    return prefix..r
end

for i = 1, KEY_SPACE do
    local key = prefix..i
    cache[key] = 0
    local ok,err = c1:do_cmd_quick("set", key, 0)
    if not ok then
        ngx.say("initialization, failed to set "..key.." to 0. err=" .. (err or "nil"))
        return 
    end
end

ngx.say("prefix="..prefix)
ngx.say("round="..round)
--ngx.say("delay="..delay)
ngx.say("batch="..batch)

for i = 1, round do

    for j = 1, batch do
        local key = genkey()
    
        local res,err = c1:do_cmd_quick("get",key)
        if not res then
            rfails = rfails+1
        elseif type(res) ~= "userdata" then
            local expected = cache[key]
            if tonumber(expected) > tonumber(res) then       -- acked, but the write was lost
                wlosts = wlosts + (tonumber(expected) - tonumber(res))
            elseif tonumber(expected) < tonumber(res) then   -- didn't ack, but the write was done
                wnoack = wnoack + (tonumber(res) - tonumber(expected))
            else
                -- expected equals to res
            end
        end
        reads = reads + 1
    
        local res,err = c1:do_cmd_quick("incr", key)
        if not res then
            wfails = wfails + 1
        else
            cache[key] = res
        end
        writes = writes + 1
    end

    ngx.say("\r\n======================")
    ngx.say("Num Read       :  " .. reads)
    ngx.say("Num Write      :  " .. writes)
    ngx.say("Read Fail      :  " .. rfails)
    ngx.say("Write Fail     :  " .. wfails)
    ngx.say("Lost Write     :  " .. wlosts)
    ngx.say("Un-acked Write :  " .. wnoack)
    ngx.flush()
end
