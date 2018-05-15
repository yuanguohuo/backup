local ok, utils = pcall(require, "common.utils")
if not ok or not utils then
    ngx.say("failed to load common.utils:" .. (utils or "nil"))
    return
end

local ok, lock = pcall(require, "common.lock")
if not ok or not lock then
    ngx.say("failed to load common.lock:" .. (lock or "nil"))
    return
end

local me = ngx.var.arg_me

local lock_key = "TestKey001"

----- 1. Lock --------
local ok,ret = lock.lock(lock_key, 10)  --ttl=10 seconds
if nil == ok  then
    ngx.say(utils.timestamp()/1000, " ", me, " Lock: redis API error: ", ret)
    ngx.flush()
    return 
end

while false == ok do  --lock conflict, retry
    local slp = utils.random() * 1000  -- 0~1 seconds

    --ngx.say(utils.timestamp()/1000, " ", me, " Lock failure: ", ret, " sleep ", tostring(slp/1000), " seconds and retry")
    --ngx.flush()
    utils.msleep(slp)

    ok,ret = lock.lock(lock_key, 10)  --ttl=10 seconds
    if nil == ok then
        ngx.say(utils.timestamp()/1000, " ", me, " Lock: redis API error in retry: ", ret)
        ngx.flush()
        return 
    end
end

local sig = ret

ngx.say(utils.timestamp()/1000, " ", me.." Lock success: "..sig.." 10 seconds")
ngx.flush()


----- 2. Sleep for a random period --------
local t = utils.random() * 15000  -- 0~15 seconds
ngx.say(utils.timestamp()/1000, " ", me.." sleep "..tostring(t/1000).." seconds")
ngx.flush()
utils.msleep(t)

----- 3. extend lock for 10 seconds--------
local ok,ret = lock.extend_lock(lock_key, sig, 10)
if nil == ok  then
    ngx.say(utils.timestamp()/1000, " ", me, " Extend_Lock: redis API error: ", ret)
    ngx.flush()
    return 
end
if false == ok then
    ngx.say(utils.timestamp()/1000, " ", me, " Extend_Lock failure: ", ret)
    ngx.flush()
    return 
end

ngx.say(utils.timestamp()/1000, " ", me.." Extend_Lock success: "..sig.." 10 seconds")
ngx.flush()

----- 4. Sleep for a random period --------
local t = utils.random() * 15000  -- 0~15 seconds
ngx.say(utils.timestamp()/1000, " ", me.." sleep "..tostring(t/1000).." seconds")
ngx.flush()
utils.msleep(t)

----- 5. Unlock --------
local ok,ret = lock.unlock(lock_key, sig)
if nil == ok  then
    ngx.say(utils.timestamp()/1000, " ", me, " UnLock: redis API error: ", ret)
    ngx.flush()
    return 
end
if false == ok then
    ngx.say(utils.timestamp()/1000, " ", me, " UnLock failure: ", ret)
    ngx.flush()
    return 
end

ngx.say(utils.timestamp()/1000, " ", me, " UnLock Succeeded")
ngx.flush()
