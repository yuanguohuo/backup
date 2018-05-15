local utils = require("common.utils")

local ok,uuid = pcall(require, "resty.uuid")
if not ok or not uuid then
    error("failed to load resty.uuid:" .. (uuid or "nil"))
end

local ok,sha1 = pcall(require, "sha1")
if not ok or not sha1 then
    error("failed to load sha1:" .. (sha1 or "nil"))
end

--[[
ngx.say("--------------[package.loaded]--------------")
for n in pairs(package.loaded) do
    ngx.say(n)
end

ngx.say("--------------[package.preload]--------------")
for n in pairs(package.preload) do
    ngx.say(n)
end

ngx.say("--------------[package.path]--------------")
ngx.say(package.path)

ngx.say("--------------[package.cpath]--------------")
ngx.say(package.cpath)

ngx.say("\r\n-------------------------------------------\r\n")

--]]

local ok,c = pcall(require, "redis.cluster-instance")
if not ok or not c then
    ngx.say("failed to load redis.cluster-instance. err="..(c or "nil"))
    return
end

function do_cmd_quick_Test_Negagive()
    --1. missing cmd 
    local res,err = c:do_cmd_quick()
    if err ~= "Invalid Command: Command missing" then
        ngx.say("ERROR: do_cmd_quick_Test_Negagive 1")
        return false
    end

    --2. transaction should not be supported
    local res,err = c:do_cmd_quick("multi")
    if err ~= "Command Not Supported: use do_cmd() instead" then
        ngx.say("ERROR: do_cmd_quick_Test_Negagive 2")
        return false
    end

    --3. eval should not be supported
    local res,err = c:do_cmd_quick("eval")
    if err ~= "Command Not Supported: use eval() or do_cmd() instead" then
        ngx.say("ERROR: do_cmd_quick_Test_Negagive 3")
        return false
    end

    --4. missing key
    local res,err = c:do_cmd_quick("get")
    if err ~= "Invalid Command: Key missing" then
        ngx.say("ERROR: do_cmd_quick_Test_Negagive 4")
        return false
    end

    --5. un-supported command
    local res,err = c:do_cmd_quick("xxxxx", "key")
    if err ~= "Command Not Supported" then
        ngx.say("ERROR: do_cmd_quick_Test_Negagive 5")
        return false
    end

    return true
end

function do_cmd_quick_Test_SetGetExistDelete()
    --1. wrong number of arguments, should fail
    local res,err = c:do_cmd_quick("set", "foobar")
    if res then
        ngx.say("ERROR: do_cmd_quick_Test_SetGetExistDelete 1")
        return false
    end

    --2. wrong number of arguments, should fail
    local res,err = c:do_cmd_quick("set", "foobar", 1, 2)
    if res then
        ngx.say("ERROR: do_cmd_quick_Test_SetGetExistDelete 2")
        return false
    end

    --3. positive 
    local res,err = c:do_cmd_quick("set", "foobar", 100)
    if not res then
        ngx.say("ERROR: do_cmd_quick_Test_SetGetExistDelete 3")
        return false
    end

    --4.test
    local res,err = c:do_cmd_quick("get", "foobar")
    if tonumber(res) ~= 100 then
        ngx.say("ERROR: do_cmd_quick_Test_SetGetExistDelete 4", " res=", utils.stringify(res))
        return false
    end

    --5. exists
    local res,err = c:do_cmd_quick("exists", "foobar")
    if tonumber(res) ~= 1 then
        ngx.say("ERROR: do_cmd_quick_Test_SetGetExistDelete 5", " res=", utils.stringify(res))
        return false
    end

    --6. delete
    local res,err = c:do_cmd_quick("del", "foobar")
    if not res then
        ngx.say("ERROR: do_cmd_quick_Test_SetGetExistDelete 6", " res=", utils.stringify(res))
        return false
    end

    --7. exists
    local res,err = c:do_cmd_quick("exists", "foobar")
    if tonumber(res) ~= 0 then
        ngx.say("ERROR: do_cmd_quick_Test_SetGetExistDelete 7", " res=", utils.stringify(res))
        return false
    end

    return true
end

function do_cmd_quick_Test_Incr()
    local res, err = c:do_cmd_quick("del", "foobar")
    if not res then
        ngx.say("ERROR: do_cmd_quick_Test_Incr 1")
        return false
    end

    local steps = 12345
    for i = 1, steps do
        local res, err = c:do_cmd_quick("incr", "foobar")
        if not res then
            ngx.say("ERROR: failed to incr foobar. err="..(err or "nil"))
            return false
        end
    end

    local res, err = c:do_cmd_quick("get", "foobar")
    if not res then
        ngx.say("ERROR: failed to get foobar after incr. err="..(err or "nil"))
        return false
    else
        if not res == steps then
            ngx.say("ERROR: incr failed, expected="..(steps or "nil")..", but actual="..(res or "nil"))
            return false
        end
    end

    local res, err = c:do_cmd_quick("del", "foobar")
    if not res then
        ngx.say("ERROR: do_cmd_quick_Test_Incr 2")
        return false
    end

    return true
end


function do_cmd_quick_Test_Hash_SetGetExistDelete()
    for i = 1, 15 do
        for j = 1,15 do
            local res, err = c:do_cmd_quick("hset", "myhash"..i, "mykey"..j, "value"..(i*j))
            if not res then
                ngx.say("ERROR: failed to hset myhash"..i.." mykey"..j..". err="..(err or "nil"))
                return false
            end
        end
    end

    for i = 1, 15 do
        for j = 1, 15 do
            local res, err = c:do_cmd_quick("hget", "myhash"..i, "mykey"..j)
            local expected = "value"..(i*j)
            if res ~= expected then
                ngx.say("ERROR: failed to hget myhash"..i.." mykey"..j..". res="..(res or "nil").."; err="..(err or "nil"))
                return false
            end
        end
    end

    for i = 1, 15 do
        for j = 1, 15 do
            local res, err = c:do_cmd_quick("hexists", "myhash"..i, "mykey"..j)
            if tonumber(res) ~= 1 then
                ngx.say("ERROR: do_cmd_quick_Test_Hash_SetGetExistDelete 1")
                return false
            end

            local res, err = c:do_cmd_quick("hdel", "myhash"..i, "mykey"..j)
            if not res then
                ngx.say("ERROR: do_cmd_quick_Test_Hash_SetGetExistDelete 2")
                return false
            end

            local res, err = c:do_cmd_quick("hexists", "myhash"..i, "mykey"..j)
            if tonumber(res) ~= 0 then
                ngx.say("ERROR: do_cmd_quick_Test_Hash_SetGetExistDelete 3")
                return false
            end
        end
    end

    for i = 1, 15 do
        local res, err = c:do_cmd_quick("del", "myhash"..i)
        if not res then
            ngx.say("ERROR: do_cmd_quick_Test_Hash_SetGetExistDelete 4")
            return false
        end
    end

    return true
end

function do_cmd_quick_Test_Hash_ScanGetall()
    local keys = 400
    for i=1,keys do
        local res, err = c:do_cmd_quick("hset", "foobar", "key"..tostring(i), "value"..tostring(i))
        if not res then
            ngx.say("ERROR: do_cmd_quick_Test_Hash_ScanGetall 1")
            return false
        end
    end

    local cursor = 0 
    local num = 0
    while true do
        local res, err = c:do_cmd_quick("hscan", "foobar", cursor, "MATCH", "key*", "COUNT", 16)

        num = num + tonumber(#res[2])
        cursor = tonumber(res[1])

        if cursor == 0 then
            break
        end
    end

    if num ~= keys*2 then
        ngx.say("ERROR: do_cmd_quick_Test_Hash_ScanGetall 1")
        return false
    end

    local res, err = c:do_cmd_quick("hgetall", "foobar")

    if #res ~= keys*2 then
        ngx.say("ERROR: do_cmd_quick_Test_Hash_ScanGetall 2")
        return false
    end

    local res, err = c:do_cmd_quick("del", "foobar")
    if not res then
        ngx.say("ERROR: do_cmd_quick_Test_Hash_ScanGetall 3")
        return false
    end

    return true
end

function test_transaction_on_hash()
    --prepare
    local res, err = c:do_cmd_quick("del", "foobar")
    if not res then
        ngx.say("ERROR: prepare failed, del foobar: ", err)
        return false
    end
    local res, err = c:do_cmd_quick("hset", "foobar", "version", 1)
    if not res then
        ngx.say("ERROR: prepare failed, hset: ", err)
        return false
    end

    local term1 = c:create_terminal("foobar")
    local ok,err = term1:connect()
    if not ok then
        ngx.say("ERROR: test_transaction_on_hash 1")
        return false
    end

    local term2 = c:create_terminal("foobar")
    local ok,err = term2:connect()
    if not ok then
        ngx.say("ERROR: test_transaction_on_hash 2")
        return false
    end

    local ok,err = c:do_cmd(term1, "watch", "foobar")
    if not ok then
        ngx.say("ERROR: test_transaction_on_hash 3")
        return false
    end

    local ok,err = c:do_cmd(term2, "watch", "foobar")
    if not ok then
        ngx.say("ERROR: test_transaction_on_hash 4")
        return false
    end

    local v1,err = c:do_cmd(term1, "hget", "foobar", "version")
    ngx.say("term1 get: "..utils.stringify(v1))
    if not v1 then
        ngx.say("ERROR: test_transaction_on_hash 5")
        return false
    end
    if tonumber(v1) ~= 1 then
        ngx.say("ERROR: test_transaction_on_hash 6")
        return false
    end

    local v2,err = c:do_cmd(term2, "hget", "foobar", "version")
    ngx.say("term2 get: "..utils.stringify(v2))
    if not v2 then
        ngx.say("ERROR: test_transaction_on_hash 7")
        return false
    end
    if tonumber(v2) ~= 1 then
        ngx.say("ERROR: test_transaction_on_hash 8")
        return false
    end

    local ok,err = c:do_cmd(term1, "multi")
    if not ok then
        ngx.say("ERROR: test_transaction_on_hash 9: ", err)
        return false
    end

    local ok,err = c:do_cmd(term2, "multi")
    if not ok then
        ngx.say("ERROR: test_transaction_on_hash 10: ", err)
        return false
    end

    local res1,err = c:do_cmd(term1, "hset", "foobar", "version", 100)
    ngx.say("term1 hset: "..utils.stringify(res1))
    if not res1 then
        ngx.say("ERROR: test_transaction_on_hash 11, "..utils.stringify(res1))
        return false
    end

    local res2,err = c:do_cmd(term2, "hset", "foobar", "version", 200)
    ngx.say("term2 hset: "..utils.stringify(res2))
    if not res1 then
        ngx.say("ERROR: test_transaction_on_hash 12, "..utils.stringify(res2))
        return false
    end

    local ok,err = c:do_cmd(term1, "exec")
    ngx.say("term1 exec: "..utils.stringify(ok))
    if not ok then
        ngx.say("ERROR: test_transaction_on_hash 13, "..utils.stringify(ok))
        return false
    end

    local ok,err = c:do_cmd(term2, "exec")
    ngx.say("term2 exec: "..utils.stringify(ok))
    if ok ~= ngx.null then  ---Should fail !!!!!
        ngx.say("ERROR: test_transaction_on_hash 14, "..utils.stringify(ok))
        return false
    end

    local res, err = c:do_cmd_quick("hget", "foobar", "version")
    ngx.say("final value: "..utils.stringify(res))
    if tonumber(res) ~= 100 then
        ngx.say("ERROR: test failed: res=", err)
    end

    --clean
    local res, err = c:do_cmd_quick("del", "foobar")
    if not res then
        ngx.say("ERROR: clean failed: ", err)
        return false
    end

    return true
end


function do_cmd_quick_Test_mget_mset()
    local kprefix = "{mget_mset_key}_"

    local k1 = uuid()
    local k2 = uuid()
    local k3 = uuid()
    local k4 = uuid()

    local res,err = c:do_cmd_quick("mget", kprefix..k1, kprefix..k2, kprefix..k3, kprefix..k4)
    if not res then
        ngx.say("ERROR: do_cmd_quick_Test_mget_mset failed 1")
        return false
    end
    for i = 1,4 do
        if res[i] ~= ngx.null then
            ngx.say("ERROR: do_cmd_quick_Test_mget_mset failed 2")
            return false
        end
    end

    local res,err = c:do_cmd_quick("mset", kprefix..k1, k1,  kprefix..k2, k2, kprefix..k4, k4)
    if not res then
        ngx.say("ERROR: do_cmd_quick_Test_mget_mset failed 3")
        return false
    end

    local res,err = c:do_cmd_quick("mget", kprefix..k1, kprefix..k2, kprefix..k3, kprefix..k4)
    if not res then
        ngx.say("ERROR: do_cmd_quick_Test_mget_mset failed 4")
        return false
    end

    if res[1] ~= k1 or  res[2] ~= k2 or  res[4] ~= k4 then
        ngx.say("ERROR: do_cmd_quick_Test_mget_mset failed 5")
        return false
    end

    if res[3] ~= ngx.null then
        ngx.say("ERROR: do_cmd_quick_Test_mget_mset failed 6")
        return false
    end

    local res,err = c:do_cmd_quick("del", kprefix..k1, kprefix..k2, kprefix..k3, kprefix..k4)
    if not res then
        ngx.say("ERROR: do_cmd_quick_Test_mget_mset failed 7")
        return false
    end

    return true
end


function test_eval_1()
    local script = "local res1,err1 = redis.call('hset', KEYS[1], ARGV[1], ARGV[2]) "..
                   "local res2,err2 = redis.call('hset', KEYS[1], ARGV[3], ARGV[4]) "..
                   "return {{res1,err1},{res2,err2}}"
    
    local k1 = "foo"
    local v1 = uuid()
    local k2 = "bar"
    local v2 = uuid()

    local res,err = c:eval(script, 1, "hash004", k1,v1,k2,v2)

    if not res then
        ngx.say("ERROR: test_eval_1 eval failed: ", err)
        return false
    end

    local res1,err1 = c:do_cmd_quick("hgetall", "hash004")
    ngx.say("res1="..utils.stringify(res1))

    if not res1 or nil == next(res1) then
        ngx.say("ERROR: test_eval_1 failed to get result: ", err1)
        return false
    end

    if res1[2] ~= v1 or res1[4] ~= v2 then
        ngx.say("ERROR: test_eval_1 result doesn't match: v1="..v1..", res1[2]="..utils.stringify(res1[2])..", v2="..v2..", res1[4]="..utils.stringify(res1[4]))
        return false
    end

    return true
end


function test_eval_2()
    local script = "return redis.call('get', KEYS[1])"
    local res,err = c:eval(script, 1, "KeyNotExist")
    ngx.say("get non-existing key ="..utils.stringify(res)) --userdata: NULL
    if res ~= ngx.null then
        ngx.say("ERROR: test_eval_2 1")
        return false
    end

    local script = "return redis.call('del', KEYS[1])"
    local res,err = c:eval(script, 1, "KeyNotExist")
    ngx.say("del non-existing key ="..utils.stringify(res)) -- 0
    if tonumber(res) ~= 0 then
        ngx.say("ERROR: test_eval_2 2")
        return false
    end

    local res,err = c:do_cmd_quick("set", "KeyExist", "yyyy")
    if not res then
        ngx.say("ERROR: test_eval_2 3")
        return false
    end

    local script = "return redis.call('del', KEYS[1])"
    local res,err = c:eval(script, 1, "KeyExist")
    ngx.say("del existing key ="..utils.stringify(res)) -- 0
    if tonumber(res) ~= 1 then
        ngx.say("ERROR: test_eval_2 4")
        return false
    end


    local script = "return true"
    local res,err = c:eval(script, 1, "KeyNotExist")
    ngx.say("true ="..utils.stringify(res))     --1
    if tonumber(res) ~= 1 then
        ngx.say("ERROR: test_eval_2 5")
        return false
    end

    local script = "return false"
    local res,err = c:eval(script, 1, "KeyNotExist")
    ngx.say("false ="..utils.stringify(res))    --userdata: NULL
    if res ~= ngx.null then
        ngx.say("ERROR: test_eval_2 6")
        return false
    end

    local script = "return nil"
    local res,err = c:eval(script, 1, "KeyNotExist")
    ngx.say("nil ="..utils.stringify(res))    --userdata: NULL
    if res ~= ngx.null then
        ngx.say("ERROR: test_eval_2 7")
        return false
    end

    return true
end


function test_eval_3()
    local script = "local res1, err1 = redis.call('set', KEYS[1], ARGV[1]) " ..
                   "local res2, err2 = redis.call('set', KEYS[2], ARGV[2]) " ..
                   "local res3, err3 = redis.call('set', KEYS[3], ARGV[3]) " ..
                   "local res4, err4 = redis.call('set', KEYS[4], ARGV[4]) " ..
                   "local res5, err5 = redis.call('set', KEYS[5], ARGV[5]) " ..
                   "local res6, err6 = redis.call('set', KEYS[6], ARGV[6]) " ..
                   "local res7, err7 = redis.call('set', KEYS[7], ARGV[7]) " ..
                   "local res8, err8 = redis.call('set', KEYS[8], ARGV[8]) " ..
                   "return {{res1,err1}, {res2,err2}, {res3,err3}, {res4,err4}, {res5,err5}, {res6,err6}, {res7,err7}, {res8,err8}}"

    -- negative test: keys don't reside on the same slot
    local res,err = c:eval(script, 8, "EVAL_KEY_AA", "EVAL_KEY_BB", "EVAL_KEY_CC", "EVAL_KEY_DD", "EVAL_KEY_EE", "EVAL_KEY_FF", "EVAL_KEY_GG", "EVAL_KEY_HH")
    if err ~= "CROSSSLOT Keys in request don't hash to the same slot" then
        ngx.say("ERROR: test_eval_3 1: err=" .. err)
        return false
    end

    --positive test
    local res,err = c:eval(script, 8, "{EVAL_KEY}_AA", "{EVAL_KEY}_BB", "{EVAL_KEY}_CC", "{EVAL_KEY}_DD", 
                                      "{EVAL_KEY}_EE", "{EVAL_KEY}_FF", "{EVAL_KEY}_GG", "{EVAL_KEY}_HH",
                                      "val_a", "val_b", "val_c", "val_d",
                                      "val_e", "val_f", "val_g", "val_h")


    local script = "local res1, err1 = redis.call('get', KEYS[1]) " ..
                   "local res2, err2 = redis.call('get', KEYS[2]) " ..
                   "local res3, err3 = redis.call('get', KEYS[3]) " ..
                   "local res4, err4 = redis.call('get', KEYS[4]) " ..
                   "local res5, err5 = redis.call('get', KEYS[5]) " ..
                   "local res6, err6 = redis.call('get', KEYS[6]) " ..
                   "local res7, err7 = redis.call('get', KEYS[7]) " ..
                   "local res8, err8 = redis.call('get', KEYS[8]) " ..
                   "return {{res1,err1}, {res2,err2}, {res3,err3}, {res4,err4}, {res5,err5}, {res6,err6}, {res7,err7}, {res8,err8}}"

    local res,err = c:eval(script, 8, "{EVAL_KEY}_AA", "{EVAL_KEY}_BB", "{EVAL_KEY}_CC", "{EVAL_KEY}_DD", 
                                      "{EVAL_KEY}_EE", "{EVAL_KEY}_FF", "{EVAL_KEY}_GG", "{EVAL_KEY}_HH")

    if err then
        ngx.say("ERROR: test_eval_3 2")
        return false
    end

    if res[1][1] ~= "val_a" or 
       res[2][1] ~= "val_b" or
       res[3][1] ~= "val_c" or
       res[4][1] ~= "val_d" or
       res[5][1] ~= "val_e" or
       res[6][1] ~= "val_f" or
       res[7][1] ~= "val_g" or
       res[8][1] ~= "val_h" then
        ngx.say("ERROR: test_eval_3 3")
        return false
   end

    return true
end


function test_evalsha_1()
    local script = "local res1,err1 = redis.call('hset', KEYS[1], ARGV[1], ARGV[2]) "..
                   "local res2,err2 = redis.call('hset', KEYS[1], ARGV[3], ARGV[4]) "..
                   "return {{res1,err1},{res2,err2}}"
    local script_sha1_hex = sha1(script)

    local hashKey001 = "test_evalsha_hash001"

    local k1 = "foo"
    local k2 = "bar"

    local v1 = uuid()
    local v2 = uuid()

    local res,err,eval_used = c:evalsha(script, script_sha1_hex, 1, hashKey001, k1,v1,k2,v2)

    if not res then
        ngx.say("ERROR: test_evalsha_1 failed: ", err)
        return false
    end

    local res1,err1 = c:do_cmd_quick("hgetall", hashKey001)
    ngx.say("res1="..utils.stringify(res1))

    if not res1 or nil == next(res1) then
        ngx.say("ERROR: test_evalsha_1 failed to get result: ", err1)
        return false
    end

    if res1[2] ~= v1 or res1[4] ~= v2 then
        ngx.say("ERROR: test_evalsha_1 result doesn't match: v1="..v1..", res1[2]="..utils.stringify(res1[2])..", v2="..v2..", res1[4]="..utils.stringify(res1[4]))
        return false
    end

    if eval_used then
        ngx.say("script not matched in redis server, 'eval' was used. script=", script)
    else
        ngx.say("script matched in redis server, 'evalsha' succeeded. script=", script)
    end


    ----- Run the script again, it should be matched in redis server;

    local v3 = uuid()
    local v4 = uuid()

    local res,err,eval_used = c:evalsha(script, script_sha1_hex, 1, hashKey001, k1,v3,k2,v4)

    if not res then
        ngx.say("ERROR: test_evalsha_1 failed when re-run the script: ", err)
        return false
    end

    local res1,err1 = c:do_cmd_quick("hgetall", hashKey001)
    ngx.say("res1="..utils.stringify(res1))

    if not res1 or nil == next(res1) then
        ngx.say("ERROR: test_evalsha_1 failed to get result when re-run the script: ", err1)
        return false
    end

    if res1[2] ~= v3 or res1[4] ~= v4 then
        ngx.say("ERROR: test_evalsha_1 result doesn't match: v3="..v3..", res1[2]="..utils.stringify(res1[2])..", v4="..v4..", res1[4]="..utils.stringify(res1[4]))
        return false
    end

    if eval_used then
        ngx.say("ERROR: test_evalsha_1 failed: 'eval' should NOT be used because the script should be matched in redis server")
        return false
    end

    return true
end

local testFuncs = {
    do_cmd_quick_Test_Negagive,
    do_cmd_quick_Test_SetGetExistDelete,
    do_cmd_quick_Test_Incr,
    do_cmd_quick_Test_Hash_SetGetExistDelete,
    do_cmd_quick_Test_Hash_ScanGetall,
    test_transaction_on_hash,
    test_eval_1,
    test_eval_2,
    test_eval_3,
    test_evalsha_1,
    do_cmd_quick_Test_mget_mset,
}

local fail = 0
local succ = 0
local total = 0 
for i,func in ipairs(testFuncs) do
    total = total + 1
    local ret = func()
    if not ret then
        fail = fail + 1
        ngx.say("ERROR: test "..tostring(i).. " failed")
        break
    else
        succ = succ + 1
    end
end
ngx.say("Total   : "..tostring(total))
ngx.say("Success : "..tostring(succ))
ngx.say("Failure : "..tostring(fail))

if total == succ then
    ngx.say("All basic tests have succeeded")
end
