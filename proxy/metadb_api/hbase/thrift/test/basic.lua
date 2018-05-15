local ok, new_tab = pcall(require, "table.new")
if not ok or type(new_tab) ~= "function" then
    new_tab = function (narr, nrec) return {} end
end

local ok, utils = pcall(require, "common.utils")
if not ok or not utils then
    error("failed to load common.utils:" .. (utils or "nil"))
end

local ok, hb = pcall(require, "hbase.hbase-instance")
if not ok or not hb then
    error("failed to load hbase.hbase-instance:" .. (hb or "nil"))
end

local ok,uuid = pcall(require, "resty.uuid")
if not ok or not uuid then
    error("failed to load resty.uuid:" .. (uuid or "nil"))
end

local clean = true 
local verbose = true
local TABLE = "bucket"
local onlyClean = false 

function checkRow(table_name, rowKey, args)
    local query_columns = {}
    local expectHash = {}
    for i=1, #args-2, 3 do
        local fc = args[i]..":"..args[i+1]
        table.insert(query_columns, fc)
        expectHash[fc] = args[i+2]
    end

    local code,ok,ret = hb:get(table_name, rowKey, query_columns)

    if (ret==nil) then
        ngx.say("checkRow failed, HBase get failed, table_name=", table_name, " rowKey=", rowKey, " code=", code)
        return false
    end

    for k,v in pairs(ret) do
        if expectHash[k] ~= v then
            ngx.say("ERROR: checkRow failed: key=", k, " Expected=", expectHash[k], " Actual=", v)
            return false
        end
    end

    for k,v in pairs(expectHash) do
        if ret[k] ~= v then
            ngx.say("ERROR: checkRow failed: key=", k, " Actual=", ret[k], " Expected=", v)
            return false
        end
    end

    return true
end

local function putRow(table_name, rowKey, args)
    local code, ok = hb:put(table_name, rowKey, args)
    if not ok then
        ngx.say("putRow failed, hbase put failed, innercode="..code)
        return false
    end
    return checkRow(table_name, rowKey, args)
end


local function TestPut()
    local rowKey = "Row0001"
    
    if not onlyClean then
        --1. args is nil, should fail, code = "1100" 
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "========Negative Test: put nil")
        local code, ok = hb:put(TABLE, rowKey)
        if ok or code ~= "1100" then
            ngx.say("ERROR: put test failed, args=nil should not be allowed, but it was")
            return false
        end

        --2. args is not a table, should fail, code = "1100" 
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "========Negative Test: put args is non-table")
        local code, ok = hb:put(TABLE, rowKey, 33)
        if ok or code ~= "1100" then
            ngx.say("ERROR: put test failed, args is non-table should not be allowed, but it was")
            return false
        end

        --3. args is empty table, should fail, code = "1100"
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "========Negative Test: put empty")
        local code, ok = hb:put(TABLE, rowKey, {})
        if ok or code ~= "1100" then
            ngx.say("ERROR: put test failed, args={} should not be allowed, but it was")
            return false
        end

        --4. cannot put COUNTER value, should fail, code = "1110"
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "========Negative Test: put COUNTER value")
        local code, ok = hb:put(TABLE, rowKey, {"info", "info1", "SomeValue", "stats", "objects", 1000})
        if ok or code ~= "1110" then
            ngx.say("ERROR: put test failed, COUNTER values should not be allowed, but it was")
            return false
        end

        --5. successful put
        local args = {
            "info",    "info1",     "info11111111", 
            "info",    "info2",     "info22222222",
            "quota",   "quota1",    "quota1111111",
            "quota",   "quota2",    "quota2222222",
            "quota",   "quota3",    "quoto3333333",
            "ver",     "ver1",      "ver111111111",
            "ver",     "ver2",      "ver222222222",
            "exattrs", "exattrs1",  "exattrs11111",
            "exattrs", "exattrs2",  "exattrs22222",
            "exattrs", "exattrs3",  "exattrs33333",
            "exattrs", "exattrs4",  "exattrs44444",
            --"stats", "size_bytes", 0
            --"stats", "objects",    0
        }
        local ok = putRow(TABLE, rowKey, args)
        if not ok then
            ngx.say("ERROR: put test failed: putRow failed")
            return false
        end
    end

    if clean or onlyClean then
        hb:delete(TABLE, rowKey)
    end

    ngx.say("OK: put test successful")
    ngx.flush()
    return true
end

function TestGet()
    local rowKey = "Row0002"

    if not onlyClean then
        local args = {
            "info",    "info1",     "info11111111", 
            "info",    "info2",     "info22222222",
            "quota",   "quota1",    "quota1111111",
            "quota",   "quota2",    "quota2222222",
            "quota",   "quota3",    "quota3333333",
            "ver",     "ver1",      "ver111111111",
            "ver",     "ver2",      "ver222222222",
            "exattrs", "exattrs1",  "exattrs11111",
            "exattrs", "exattrs2",  "exattrs22222",
            "exattrs", "exattrs3",  "exattrs33333",
            "exattrs", "exattrs4",  "exattrs44444",
            --"stats", "size_bytes", 0
            --"stats", "objects",    0
        }

        --prepare
        local ok = putRow(TABLE, rowKey, args) --there we get entire row
        if not ok then
            ngx.say("ERROR: get test failed, prepare failed")
            return false
        end

        local code2,ok2,current = hb:increment(TABLE, rowKey, {"stats", "size_bytes", 0})
        if  current["stats:size_bytes"] ~= 0 then
            ngx.say("ERROR: get test failed, prepare failed, increment failed")
            return false
        end

        local code3,ok3,current = hb:increment(TABLE, rowKey, {"stats", "size_bytes", -100,  "stats", "objects", 3000})
        if current["stats:size_bytes"] ~= -100 or current["stats:objects"] ~= 3000 then
            ngx.say("ERROR: get test failed, prepare failed, increment failed")
            return false
        end


        if not ok2 or not ok3 then
            ngx.say("ERROR: get test failed, prepare failed, prepare counters failed")
            return false
        end


        --1. get entire row
        local code,ok,ret = hb:get(TABLE, rowKey, 
           {
            "info:info1",
            "info:info2",
            "quota:quota1",
            "quota:quota2",
            "quota:quota3",
            "ver:ver1",
            "ver:ver2",
            "exattrs:exattrs1",
            "exattrs:exattrs2",
            "exattrs:exattrs3",
            "exattrs:exattrs4",
            "stats:size_bytes",
            "stats:objects",
           })
        if not ok then
            ngx.say("ERROR: get test failed, get entire row failed, innercode="..code)
            return false
        end
        if ret["info:info1"] ~= "info11111111" or 
            ret["info:info2"] ~= "info22222222" or
            ret["quota:quota1"] ~= "quota1111111" or
            ret["quota:quota2"] ~= "quota2222222" or
            ret["quota:quota3"] ~= "quota3333333" or
            ret["ver:ver1"] ~= "ver111111111" or
            ret["ver:ver2"] ~= "ver222222222" or
            ret["exattrs:exattrs1"] ~= "exattrs11111" or
            ret["exattrs:exattrs2"] ~= "exattrs22222" or
            ret["exattrs:exattrs3"] ~= "exattrs33333" or
            ret["exattrs:exattrs4"] ~= "exattrs44444" or
            ret["stats:size_bytes"] ~= -100 or 
            ret["stats:objects"] ~= 3000 then 
            ngx.say("ERROR: get test failed, get entire row, check result failed")
            return false
        end

        --2. args is not a table, should fail, code = "1100" 
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "========Negative Test: get args is non-table")
        local code,ok,ret = hb:get(TABLE, rowKey, 33)
        if ok or code ~= "1100" then
            ngx.say("ERROR: get test failed, args is non-table should not be allowed, but it was")
            return false
        end

        --3. get family that does not exist, it should fail 
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "========Negative Test: get non existent family")
        local code,ok,ret = hb:get(TABLE, rowKey, {"info:info1", "NotExistFamily:col"})
        if ok or code ~= "1102" then
            ngx.say("ERROR: get test failed, get NotExistFamily should fail, but it didn't")
            return false
        end

        --4. get column that doesn't exist; it should succeed and the value is nil 
        local code,ok,ret = hb:get(TABLE, rowKey, {"quota:quota1", "ver:NotExistColumn"})
        if not ok then
            ngx.say("ERROR: get test failed, get ver:NotExistColumn should succeed, but it didn't")
            return false
        end

        local got_quota1 = ret["quota:quota1"]
        local got_notExist = ret["ver:NotExistColumn"]

        if got_quota1 ~= "quota1111111" or got_notExist ~= nil then
            ngx.say("ERROR: get test failed, get ver:NotExistColumn, checkRow failed")
            return false
        end

        --5. get a row that doesn't exit, it should succeed, and an empty table is returned
        local code,ok,ret = hb:get(TABLE, "RowkeyNotExist", {"quota:quota1"})
        if not ok then
            ngx.say("ERROR: get test failed, get RowkeyNotExist should succeed, but it didn't")
            return false
        end

        if not ret or type(ret) ~= "table" or nil ~= next(ret) then
            ngx.say("ERROR: get test failed, get RowkeyNotExist should return an emtpy table, but it didn't")
            return false
        end
    end

    if clean or onlyClean then
        hb:delete(TABLE, rowKey)
    end
    ngx.say("OK: get test successful")
    ngx.flush()
    return true
end

function TestIncrement()
    local rowKey = "Row0003"

    if not onlyClean then

        --1. bondary test
        -- prepare for bondary test
        hb:delete(TABLE, rowKey)

        local code,ok,current = hb:increment(TABLE, rowKey, {"stats", "objects", -1})
        --ngx.say(utils.stringify(current))
        if current["stats:objects"] ~= -1 then
            ngx.say("ERROR: increment test failed, boundary test1 failed")
            return false
        end
        local code,ok,current = hb:increment(TABLE, rowKey, {"stats", "objects", -0})
        --ngx.say(utils.stringify(current))
        if current["stats:objects"] ~= -1 then
            ngx.say("ERROR: increment test failed, boundary test2 failed")
            return false
        end

        local code,ok,current = hb:increment(TABLE, rowKey, {"stats", "objects", 0})
        --ngx.say(utils.stringify(current))
        if current["stats:objects"] ~= -1 then
            ngx.say("ERROR: increment test failed, boundary test3 failed")
            return false
        end

        local code,ok,current = hb:increment(TABLE, rowKey, {"stats", "objects", 1})
        --ngx.say(utils.stringify(current))
        if current["stats:objects"] ~= 0 then
            ngx.say("ERROR: increment test failed, boundary test4 failed")
            return false
        end


        --Yuanguo: for hbase the range of COUNTER value is:
        --           -2^63  ~  2^63-1
        --But for LUA, all numbers are double, and it cannot express -2^63 accurately. A 
        --number close to -2^63 (not equal) will "equal to" -2^63. 
        --As a result, boundary test 5 - 7 are not accurate.
        --
        -- 2^63 = 9223372036854775808

        local code,ok,current = hb:increment(TABLE, rowKey, {"stats", "size_bytes", -9223372036854775808}) --the minimum value
        --ngx.say(utils.stringify(current))
        if current["stats:size_bytes"] ~= -9223372036854775808 then
            ngx.say("ERROR: increment test failed, boundary test5 failed")
            return false
        end

        local code,ok,current = hb:increment(TABLE, rowKey, {"stats", "size_bytes", 4611686018427387904})
        --ngx.say(utils.stringify(current))
        if current["stats:size_bytes"] ~= -4611686018427387904 then
            ngx.say("ERROR: increment test failed, boundary test6 failed")
            return false
        end

        local code,ok,current = hb:increment(TABLE, rowKey, {"stats", "size_bytes", 4611686018427387904})
        --ngx.say(utils.stringify(current))
        if current["stats:size_bytes"] ~= 0 then
            ngx.say("ERROR: increment test failed, boundary test7 failed")
            return false
        end

        --Yuanguo: we get 2^63-1 by accumulating:
        -- 2^10 * 2^53  - 1  = (2^10-1) * 2^53 + 2^53 - 1 
        -- 2^53 = 9007199254740992

        for y = 1, 1023, 1 do
            local code,ok,current = hb:increment(TABLE, rowKey, {"stats", "size_bytes", 9007199254740992})
        end
        local code,ok,current = hb:increment(TABLE, rowKey, {"stats", "size_bytes", 9007199254740991})
        --ngx.say(utils.stringify(current))
        if current["stats:size_bytes"] ~= 9223372036854775807 then
            ngx.say("ERROR: increment test failed, boundary test8 failed")
            return false
        end

        for y = 1, 1023, 1 do
            local code,ok,current = hb:increment(TABLE, rowKey, {"stats", "size_bytes", -9007199254740992})
        end
        local code,ok,current = hb:increment(TABLE, rowKey, {"stats", "size_bytes", -9007199254740991})
        if current["stats:size_bytes"] ~= 0 then
            ngx.say("ERROR: increment test failed, boundary test9 failed")
            return false
        end

        --2. increment
        -- get initial value. After boundary tests, initial values shoud be
        -- zeros;
        local code,ok,ret = hb:get(TABLE, rowKey, {"stats:objects", "stats:size_bytes"})
        if not ok then
            ngx.say("ERROR: increment test failed, get initial value failed.")
            return false
        end

        local stats_objects = ret["stats:objects"]
        local stats_size_bytes = ret["stats:size_bytes"] 

        if verbose then
            ngx.say("stats_objects     : "..utils.stringify(stats_objects))
            ngx.say("stats_size_bytes  : "..utils.stringify(stats_size_bytes))
        end

        if stats_objects ~=0 or stats_size_bytes ~=0 then
            ngx.say("ERROR: get test failed, intial values are not zeros")
            return false
        end

        local code3,ok3,current = hb:increment(TABLE, rowKey, {"stats", "objects", -1000})
        if current["stats:objects"] ~= stats_objects-1000 then
            ngx.say("ERROR: get test failed, prepare failed, increment failed")
            return false
        end

        local code4,ok4,current = hb:increment(TABLE, rowKey, {"stats", "objects", "1100", "stats", "size_bytes", 80000})
        if current["stats:objects"] ~= stats_objects-1000+1100 or current["stats:size_bytes"] ~= stats_size_bytes+80000 then
            ngx.say("ERROR: get test failed, prepare failed, increment failed")
            return false
        end
        if not ok3 or not ok4 then
            ngx.say("ERROR: increment failed, code2="..code2..", code3="..code..", code4="..code4)
            return false
        end

        --3. get final value
        local code,ok,ret = hb:get(TABLE, rowKey, {"stats:objects", "stats:size_bytes"})
        if not ok then
            ngx.say("ERROR: increment test failed, get final value failed.")
            return false
        end

        local stats_objects1 = ret["stats:objects"] 
        local stats_size_bytes1 = ret["stats:size_bytes"] 

        if verbose then
            ngx.say("stats_objects     : "..utils.stringify(stats_objects1))
            ngx.say("stats_size_bytes  : "..utils.stringify(stats_size_bytes1))
        end

        --4. compare
        if
            stats_objects1       ~= stats_objects-1000+1100      or
            stats_size_bytes1    ~= stats_size_bytes + 80000  then
            ngx.say("ERROR: increment test failed, values are incorrect after increment")
            return false
        end

        --5 increment non-COUNTER values
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "========Negative Test: increment non COUNTER value")
        local code,ok = hb:increment(TABLE, rowKey, {"stats", "objects", 500, "info", "info1", 399})
        if ok or code ~= "1109" then
            ngx.say("ERROR: increment test failed, increment should NOT allowed for non-COUNTER values,but it was")
            return false
        end

        --6. args is nil, should fail, code = "1100" 
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "========Negative Test: increment args is nil")
        local code,ok,ret = hb:increment(TABLE, rowKey, nil)
        if ok or code ~= "1100" then
            ngx.say("ERROR: increment test failed, args is nil should not be allowed, but it was")
            return false
        end

        --7. args is not a table, should fail, code = "1100" 
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "========Negative Test: increment args is non-table")
        local code,ok,ret = hb:increment(TABLE, rowKey, 33)
        if ok or code ~= "1100" then
            ngx.say("ERROR: increment test failed, args is non-table should not be allowed, but it was")
            return false
        end

        --8. args is empty table, should fail, code = "1100" 
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "========Negative Test: increment args is empty table")
        local code,ok,ret = hb:increment(TABLE, rowKey, {})
        if ok or code ~= "1100" then
            ngx.say("ERROR: increment test failed, args is empty table should not be allowed, but it was")
            return false
        end
    end

    if clean or onlyClean then
        hb:delete(TABLE, rowKey)
    end
    ngx.say("OK: increment test successful")
    ngx.flush()
    return true
end

function TestCheckAndPut()
    local rowKey = "Row0004"
    local rowKeyCheckAndPut = "rowKeyCheckAndPut"
    local rowKey_NotExist = "RowDidnotExist"

    if not onlyClean then
        local args = {
            "info",    "info1",     "info11111111", 
            "info",    "info2",     "info22222222",
            "quota",   "quota1",    "quota1111111",
            "quota",   "quota2",    "quota2222222",
            "quota",   "quota3",    "quoto3333333",
            "ver",     "ver1",      "ver111111111",
            "ver",     "ver2",      "ver222222222",
            "exattrs", "exattrs1",  "exattrs11111",
            "exattrs", "exattrs2",  "exattrs22222",
            "exattrs", "exattrs3",  "exattrs33333",
            "exattrs", "exattrs4",  "exattrs44444",
        }

        --prepare
        local ok = putRow(TABLE, rowKey, args)
        if not ok then
            ngx.say("ERROR: checkAndPut test failed, prepare failed")
            return false
        end

        --1. args is nil, should fail, code = "1100" 
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "========Negative Test: checkAndPut args is nil")
        local code,ok,ret = hb:checkAndPut(TABLE, rowKey, "info", "info1", "info11111111", nil)
        if ok or code ~= "1100" then
            ngx.say("ERROR: checkAndPut test failed, args is nil, it should not be allowed, but it was")
            return false
        end

        --2. args is not a table, should fail, code = "1100" 
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "========Negative Test: checkAndPut args is non-table")
        local code,ok,ret = hb:checkAndPut(TABLE, rowKey, "info", "info1", "info11111111", 33)
        if ok or code ~= "1100" then
            ngx.say("ERROR: checkAndPut test failed, args is non-table, it should not be allowed, but it was")
            return false
        end

        --3. args is an empty table, should fail, code = "1100" 
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "========Negative Test: checkAndPut args is an empty table")
        local code,ok,ret = hb:checkAndPut(TABLE, rowKey, "info", "info1", "info11111111", {})
        if ok or code ~= "1100" then
            ngx.say("ERROR: checkAndPut test failed, args is non-table, it should not be allowed, but it was")
            return false
        end

        --4. failure check
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "========Negative Test: checkAndPut the checked value is incorrect")
        local code,ok = hb:checkAndPut(TABLE, rowKey, "info", "info1", "SomeValueNotExpected", {"info", "info2", "XXXXXXXX", "ver", "ver1", "YYYYYYYY"})
        if ok then
            ngx.say("ERROR: checkAndPut test failed. The op should NOT succeed but it did")
            return false
        end

        if code~= "1121" then
            ngx.say("ERROR: checkAndPut test failed. error code 1121 is expected, but it is: ", code)
            return false
        end

        --5. checkAndPut a COUNTER value, should fail
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "========Negative Test: checkAndPut a COUNTER value")
        local code,ok = hb:checkAndPut(TABLE, rowKey, "info", "info1", "SomeValue", {"info", "info2", "XXXXXXXX", "stats", "objects", "YYYYYYYY"})
        if ok or code~= "1110" then
            ngx.say("ERROR: checkAndPut test failed. checkAndPut COUNTER value should not be allowed, but it was")
            return false
        end

        --6. check successful and put the same column with same value, should succeed
        local code,ok = hb:checkAndPut(TABLE, rowKey, "info", "info1", "info11111111", {"info", "info1", "info11111111"})
        if not ok then
            ngx.say("ERROR: checkAndPut test failed. put the same column with the same value. innercode="..code)
            return false
        end
        local ok = checkRow(TABLE, rowKey, 
        {
            "info",   "info1",         "info11111111",
        })
        if not ok then
            ngx.say("ERROR: checkAndPut test failed, put the same column with the same value. checkRow failed")
            return false
        end

        --7. check successful and put the same column with different value, should succeed
        local code,ok = hb:checkAndPut(TABLE, rowKey, "info", "info1", "info11111111", {"info", "info1", "XXXXXXXX"})
        if not ok then
            ngx.say("ERROR: checkAndPut test failed. put the same column with different value. innercode="..code)
            return false
        end
        local ok = checkRow(TABLE, rowKey, 
        {
            "info",   "info1",         "XXXXXXXX",
        })
        if not ok then
            ngx.say("ERROR: checkAndPut test failed, put the same column with different value. checkRow failed")
            return false
        end

        --8. check successful and put another column, should succeed
        local code,ok = hb:checkAndPut(TABLE, rowKey, "info", "info2", "info22222222", {"info", "info1", "YYYYYYYY"})
        if not ok then
            ngx.say("ERROR: checkAndPut test failed. put another column. innercode="..code)
            return false
        end
        local ok = checkRow(TABLE, rowKey, 
        {
            "info",   "info1",         "YYYYYYYY",
        })
        if not ok then
            ngx.say("ERROR: checkAndPut test failed, put another column. checkRow failed")
            return false
        end

        --9. check successful and put several columns, should succeed
        local code,ok = hb:checkAndPut(TABLE, rowKey, "info", "info2", "info22222222", {"info", "info1", "XXXXXXXX", "info", "info2", "YYYYYYYY", "ver", "ver1", "ZZZZZZZZ"})
        if not ok then
            ngx.say("ERROR: checkAndPut test failed. put another column. innercode="..code)
            return false
        end
        local ok = checkRow(TABLE, rowKey, 
        {
            "info",   "info1",         "XXXXXXXX",
            "info",   "info2",         "YYYYYYYY",
            "ver",    "ver1",          "ZZZZZZZZ",
        })
        if not ok then
            ngx.say("ERROR: checkAndPut test failed, put another column. checkRow failed")
            return false
        end

        --10a. value nil, expect nil, should succeed
        local code,ok = hb:checkAndPut(TABLE, rowKey, "info", "NotExistColumn", nil, {"info", "info1", "PPPPPPPP", "ver", "ver1", "QQQQQQQQ"})
        if not ok then
            ngx.say("ERROR: checkAndPut test failed. value nil, expect nil, should succeed, but failed with innercode="..code)
            return false
        end
        local ok = checkRow(TABLE, rowKey, 
        {
            "info",   "info1",         "PPPPPPPP",
            "ver",    "ver1",          "QQQQQQQQ",
        })
        if not ok then
            ngx.say("ERROR: checkAndPut test failed, check on nil. checkRow failed")
            return false
        end


        --10b. row didn't exist (so the value nil), expect nil, should succeed
        local code,ok = hb:checkAndPut(TABLE, rowKey_NotExist, "info", "info1", nil, {"info", "info1", "PPPPPPPP", "ver", "ver1", "QQQQQQQQ"})
        if not ok then
            ngx.say("ERROR: checkAndPut test failed. row didn't exist (so the value nil), expect nil, should succeed, but failed with innercode="..code)
            return false
        end
        local ok = checkRow(TABLE, rowKey_NotExist, 
        {
            "info",   "info1",         "PPPPPPPP",
            "ver",    "ver1",          "QQQQQQQQ",
        })
        if not ok then
            ngx.say("ERROR: checkAndPut test failed, check on nil (row didn't exist). checkRow failed")
            return false
        end

        --11. value not nil, expect nil, should fail
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "========Negative Test: checkAndPut the checked value is not nil")
        local code,ok = hb:checkAndPut(TABLE, rowKey, "info", "info1", nil, {"info", "info1", "12345678", "ver", "ver1", "ABCDEFGH"})
        if ok then
            ngx.say("ERROR: checkAndPut test failed. Value not nil, expect nil, should fail but it didn't")
            return false
        end
        if code~= "1121" then
            ngx.say("ERROR: checkAndPut test failed. Value not nil, expect nil, should fail with 1121 but it is: ", code)
            return false
        end

        -- the value should not be changed.
        local ok = checkRow(TABLE, rowKey,
        {
            "info",   "info1",         "PPPPPPPP",
            "ver",    "ver1",          "QQQQQQQQ",
        })
        if not ok then
            ngx.say("ERROR: checkAndPut test failed, check on nil. checkRow failed")
            return false
        end

        --12. value nil, expect not nil, should fail
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "========Negative Test: checkAndPut the checked value is not nil")
        local code,ok = hb:checkAndPut(TABLE, rowKey, "info", "NotExistColumnSoValIsNil", "ABC", {"info", "info1", "12345678", "ver", "ver1", "ABCDEFGH"})
        if ok then
            ngx.say("ERROR: checkAndPut test failed. Value nil, expect not nil, should fail but it didn't")
            return false
        end

        if code~= "1121" then
            ngx.say("ERROR: checkAndPut test failed. Value nil, expect not nil, should fail with 1121 but it is: ", code)
            return false
        end

        -- the value should not be changed.
        local ok = checkRow(TABLE, rowKey,
        {
            "info",   "info1",         "PPPPPPPP",
            "ver",    "ver1",          "QQQQQQQQ",
        })
        if not ok then
            ngx.say("ERROR: checkAndPut test failed, check on nil. checkRow failed")
            return false
        end

        --13. check on COUNTER value 
        local code,ok = hb:increment(TABLE, rowKey, {"stats", "objects", 10000,  "stats", "size_bytes", 100})
        local code,ok = hb:increment(TABLE, rowKey, {"stats", "objects", 10000,  "stats", "size_bytes", -100})

        local code,ok,ret = hb:get(TABLE, rowKey, {"stats:objects", "stats:size_bytes"})
        if not ok then
            ngx.say("ERROR: checkAndPut test failed, check on counter. prepare failed, get counters failed")
            return false
        end

        local stats_objects = ret["stats:objects"] 
        local stats_size_bytes = ret["stats:size_bytes"] 

        if verbose then
            ngx.say("stats_objects     : "..utils.stringify(stats_objects))
            ngx.say("stats_size_bytes  : "..utils.stringify(stats_size_bytes))
        end

        if stats_objects ~= 20000 or stats_size_bytes ~= 0 then
            ngx.say("ERROR: checkAndPut test failed, check on counter. prepare failed, check counters failed")
            return false
        end

        -- 13.a value not 0, expect correct value, should succeed
        local code,ok = hb:checkAndPut(TABLE, rowKey, "stats", "objects", stats_objects, {"info", "info1", "ValueNotZeroExpectCorrectValue"})
        if not ok then
            ngx.say("ERROR: checkAndPut test failed. value not 0, expect correct value. innercode="..code)
            return false
        end
        local ok = checkRow(TABLE, rowKey, 
        {
            "info",   "info1",         "ValueNotZeroExpectCorrectValue",
        })
        if not ok then
            ngx.say("ERROR: checkAndPut test failed, check on non-zero COUNTER. checkRow failed")
            return false
        end

        -- 13.b value not 0, expect incorrect value, should fail
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "========Negative Test: checkAndPut, value not 0, expect incorrect value")
        local code,ok = hb:checkAndPut(TABLE, rowKey, "stats", "objects", stats_objects-1, {"info", "info1", "ValueNotZeroExpectIncorrectValue"})
        if ok then
            ngx.say("ERROR: checkAndPut test failed. value not 0, expect incorrect value, should fail but it didn't")
            return false
        end

        if code ~= "1121" then
            ngx.say("ERROR: checkAndPut test failed. value not 0, expect incorrect value, should fail with 1121 but it is: ", code)
            return false
        end

        --value should not be chanted;
        local ok = checkRow(TABLE, rowKey, 
        {
            "info",   "info1",         "ValueNotZeroExpectCorrectValue",
        })
        if not ok then
            ngx.say("ERROR: checkAndPut test failed, value not 0, expect incorrect value, checkRow failed")
            return false
        end

        -- 13.c  value 0, expect 0, should succeed 
        local code,ok = hb:checkAndPut(TABLE, rowKey, "stats", "size_bytes", 0, {"info", "info2", "ValueZeroExpectZero"})
        if not ok then
            ngx.say("ERROR: checkAndPut test failed. value 0, expect 0. innercode="..code)
            return false
        end
        local ok = checkRow(TABLE, rowKey, 
        {
            "info",   "info2",         "ValueZeroExpectZero",
        })
        if not ok then
            ngx.say("ERROR: checkAndPut test failed, check on COUNTER 0. checkRow failed")
            return false
        end

        -- 13.d  value not 0, expect 0, should fail
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "========Negative Test: checkAndPut, value not 0, expect 0")
        local code,ok = hb:checkAndPut(TABLE, rowKey, "stats", "objects", 0, {"info", "info1", "ValueNotZeroExpectZero"})
        if ok then
            ngx.say("ERROR: checkAndPut test failed. value not 0, expect 0, should fail but it didn't")
            return false
        end
        if code ~= "1121" then
            ngx.say("ERROR: checkAndPut test failed. value not 0, expect 0, should fail with 1121 but it is:", code)
            return false
        end


        --value should not be changed
        local ok = checkRow(TABLE, rowKey, 
        {
            "info",   "info1",         "ValueNotZeroExpectCorrectValue",
        })
        if not ok then
            ngx.say("ERROR: checkAndPut test failed, value not 0, expect 0. checkRow failed")
            return false
        end

        -- 13.e  value 0, expect not 0, should fail
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "========Negative Test: checkAndPut, value 0, expect not 0")
        local code,ok = hb:checkAndPut(TABLE, rowKey, "stats", "size_bytes", 33, {"info", "info1", "ValueZeroExpectNotZero"})
        if ok then
            ngx.say("ERROR: checkAndPut test failed. value 0, expect not 0, should fail but it didn't")
            return false
        end
        if code ~= "1121" then
            ngx.say("ERROR: checkAndPut test failed. value 0, expect not 0, should fail with 1121 but it is: ", code)
            return false
        end

        --value should not be changed
        local ok = checkRow(TABLE, rowKey, 
        {
            "info",   "info1",         "ValueNotZeroExpectCorrectValue",
        })
        if not ok then
            ngx.say("ERROR: checkAndPut test failed, value 0, expect not 0, checkRow failed")
            return false
        end

        -- 13.f  value 0, expect nil, should fail 
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "========Negative Test: checkAndPut, value 0, expect nil")
        local code,ok = hb:checkAndPut(TABLE, rowKey, "stats", "size_bytes", nil, {"info", "info1", "ValueZeroExpectNil"})
        if ok then
            ngx.say("ERROR: checkAndPut test failed. value 0, expect nil, should fail but it succeeded")
            return false
        end

        if code ~= "1121" then
            ngx.say("ERROR: checkAndPut test failed. value 0, expect nil, should fail with 1121 but code=", code)
            return false
        end

        --value should not be changed
        local ok = checkRow(TABLE, rowKey, 
        {
            "info",   "info1",         "ValueNotZeroExpectCorrectValue",
        })
        if not ok then
            ngx.say("ERROR: checkAndPut test failed, value 0, expect nil, checkRow failed")
            return false
        end

        --- number/counter tests
        --- Tests 14-15 are for check-on-numbers/counters.
        ---      0   == nil    false 
        --       100 == nil    false
        ---      nil == 0      false 
        --       100 == 0      false
        ---      nil == nil    true 
        ---      0   == 0      true 

        local StatOfTestChkAndPut = {
            "info",    "info1",     "info11111111", 
            "quota",   "quota1",    "quota1111111",
        }
        --prepare
        local ok = putRow(TABLE, rowKeyCheckAndPut, StatOfTestChkAndPut)
        if not ok then
            ngx.say("ERROR: checkAndPut test failed, number/counter tests failed. prepare failed")
            return false
        end

        -- let stats:size_bytes = 0
        local code1,ok1 = hb:increment(TABLE, rowKeyCheckAndPut, {"stats", "size_bytes", 100})
        local code2,ok2 = hb:increment(TABLE, rowKeyCheckAndPut, {"stats", "size_bytes", -100})
        if not ok1 or not ok2 then
            ngx.say("checkAndPut test failed. COUNTER Test, prepare failed. code1=", code1, " code2=", code2)
            return false
        end

        local rowStatKeeper = 
        {
            "info",    "info1",     "info11111111", 
            "quota",   "quota1",    "quota1111111",
            "stats",       "size_bytes",    0,
            "stats",       "objects",       nil,
        }

        local ok = checkRow(TABLE, rowKeyCheckAndPut, rowStatKeeper)
        if not ok then
            ngx.say("checkAndPut test failed. COUNTER Test, prepare failed. checkRow failed")
            return false
        end

        --Test 14: Positive tests for numbers/counters;
        local positive_checks = 
        {
            {TABLE, rowKeyCheckAndPut, "stats", "objects",     nil},             -- nil == null/empty   true 
            {TABLE, rowKeyCheckAndPut, "stats", "size_bytes", 0},                -- 0 ==  0             true 
        }
        for i,chk in ipairs(positive_checks) do

            local v_info1    = uuid()
            local v_quota1   = uuid()

            local put14 = 
            {
                "info",       "info1",       v_info1,
                "quota",      "quota1",      v_quota1,
            }

            chk[6] = put14

            local code, ok = hb:checkAndPut(unpack(chk))
            if not ok then
                ngx.say("checkAndPut test failed. Test-14. HBase checkAndPut failed, innercode=", code, " i=", i)
                return false
            end

            rowStatKeeper = 
            {
                "info",       "info1",       v_info1,
                "quota",      "quota1",      v_quota1,
                "stats",      "size_bytes",  0,
                "stats",      "objects",     nil,
            }

            local ok = checkRow(TABLE, rowKeyCheckAndPut, rowStatKeeper)
            if not ok then
                ngx.say("checkAndPut test failed. Test-14. checkRow failed.")
                return false
            end
        end

        --Test 15: Negative tests for numbers/counters;
        local put15 = 
        {
            "info",       "info1",       "NotPut",
            "quota",      "quota1",      "NotPut",
        }
        local negative_checks = 
        {
            {TABLE, rowKeyCheckAndPut, "stats", "objects", 0,   put15},                 -- 0 ==  null/empty   false 
            {TABLE, rowKeyCheckAndPut, "stats", "objects", 100, put15},                 -- 100== null/empty   false 

            {TABLE, rowKeyCheckAndPut, "stats", "size_bytes", nil, put15},              --  nil == 0   false 
            {TABLE, rowKeyCheckAndPut, "stats", "size_bytes", 100, put15},              --  100 == 0   false 
        }

        for i,chk in ipairs(negative_checks) do
            local code, ok, result = hb:checkAndPut(unpack(chk))
            if ok then
                ngx.say("checkAndPut test failed. Test-15. HBase checkAndPut should fail but it succeeded, i=", i)
                return false
            end

            if code ~= "1121" then
                ngx.say("checkAndPut test failed. Test-15. HBase checkAndPut should fail with 1121 but code=", code, " i=", i)
                return false
            end

            local ok = checkRow(TABLE, rowKeyCheckAndPut, rowStatKeeper)
            if not ok then
                ngx.say("checkAndPut test failed. Test-15. checkRow failed, i=", i)
                return false
            end
        end
    end

    if clean or onlyClean then
        hb:delete(TABLE, rowKey)
        hb:delete(TABLE, rowKey_NotExist)
        hb:delete(TABLE, rowKeyCheckAndPut)
    end

    ngx.say("OK: checkAndPut test successful")
    ngx.flush()
    return true
end

function TestDelete()
    if not onlyClean then
        --1. args is not table, should fail
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "========Negative Test: delete, args is not a table")
        local code,ok = hb:delete(TABLE, rowKey, 33)
        if ok or code~= "1100" then
            ngx.say("ERROR: delete test failed. the args is not a table, it should not be allowed, but it was")
            return false
        end

        --2. args is nil, that is to delete the entire row, should succeed
        local rowKey = "Row0005"
        local args = {
            "info",    "info1",     "info11111111", 
            "quota",   "quota1",    "quota1111111",
        }
        --prepare
        local ok = putRow(TABLE, rowKey, args)
        if not ok then
            ngx.say("ERROR: delete test failed, args is nil, prepare failed")
            return false
        end

        local code,ok = hb:delete(TABLE, rowKey, nil)
        if not ok then
            ngx.say("ERROR: delete test failed. the args is nil, it should succeed and delete the entire row, but it didn't")
            return false
        end
        local ok = checkRow(TABLE, rowKey, 
        {
            "info",   "info1",         nil,
            "quota",   "quota1",       nil,
        })
        if not ok then
            ngx.say("ERROR: delete test failed, the args is nil, checkRow failed")
            return false
        end

        --3. args is an empty table, delete the entire row, should succeed
        local rowKey = "Row0006"
        local args = {
            "info",    "info1",     "info11111111", 
            "quota",   "quota1",    "quota1111111",
        }
        --prepare
        local ok = putRow(TABLE, rowKey, args)
        if not ok then
            ngx.say("ERROR: delete test failed, args is an empty table, prepare failed")
            return false
        end

        local code,ok = hb:delete(TABLE, rowKey, {})
        if not ok then
            ngx.say("ERROR: delete test failed. the args an empty table, it should succeed and delete the entire row, but it didn't")
            return false
        end
        local ok = checkRow(TABLE, rowKey, 
        {
            "info",   "info1",         nil,
            "quota",   "quota1",       nil,
        })
        if not ok then
            ngx.say("ERROR: delete test failed, the args is an emtpy table, checkRow failed")
            return false
        end

        --prepare for 4-10
        local rowKey = "Row0007"

        local args = {
            "info",    "info1",     "info11111111", 
            "info",    "info2",     "info22222222",
            "quota",   "quota1",    "quota1111111",
            "quota",   "quota2",    "quota2222222",
            "quota",   "quota3",    "quoto3333333",
            "ver",     "ver1",      "ver111111111",
            "ver",     "ver2",      "ver222222222",
            "exattrs", "exattrs1",  "exattrs11111",
            "exattrs", "exattrs2",  "exattrs22222",
            "exattrs", "exattrs3",  "exattrs33333",
            "exattrs", "exattrs4",  "exattrs44444",
        }

        local ok = putRow(TABLE, rowKey, args)
        if not ok then
            ngx.say("ERROR: delete test failed, prepare for 4-10 failed")
            return false
        end

        local code3,ok3 = hb:increment(TABLE, rowKey, {"stats", "objects", -1000})
        local code4,ok4 = hb:increment(TABLE, rowKey, {"stats", "objects", "1100", "stats", "size_bytes", 100})
        if not ok3 or not ok4 then
            ngx.say("ERROR: delete test failed, prepare for 4-10 failed, prepare counters. code2="..code2..", code3="..code..", code4="..code4)
            return false
        end

        local code,ok,ret = hb:get(TABLE, rowKey, {"stats:objects", "stats:size_bytes"})
        if not ok then
            ngx.say("ERROR: delete test failed, prepare for 4-10 failed, get counters failed")
            return false
        end

        local stats_objects = ret["stats:objects"]
        local stats_size_bytes = ret["stats:size_bytes"]

        if verbose then
            ngx.say("stats_objects     : "..utils.stringify(stats_objects))
            ngx.say("stats_size_bytes  : "..utils.stringify(stats_size_bytes))
        end

        if stats_objects ~= 100 or stats_size_bytes ~= 100 then
            ngx.say("ERROR: delete test failed, prepare for 4-10 failed, check counters failed")
            return false
        end

        --4. delete a cell
        local code,ok = hb:delete(TABLE, rowKey, {"info:info1"})
        if not ok then
            ngx.say("ERROR: delete test failed. delete a cell failed, innercode="..code)
            return false
        end
        local ok = checkRow(TABLE, rowKey, 
        {
            "info",   "info1",      nil,
            "info",    "info2",     "info22222222",
            "quota",   "quota1",    "quota1111111",
            "quota",   "quota2",    "quota2222222",
            "quota",   "quota3",    "quoto3333333",
            "ver",     "ver1",      "ver111111111",
            "ver",     "ver2",      "ver222222222",
            "exattrs", "exattrs1",  "exattrs11111",
            "exattrs", "exattrs2",  "exattrs22222",
            "exattrs", "exattrs3",  "exattrs33333",
            "exattrs", "exattrs4",  "exattrs44444",
            "stats", "objects",     100, 
            "stats", "size_bytes",  100,
        })
        if not ok then
            ngx.say("ERROR: delete test failed, delete a cell, checkRow failed")
            return false
        end

        --5. delete a counter 
        local code,ok = hb:delete(TABLE, rowKey, {"stats:objects"})
        if not ok then
            ngx.say("ERROR: delete test failed. delete a COUNTER failed, innercode="..code)
            return false
        end
        local ok = checkRow(TABLE, rowKey, 
        {
            "info",   "info1",      nil,
            "info",    "info2",     "info22222222",
            "quota",   "quota1",    "quota1111111",
            "quota",   "quota2",    "quota2222222",
            "quota",   "quota3",    "quoto3333333",
            "ver",     "ver1",      "ver111111111",
            "ver",     "ver2",      "ver222222222",
            "exattrs", "exattrs1",  "exattrs11111",
            "exattrs", "exattrs2",  "exattrs22222",
            "exattrs", "exattrs3",  "exattrs33333",
            "exattrs", "exattrs4",  "exattrs44444",
            "stats", "objects",     nil, 
            "stats", "size_bytes",  100,
        })
        if not ok then
            ngx.say("ERROR: delete test failed, delete a COUNTER, checkRow failed")
            return false
        end

        --6. delete a family
        local code,ok = hb:delete(TABLE, rowKey, {"quota"})
        if not ok then
            ngx.say("ERROR: delete test failed. delete a family, innercode="..code)
            return false
        end
        local ok = checkRow(TABLE, rowKey, 
        {
            "info",   "info1",      nil,
            "info",    "info2",     "info22222222",
            "quota",   "quota1",    nil,
            "quota",   "quota2",    nil,
            "quota",   "quota3",    nil,
            "ver",     "ver1",      "ver111111111",
            "ver",     "ver2",      "ver222222222",
            "exattrs", "exattrs1",  "exattrs11111",
            "exattrs", "exattrs2",  "exattrs22222",
            "exattrs", "exattrs3",  "exattrs33333",
            "exattrs", "exattrs4",  "exattrs44444",
            "stats", "objects",     nil, 
            "stats", "size_bytes",  100,
        })
        if not ok then
            ngx.say("ERROR: delete test failed, delete a family, checkRow failed")
            return false
        end

        --7. delete 2 families and cells 
        local code,ok = hb:delete(TABLE, rowKey, {"stats", "info", "exattrs:exattrs1", "ver:ver1"})
        if not ok then
            ngx.say("ERROR: delete test failed. delete 2 families and other cells, innercode="..code)
            return false
        end
        local ok = checkRow(TABLE, rowKey, 
        {
            "info",   "info1",      nil,
            "info",    "info2",     nil,
            "quota",   "quota1",    nil,
            "quota",   "quota2",    nil,
            "quota",   "quota3",    nil,
            "ver",     "ver1",      nil,
            "ver",     "ver2",      "ver222222222",
            "exattrs", "exattrs1",  nil,
            "exattrs", "exattrs2",  "exattrs22222",
            "exattrs", "exattrs3",  "exattrs33333",
            "exattrs", "exattrs4",  "exattrs44444",
            "stats", "objects",     nil, 
            "stats", "size_bytes",  nil,
        })
        if not ok then
            ngx.say("ERROR: delete test failed, delete 2 families and other cells, checkRow failed")
            return false
        end

        --8. delete a cell that doesn't exist, should succeed
        local code,ok = hb:delete(TABLE, rowKey, {"exattrs:NotExistColumn", "ver:ver2"})
        if not ok then
            ngx.say("ERROR: delete test failed. delete a column that doesn't exist failed, innercode="..code)
            return false
        end
        local ok = checkRow(TABLE, rowKey, 
        {
            "info",   "info1",      nil,
            "info",    "info2",     nil,
            "quota",   "quota1",    nil,
            "quota",   "quota2",    nil,
            "quota",   "quota3",    nil,
            "ver",     "ver1",      nil,
            "ver",     "ver2",      nil,
            "exattrs", "exattrs1",  nil,
            "exattrs", "exattrs2",  "exattrs22222",
            "exattrs", "exattrs3",  "exattrs33333",
            "exattrs", "exattrs4",  "exattrs44444",
            "stats", "objects",     nil, 
            "stats", "size_bytes",  nil,
        })
        if not ok then
            ngx.say("ERROR: delete test failed. delete a column that doesn't exist, checkRow failed")
            return false
        end

        --9. delete a family that doesn't exist, should succeed. NOTE, although succeed, others (e.g. "exattrs:exattrs2" below) are not deleted
        local code,ok = hb:delete(TABLE, rowKey, {"NotExistFamily", "exattrs:exattrs2"})
        if not ok then
            ngx.say("ERROR: delete test failed. delete a family that doesn't exist failed, innercode="..code)
            return false
        end
        local ok = checkRow(TABLE, rowKey, 
        {
            "info",   "info1",      nil,
            "info",    "info2",     nil,
            "quota",   "quota1",    nil,
            "quota",   "quota2",    nil,
            "quota",   "quota3",    nil,
            "ver",     "ver1",      nil,
            "ver",     "ver2",      nil,
            "exattrs", "exattrs1",  nil,
            "exattrs", "exattrs2",  "exattrs22222", --Yuanguo: although HBase didn't throw out error, "exattrs:exattrs2" is not deleted
            "exattrs", "exattrs3",  "exattrs33333",
            "exattrs", "exattrs4",  "exattrs44444",
            "stats", "objects",     nil, 
            "stats", "size_bytes",  nil,
        })
        if not ok then
            ngx.say("ERROR: delete test failed. delete a family that doesn't exist, checkRow failed")
            return false
        end

        --10. delete a row that doesn't exist, should succeed.   Nothing happened here, although HBase didn't throw out error
        local code,ok = hb:delete(TABLE, "NotExistRow", {})
        if not ok then
            ngx.say("ERROR: delete test failed. delete a family that doesn't exist failed, innercode="..code)
            return false
        end
    end

    if clean or onlyClean then
        for k = 5, 7 do
            local key = "Row000"..tostring(k)
            hb:delete(TABLE, key)
        end
    end

    ngx.say("OK: delete test successful")
    ngx.flush()
    return true
end


function TestCheckAndDelete()
    if not onlyClean then
        --prepare for 1-2
        local rowKey = "Row0008"
        local args = {
            "info",    "info1",     "info11111111", 
            "info",    "info2",     "info22222222",
        }

        local ok = putRow(TABLE, rowKey, args)
        if not ok then
            ngx.say("ERROR: checkAndDelete test failed, prepare for 1-2 failed")
            return false
        end

        --1. args is not a table, should fail, code = "1100" 
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "========Negative Test: checkAndDelete, args is not a table")
        local code,ok = hb:checkAndDelete(TABLE, rowKey, "info", "info1", "info11111111", 55)
        if ok  or code ~= "1100" then
            ngx.say("ERROR: checkAndDelete test failed. args is not a table, it should not be allowed, but it was")
            return false
        end

        --2. args is nil, that is to delete the entire row, should succeed
        local code,ok = hb:checkAndDelete(TABLE, rowKey, "info", "info1", "info11111111", nil)
        if not ok then
            ngx.say("ERROR: checkAndDelete test failed. args is nil, innercode="..code)
            return false
        end
        local ok = checkRow(TABLE, rowKey, 
        {
            "info",   "info1",      nil,
            "info",    "info2",     nil,
        })
        if not ok then
            ngx.say("ERROR: checkAndDelete test failed. args is nil, checkRow failed")
            return false
        end

        --3. value nil, expect nil, args is an empty table, should succeed and delete the entire row  
        local rowKey = "Row0009"
        local args = {
            "info",    "info1",     "info11111111", 
            "quota",   "quota1",    "quota1111111",
            "ver",     "ver2",      "ver222222222",
            "exattrs", "exattrs2",  "exattrs22222",
        }

        local ok = putRow(TABLE, rowKey, args)
        if not ok then
            ngx.say("ERROR: checkAndDelete test failed, prepare for 3 failed")
            return false
        end

        local code,ok = hb:checkAndDelete(TABLE, rowKey, "info", "NotExistColumnSoValIsNil", nil, {})
        if not ok then
            ngx.say("ERROR: checkAndDelete test failed. args is an empty table, innercode="..code)
            return false
        end
        local ok = checkRow(TABLE, rowKey, 
        {
            "info",    "info1",     nil, 
            "quota",   "quota1",    nil,
            "ver",     "ver2",      nil,
            "exattrs", "exattrs2",  nil,
        })
        if not ok then
            ngx.say("ERROR: checkAndDelete test failed. args is an empty table, checkRow failed")
            return false
        end

        --4. check on COUNTER value, delete 2 families and cells 
        local rowKey = "Row0010"

        local args = {
            "info",    "info1",     "info11111111", 
            "quota",   "quota1",    "quota1111111",
            "quota",   "quota2",    "quota2222222",
            "ver",     "ver1",      "ver111111111",
            "exattrs", "exattrs1",  "exattrs11111",
            "exattrs", "exattrs2",  "exattrs22222",
            "exattrs", "exattrs4",  "exattrs44444",
        }

        local ok = putRow(TABLE, rowKey, args)
        if not ok then
            ngx.say("ERROR: checkAndDelete test failed, prepare for 4 failed")
            return false
        end

        local code1,ok1 = hb:increment(TABLE, rowKey, {"stats", "objects", 0})
        local code2,ok2 = hb:increment(TABLE, rowKey, {"stats", "objects", "10000",  "stats", "size_bytes", 500})

        local code,ok,ret = hb:get(TABLE, rowKey, {"stats:objects", "stats:size_bytes"})
        if not ok then
            ngx.say("ERROR: checkAndDelete test failed, prepare for 4 failed, get counters failed")
            return false
        end

        local stats_objects= ret["stats:objects"]
        local stats_size_bytes = ret["stats:size_bytes"]

        if verbose then
            ngx.say("stats_objects     : "..utils.stringify(stats_objects))
            ngx.say("stats_size_bytes  : "..utils.stringify(stats_size_bytes))
        end

        if stats_objects ~= 10000 or stats_size_bytes ~= 500 then
            ngx.say("ERROR: checkAndDelete test failed, prepare for 4 failed, check counters failed")
            return false
        end

        local code,ok = hb:checkAndDelete(TABLE, rowKey, "stats", "size_bytes", 500, {"info", "quota", "exattrs:exattrs1"})
        if not ok then
            ngx.say("ERROR: checkAndDelete test failed. check on COUNTER value, innercode="..code)
            return false
        end
        local ok = checkRow(TABLE, rowKey, 
        {
            "info",    "info1",     nil, 
            "quota",   "quota1",    nil,
            "quota",   "quota2",    nil,
            "ver",     "ver1",      "ver111111111",
            "exattrs", "exattrs1",  nil,
            "exattrs", "exattrs2",  "exattrs22222",
            "exattrs", "exattrs4",  "exattrs44444",
        })
        if not ok then
            ngx.say("ERROR: checkAndDelete test failed. check on COUNTER value, checkRow failed")
            return false
        end

        --5. failure check 
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "========Negative Test: checkAndDelete, value is incorrect")
        local code,ok = hb:checkAndDelete(TABLE, rowKey, "stats", "size_bytes", 10, {"exattrs:exattrs2"})
        if ok then
            ngx.say("ERROR: checkAndDelete test failed. value is incorrect, should fail, but it didn't")
            return false
        end

        if code ~= "1121" then
            ngx.say("ERROR: checkAndDelete test failed. value is incorrect, should fail with 1121 but it is: ", code)
            return false
        end

        --values are not changed
        local ok = checkRow(TABLE, rowKey, 
        {
            "exattrs", "exattrs2",  "exattrs22222",
        })
        if not ok then
            ngx.say("ERROR: checkAndDelete test failed. value is incorrect, checkRow failed")
            return false
        end
    end

    if clean or onlyClean then
        hb:delete(TABLE, "Row0008")
        hb:delete(TABLE, "Row0009")
        hb:delete(TABLE, "Row0010")
    end

    ngx.say("OK: checkAndDelete test successful")
    ngx.flush()
    return true
end


function TestMutateRow()

    local rowKey = "Row0011"
    if not onlyClean then
        local args = {
            "info",    "info1",     "info11111111", 
            "info",    "info2",     "info22222222",
            "quota",   "quota1",    "quota1111111",
            "quota",   "quota2",    "quota2222222",
            "quota",   "quota3",    "quoto3333333",
            "ver",     "ver1",      "ver111111111",
            "ver",     "ver2",      "ver222222222",
            "exattrs", "exattrs1",  "exattrs11111",
            "exattrs", "exattrs2",  "exattrs22222",
            "exattrs", "exattrs3",  "exattrs33333",
            "exattrs", "exattrs4",  "exattrs44444",
            --"stats", "size_bytes", 0
            --"stats", "objects",    0
        }

        --prepare
        local ok = putRow(TABLE, rowKey, args)
        if not ok then
            ngx.say("ERROR: TestMutateRow test failed, prepare failed")
            return false
        end

        local code1,ok1 = hb:increment(TABLE, rowKey, {"stats", "objects", 0})
        local code2,ok2 = hb:increment(TABLE, rowKey, {"stats", "objects", "10000",  "stats", "size_bytes", 500})
        local ok = checkRow(TABLE, rowKey, 
        {
            "stats", "objects",    10000, 
            "stats", "size_bytes", 500, 
        })
        if not ok then
            ngx.say("ERROR: mutateRow test failed. check counters failed")
            return false
        end

        --1. put and del are all nil or empty
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "========Negative Test: mutateRow, put nil del nil")
        local code1, ok1 = hb:mutateRow(TABLE, rowKey, nil, nil)
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "========Negative Test: mutateRow, put empty del nil")
        local code2, ok2 = hb:mutateRow(TABLE, rowKey, {},  nil)
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "========Negative Test: mutateRow, put empty del empty")
        local code3, ok3 = hb:mutateRow(TABLE, rowKey, {},  {})
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "========Negative Test: mutateRow, put nil del empty")
        local code4, ok4 = hb:mutateRow(TABLE, rowKey, nil,  {})
        if ok1 or ok2 or ok3 or ok4 or
            code1 ~= "1100" or
            code2 ~= "1100" or
            code3 ~= "1100" or
            code4 ~= "1100" then
            ngx.say("mutateRow test failed, both put and del are nil/empty should NOT be allowed, but it was.")
            return false
        end

        --2. put COUNTER, should fail
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "========Negative Test: mutateRow, put COUNTER value")
        local put = {"info", "info1", "AAAAAAAA", "ver", "ver1", "BBBBBBBB", "ver", "ver2", "CCCCCCCC", "stats", "size_bytes", 500}
        local code, ok = hb:mutateRow(TABLE, rowKey, put)
        if ok or code ~= "1110" then
            ngx.say("mutateRow test failed, only put, innercode="..code)
            return false
        end

        --3. put and delete have intersections
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "========Negative Test: mutateRow, put and delete have intersections")
        local code1, ok1 = hb:mutateRow(TABLE, rowKey, {"info", "info1", "XXXXXXXX"},{"ver", "info", "quota:quota1"})
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "========Negative Test: mutateRow, put and delete have intersections")
        local code2, ok2 = hb:mutateRow(TABLE, rowKey, {"info", "info1", "XXXXXXXX"},{"ver", "info:info1", "quota:quota1"})
        if ok1 or ok2 or code1 ~= "1100" or code2 ~= "1100" then
            ngx.say("mutateRow test failed, put and delete have intersections, it shouldn't be allowed, but it was")
            return false
        end

        --4. put only
        local put = {"info", "info1", "AAAAAAAA", "ver", "ver1", "BBBBBBBB", "ver", "ver2", "CCCCCCCC"}
        local code, ok = hb:mutateRow(TABLE, rowKey, put, {})
        if not ok then
            ngx.say("mutateRow test failed, only put, innercode="..code)
            return false
        end
        local ok = checkRow(TABLE, rowKey, 
        {
            "info", "info1", "AAAAAAAA", 
            "info",    "info2",     "info22222222",
            "quota",   "quota1",    "quota1111111",
            "quota",   "quota2",    "quota2222222",
            "quota",   "quota3",    "quoto3333333",
            "ver",  "ver1",  "BBBBBBBB", 
            "ver",  "ver2",  "CCCCCCCC",
            "exattrs", "exattrs1",  "exattrs11111",
            "exattrs", "exattrs2",  "exattrs22222",
            "exattrs", "exattrs3",  "exattrs33333",
            "exattrs", "exattrs4",  "exattrs44444",
        })
        if not ok then
            ngx.say("ERROR: mutateRow test failed. put only, checkRow failed")
            return false
        end

        --5. delete only
        local del = {"info", "ver:ver1", "stats:size_bytes"}
        local code, ok = hb:mutateRow(TABLE, rowKey, nil, del)
        if not ok then
            ngx.say("mutateRow test failed, delete only, innercode="..code)
            return false
        end
        local ok = checkRow(TABLE, rowKey, 
        {
            "info", "info1", nil, 
            "info", "info2", nil, 
            "ver",  "ver1",  nil, 
            "stats", "size_bytes", nil,
        })
        if not ok then
            ngx.say("ERROR: mutateRow test failed. delete only, checkRow failed")
            return false
        end

        --6. counter that's deleted can be incremented
        local code1,ok1 = hb:increment(TABLE, rowKey, {"stats", "objects", "10000",  "stats", "size_bytes", 500})
        local ok = checkRow(TABLE, rowKey, 
        {
            "stats", "size_bytes", 500, 
        })
        if not ok then
            ngx.say("ERROR: mutateRow test failed. increment a deleted COUNTER, checkRow failed")
            return false
        end

        --7. both put and del
        local put = {"quota", "quota1", "TTTTTTTT", "exattrs", "exattrs1", "PPPPPPPP"}
        local del = {"info", "ver:ver1", "stats:size_bytes"}
        local code, ok = hb:mutateRow(TABLE, rowKey, put, del)
        if not ok then
            ngx.say("mutateRow test failed, put and delete, innercode="..code)
            return false
        end
        local ok = checkRow(TABLE, rowKey, 
        {
            "quota",   "quota1",     "TTTTTTTT",
            "exattrs", "exattrs1",   "PPPPPPPP",
            "info",    "info1",      nil, 
            "info",    "info2",      nil, 
            "ver",     "ver1",       nil,
            "stats",   "size_bytes", nil,
        })
        if not ok then
            ngx.say("ERROR: mutateRow test failed. delete only, checkRow failed")
            return false
        end
    end

    if clean or onlyClean then
        hb:delete(TABLE, rowKey)
    end

    ngx.say("OK: TestMutateRow test successful")
    ngx.flush()
    return true
end

function TestCheckAndMutateRow()
    local rowKey001 = "TestCheckAndMutateRow_Row001"
    local rowKey002 = "TestCheckAndMutateRow_Row002"
    local rowKey003 = "TestCheckAndMutateRow_Row003"
    if not onlyClean then

        --prepare
        local args = {
            "info",    "info1",     "AAAA12345678", 
            "info",    "info2",     "info22222222",
            "quota",   "quota1",    "quota1111111",
            "quota",   "quota2",    "quota2222222",
            "quota",   "quota3",    "quoto3333333",
            "ver",     "ver1",      "ver111111111",
            "ver",     "ver2",      "ver222222222",
            "exattrs", "exattrs1",  "exattrs11111",
            "exattrs", "exattrs2",  "exattrs22222",
            "exattrs", "exattrs3",  "exattrs33333",
            "exattrs", "exattrs4",  "exattrs44444",
        }

        local ok = putRow(TABLE, rowKey001, args)
        if not ok then
            ngx.say("ERROR: TestMutateRow test failed, prepare failed")
            return false
        end

        local code1,ok1 = hb:increment(TABLE, rowKey001, {"stats", "objects", 0})
        local code2,ok2 = hb:increment(TABLE, rowKey001, {"stats", "objects", "10000",  "stats", "size_bytes", 500})
        local ok = checkRow(TABLE, rowKey001, 
        {
            "stats", "objects",    10000, 
            "stats", "size_bytes", 500, 
        })
        if not ok then
            ngx.say("ERROR: checkAndMutateRow test failed. check counters failed")
            return false
        end

        --1. put and del are all nil or empty
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "========Negative Test: checkAndMutateRow, put nil del nil")
        local code1, ok1 = hb:checkAndMutateRow(TABLE, rowKey001, "info", "info1", hb.CompareOp.EQUAL, "info11111111", nil, nil)
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "========Negative Test: checkAndMutateRow, put empty del nil")
        local code2, ok2 = hb:checkAndMutateRow(TABLE, rowKey001, "info", "info1", hb.CompareOp.EQUAL, "info11111111", {},  nil)
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "========Negative Test: checkAndMutateRow, put empty del empty")
        local code3, ok3 = hb:checkAndMutateRow(TABLE, rowKey001, "info", "info1", hb.CompareOp.EQUAL, "info11111111", {},  {})
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "========Negative Test: checkAndMutateRow, put nil del empty")
        local code4, ok4 = hb:checkAndMutateRow(TABLE, rowKey001, "info", "info1", hb.CompareOp.EQUAL, "info11111111", nil,  {})
        if ok1 or ok2 or ok3 or ok4 or
            code1 ~= "1100" or
            code2 ~= "1100" or
            code3 ~= "1100" or
            code4 ~= "1100" then
            ngx.say("checkAndMutateRow test failed, both put and del are nil/empty should NOT be allowed, but it was.")
            return false
        end

        --2. put COUNTER, should fail
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "========Negative Test: checkAndMutateRow, put COUNTER value")
        local put = {"info", "info1", "AAAAAAAA", "ver", "ver1", "BBBBBBBB", "ver", "ver2", "CCCCCCCC", "stats", "size_bytes", 500}
        local code, ok = hb:checkAndMutateRow(TABLE, rowKey001, "info", "info1", hb.CompareOp.EQUAL, "info11111111", put)
        if ok or code ~= "1110" then
            ngx.say("checkAndMutateRow test failed, put COUNTER value, innercode="..code)
            return false
        end

        --3. put and delete have intersections
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "========Negative Test: checkAndMutateRow, put and delete have intersections")
        local code1, ok1 = hb:checkAndMutateRow(TABLE, rowKey001, "info", "info1", hb.CompareOp.EQUAL, "info11111111", {"info", "info1", "XXXXXXXX"},{"ver", "info", "quota:quota1"})
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "========Negative Test: checkAndMutateRow, put and delete have intersections")
        local code2, ok2 = hb:checkAndMutateRow(TABLE, rowKey001, "info", "info1", hb.CompareOp.EQUAL, "info11111111", {"info", "info1", "XXXXXXXX"},{"ver", "info:info1", "quota:quota1"})
        if ok1 or ok2 or code1 ~= "1100" or code2 ~= "1100" then
            ngx.say("checkAndMutateRow test failed, put and delete have intersections, it shouldn't be allowed, but it was")
            return false
        end

        --4. put only
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "========Positive Test: checkAndMutateRow, 'AAAA12345678' >= 'AAAA12345678'")
        local put = {"info", "info1", "AAAAAAAA", "ver", "ver1", "BBBBBBBB", "ver", "ver2", "CCCCCCCC"}
        local code, ok = hb:checkAndMutateRow(TABLE, rowKey001, "info", "info1", hb.CompareOp.GREATER_OR_EQUAL, "AAAA12345678", put, {})
        if not ok then
            ngx.say("checkAndMutateRow test failed, only put, innercode="..code)
            return false
        end
        local ok = checkRow(TABLE, rowKey001, 
        {
            "info", "info1", "AAAAAAAA", 
            "info",    "info2",     "info22222222",
            "quota",   "quota1",    "quota1111111",
            "quota",   "quota2",    "quota2222222",
            "quota",   "quota3",    "quoto3333333",
            "ver",  "ver1",  "BBBBBBBB", 
            "ver",  "ver2",  "CCCCCCCC",
            "exattrs", "exattrs1",  "exattrs11111",
            "exattrs", "exattrs2",  "exattrs22222",
            "exattrs", "exattrs3",  "exattrs33333",
            "exattrs", "exattrs4",  "exattrs44444",
            "stats",   "objects",    10000, 
            "stats",   "size_bytes", 500, 
        })
        if not ok then
            ngx.say("ERROR: checkAndMutateRow test failed. put only, checkRow failed")
            return false
        end

        --5. delete only
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "========Positive Test: checkAndMutateRow, 'BBBBBCCC' >= 'BBBBBBBB'")
        local del = {"info", "ver:ver1", "stats:size_bytes"}
        local code, ok = hb:checkAndMutateRow(TABLE, rowKey001, "ver", "ver1", hb.CompareOp.GREATER_OR_EQUAL, "BBBBBCCC", nil, del)
        if not ok then
            ngx.say("checkAndMutateRow test failed, delete only, innercode="..code)
            return false
        end
        local ok = checkRow(TABLE, rowKey001, 
        {
            "info",    "info1",     nil, 
            "info",    "info2",     nil,
            "quota",   "quota1",    "quota1111111",
            "quota",   "quota2",    "quota2222222",
            "quota",   "quota3",    "quoto3333333",
            "ver",  "ver1",  nil, 
            "ver",  "ver2",  "CCCCCCCC",
            "exattrs", "exattrs1",  "exattrs11111",
            "exattrs", "exattrs2",  "exattrs22222",
            "exattrs", "exattrs3",  "exattrs33333",
            "exattrs", "exattrs4",  "exattrs44444",
            "stats",   "objects",    10000, 
            "stats",   "size_bytes", nil, 
        })
        if not ok then
            ngx.say("ERROR: checkAndMutateRow test failed. delete only, checkRow failed")
            return false
        end

        --6. counter that's deleted can be incremented
        local code1,ok1 = hb:increment(TABLE, rowKey001, {"stats", "objects", "10000",  "stats", "size_bytes", 500})
        local ok = checkRow(TABLE, rowKey001, 
        {
            "info",    "info1",     nil, 
            "info",    "info2",     nil,
            "quota",   "quota1",    "quota1111111",
            "quota",   "quota2",    "quota2222222",
            "quota",   "quota3",    "quoto3333333",
            "ver",  "ver1",  nil, 
            "ver",  "ver2",  "CCCCCCCC",
            "exattrs", "exattrs1",  "exattrs11111",
            "exattrs", "exattrs2",  "exattrs22222",
            "exattrs", "exattrs3",  "exattrs33333",
            "exattrs", "exattrs4",  "exattrs44444",
            "stats",   "objects",    20000, 
            "stats",   "size_bytes", 500, 
        })
        if not ok then
            ngx.say("ERROR: checkAndMutateRow test failed. increment a deleted COUNTER, checkRow failed")
            return false
        end

        --7. both put and del
        local put = {"quota", "quota1", "TTTTTTTT", "exattrs", "exattrs1", "PPPPPPPP"}
        local del = {"info", "ver:ver1"}
        local code, ok = hb:checkAndMutateRow(TABLE, rowKey001, "stats", "size_bytes", hb.CompareOp.GREATER_OR_EQUAL, 501, put, del)
        if not ok then
            ngx.say("checkAndMutateRow test failed, put and delete, innercode="..code)
            return false
        end
        local ok = checkRow(TABLE, rowKey001, 
        {
            "info",    "info1",     nil, 
            "info",    "info2",     nil,
            "quota",   "quota1",    "TTTTTTTT",
            "quota",   "quota2",    "quota2222222",
            "quota",   "quota3",    "quoto3333333",
            "ver",  "ver1",  nil, 
            "ver",  "ver2",  "CCCCCCCC",
            "exattrs", "exattrs1",  "PPPPPPPP",
            "exattrs", "exattrs2",  "exattrs22222",
            "exattrs", "exattrs3",  "exattrs33333",
            "exattrs", "exattrs4",  "exattrs44444",
            "stats",   "objects",    20000, 
            "stats",   "size_bytes", 500, 

        })
        if not ok then
            ngx.say("ERROR: checkAndMutateRow test failed. delete only, checkRow failed")
            return false
        end


        --- Above tests are for official version of hbase ----
        --- Tests 8-17 are for Yuanguo's modifications: comparison about null/empty are like this:
        --            null/empty <  null/empty      false
        --            null/empty <= null/empty      true
        --            null/empty == null/empty      true
        --            null/empty != null/empty      false
        --            null/empty >= null/empty      true
        --            null/empty >  null/empty      false
        --            
        --            "AnyString" <  null/empty      false
        --            "AnyString" <= null/empty      false
        --            "AnyString" == null/empty      false
        --            "AnyString" != null/empty      true
        --            "AnyString" >= null/empty      true
        --            "AnyString" >  null/empty      true
        --            
        --            null/empty <  "AnyString"     true
        --            null/empty <= "AnyString"     true
        --            null/empty == "AnyString"     false
        --            null/empty != "AnyString"     true
        --            null/empty >= "AnyString"     false
        --            null/empty >  "AnyString"     false

        --Test-8: Positive, null/empty <= null/empty (non-existent row)
        local put8 = 
        {
            "quota", "quota1", "TTTTTTTT", 
            "exattrs", "exattrs1", "PPPPPPPP"
        }
        local del8 = {"info", "ver:ver1"}
        local code, ok = hb:checkAndMutateRow(TABLE, rowKey002, "info", "info2", hb.CompareOp.LESS_OR_EQUAL, nil, put8, del8)
        if not ok then
            ngx.say("checkAndMutateRow test failed. Test-8. HBase checkAndMutateRow failed, innercode=", code)
            return false
        end
        local ok = checkRow(TABLE, rowKey002, 
        {
            "info",      "info2",     nil,
            "quota",     "quota1",    "TTTTTTTT", 
            "exattrs",   "exattrs1",  "PPPPPPPP"
        })
        if not ok then
            ngx.say("checkAndMutateRow test failed. Test-8. checkRow failed.")
            return false
        end

        --Test-9: Positive, "XX" >= null/empty (non-existent column)
        local put9 = 
        {
            "info",       "info1",       "info1-9",
            "quota",      "quota1",      "quota1-9", 
            "exattrs",    "exattrs1",    "exattrs1-9",
        }
        local del9 = {}
        local code, ok = hb:checkAndMutateRow(TABLE, rowKey002, "info", "NotExistColumn", hb.CompareOp.GREATER_OR_EQUAL, "XX", put9, del9)
        if not ok then
            ngx.say("checkAndMutateRow test failed. Test-9. HBase checkAndMutateRow failed, innercode=", code)
            return false
        end
        local ok = checkRow(TABLE, rowKey002, 
        {
            "info",       "info1",       "info1-9",
            "quota",      "quota1",      "quota1-9", 
            "exattrs",    "exattrs1",    "exattrs1-9",
        })
        if not ok then
            ngx.say("checkAndMutateRow test failed. Test-9. checkRow failed.")
            return false
        end

        --Test-10: Positive, "XX" > null/empty (non-existent row)
        local put10 = 
        {
            "info",       "info1",       "info1-10",
            "quota",      "quota1",      "quota1-10", 
            "exattrs",    "exattrs1",    "exattrs1-10",
        }
        local del10 = {"ver:ver1"}
        local code, ok = hb:checkAndMutateRow(TABLE, rowKey003, "info", "info2", hb.CompareOp.GREATER, "XX", put10, del10)
        if not ok then
            ngx.say("checkAndMutateRow test failed. Test-10. HBase checkAndMutateRow failed, innercode=", code)
            return false
        end
        local ok = checkRow(TABLE, rowKey003, 
        {
            "info",       "info1",       "info1-10",
            "quota",      "quota1",      "quota1-10", 
            "exattrs",    "exattrs1",    "exattrs1-10",
            "ver",        "ver1",        nil,
        })
        if not ok then
            ngx.say("checkAndMutateRow test failed. Test-10. checkRow failed.")
            return false
        end


        --Test-11: Positive, nil == null/empty (non-existent Column)
        local put11 = 
        {
            "quota",      "quota1",      "quota1-11", 
            "exattrs",    "exattrs1",    "exattrs1-11",
            "ver",        "ver1",        "ver1-11",
        }
        local del11 = {"info"}
        local code, ok = hb:checkAndMutateRow(TABLE, rowKey003, "info", "NotExistColumn", hb.CompareOp.EQUAL, nil, put11, del11)
        if not ok then
            ngx.say("checkAndMutateRow test failed. Test-11. HBase checkAndMutateRow failed, innercode=", code)
            return false
        end
        local ok = checkRow(TABLE, rowKey003, 
        {
            "quota",      "quota1",      "quota1-11", 
            "exattrs",    "exattrs1",    "exattrs1-11",
            "ver",        "ver1",        "ver1-11",
        })
        if not ok then
            ngx.say("checkAndMutateRow test failed. Test-11. checkRow failed.")
            return false
        end

        --Test-12: Positive, nul >= null/empty (non-existent Column)
        local put12 = 
        {
            "info",       "info1",       "info1-12",
            "exattrs",    "exattrs1",    "exattrs1-12",
            "ver",        "ver1",        "ver1-12",
        }
        local del12 = nil
        local code, ok = hb:checkAndMutateRow(TABLE, rowKey003, "info", "NotExistColumn", hb.CompareOp.GREATER_OR_EQUAL, nil, put12, del12)
        if not ok then
            ngx.say("checkAndMutateRow test failed. Test-12. HBase checkAndMutateRow failed, innercode=", code)
            return false
        end
        local ok = checkRow(TABLE, rowKey003, 
        {
            "quota",      "quota1",      "quota1-11", 
            "info",       "info1",       "info1-12",
            "exattrs",    "exattrs1",    "exattrs1-12",
            "ver",        "ver1",        "ver1-12",
        })
        if not ok then
            ngx.say("checkAndMutateRow test failed. Test-12. checkRow failed.")
            return false
        end

        --Test-13: Positive, "XX" != null/empty (non-existent Column)
        local put13 = 
        {
            "info",       "info1",       "info1-13",
            "exattrs",    "exattrs1",    "exattrs1-13",
            "ver",        "ver1",        "ver1-13",
        }
        local del13 = {"quota:quota1"} 
        local code, ok = hb:checkAndMutateRow(TABLE, rowKey003, "info", "NotExistColumn", hb.CompareOp.NOT_EQUAL, "XX", put13, del13)
        if not ok then
            ngx.say("checkAndMutateRow test failed. Test-13. HBase checkAndMutateRow failed, innercode=", code)
            return false
        end
        local ok = checkRow(TABLE, rowKey003, 
        {
            "info",       "info1",       "info1-13",
            "exattrs",    "exattrs1",    "exattrs1-13",
            "ver",        "ver1",        "ver1-13",
        })
        if not ok then
            ngx.say("checkAndMutateRow test failed. Test-13. checkRow failed.")
            return false
        end

        --Test-14: Positive, nil < "info1-13"
        local put14 = 
        {
            "info",       "info1",       "info1-14",
            "exattrs",    "exattrs1",    "exattrs1-14",
            "ver",        "ver1",        "ver1-14",
        }
        local del14 
        local code, ok = hb:checkAndMutateRow(TABLE, rowKey003, "info", "info1", hb.CompareOp.LESS, nil, put14, del14)
        if not ok then
            ngx.say("checkAndMutateRow test failed. Test-14. HBase checkAndMutateRow failed, innercode=", code)
            return false
        end
        local ok = checkRow(TABLE, rowKey003, 
        {
            "info",       "info1",       "info1-14",
            "exattrs",    "exattrs1",    "exattrs1-14",
            "ver",        "ver1",        "ver1-14",
        })
        if not ok then
            ngx.say("checkAndMutateRow test failed. Test-14. checkRow failed.")
            return false
        end

        --Test-15: Positive, nil <= "info1-14"
        local put15 = 
        {
            "info",       "info1",       "info1-15",
            "exattrs",    "exattrs1",    "exattrs1-15",
            "ver",        "ver1",        "ver1-15",
        }
        local del15 
        local code, ok = hb:checkAndMutateRow(TABLE, rowKey003, "info", "info1", hb.CompareOp.LESS_OR_EQUAL, nil, put15, del15)
        if not ok then
            ngx.say("checkAndMutateRow test failed. Test-15. HBase checkAndMutateRow failed, innercode=", code)
            return false
        end
        local ok = checkRow(TABLE, rowKey003, 
        {
            "info",       "info1",       "info1-15",
            "exattrs",    "exattrs1",    "exattrs1-15",
            "ver",        "ver1",        "ver1-15",
        })
        if not ok then
            ngx.say("checkAndMutateRow test failed. Test-15. checkRow failed.")
            return false
        end

        --Test-16: Positive, nil != "info1-15"
        local put16 = 
        {
            "info",       "info1",       "info1-16",
            "exattrs",    "exattrs1",    "exattrs1-16",
        }
        local del16 = {"ver"}
        local code, ok = hb:checkAndMutateRow(TABLE, rowKey003, "info", "info1", hb.CompareOp.NOT_EQUAL, nil, put16, del16)
        if not ok then
            ngx.say("checkAndMutateRow test failed. Test-16. HBase checkAndMutateRow failed, innercode=", code)
            return false
        end
        local ok = checkRow(TABLE, rowKey003, 
        {
            "info",       "info1",       "info1-16",
            "exattrs",    "exattrs1",    "exattrs1-16",
            "ver",        "ver1",        nil,
        })
        if not ok then
            ngx.say("checkAndMutateRow test failed. Test-16. checkRow failed.")
            return false
        end
        
        --Test-17: Negative, condition-check-failure 
        local put17 = 
        {
            "exattrs",    "exattrs1",    "NotPut",
            "ver",        "ver1",        "NotPut",
        }
        local del17 = {"info"}

        local checks = 
        {
            {TABLE, rowKey003, "info", "NotExistColumnXXXX", hb.CompareOp.LESS, nil, put17, del17},              -- null/empty < null/empty
            {TABLE, rowKey003, "info", "NotExistColumnXXXX", hb.CompareOp.NOT_EQUAL, nil, put17, del17},         -- null/empty != null/empty
            {TABLE, rowKey003, "info", "NotExistColumnXXXX", hb.CompareOp.GREATER, nil, put17, del17},           -- null/empty > null/empty
            {TABLE, rowKey003, "info", "NotExistColumnXXXX", hb.CompareOp.LESS, "XX", put17, del17},             -- "XX" < null/empty
            {TABLE, rowKey003, "info", "NotExistColumnXXXX", hb.CompareOp.EQUAL, "XX", put17, del17},            -- "XX" == null/empty
            {TABLE, rowKey003, "info", "NotExistColumnXXXX", hb.CompareOp.LESS_OR_EQUAL, "XX", put17, del17},    -- "XX" <= null/empty
            {TABLE, rowKey003, "info", "info1", hb.CompareOp.GREATER, nil, put17, del17},                        -- null/empty > "info1-16" 
            {TABLE, rowKey003, "info", "info1", hb.CompareOp.EQUAL, nil, put17, del17},                          -- null/empty == "info1-16" 
            {TABLE, rowKey003, "info", "info1", hb.CompareOp.GREATER_OR_EQUAL, nil, put17, del17},               -- null/empty >= "info1-16" 
        }
        for i, chk in ipairs(checks) do
            local code, ok = hb:checkAndMutateRow(unpack(chk))
            if ok then
                ngx.say("checkAndMutateRow test failed. Test-17. HBase checkAndMutateRow should fail,but it succeeded")
                return false
            end

            if code ~= "1121" then
                ngx.say("checkAndMutateRow test failed. Test-17. HBase checkAndMutateRow should fail with 1121, but code=", code)
                return false
            end

            local ok = checkRow(TABLE, rowKey003, 
            {
                "info",       "info1",       "info1-16",
                "exattrs",    "exattrs1",    "exattrs1-16",
                "ver",        "ver1",        nil,
            })
            if not ok then
                ngx.say("checkAndMutateRow test failed. Test-17. checkRow failed.")
                return false
            end
        end

        --- number/counter tests
        --- Tests 18-19 are for check-on-numbers/counters.
        ---      0 == nil    false 
        ---      0 != nil    true
        ---      0 >  nil    true
        ---      0 >= nil    true
        ---      0 <  nil    false 
        ---      0 <= nil    false 
        --       100 == nil  false
        --       100 < nil   false 
        --       100 > nil   true 
        --
        ---      nil == 0   false 
        ---      nil != 0   true 
        ---      nil >  0   false 
        ---      nil >= 0   false
        ---      nil <  0   true 
        ---      nil <= 0   true 
        --       100 == 0   false
        --       100 < 0    false
        --       100 >  0   true
        --
        ---      nil == nil  true 
        ---      0 == 0      true 

        -- let stats:size_bytes = 0
        local code1,ok1 = hb:increment(TABLE, rowKey003, {"stats", "size_bytes", 100})
        local code2,ok2 = hb:increment(TABLE, rowKey003, {"stats", "size_bytes", -100})
        if not ok1 or not ok2 then
            ngx.say("checkAndMutateRow test failed. COUNTER Test, prepare failed. code1=", code1, " code2=", code2)
            return false
        end
        local ok = checkRow(TABLE, rowKey003, 
        {
            "stats",       "size_bytes",    0,
            "stats",       "objects",       nil,
        })
        if not ok then
            ngx.say("checkAndMutateRow test failed. COUNTER Test, prepare failed. checkRow failed")
            return false
        end

        local v_info1    = nil
        local v_exattrs1 = nil 

        --Test 18: Positive tests for numbers/counters;
        local positive_checks = 
        {
            {TABLE, rowKey003, "stats", "objects", hb.CompareOp.NOT_EQUAL, 0},               -- 0 !=  null/empty   true
            {TABLE, rowKey003, "stats", "objects", hb.CompareOp.GREATER, 0},                 -- 0 >   null/empty   true 
            {TABLE, rowKey003, "stats", "objects", hb.CompareOp.GREATER_OR_EQUAL, 0},        -- 0 >=  null/empty   true 
            {TABLE, rowKey003, "stats", "objects", hb.CompareOp.GREATER_OR_EQUAL, 100},      -- 100 > null/empty   true 

            {TABLE, rowKey003, "stats", "size_bytes", hb.CompareOp.NOT_EQUAL, nil},          --  nil != 0   true 
            {TABLE, rowKey003, "stats", "size_bytes", hb.CompareOp.LESS, nil},               --  nil <  0   true 
            {TABLE, rowKey003, "stats", "size_bytes", hb.CompareOp.LESS_OR_EQUAL, nil},      --  nil <= 0   true 
            {TABLE, rowKey003, "stats", "size_bytes", hb.CompareOp.GREATER, 100},            --  100 >  0   true 

            {TABLE, rowKey003, "stats", "objects",    hb.CompareOp.EQUAL, nil},              -- nil == null/empty   true 
            {TABLE, rowKey003, "stats", "size_bytes", hb.CompareOp.EQUAL, 0},                -- 0 ==  0             true 
        }
        for i,chk in ipairs(positive_checks) do

            v_info1    = uuid()
            v_exattrs1 = uuid()

            local put18 = 
            {
                "info",       "info1",       v_info1,
                "exattrs",    "exattrs1",    v_exattrs1,
            }
            local del18 = {"ver"}

            chk[7] = put18
            chk[8] = del18

            local code, ok = hb:checkAndMutateRow(unpack(chk))
            if not ok then
                ngx.say("checkAndMutateRow test failed. Test-18. HBase checkAndMutateRow failed, innercode=", code, " i=", i)
                return false
            end

            local ok = checkRow(TABLE, rowKey003, 
            {
                "info",       "info1",       v_info1,
                "exattrs",    "exattrs1",    v_exattrs1,
                "ver",        "ver1",        nil,
            })
            if not ok then
                ngx.say("checkAndMutateRow test failed. Test-18. checkRow failed.")
                return false
            end
        end

        --Test 19: Negative tests for numbers/counters;
        local put19 = 
        {
            "info",       "info1",       "NotPut",
            "exattrs",    "exattrs1",    "NotPut",
        }
        local del19 = {"ver"}
        local negative_checks = 
        {
            {TABLE, rowKey003, "stats", "objects", hb.CompareOp.EQUAL, 0, put19, del19},                   -- 0 ==  null/empty   false 
            {TABLE, rowKey003, "stats", "objects", hb.CompareOp.LESS, 0, put19, del19},                    -- 0 <   null/empty   false 
            {TABLE, rowKey003, "stats", "objects", hb.CompareOp.LESS_OR_EQUAL, 0, put19, del19},           -- 0 <=  null/empty   false 
            {TABLE, rowKey003, "stats", "objects", hb.CompareOp.EQUAL, 100, put19, del19},                 -- 100== null/empty   false 
            {TABLE, rowKey003, "stats", "objects", hb.CompareOp.LESS, 100, put19, del19},                  -- 100 < null/empty   false 

            {TABLE, rowKey003, "stats", "size_bytes", hb.CompareOp.EQUAL, nil, put19, del19},              --  nil == 0   false 
            {TABLE, rowKey003, "stats", "size_bytes", hb.CompareOp.GREATER, nil, put19, del19},            --  nil >  0   false 
            {TABLE, rowKey003, "stats", "size_bytes", hb.CompareOp.GREATER_OR_EQUAL, nil, put19, del19},   --  nil >= 0   false 
            {TABLE, rowKey003, "stats", "size_bytes", hb.CompareOp.EQUAL, 100, put19, del19},              --  100 == 0   false 
            {TABLE, rowKey003, "stats", "size_bytes", hb.CompareOp.LESS, 100, put19, del19},               --  100 <  0   false 
        }
        for i,chk in ipairs(negative_checks) do
            local code, ok = hb:checkAndMutateRow(unpack(chk))
            if ok then
                ngx.say("checkAndMutateRow test failed. Test-19. HBase checkAndMutateRow should fail but it succeeded")
                return false
            end

            if code ~= "1121" then
                ngx.say("checkAndMutateRow test failed. Test-19. HBase checkAndMutateRow should fail with 1121 but code=", code)
                return false
            end

            local ok = checkRow(TABLE, rowKey003, 
            {
                "info",       "info1",       v_info1,
                "exattrs",    "exattrs1",    v_exattrs1,
                "ver",        "ver1",        nil,
            })
            if not ok then
                ngx.say("checkAndMutateRow test failed. Test-19. checkRow failed.")
                return false
            end
        end
    end

    --clean
    if clean or onlyClean then
        hb:delete(TABLE, rowKey001)
        hb:delete(TABLE, rowKey002)
        hb:delete(TABLE, rowKey003)
    end
    ngx.say("OK: checkAndMutateRow test successful")
    ngx.flush()
    return true
end

function compareResult(result, expectedResult)
    if (result == nil) then
        ngx.say("ERROR: compareResult failed: result is nil")
        return false
    end

    if(expectedResult == nil) then
        expectedResult = {}
    end

    local expectHash = {}
    for i=1, #expectedResult-2, 3 do
        local fam = expectedResult[i]
        local col = expectedResult[i+1]
        local val = expectedResult[i+2]
        expectHash[fam..":"..col] = val
    end

    for k,v in pairs(result) do
        if expectHash[k] ~= v then
            ngx.say("ERROR: compareResult failed: key=", k, " Expected=", expectHash[k], " Actual=", v)
            return false
        end
    end

    for k,v in pairs(expectHash) do
        if result[k] ~= v then
            ngx.say("ERROR: compareResult failed: key=", k, " Actual=", result[k], " Expected=", v)
            return false
        end
    end

    return true;
end

function TestCheckAndMutateAndGetRow()

    local rowKey001 = "HBASE_TEST_CheckAndMutateAndGetRow_Row001"
    local rowKey002 = "HBASE_TEST_CheckAndMutateAndGetRow_Row002"
    local rowKey003 = "HBASE_TEST_CheckAndMutateAndGetRow_Row003"
    local rowKey004 = "HBASE_TEST_CheckAndMutateAndGetRow_Row004"
    local rowKey005 = "HBASE_TEST_CheckAndMutateAndGetRow_Row005"

    if not onlyClean then

        --Test-1: Positive test.  'X' >= empty/null (non-existent row); Only put;
        local put1 =
        {
            "info",    "info1",     "info1", 
            "info",    "info2",     "info2",
            "quota",   "quota1",    "quota1",
            "ver",     "ver1",      "ver1",
            "exattrs", "exattrs1",  "exattrs1",
        }
        local del1 = nil

        local innercode,ok,result = hb:checkAndMutateAndGetRow(TABLE, rowKey001, "info", "info1", hb.CompareOp.GREATER_OR_EQUAL, "X", put1, del1)

        if not ok  then
            ngx.say("ERROR: TestCheckAndMutateAndGetRow test failed. Test-1, checkAndMutateAndGetRow failed, innercode=", innercode)
            return false
        end

        if not compareResult(result, {}) then
            ngx.say("ERROR: TestCheckAndMutateAndGetRow test failed. Test-1, compareResult failed")
            return false
        end

        if not checkRow(TABLE, rowKey001, put1) then
            ngx.say("ERROR: TestCheckAndMutateAndGetRow test failed. Test-1, checkRow failed")
            return false
        end

        --Test-2: Positive test.  'X' > empty/null (non-existent row); Only delete;
        --Row rowKey002 will not be created;
        local put2 = nil
        local del2 = {"info", "quota:quota1"}

        local innercode,ok,result = hb:checkAndMutateAndGetRow(TABLE, rowKey002, "info", "info1", hb.CompareOp.GREATER, "X", put2, del2)

        if not ok  then
            ngx.say("ERROR: TestCheckAndMutateAndGetRow test failed. Test-2, checkAndMutateAndGetRow failed, innercode=", innercode)
            return false
        end

        if not compareResult(result, {}) then
            ngx.say("ERROR: TestCheckAndMutateAndGetRow test failed. Test-2, compareResult failed")
            return false
        end

        --Test-3: Positive test.  'X' >= empty/null (non-existent row); put and delete;
        local put3 =
        {
            "info",    "info1",     "info1", 
            "info",    "info2",     "info2",
            "quota",   "quota1",    "quota1",
        }
        local del3 = {"exattrs", "ver:ver1"}

        local innercode,ok,result = hb:checkAndMutateAndGetRow(TABLE, rowKey003, "info", "info1", hb.CompareOp.GREATER_OR_EQUAL, "X", put3, del3)

        if not ok  then
            ngx.say("ERROR: TestCheckAndMutateAndGetRow test failed. Test-3, checkAndMutateAndGetRow failed, innercode=", innercode)
            return false
        end

        if not compareResult(result, {}) then
            ngx.say("ERROR: TestCheckAndMutateAndGetRow test failed. Test-3, compareResult failed")
            return false
        end


        local row003Stat = put3
        if not checkRow(TABLE, rowKey003, row003Stat) then
            ngx.say("ERROR: TestCheckAndMutateAndGetRow test failed. Test-3, checkRow failed")
            return false
        end

        --Test-4: Positive test.  'X' > empty/null (non-existent column); Only put;
        local put4 =
        {
            "info",    "info1",     "info1-4", 
            "info",    "info2",     "info2-4",
            "quota",   "quota1",    "quota1-4",
            "ver",     "ver1",      "ver1-4",
        }
        local del4 = nil 

        local innercode,ok,result = hb:checkAndMutateAndGetRow(TABLE, rowKey003, "info", "NotExistColumn", hb.CompareOp.GREATER, "X", put4, del4)

        if not ok  then
            ngx.say("ERROR: TestCheckAndMutateAndGetRow test failed. Test-4, checkAndMutateAndGetRow failed, innercode=", innercode)
            return false
        end

        if not compareResult(result, row003Stat) then
            ngx.say("ERROR: TestCheckAndMutateAndGetRow test failed. Test-4, compareResult failed")
            return false
        end

        row003Stat = put4
        if not checkRow(TABLE, rowKey003, row003Stat) then
            ngx.say("ERROR: TestCheckAndMutateAndGetRow test failed. Test-4, checkRow failed")
            return false
        end

        --Test-5: Positive test.  nil = empty/null (non-existent column); put and delete;
        local put5 =
        {
            "ver",     "ver2",      "ver2-5",
        }
        local del5 =  {"info", "ver:ver1"} 

        local innercode,ok,result = hb:checkAndMutateAndGetRow(TABLE, rowKey003, "info", "NotExistColumn", hb.CompareOp.EQUAL, nil, put5, del5)

        if not ok  then
            ngx.say("ERROR: TestCheckAndMutateAndGetRow test failed. Test-5, checkAndMutateAndGetRow failed, innercode=", innercode)
            return false
        end

        if not compareResult(result, row003Stat) then
            ngx.say("ERROR: TestCheckAndMutateAndGetRow test failed. Test-5, compareResult failed")
            return false
        end

        row003Stat = 
        {
            "quota",   "quota1",    "quota1-4",
            "ver",     "ver2",      "ver2-5",
        }
        if not checkRow(TABLE, rowKey003, row003Stat) then
            ngx.say("ERROR: TestCheckAndMutateAndGetRow test failed. Test-5, checkRow failed")
            return false
        end


        --Test-6: Positive test.  nil < "ver2-5"; put and delete;
        local put6 =
        {
            "info",    "info1",     "info1-6", 
            "info",    "info2",     "info2-6",
            "ver",     "ver1",      "ver1-6",
            "quota",   "quota1",    "quota1-6",
        }
        local del6 =  {} 

        local innercode,ok,result = hb:checkAndMutateAndGetRow(TABLE, rowKey003, "ver", "ver2", hb.CompareOp.LESS, nil, put6, del6)

        if not ok  then
            ngx.say("ERROR: TestCheckAndMutateAndGetRow test failed. Test-6, checkAndMutateAndGetRow failed, innercode=", innercode)
            return false
        end

        if not compareResult(result, row003Stat) then
            ngx.say("ERROR: TestCheckAndMutateAndGetRow test failed. Test-6, compareResult failed")
            return false
        end

        row003Stat =
        {
            "info",    "info1",     "info1-6", 
            "info",    "info2",     "info2-6",
            "ver",     "ver1",      "ver1-6",
            "ver",     "ver2",      "ver2-5",
            "quota",   "quota1",    "quota1-6",
        }
        if not checkRow(TABLE, rowKey003, row003Stat) then
            ngx.say("ERROR: TestCheckAndMutateAndGetRow test failed. Test-6, checkRow failed")
            return false
        end

        --Test-7: Positive test.  "ver2-999" > "ver2-5"; put and delete;
        local put7 =
        {
            "info",    "info2",     "info2-7",
            "ver",     "ver1",      "ver1-7",
        }
        local del7 =  {"quota", "info:info1"} 

        local innercode,ok,result = hb:checkAndMutateAndGetRow(TABLE, rowKey003, "ver", "ver2", hb.CompareOp.GREATER, "ver2-999", put7, del7)

        if not ok  then
            ngx.say("ERROR: TestCheckAndMutateAndGetRow test failed. Test-7, checkAndMutateAndGetRow failed, innercode=", innercode)
            return false
        end

        if not compareResult(result, row003Stat) then
            ngx.say("ERROR: TestCheckAndMutateAndGetRow test failed. Test-7, compareResult failed")
            return false
        end

        row003Stat =
        {
            "info",    "info2",     "info2-7",
            "ver",     "ver1",      "ver1-7",
            "ver",     "ver2",      "ver2-5",
        }
        if not checkRow(TABLE, rowKey003, row003Stat) then
            ngx.say("ERROR: TestCheckAndMutateAndGetRow test failed. Test-7, checkRow failed")
            return false
        end


        --Test-8: Positive test.  2 (number) > null/empty (non-existent column); put and delete;
        local put8 =
        {
            "info",    "info1",     "info1-8",
            "quota",   "quota1",    "quota1-8",
            "quota",   "quota2",    "quota2-8",
        }
        local del8 =  {"ver:ver2", "info:info2"} 

        local innercode,ok,result = hb:checkAndMutateAndGetRow(TABLE, rowKey003, "stats", "NotExistColumn", hb.CompareOp.GREATER, 2, put8, del8)

        if not ok  then
            ngx.say("ERROR: TestCheckAndMutateAndGetRow test failed. Test-8, checkAndMutateAndGetRow failed, innercode=", innercode)
            return false
        end

        if not compareResult(result, row003Stat) then
            ngx.say("ERROR: TestCheckAndMutateAndGetRow test failed. Test-8, compareResult failed")
            return false
        end

        row003Stat =
        {
            "info",    "info1",     "info1-8",
            "quota",   "quota1",    "quota1-8",
            "quota",   "quota2",    "quota2-8",
            "ver",     "ver1",      "ver1-7",
        }
        if not checkRow(TABLE, rowKey003, row003Stat) then
            ngx.say("ERROR: TestCheckAndMutateAndGetRow test failed. Test-8, checkRow failed")
            return false
        end


        --Test-9: Positive test.  0 > null/empty (non-existent column); put;
        local put9 =
        {
            "info",    "info1",     "info1-9",
            "quota",   "quota1",    "quota1-9",
            "quota",   "quota2",    "quota2-9",
            "ver",     "ver1",      "ver1-9",
        }
        local del9 =  {}

        local innercode,ok,result = hb:checkAndMutateAndGetRow(TABLE, rowKey003, "stats", "objects", hb.CompareOp.GREATER, 0, put9, del9)

        if not ok  then
            ngx.say("ERROR: TestCheckAndMutateAndGetRow test failed. Test-9, checkAndMutateAndGetRow failed, innercode=", innercode)
            return false
        end

        if not compareResult(result, row003Stat) then
            ngx.say("ERROR: TestCheckAndMutateAndGetRow test failed. Test-9, compareResult failed")
            return false
        end

        row003Stat = 
        {
            "info",    "info1",     "info1-9",
            "quota",   "quota1",    "quota1-9",
            "quota",   "quota2",    "quota2-9",
            "ver",     "ver1",      "ver1-9",
        }
        if not checkRow(TABLE, rowKey003, row003Stat) then
            ngx.say("ERROR: TestCheckAndMutateAndGetRow test failed. Test-9, checkRow failed")
            return false
        end

        --Test-10: Positive test.  nil = 0 (column +100 first, and then -100); put and delete;
        local code1,ok1 = hb:increment(TABLE, rowKey003, {"stats", "objects", 10000,  "stats", "size_bytes", 100})
        local code2,ok2 = hb:increment(TABLE, rowKey003, {"stats", "objects", 10000,  "stats", "size_bytes", -100})
        if not ok1 or not ok2 then
            ngx.say("ERROR: TestCheckAndMutateAndGetRow test failed. Test-10, prepare failed")
            return false
        end
        --stats:size_bytes should be 0 now;

        local put10 =
        {
            "info",    "info1",     "info1-10",
            "quota",   "quota1",    "quota1-10",
            "quota",   "quota2",    "quota2-10",
        }
        local del10 =  {"ver"}

        local innercode,ok,result = hb:checkAndMutateAndGetRow(TABLE, rowKey003, "stats", "size_bytes", hb.CompareOp.EQUAL, 0, put10, del10)

        if not ok  then
            ngx.say("ERROR: TestCheckAndMutateAndGetRow test failed. Test-10, checkAndMutateAndGetRow failed, innercode=", innercode)
            return false
        end


        row003Stat = 
        {
            "info",    "info1",     "info1-9",
            "quota",   "quota1",    "quota1-9",
            "quota",   "quota2",    "quota2-9",
            "ver",     "ver1",      "ver1-9",
            "stats",   "size_bytes",  0,
            "stats",   "objects",     20000,
        }
        if not compareResult(result, row003Stat) then
            ngx.say("ERROR: TestCheckAndMutateAndGetRow test failed. Test-10, compareResult failed")
            return false
        end

        row003Stat = 
        {
            "info",    "info1",     "info1-10",
            "quota",   "quota1",    "quota1-10",
            "quota",   "quota2",    "quota2-10",
            "stats",   "size_bytes",  0,
            "stats",   "objects",     20000,
        }
        if not checkRow(TABLE, rowKey003, row003Stat) then
            ngx.say("ERROR: TestCheckAndMutateAndGetRow test failed. Test-10, checkRow failed")
            return false
        end

        --Test-11: Positive test.  20001 > 20000 ; put;
        local put11 =
        {
            "info",    "info1",     "info1-11",
            "quota",   "quota1",    "quota1-11",
            "quota",   "quota2",    "quota2-11",
        }
        local del11

        local innercode,ok,result = hb:checkAndMutateAndGetRow(TABLE, rowKey003, "stats", "objects", hb.CompareOp.GREATER, 20001, put11, del11)

        if not ok  then
            ngx.say("ERROR: TestCheckAndMutateAndGetRow test failed. Test-11, checkAndMutateAndGetRow failed, innercode=", innercode)
            return false
        end

        if not compareResult(result, row003Stat) then
            ngx.say("ERROR: TestCheckAndMutateAndGetRow test failed. Test-11, compareResult failed")
            return false
        end

        row003Stat = 
        {
            "info",    "info1",     "info1-11",
            "quota",   "quota1",    "quota1-11",
            "quota",   "quota2",    "quota2-11",
            "stats",   "size_bytes",  0,
            "stats",   "objects",     20000,
        }
        if not checkRow(TABLE, rowKey003, row003Stat) then
            ngx.say("ERROR: TestCheckAndMutateAndGetRow test failed. Test-11, checkRow failed")
            return false
        end

        --Test-12: Negative test.  condition-check-failure ; 
        local put12 =
        {
            "info",    "info1",     "info1-12",
            "quota",   "quota1",    "quota1-12",
            "quota",   "quota2",    "quota2-12",
        }
        local del12 = {"ver"}

        local checks = 
        {
            {TABLE, rowKey003, "info", "NotExistColumn", hb.CompareOp.LESS, "XX", put12, del12},          -- "XX" < null/empty
            {TABLE, rowKey003, "info", "NotExistColumn", hb.CompareOp.EQUAL, "XX", put12, del12},         -- "XX" == null/empty
            {TABLE, rowKey003, "info", "NotExistColumn", hb.CompareOp.LESS_OR_EQUAL, "XX", put12, del12}, -- "XX" <= null/empty
            {TABLE, rowKey003, "info", "NotExistColumn", hb.CompareOp.LESS, nil, put12, del12},           -- nil < null/empty
            {TABLE, rowKey003, "info", "NotExistColumn", hb.CompareOp.GREATER, nil, put12, del12},        -- nil > null/empty
            {TABLE, rowKey003, "info", "info1", hb.CompareOp.GREATER, nil, put12, del12},                 -- nil > "info1-11" 
            {TABLE, rowKey003, "info", "info1", hb.CompareOp.EQUAL, nil, put12, del12},                   -- nil == "info1-11" 
            {TABLE, rowKey003, "info", "info1", hb.CompareOp.GREATER_OR_EQUAL, nil, put12, del12},        -- nil >= "info1-11" 
            {TABLE, rowKey003, "info", "info1", hb.CompareOp.EQUAL, "info1-22", put12, del12},            -- "info1-22" == "info1-11" 
            {TABLE, rowKey003, "info", "info1", hb.CompareOp.LESS, "info1-22", put12, del12},             -- "info1-22" < "info1-11" 
            {TABLE, rowKey003, "info", "info1", hb.CompareOp.LESS_OR_EQUAL, "info1-22", put12, del12},    -- "info1-22" <= "info1-11" 
            {TABLE, rowKey003, "info", "info1", hb.CompareOp.GREATER, "info1-00", put12, del12},          -- "info1-00" > "info1-11" 
            {TABLE, rowKey003, "info", "info1", hb.CompareOp.EQUAL, "info1-00", put12, del12},            -- "info1-00" == "info1-11" 
            {TABLE, rowKey003, "info", "info1", hb.CompareOp.GREATER_OR_EQUAL, "info1-00", put12, del12}, -- "info1-00" >= "info1-11" 
            {TABLE, rowKey003, "stats", "size_bytes", hb.CompareOp.LESS, 3, put12, del12},                -- 3 < 0 
            {TABLE, rowKey003, "stats", "size_bytes", hb.CompareOp.EQUAL, 3, put12, del12},               -- 3 == 0 
            {TABLE, rowKey003, "stats", "size_bytes", hb.CompareOp.LESS_OR_EQUAL, 3, put12, del12},       -- 3 <= 0 
            {TABLE, rowKey003, "stats", "objects", hb.CompareOp.LESS, 20001, put12, del12},               -- 20001 < 20000
            {TABLE, rowKey003, "stats", "objects", hb.CompareOp.EQUAL, 20001, put12, del12},              -- 20001 == 20000
            {TABLE, rowKey003, "stats", "objects", hb.CompareOp.LESS_OR_EQUAL, 20001, put12, del12},      -- 20001 <= 20000
            {TABLE, rowKey003, "stats", "objects", hb.CompareOp.GREATER, 19999, put12, del12},            -- 19999 > 20000
            {TABLE, rowKey003, "stats", "objects", hb.CompareOp.EQUAL, 19999, put12, del12},              -- 19999 == 20000
            {TABLE, rowKey003, "stats", "objects", hb.CompareOp.GREATER_OR_EQUAL, 19999, put12, del12},   -- 19999 >= 20000
            {TABLE, rowKey003, "stats", "objects", hb.CompareOp.GREATER, 0, put12, del12},                -- 0 > 20000
            {TABLE, rowKey003, "stats", "objects", hb.CompareOp.EQUAL, 0, put12, del12},                  -- 0 == 20000
            {TABLE, rowKey003, "stats", "objects", hb.CompareOp.GREATER_OR_EQUAL, 0, put12, del12},       -- 0 >= 20000
        }

        for i,chk in ipairs(checks) do
            local innercode,ok,result = hb:checkAndMutateAndGetRow(unpack(chk))

            if ok  then
                ngx.say("ERROR: TestCheckAndMutateAndGetRow test failed. Test-12, checkAndMutateAndGetRow should fail but it succeeded. i=", i)
                return false
            end

            if innercode ~= "1121"  then
                ngx.say("ERROR: TestCheckAndMutateAndGetRow test failed. Test-12, checkAndMutateAndGetRow should fail with 1121, but innercode=", innercode, ". i=",i)
                return false
            end

            if not compareResult(result, row003Stat) then
                ngx.say("ERROR: TestCheckAndMutateAndGetRow test failed. Test-12, compareResult failed. i=", i)
                return false
            end
            if not checkRow(TABLE, rowKey003, row003Stat) then
                ngx.say("ERROR: TestCheckAndMutateAndGetRow test failed. Test-12, checkRow failed. i=", i)
                return false
            end
        end

        --- Tests 13-22 are for comparison about null/empty, the rules are like this:
        --            null/empty <  null/empty      false
        --            null/empty <= null/empty      true
        --            null/empty == null/empty      true
        --            null/empty != null/empty      false
        --            null/empty >= null/empty      true
        --            null/empty >  null/empty      false
        --            
        --            "AnyString" <  null/empty      false
        --            "AnyString" <= null/empty      false
        --            "AnyString" == null/empty      false
        --            "AnyString" != null/empty      true
        --            "AnyString" >= null/empty      true
        --            "AnyString" >  null/empty      true
        --            
        --            null/empty <  "AnyString"     true
        --            null/empty <= "AnyString"     true
        --            null/empty == "AnyString"     false
        --            null/empty != "AnyString"     true
        --            null/empty >= "AnyString"     false
        --            null/empty >  "AnyString"     false

        --Test-13: Positive, null/empty <= null/empty (non-existent row)
        local put13 = 
        {
            "quota",   "quota1",     "quota1-13", 
            "exattrs", "exattrs1",   "exattrs1-13"
        }
        local del13 = {"info", "ver:ver1"}
        local code, ok, result = hb:checkAndMutateAndGetRow(TABLE, rowKey004, "info", "info2", hb.CompareOp.LESS_OR_EQUAL, nil, put13, del13)
        if not ok then
            ngx.say("checkAndMutateAndGetRow test failed. Test-13. HBase checkAndMutateAndGetRow failed, innercode=", code)
            return false
        end

        if not compareResult(result, {}) then
            ngx.say("checkAndMutateAndGetRow test failed. Test-13. compareResult failed")
            return false
        end

        local row004Stat = 
        {
            "quota",   "quota1",     "quota1-13", 
            "exattrs", "exattrs1",   "exattrs1-13",
            "ver",     "ver1",       nil,
        }

        local ok = checkRow(TABLE, rowKey004, row004Stat)
        if not ok then
            ngx.say("checkAndMutateAndGetRow test failed. Test-13. checkRow failed.")
            return false
        end

        --Test-14: Positive, "XX" >= null/empty (non-existent column)
        local put14 = 
        {
            "info",       "info1",       "info1-14",
            "quota",      "quota1",      "quota1-14", 
            "exattrs",    "exattrs1",    "exattrs1-14",
        }
        local del14 = {}
        local code, ok, result = hb:checkAndMutateAndGetRow(TABLE, rowKey004, "info", "NotExistColumn", hb.CompareOp.GREATER_OR_EQUAL, "XX", put14, del14)
        if not ok then
            ngx.say("checkAndMutateAndGetRow test failed. Test-14. HBase checkAndMutateAndGetRow failed, innercode=", code)
            return false
        end

        if not compareResult(result, row004Stat) then
            ngx.say("checkAndMutateAndGetRow test failed. Test-14. compareResult failed")
            return false
        end

        row004Stat = 
        {
            "info",       "info1",       "info1-14",
            "quota",      "quota1",      "quota1-14", 
            "exattrs",    "exattrs1",    "exattrs1-14",
        }

        local ok = checkRow(TABLE, rowKey004, row004Stat)
        if not ok then
            ngx.say("checkAndMutateAndGetRow test failed. Test-14. checkRow failed.")
            return false
        end

        --Test-15: Positive, "XX" > null/empty (non-existent row)
        local put15 = 
        {
            "info",       "info1",       "info1-15",
            "quota",      "quota1",      "quota1-15", 
            "exattrs",    "exattrs1",    "exattrs1-15",
        }
        local del15 = {"ver:ver1"}
        local code, ok, result = hb:checkAndMutateAndGetRow(TABLE, rowKey005, "info", "info2", hb.CompareOp.GREATER, "XX", put15, del15)
        if not ok then
            ngx.say("checkAndMutateAndGetRow test failed. Test-15. HBase checkAndMutateAndGetRow failed, innercode=", code)
            return false
        end

        if not compareResult(result, {}) then
            ngx.say("checkAndMutateAndGetRow test failed. Test-15. compareResult failed")
            return false
        end

        local row005Stat = 
        {
            "info",       "info1",       "info1-15",
            "quota",      "quota1",      "quota1-15", 
            "exattrs",    "exattrs1",    "exattrs1-15",
        }

        local ok = checkRow(TABLE, rowKey005, row005Stat)
        if not ok then
            ngx.say("checkAndMutateAndGetRow test failed. Test-15. checkRow failed.")
            return false
        end


        --Test-16: Positive, nil == null/empty (non-existent Column)
        local put16 = 
        {
            "quota",      "quota1",      "quota1-16", 
            "exattrs",    "exattrs1",    "exattrs1-16",
            "ver",        "ver1",        "ver1-16",
        }
        local del16 = {"info"}
        local code, ok, result = hb:checkAndMutateAndGetRow(TABLE, rowKey005, "info", "NotExistColumn", hb.CompareOp.EQUAL, nil, put16, del16)
        if not ok then
            ngx.say("checkAndMutateAndGetRow test failed. Test-16. HBase checkAndMutateAndGetRow failed, innercode=", code)
            return false
        end

        if not compareResult(result, row005Stat) then
            ngx.say("checkAndMutateAndGetRow test failed. Test-16. compareResult failed")
            return false
        end

        row005Stat = 
        {
            "quota",      "quota1",      "quota1-16", 
            "exattrs",    "exattrs1",    "exattrs1-16",
            "ver",        "ver1",        "ver1-16",
        }

        local ok = checkRow(TABLE, rowKey005, row005Stat)
        if not ok then
            ngx.say("checkAndMutateAndGetRow test failed. Test-16. checkRow failed.")
            return false
        end

        --Test-17: Positive, nul >= null/empty (non-existent Column)
        local put17 = 
        {
            "info",       "info1",       "info1-17",
            "exattrs",    "exattrs1",    "exattrs1-17",
            "ver",        "ver1",        "ver1-17",
        }
        local del17 = nil
        local code, ok, result = hb:checkAndMutateAndGetRow(TABLE, rowKey005, "info", "NotExistColumn", hb.CompareOp.GREATER_OR_EQUAL, nil, put17, del17)
        if not ok then
            ngx.say("checkAndMutateAndGetRow test failed. Test-17. HBase checkAndMutateAndGetRow failed, innercode=", code)
            return false
        end

        if not compareResult(result, row005Stat) then
            ngx.say("checkAndMutateAndGetRow test failed. Test-17. compareResult failed")
            return false
        end

        row005Stat = 
        {
            "quota",      "quota1",      "quota1-16", 
            "info",       "info1",       "info1-17",
            "exattrs",    "exattrs1",    "exattrs1-17",
            "ver",        "ver1",        "ver1-17",
        }

        local ok = checkRow(TABLE, rowKey005, row005Stat)
        if not ok then
            ngx.say("checkAndMutateAndGetRow test failed. Test-17. checkRow failed.")
            return false
        end

        --Test-18: Positive, "XX" != null/empty (non-existent Column)
        local put18 = 
        {
            "info",       "info1",       "info1-18",
            "exattrs",    "exattrs1",    "exattrs1-18",
            "ver",        "ver1",        "ver1-18",
        }
        local del18 = {"quota:quota1"} 
        local code, ok, result = hb:checkAndMutateAndGetRow(TABLE, rowKey005, "info", "NotExistColumn", hb.CompareOp.NOT_EQUAL, "XX", put18, del18)
        if not ok then
            ngx.say("checkAndMutateAndGetRow test failed. Test-18. HBase checkAndMutateAndGetRow failed, innercode=", code)
            return false
        end

        if not compareResult(result, row005Stat) then
            ngx.say("checkAndMutateAndGetRow test failed. Test-18. compareResult failed")
            return false
        end

        row005Stat = 
        {
            "info",       "info1",       "info1-18",
            "exattrs",    "exattrs1",    "exattrs1-18",
            "ver",        "ver1",        "ver1-18",
        }

        local ok = checkRow(TABLE, rowKey005, row005Stat)
        if not ok then
            ngx.say("checkAndMutateAndGetRow test failed. Test-18. checkRow failed.")
            return false
        end

        --Test-19: Positive, nil < "info1-18"
        local put19 = 
        {
            "info",       "info1",       "info1-19",
            "exattrs",    "exattrs1",    "exattrs1-19",
            "ver",        "ver1",        "ver1-19",
        }
        local del19 
        local code, ok, result = hb:checkAndMutateAndGetRow(TABLE, rowKey005, "info", "info1", hb.CompareOp.LESS, nil, put19, del19)
        if not ok then
            ngx.say("checkAndMutateAndGetRow test failed. Test-19. HBase checkAndMutateAndGetRow failed, innercode=", code)
            return false
        end

        if not compareResult(result, row005Stat) then
            ngx.say("checkAndMutateAndGetRow test failed. Test-19. compareResult failed")
            return false
        end

        row005Stat = 
        {
            "info",       "info1",       "info1-19",
            "exattrs",    "exattrs1",    "exattrs1-19",
            "ver",        "ver1",        "ver1-19",
        }

        local ok = checkRow(TABLE, rowKey005, row005Stat)
        if not ok then
            ngx.say("checkAndMutateAndGetRow test failed. Test-19. checkRow failed.")
            return false
        end

        --Test-20: Positive, nil <= "info1-14"
        local put20 = 
        {
            "info",       "info1",       "info1-20",
            "exattrs",    "exattrs1",    "exattrs1-20",
            "ver",        "ver1",        "ver1-20",
        }
        local del20 
        local code, ok, result = hb:checkAndMutateAndGetRow(TABLE, rowKey005, "info", "info1", hb.CompareOp.LESS_OR_EQUAL, nil, put20, del20)
        if not ok then
            ngx.say("checkAndMutateAndGetRow test failed. Test-20. HBase checkAndMutateAndGetRow failed, innercode=", code)
            return false
        end

        if not compareResult(result, row005Stat) then
            ngx.say("checkAndMutateAndGetRow test failed. Test-20. compareResult failed")
            return false
        end

        row005Stat = 
        {
            "info",       "info1",       "info1-20",
            "exattrs",    "exattrs1",    "exattrs1-20",
            "ver",        "ver1",        "ver1-20",
        }

        local ok = checkRow(TABLE, rowKey005, row005Stat)
        if not ok then
            ngx.say("checkAndMutateAndGetRow test failed. Test-20. checkRow failed.")
            return false
        end

        --Test-21: Positive, nil != "info1-15"
        local put21 = 
        {
            "info",       "info1",       "info1-21",
            "exattrs",    "exattrs1",    "exattrs1-21",
        }
        local del21 = {"ver"}
        local code, ok, result = hb:checkAndMutateAndGetRow(TABLE, rowKey005, "info", "info1", hb.CompareOp.NOT_EQUAL, nil, put21, del21)
        if not ok then
            ngx.say("checkAndMutateAndGetRow test failed. Test-21. HBase checkAndMutateAndGetRow failed, innercode=", code)
            return false
        end

        if not compareResult(result, row005Stat) then
            ngx.say("checkAndMutateAndGetRow test failed. Test-21. compareResult failed")
            return false
        end

        row005Stat = 
        {
            "info",       "info1",       "info1-21",
            "exattrs",    "exattrs1",    "exattrs1-21",
            "ver",        "ver1",        nil,
        }

        local ok = checkRow(TABLE, rowKey005, row005Stat)
        if not ok then
            ngx.say("checkAndMutateAndGetRow test failed. Test-21. checkRow failed.")
            return false
        end
        
        --Test-22: Negative, condition-check-failure 
        local put22 = 
        {
            "exattrs",    "exattrs1",    "NotPut",
            "ver",        "ver1",        "NotPut",
        }
        local del22 = {"info"}

        local checks = 
        {
            {TABLE, rowKey005, "info", "NotExistColumnXXXX", hb.CompareOp.LESS, nil, put22, del22},              -- null/empty < null/empty
            {TABLE, rowKey005, "info", "NotExistColumnXXXX", hb.CompareOp.NOT_EQUAL, nil, put22, del22},         -- null/empty != null/empty
            {TABLE, rowKey005, "info", "NotExistColumnXXXX", hb.CompareOp.GREATER, nil, put22, del22},           -- null/empty > null/empty
            {TABLE, rowKey005, "info", "NotExistColumnXXXX", hb.CompareOp.LESS, "XX", put22, del22},             -- "XX" < null/empty
            {TABLE, rowKey005, "info", "NotExistColumnXXXX", hb.CompareOp.EQUAL, "XX", put22, del22},            -- "XX" == null/empty
            {TABLE, rowKey005, "info", "NotExistColumnXXXX", hb.CompareOp.LESS_OR_EQUAL, "XX", put22, del22},    -- "XX" <= null/empty
            {TABLE, rowKey005, "info", "info1", hb.CompareOp.GREATER, nil, put22, del22},                        -- null/empty > "info1-16" 
            {TABLE, rowKey005, "info", "info1", hb.CompareOp.EQUAL, nil, put22, del22},                          -- null/empty == "info1-16" 
            {TABLE, rowKey005, "info", "info1", hb.CompareOp.GREATER_OR_EQUAL, nil, put22, del22},               -- null/empty >= "info1-16" 
        }
        for i, chk in ipairs(checks) do
            local code, ok, result = hb:checkAndMutateAndGetRow(unpack(chk))
            if ok then
                ngx.say("checkAndMutateAndGetRow test failed. Test-22. HBase checkAndMutateAndGetRow should fail,but it succeeded")
                return false
            end

            if code ~= "1121" then
                ngx.say("checkAndMutateAndGetRow test failed. Test-22. HBase checkAndMutateAndGetRow should fail with 1121, but code=", code)
                return false
            end

            if not compareResult(result, row005Stat) then
                ngx.say("checkAndMutateAndGetRow test failed. Test-22. compareResult failed")
                return false
            end

            local ok = checkRow(TABLE, rowKey005, row005Stat)
            if not ok then
                ngx.say("checkAndMutateAndGetRow test failed. Test-22. checkRow failed.")
                return false
            end
        end

        --- number/counter tests
        --- Tests 23-24 are for check-on-numbers/counters.
        ---      0 == nil    false 
        ---      0 != nil    true
        ---      0 >  nil    true
        ---      0 >= nil    true
        ---      0 <  nil    false 
        ---      0 <= nil    false 
        --       100 == nil  false
        --       100 < nil   false 
        --       100 > nil   true 
        --
        ---      nil == 0   false 
        ---      nil != 0   true 
        ---      nil >  0   false 
        ---      nil >= 0   false
        ---      nil <  0   true 
        ---      nil <= 0   true 
        --       100 == 0   false
        --       100 < 0    false
        --       100 >  0   true
        --
        ---      nil == nil  true 
        ---      0 == 0      true 

        -- let stats:size_bytes = 0
        local code1,ok1 = hb:increment(TABLE, rowKey005, {"stats", "size_bytes", 100})
        local code2,ok2 = hb:increment(TABLE, rowKey005, {"stats", "size_bytes", -100})
        if not ok1 or not ok2 then
            ngx.say("checkAndMutateAndGetRow test failed. COUNTER Test, prepare failed. code1=", code1, " code2=", code2)
            return false
        end
        local ok = checkRow(TABLE, rowKey005, 
        {
            "stats",       "size_bytes",    0,
            "stats",       "objects",       nil,
        })
        if not ok then
            ngx.say("checkAndMutateAndGetRow test failed. COUNTER Test, prepare failed. checkRow failed")
            return false
        end

        row005Stat = 
        {
            "info",       "info1",       "info1-21",
            "exattrs",    "exattrs1",    "exattrs1-21",
            "stats",      "size_bytes",  0,
            "stats",       "objects",    nil,
        }

        --Test 23: Positive tests for numbers/counters;
        local positive_checks = 
        {
            {TABLE, rowKey005, "stats", "objects", hb.CompareOp.NOT_EQUAL, 0},               -- 0 !=  null/empty   true
            {TABLE, rowKey005, "stats", "objects", hb.CompareOp.GREATER, 0},                 -- 0 >   null/empty   true 
            {TABLE, rowKey005, "stats", "objects", hb.CompareOp.GREATER_OR_EQUAL, 0},        -- 0 >=  null/empty   true 
            {TABLE, rowKey005, "stats", "objects", hb.CompareOp.GREATER_OR_EQUAL, 100},      -- 100 > null/empty   true 

            {TABLE, rowKey005, "stats", "size_bytes", hb.CompareOp.NOT_EQUAL, nil},          --  nil != 0   true 
            {TABLE, rowKey005, "stats", "size_bytes", hb.CompareOp.LESS, nil},               --  nil <  0   true 
            {TABLE, rowKey005, "stats", "size_bytes", hb.CompareOp.LESS_OR_EQUAL, nil},      --  nil <= 0   true 
            {TABLE, rowKey005, "stats", "size_bytes", hb.CompareOp.GREATER, 100},            --  100 >  0   true 

            {TABLE, rowKey005, "stats", "objects",    hb.CompareOp.EQUAL, nil},              -- nil == null/empty   true 
            {TABLE, rowKey005, "stats", "size_bytes", hb.CompareOp.EQUAL, 0},                -- 0 ==  0             true 
        }
        for i,chk in ipairs(positive_checks) do

            local v_info1    = uuid()
            local v_exattrs1 = uuid()

            local put23 = 
            {
                "info",       "info1",       v_info1,
                "exattrs",    "exattrs1",    v_exattrs1,
            }
            local del23 = {"ver"}

            chk[7] = put23
            chk[8] = del23

            local code, ok, result = hb:checkAndMutateAndGetRow(unpack(chk))
            if not ok then
                ngx.say("checkAndMutateAndGetRow test failed. Test-23. HBase checkAndMutateAndGetRow failed, innercode=", code, " i=", i)
                return false
            end


            if not compareResult(result, row005Stat) then
                ngx.say("checkAndMutateAndGetRow test failed. Test-23. compareResult failed")
                return false
            end

            row005Stat = 
            {
                "info",       "info1",       v_info1,
                "exattrs",    "exattrs1",    v_exattrs1,
                "stats",      "size_bytes",  0,
                "stats",       "objects",    nil,
            }

            local ok = checkRow(TABLE, rowKey005, row005Stat)
            if not ok then
                ngx.say("checkAndMutateAndGetRow test failed. Test-23. checkRow failed.")
                return false
            end
        end


        --Test 24: Negative tests for numbers/counters;
        local put24 = 
        {
            "info",       "info1",       "NotPut",
            "exattrs",    "exattrs1",    "NotPut",
        }
        local del24 = {"ver"}
        local negative_checks = 
        {
            {TABLE, rowKey005, "stats", "objects", hb.CompareOp.EQUAL, 0, put24, del24},                   -- 0 ==  null/empty   false 
            {TABLE, rowKey005, "stats", "objects", hb.CompareOp.LESS, 0, put24, del24},                    -- 0 <   null/empty   false 
            {TABLE, rowKey005, "stats", "objects", hb.CompareOp.LESS_OR_EQUAL, 0, put24, del24},           -- 0 <=  null/empty   false 
            {TABLE, rowKey005, "stats", "objects", hb.CompareOp.EQUAL, 100, put24, del24},                 -- 100== null/empty   false 
            {TABLE, rowKey005, "stats", "objects", hb.CompareOp.LESS, 100, put24, del24},                  -- 100 < null/empty   false 

            {TABLE, rowKey005, "stats", "size_bytes", hb.CompareOp.EQUAL, nil, put24, del24},              --  nil == 0   false 
            {TABLE, rowKey005, "stats", "size_bytes", hb.CompareOp.GREATER, nil, put24, del24},            --  nil >  0   false 
            {TABLE, rowKey005, "stats", "size_bytes", hb.CompareOp.GREATER_OR_EQUAL, nil, put24, del24},   --  nil >= 0   false 
            {TABLE, rowKey005, "stats", "size_bytes", hb.CompareOp.EQUAL, 100, put24, del24},              --  100 == 0   false 
            {TABLE, rowKey005, "stats", "size_bytes", hb.CompareOp.LESS, 100, put24, del24},               --  100 <  0   false 
        }
        for i,chk in ipairs(negative_checks) do
            local code, ok, result = hb:checkAndMutateAndGetRow(unpack(chk))
            if ok then
                ngx.say("checkAndMutateAndGetRow test failed. Test-24. HBase checkAndMutateAndGetRow should fail but it succeeded, i=", i)
                return false
            end

            if code ~= "1121" then
                ngx.say("checkAndMutateAndGetRow test failed. Test-24. HBase checkAndMutateAndGetRow should fail with 1121 but code=", code, " i=", i)
                return false
            end


            if not compareResult(result, row005Stat) then
                ngx.say("checkAndMutateAndGetRow test failed. Test-24. compareResult failed, i=", i)
                return false
            end

            local ok = checkRow(TABLE, rowKey005, row005Stat)
            if not ok then
                ngx.say("checkAndMutateAndGetRow test failed. Test-24. checkRow failed, i=", i)
                return false
            end
        end



    end

    --clean
    if clean or onlyClean then
       hb:delete(TABLE, rowKey001)
       --hb:delete(TABLE, rowKey002)    -- this row was not created;
       hb:delete(TABLE, rowKey003)
       hb:delete(TABLE, rowKey004)
       hb:delete(TABLE, rowKey005)
    end
    ngx.say("OK: TestCheckAndMutateAndGetRow test successful")
    ngx.flush()
    return true
end


function TestScan()
    if not onlyClean then
        --prepare 
        for i=10, 50, 1 do
            local rowKey = "RowScanTest00"..tostring(i)

            local args = {
                "info",    "info1",     rowKey.."_".."info11111111", 
                "info",    "info2",     rowKey.."_".."info22222222",
                "quota",   "quota1",    rowKey.."_".."quota1111111",
                "ver",     "ver1",      rowKey.."_".."ver111111111",
                "ver",     "ver2",      rowKey.."_".."ver222222222",
                "exattrs", "exattrs1",  rowKey.."_".."exattrs11111",
            }

            local ok = putRow(TABLE, rowKey, args)
            if not ok then
                ngx.say("ERROR: scan test failed, prepare failed")
                return false
            end

            local code2,ok2 = hb:increment(TABLE, rowKey, {"stats", "size_bytes", i*5})
            local code3,ok3 = hb:increment(TABLE, rowKey, {"stats", "size_bytes", i*10,  "stats", "objects", i*15})
            if not ok2 or not ok3 then
                ngx.say("ERROR: scan test failed, prepare failed, prepare counters failed")
                return false
            end
        end


        --1. open scanner-1
        local code, ok, scannerId1 = hb:openScanner(TABLE, "RowScanTest0020", "RowScanTest0030")
        if not ok or not scannerId1 then
            ngx.say("ERROR: scan test failed, openScanner failed")
            return false
        end

        --2. scan using scanner-1 
        local code, ok, ret = hb:scanHash(TABLE, scannerId1, 5)
        if not ok then
            ngx.say("ERROR: scan test failed, scan failed")
            return false
        end

        for x=20, 24, 1 do  --5 rows returned
            local key = "RowScanTest00"..tostring(x)
            if ret[key]["info:info1"] ~= key.."_".."info11111111" or
                ret[key]["info:info2"] ~= key.."_".."info22222222" or
                ret[key]["quota:quota1"] ~= key.."_".."quota1111111" or
                ret[key]["ver:ver1"] ~= key.."_".."ver111111111" or
                ret[key]["ver:ver2"] ~= key.."_".."ver222222222" or
                ret[key]["stats:size_bytes"] ~= x*15 or
                ret[key]["stats:objects"] ~= x*15 or
                ret[key]["exattrs:exattrs1"] ~= key.."_".."exattrs11111" then
                ngx.say("ERROR: scan test failed, check result failed")
                return false
            end
        end

        --3. scan using scanner-1 again 
        local code, ok, ret = hb:scanHash(TABLE, scannerId1, 3)
        if not ok then
            ngx.say("ERROR: scan test failed, scan failed")
            return false
        end

        for x=25, 27, 1 do  --3 rows returned
            local key = "RowScanTest00"..tostring(x)
            if ret[key]["info:info1"] ~= key.."_".."info11111111" or
                ret[key]["info:info2"] ~= key.."_".."info22222222" or
                ret[key]["quota:quota1"] ~= key.."_".."quota1111111" or
                ret[key]["ver:ver1"] ~= key.."_".."ver111111111" or
                ret[key]["ver:ver2"] ~= key.."_".."ver222222222" or
                ret[key]["stats:size_bytes"] ~= x*15 or
                ret[key]["stats:objects"] ~= x*15 or
                ret[key]["exattrs:exattrs1"] ~= key.."_".."exattrs11111" then
                ngx.say("ERROR: scan test failed, check result failed")
                return false
            end
        end

        --4. open scanner-2
        local code, ok, scannerId2 = hb:openScanner(TABLE, "RowScanTest0008", "RowScanTest0020", {"info", "quota:quota1", "stats"})
        if not ok or not scannerId2 then
            ngx.say("ERROR: scan test failed, openScanner-2 failed")
            return false
        end

        --5. scan using scanner-2 
        local code, ok, ret = hb:scanHash(TABLE, scannerId2, 6)
        if not ok then
            ngx.say("ERROR: scan test failed, scan failed")
            return false
        end

        --we don't have 8, 9
        for x=8, 9, 1 do
            local key = "RowScanTest00"..tostring(x)
            if ret[key] ~= nil then
                ngx.say("ERROR: scan test failed, unexpected rows returned")
                return false
            end
        end

        for x=10, 15, 1 do  --6 rows returned
            local key = "RowScanTest00"..tostring(x)
            if ret[key]["info:info1"] ~= key.."_".."info11111111" or
                ret[key]["info:info2"] ~= key.."_".."info22222222" or
                ret[key]["quota:quota1"] ~= key.."_".."quota1111111" or
                ret[key]["ver:ver1"] ~= nil or
                ret[key]["ver:ver2"] ~= nil or
                ret[key]["stats:size_bytes"] ~= x*15 or
                ret[key]["stats:objects"] ~= x*15 or
                ret[key]["exattrs:exattrs1"] ~= nil then
                ngx.say("ERROR: scan test failed, check result failed")
                return false
            end
        end

        --6. scan using scanner-1 again 
        local code, ok, ret = hb:scanHash(TABLE, scannerId1, 8)  --Yuanguo: we don't have 8 rows, we only have 2 rows
        if not ok then
            ngx.say("ERROR: scan test failed, scan failed")
            return false
        end
        for x=28, 29, 1 do  --2 rows returned
            local key = "RowScanTest00"..tostring(x)
            if ret[key]["info:info1"] ~= key.."_".."info11111111" or
                ret[key]["info:info2"] ~= key.."_".."info22222222" or
                ret[key]["quota:quota1"] ~= key.."_".."quota1111111" or
                ret[key]["ver:ver1"] ~= key.."_".."ver111111111" or
                ret[key]["ver:ver2"] ~= key.."_".."ver222222222" or
                ret[key]["stats:size_bytes"] ~= x*15 or
                ret[key]["stats:objects"] ~= x*15 or
                ret[key]["exattrs:exattrs1"] ~= key.."_".."exattrs11111" then
                ngx.say("ERROR: scan test failed, check result failed")
                return false
            end
        end

        for x=30, 31, 1 do --they should be nil; stopRow (30) is excluted
            local key = "RowScanTest00"..tostring(x)
            if ret[key] ~= nil then
                ngx.say("ERROR: scan test failed, more rows then expected returned")
                return false
            end
        end

        --7. scan using scanner-2 again
        local code, ok, ret = hb:scanHash(TABLE, scannerId2, 8)
        if not ok then
            ngx.say("ERROR: scan test failed, scan failed")
            return false
        end

        for x=16, 19, 1 do  --we only have 4 rows
            local key = "RowScanTest00"..tostring(x)
            if ret[key]["info:info1"] ~= key.."_".."info11111111" or
                ret[key]["info:info2"] ~= key.."_".."info22222222" or
                ret[key]["quota:quota1"] ~= key.."_".."quota1111111" or
                ret[key]["ver:ver1"] ~= nil or
                ret[key]["ver:ver2"] ~= nil or
                ret[key]["stats:size_bytes"] ~= x*15 or
                ret[key]["stats:objects"] ~= x*15 or
                ret[key]["exattrs:exattrs1"] ~= nil then
                ngx.say("ERROR: scan test failed, check result failed")
                return false
            end
        end

        for x=20, 21, 1 do --stopRow (20) is excluted
            local key = "RowScanTest00"..tostring(x)
            if ret[key] ~= nil then
                ngx.say("ERROR: scan test failed, more rows then expected returned")
                return false
            end
        end

        --8. close scanner-2
        local code, ok = hb:closeScanner(scannerId2)
        if not ok then
            ngx.say("ERROR: scan test failed, closeScanner-2 failed")
            return false
        end

        --9. close scanner-1
        local code, ok = hb:closeScanner(scannerId1)
        if not ok then
            ngx.say("ERROR: scan test failed, closeScanner-1 failed")
            return false
        end

        --10. open scanner-3
        local code, ok, scannerId3 = hb:openScanner(TABLE, "RowScanTest0045", "RowScanTest0055", {})
        if not ok or not scannerId3 then
            ngx.say("ERROR: scan test failed, openScanner-3 failed")
            return false
        end

        --11. scan using scanner-3 
        local code, ok, ret = hb:scanHash(TABLE, scannerId3, 11)
        if not ok then
            ngx.say("ERROR: scan test failed, scan failed")
            return false
        end

        --we don't have 51-54
        for x=51, 54, 1 do
            local key = "RowScanTest00"..tostring(x)
            if ret[key] ~= nil then
                ngx.say("ERROR: scan test failed, unexpected rows returned")
                return false
            end
        end

        for x=45, 50, 1 do  --6 rows returned
            local key = "RowScanTest00"..tostring(x)
            if ret[key]["info:info1"] ~= key.."_".."info11111111" or
                ret[key]["info:info2"] ~= key.."_".."info22222222" or
                ret[key]["quota:quota1"] ~= key.."_".."quota1111111" or
                ret[key]["ver:ver1"] ~= key.."_".."ver111111111" or
                ret[key]["ver:ver2"] ~= key.."_".."ver222222222" or
                ret[key]["stats:size_bytes"] ~= x*15 or
                ret[key]["stats:objects"] ~= x*15 or
                ret[key]["exattrs:exattrs1"] ~= key.."_".."exattrs11111" then
                ngx.say("ERROR: scan test failed, check result failed")
                return false
            end
        end

        --12. close scanner-3
        local code, ok = hb:closeScanner(scannerId3)
        if not ok then
            ngx.say("ERROR: scan test failed, closeScanner-3 failed")
            return false
        end


        --13. open scanner-4
        local code, ok, scannerId4 = hb:openScanner(TABLE, "RowScanTest00"..hb.MIN_CHAR, "RowScanTest00"..hb.MAX_CHAR, {})
        if not ok or not scannerId4 then
            ngx.say("ERROR: scan test failed, openScanner-4 failed")
            return false
        end

        --14. scan using scanner-4 
        local code, ok, ret = hb:scanHash(TABLE, scannerId4, 100)
        if not ok then
            ngx.say("ERROR: scan test failed, scan failed")
            return false
        end

        for x=10, 50, 1 do  --all rows returned
            local key = "RowScanTest00"..tostring(x)
            if ret[key]["info:info1"] ~= key.."_".."info11111111" or
                ret[key]["info:info2"] ~= key.."_".."info22222222" or
                ret[key]["quota:quota1"] ~= key.."_".."quota1111111" or
                ret[key]["ver:ver1"] ~= key.."_".."ver111111111" or
                ret[key]["ver:ver2"] ~= key.."_".."ver222222222" or
                ret[key]["stats:size_bytes"] ~= x*15 or
                ret[key]["stats:objects"] ~= x*15 or
                ret[key]["exattrs:exattrs1"] ~= key.."_".."exattrs11111" then
                ngx.say("ERROR: scan test failed, check result failed")
                return false
            end
        end

        --15. close scanner-4
        local code, ok = hb:closeScanner(scannerId4)
        if not ok then
            ngx.say("ERROR: scan test failed, closeScanner-4 failed")
            return false
        end

        --16. scan after close, should fail
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "========Negative Test: scan, scan after closeScanner")
        local code, ok = hb:scanHash(TABLE, scannerId4, 3)
        if ok or code ~= "1114" then
            ngx.say("ERROR: scan test failed, scan after closeScanner, it should fail but it didn't")
            return false
        end
    end

    --clean
    if clean or onlyClean then
        for i=10, 50, 1 do
            local rowKey = "RowScanTest00"..tostring(i)
            hb:delete(TABLE, rowKey)
        end
    end
    ngx.say("OK: scan test successful")
    ngx.flush()
    return true
end

function TestQuickScan()
    if not onlyClean then
        --prepare 
        for i=51, 70, 1 do
            local rowKey = "RowScanTest00"..tostring(i)

            local args = {
                "info",    "info1",     rowKey.."_".."info11111111", 
                "info",    "info2",     rowKey.."_".."info22222222",
                "quota",   "quota1",    rowKey.."_".."quota1111111",
                "ver",     "ver1",      rowKey.."_".."ver111111111",
                "ver",     "ver2",      rowKey.."_".."ver222222222",
                "exattrs", "exattrs1",  rowKey.."_".."exattrs11111",
            }

            local ok = putRow(TABLE, rowKey, args)
            if not ok then
                ngx.say("ERROR: quickScan test failed, prepare failed")
                return false
            end

            local code2,ok2 = hb:increment(TABLE, rowKey, {"stats", "size_bytes", i*5})
            local code3,ok3 = hb:increment(TABLE, rowKey, {"stats", "size_bytes", i*10,  "stats", "objects", i*15})
            if not ok2 or not ok3 then
                ngx.say("ERROR: quickScan test failed, prepare failed, prepare counters failed")
                return false
            end
        end


        --1. numRows is less than the range 
        local code, ok, ret = hb:quickScanHash(TABLE, "RowScanTest0048",  "RowScanTest0059", nil, 3)

        --we don't have 48-50
        for x=48, 50, 1 do 
            local key = "RowScanTest00"..tostring(x)
            if ret[key] ~= nil then
                ngx.say("ERROR: quickScan test failed, unexpected rows returned")
                return false
            end
        end

        for x=51, 53, 1 do   --although the range is 48-59, we only scan 3 rows
            local key = "RowScanTest00"..tostring(x)
            if ret[key]["info:info1"] ~= key.."_".."info11111111" or
                ret[key]["info:info2"] ~= key.."_".."info22222222" or
                ret[key]["quota:quota1"] ~= key.."_".."quota1111111" or
                ret[key]["ver:ver1"] ~= key.."_".."ver111111111" or
                ret[key]["ver:ver2"] ~= key.."_".."ver222222222" or
                ret[key]["stats:size_bytes"] ~= x*15 or
                ret[key]["stats:objects"] ~= x*15 or
                ret[key]["exattrs:exattrs1"] ~= key.."_".."exattrs11111" then
                ngx.say("ERROR: quickScan test failed, check result failed")
                return false
            end
        end

        --we don't have 54-59
        for x=48, 50, 1 do 
            local key = "RowScanTest00"..tostring(x)
            if ret[key] ~= nil then
                ngx.say("ERROR: quickScan test failed, unexpected rows returned")
                return false
            end
        end

        --2. numRows is less than the range 
        local code, ok, ret = hb:quickScanHash(TABLE, "RowScanTest0061",  "RowScanTest0075", {"stats", "info:info1", "info"}, 30)
        for x=61, 70, 1 do 
            local key = "RowScanTest00"..tostring(x)
            if ret[key]["info:info1"] ~= key.."_".."info11111111" or
                ret[key]["info:info2"] ~= key.."_".."info22222222" or
                ret[key]["quota:quota1"] ~= nil or
                ret[key]["ver:ver1"] ~= nil or
                ret[key]["ver:ver2"] ~= nil or
                ret[key]["stats:size_bytes"] ~= x*15 or
                ret[key]["stats:objects"] ~= x*15 or
                ret[key]["exattrs:exattrs1"] ~= nil then
                ngx.say("ERROR: quickScan test failed, check result failed")
                return false
            end
        end
        for x=71, 75, 1 do 
            local key = "RowScanTest00"..tostring(x)
            if ret[key] ~= nil then
                ngx.say("ERROR: quickScan test failed, unexpected rows returned")
                return false
            end
        end

        --3. stopRow is excluded 
        local code, ok, ret = hb:quickScanHash(TABLE, "RowScanTest0061",  "RowScanTest0063", {"stats", "info:info1", "info"}, 30)
        for x=61, 62, 1 do 
            local key = "RowScanTest00"..tostring(x)
            if ret[key]["info:info1"] ~= key.."_".."info11111111" or
                ret[key]["info:info2"] ~= key.."_".."info22222222" or
                ret[key]["quota:quota1"] ~= nil or
                ret[key]["ver:ver1"] ~= nil or
                ret[key]["ver:ver2"] ~= nil or
                ret[key]["stats:size_bytes"] ~= x*15 or
                ret[key]["stats:objects"] ~= x*15 or
                ret[key]["exattrs:exattrs1"] ~= nil then
                ngx.say("ERROR: quickScan test failed, check result failed")
                return false
            end
        end
        for x=63, 64, 1 do 
            local key = "RowScanTest00"..tostring(x)
            if ret[key] ~= nil then
                ngx.say("ERROR: quickScan test failed, unexpected rows returned")
                return false
            end
        end

        --4. scan all 
        local code, ok, ret = hb:quickScanHash(TABLE, "RowScanTest00"..hb.MIN_CHAR,  "RowScanTest00"..hb.MAX_CHAR, {"stats", "info:info1", "info"}, 30)
        for x=51, 70, 1 do 
            local key = "RowScanTest00"..tostring(x)
            if ret[key]["info:info1"] ~= key.."_".."info11111111" or
                ret[key]["info:info2"] ~= key.."_".."info22222222" or
                ret[key]["quota:quota1"] ~= nil or
                ret[key]["ver:ver1"] ~= nil or
                ret[key]["ver:ver2"] ~= nil or
                ret[key]["stats:size_bytes"] ~= x*15 or
                ret[key]["stats:objects"] ~= x*15 or
                ret[key]["exattrs:exattrs1"] ~= nil then
                ngx.say("ERROR: quickScan test failed, check result failed")
                return false
            end
        end
    end

    --clean
    if clean or onlyClean then
        for i=51, 70, 1 do
            local rowKey = "RowScanTest00"..tostring(i)
            hb:delete(TABLE, rowKey)
        end
    end
    ngx.say("OK: quickScan test successful")
    ngx.flush()
    return true
end

local tests = 
{
    TestPut,
    TestGet,
    TestIncrement,
    TestCheckAndPut,
    TestDelete,
    TestCheckAndDelete,
    TestMutateRow,
    TestCheckAndMutateRow,
    TestScan,
    TestQuickScan,
    TestCheckAndMutateAndGetRow,
}

local total   = #tests 
local success = 0
local failure = 0
for i, func in ipairs(tests) do
    local ok = func()
    if ok then
        success = success + 1
    else
        failure = failure + 1
        break
    end
end
local notRun = total - success - failure
ngx.say("Total   : "..tostring(total))
ngx.say("Success : "..tostring(success))
ngx.say("Failure : "..tostring(failure))
ngx.say("Skipped : "..tostring(notRun))
