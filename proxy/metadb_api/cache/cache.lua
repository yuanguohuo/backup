-- By Yuanguo, 14/04/2017

local ok, new_tab = pcall(require, "table.new")
if not ok or type(new_tab) ~= "function" then
    new_tab = function (narr, nrec) return {} end
end

local ok, utils = pcall(require, "common.utils")
if not ok or not utils then
    error("failed to load common.utils:" .. (utils or "nil"))
end

local stringify = utils.stringify

--for unit test
local UnitTest = false
if UnitTest then
    ngx = 
    {
        null = "NULL",

        DEBUG = "[DEBUG]",
        INFO  = "[INFO]",
        WARN  = "[WARN]",
        ERR   = "[ERR]",

        ctx = {reqid="TestReqID"},

        log = 
            function(level, ...)
                local args = {...}
                local str = ""
                for _,v in ipairs(args) do
                    str = str .. tostring(v)
                end
                print(level, str)
            end
    }
end

local _M = new_tab(0, 11)
_M._VERSION = '0.1'

_M.POLICY =
{
    VERY_STABLE  = "very-stable",
    STABLE       = "stable",
    STATS        = "stats",
}

--search for a member (function or variable) in 'self'.
--self: the 'cache instance'; that's the instance created by "cache:new(cacheset,backstore)"
--key : the member we are searching for;
local function __cache_index_func(self, key)
    local member = rawget(_M, key)  --search in _M
    if member then
        return member
    end

    local bs = rawget(self, "backstore")
    assert(bs)

    member = bs[key]
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", self.storName, " __cache_index_func", " key=", key, " member-type=", type(member))
    return member 
end

--params:
--  cacheset   : the tables and their column-families that should be cached 
--               in 'this cache'. For example:
--                      lcache_cacheset in lcache.lua;
function _M.new(self, storName, cacheset, backstore)
    if not backstore then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " backstore of ", storName, " cannot be nil")
        assert(false)
    end

    local newobj = {storName = storName, cacheset = cacheset, backstore = backstore}

    -- inherits both _M and backstore
    setmetatable(newobj, {__index = __cache_index_func})
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " Create Parent of ", storName)
    return newobj
end

--find out which of the requested columns are in cacheset, in what policy, and 
--which are not in cacheset;
--params:
--    table_name : the hbase table name,
--    columns    : the name of columns in format of {"family:column", "family:column", ...}
--return:
--    cache_columns : columns of a table that should be processed by 'this cache' (in the cacheset); 
--                    if none of the requested columns is in cacheset, it's an empty table;
--                    else, it's a table like this:
--                       {
--                          policy1 => {"family:column", "family:column", ...}
--                          policy2 => {"family:column", "family:column", ...}
--                          ...
--                       }
--    back_columns  : columns of a table that should be processed by backstore (not in the cacheset);
--                    if all of the requested columns are in cacheset, it's an empty table;
--                    otherwise, it's a table like this: 
--                       {"family:column", "family:column", ...}
function _M.sort_columns(self, table_name, columns)
    local cacheset4table = self.cacheset[table_name]

    if not cacheset4table then
        return {}, columns
    end

    local cache_columns  = new_tab(0,6)
    local back_columns = new_tab(64,0)

    for _,col in ipairs(columns) do
        local pcolon = string.find(col, ":")
        assert(pcolon)
        local fam = string.sub(col, 1, pcolon-1)

        local found = false
        for policy, famset in pairs(cacheset4table) do
            if famset[fam] then
                cache_columns[policy] = cache_columns[policy] or new_tab(64,0)
                table.insert(cache_columns[policy], col)
                found = true
                break
            end
        end
        if not found then
            table.insert(back_columns, col)
        end
    end

    return cache_columns, back_columns
end

--Return:
--   cache_colVals: the column values need to be processed by 'this cache';
--   back_colVals : the column values need to be processed by backstore;
function _M.sort_colVals(self, table_name, colVals)
    local cacheset4table = self.cacheset[table_name]

    if not cacheset4table then
        return {}, colVals
    end

    local cache_colVals  = new_tab(0,6)
    local back_colVals = new_tab(64,0)

    for i = 1, #colVals-2, 3 do
        local found = false 
        local fam, qua, val = colVals[i], colVals[i+1], colVals[i+2]
        for policy, famset in pairs(cacheset4table) do
            if famset[fam] then
                cache_colVals[policy] = cache_colVals[policy] or new_tab(64,0)
                table.insert(cache_colVals[policy], fam)
                table.insert(cache_colVals[policy], qua)
                table.insert(cache_colVals[policy], val)
                found = true
                break
            end
        end

        if not found then
            table.insert(back_colVals, fam)
            table.insert(back_colVals, qua)
            table.insert(back_colVals, val)
        end
    end

    return cache_colVals, back_colVals
end

--Params:
--    table_name : the name of the hbase table
--    key        : the row key
--    columns    : the name of columns to get in format of {"family:column", "family:column", ...}
--                 if it's nil or an empty table, the call will succeed and an empty table will 
--                 be returned.
--Return: innercode, ok, body; where body is a table as shown below in the case of
--        success or nil in the case of failure:
--                {
--                    [family:column] = value
--                    [family:column] = value
--                    ...
--                }
function _M.get(self, table_name, key, columns)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", self.storName, " Enter get() table_name=", table_name, 
                      " key=", key, " columns=", stringify(columns))

    local result = new_tab(0,6)

    if nil == columns  or nil == next(columns) then
        return "0000", true, result
    end

    if not self.enabled then
        self:update_miss()
        return self.backstore:get(table_name, key, columns)
    end

    local in_cacheset,out_cacheset = self:sort_columns(table_name, columns)

    --columns that need to be fetched from backstore; it consists of 2 parts:
    --   1. out_cacheset: the columns that are not in cacheset of 'this cache';
    --   2. missing     : the columns that are in cacheset of 'this cache', but missed;
    local get_from_back = out_cacheset

    local missing = new_tab(0,6)   --the 'missing table'
    for policy, cols in pairs(in_cacheset) do
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", self.storName, " get columns: policy=", policy, " cols=", stringify(cols))
        local innercode,ok,body,miss = self:process_get(policy, table_name, key, cols)
        if not ok then
            ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ", self.storName, " process_get() failed: innercode=", innercode, ". Get from backstore directly.")
            return self.backstore:get(table_name, key, columns)
        end
        for k,v in pairs(body) do
            result[k] = v
        end

        if nil ~= next(miss) then
            missing[policy] = new_tab(0,6)
            for _,c in ipairs(miss) do
                table.insert(get_from_back, c)
                missing[policy][c] = true    --mark as missed in 'missing-table';
            end
        end
    end

    --we don't need to access backstore, because we have not missing columns or columns not in cacheset;
    if nil == next(get_from_back) then
        --we deem it as "hit" if and only if we don't need to access backstore;
        self:update_hit()
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", self.storName, " hit")
        return "0000", true, result
    end

    --get_from_back is not empty thus we need to access backstore; 
    --we deem it as "miss" as long as we need to access backstore;
    self:update_miss()
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", self.storName, " miss")
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", self.storName, " get from backstore:", stringify(get_from_back))

    local innercode,ok,body = self.backstore:get(table_name, key, get_from_back)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", self.storName, " backstore:get() failed: innercode=", innercode)
        return innercode,false,nil
    end

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", self.storName, " backstore returned:", stringify(body))

    for k,v in pairs(body) do
        result[k] = v
    end

    for _,m in pairs(missing) do
        for k,_ in pairs(m) do
            m[k] = body[k]
        end
    end

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", self.storName, " update cache:", stringify(missing))

    for policy, miss in pairs(missing) do
        local innercode,ok = self:update_cache(policy, table_name, key, miss)
        if not ok then
            ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ", self.storName, " update_cache() failed: innercode=", innercode)
        end
    end

    return "0000", true, result
end

--Params:
--    table_name: the name of the table
--    key       : the row key
--    colValues : an array containing family1, column1, amount, family2, column2, amount ... they 
--                must be in triples. e.g.
--                {[1]=family1, [2]=column1, [3]=value1, [4]=family2, [5]=column2, [6]=value2, ... }
--                if it's nil or an empty table, the call will succeeded, but nothing will be done,
--                and an empty table will be returned.
--Return: innercode, ok, current
--               current: the current values of the incremented columns; it's a table like this:
--               {
--                   [family:column] = value
--                   [family:column] = value
--                   ...
--               }
function _M.increment(self, table_name, key, colValues)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", self.storName," Enter increment() table_name=", table_name, " key=", key, " colValues=", stringify(colValues))

    if not self.enabled then
        self:update_backstore_increment()
        return self.backstore:increment(table_name, key, colValues)
    end

    local current = new_tab(0,6)
    if nil == colValues or nil == next(colValues) then
        return "0000", true, current
    end

    local in_cacheset,out_cacheset = self:sort_colVals(table_name, colValues)

    local need_flush = nil
    if nil ~= next(in_cacheset) then
        self:update_cache_increment()
        --there should be only 1 policy, and it must be 'STATS'
        for policy in pairs(in_cacheset) do
            if policy ~= _M.POLICY.STATS then
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", self.storName " Invalid arguments: cannot 'increment' non-stats columns: ", stringify(in_cacheset[policy]))
                return "1001",false,nil
            end
        end

        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", self.storName, " increment columns/counters: policy=", _M.POLICY.STATS, " colValues=", stringify(in_cacheset[_M.POLICY.STATS]))

        local innercode,ok,current1 = nil,nil,nil
        innercode,ok,current1,need_flush = self:process_increment(_M.POLICY.STATS, table_name, key, in_cacheset[_M.POLICY.STATS])
        if not ok then
            ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ", self.storName, " process_increment() failed: innercode=", innercode, ". Increment backstore directly.")
            return self.backstore:increment(table_name, key, colValues)
        end

        for k,v in pairs(current1) do
            current[k] = v
        end
    end

    --out_cacheset are columns that 'this cache' don't care about, so it needs to be incremented 
    --in backstore; and need_flush also needs to; thus, merge them! 
    if need_flush and next(need_flush) then
        for i=1,#need_flush-2,3 do
            table.insert(out_cacheset, need_flush[i])
            table.insert(out_cacheset, need_flush[i+1])
            table.insert(out_cacheset, need_flush[i+2])
        end
    end

    --if we have columns that need to be incremented in backstore, do it!
    if nil ~= next(out_cacheset) then
        self:update_backstore_increment()
        local innercode,ok,current2 = nil,nil,nil
        for retry=1,3 do
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", self.storName, " increment columns/counters in backstore: retry=", retry, " ", stringify(out_cacheset))
            innercode,ok,current2 = self.backstore:increment(table_name, key, out_cacheset)
            if ok then   -- if it's timeout error, the value might be incremented more then 1 times ?
                break
            end
        end

        if not ok then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", self.storName, " backstore:increment() failed: innercode=", innercode)
            return innercode,false,nil
        end

        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", self.storName, " backstore current=", stringify(current2))
        for k,v in pairs(current2) do
            current[k] = v
        end

        --we got the most updated 'base' (when we flush the increments), so, update the 'base' in cache
        if need_flush and next(need_flush) then
            local updates = new_tab(0,6)
            for i=1,#need_flush-2,3 do
                local fc = need_flush[i] .. ":" .. need_flush[i+1]
                updates[fc] = current2[fc]
            end

            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", self.storName, " update the base part of columns/counters:", stringify(updates))
            innercode,ok = self:update_cache(_M.POLICY.STATS, table_name, key, updates) --policy must be STATS
            if not ok then
                --if failed, not a big deal, because it will be updated next time;
                ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ", self.storName, " update_cache() failed: innercode=", innercode)
            end
        end
    end

    return "0000", true, current
end

--Params:
--    table_name: the name of the table
--    key       : the row key
--    colValues : an array containing family1, column1, value, family2, column2, value ... they 
--                must be in triples, e.g.
--                if it's nil or an empty table, the call will succeeded but nothing will be done.
--                {[1]=family1, [2]=column1, [3]=value1, [4]=family2, [5]=column2, [6]=value2, ... }
--    ttl       : ttl in milli-seconds. Notice that the ttl is only set in HBase. In LCache and RCache,
--                this ttl is not set, because the row has expiration-time in LCache and RCache. Normally, 
--                the expiration-time is much shorter than ttl.
--Return: innercode, ok
function _M.put(self, table_name, key, colValues, ttl)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", self.storName, " Enter put() table_name=", table_name, 
                      " key=", key, " colValues=", stringify(colValues), " ttl=", ttl)

    if not self.enabled then
        self:update_backstore_put()
        return self.backstore:put(table_name, key, colValues, ttl)
    end

    if nil == colValues or nil == next(colValues) then
        return "0000", true
    end

    local in_cacheset,_ = self:sort_colVals(table_name, colValues)

    --'stats' values are 'COUNTER values' of HBase, so they cannot do 'put' operation
    for policy, col_vals in pairs(in_cacheset) do
        if policy == _M.POLICY.STATS then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", self.storName, " Invalid arguments: cannot 'put' stats columns: ", stringify(in_cacheset[policy]))
            return "1001",false
        end
    end

    self:update_backstore_put()

    --all of 'colValues' should be put into backstore
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", self.storName, " put into backstore:", stringify(colValues))
    local innercode,ok = self.backstore:put(table_name, key, colValues, ttl)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", self.storName, " backstore:put() failed: innercode=", innercode)
        return innercode,false
    end

    if nil ~= next(in_cacheset) then
        self:update_cache_put()
        --in_cacheset are column families that should be cached in 'this cache'. So 'this cache'
        --should process them;
        for policy, col_vals in pairs(in_cacheset) do
            local innercode,ok = self:process_put(policy, table_name, key, col_vals)
            if not ok then
                --failed to put into cache, not a big problem, because the outdated values
                --in cache will expire, and updated/new values will be cached;
                --So, we just log WARN
                ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ", self.storName, " process_put() failed: innercode=", innercode)
            end
        end
    end

    return "0000", true
end

--For the given row, do the put operation only if the value of 'checkFamily':'checkColumn' matches 'checkValue'
--Params:
--    table_name : the name of the table
--    key        : the key of the given row
--    checkFamily: the family to check
--    checkColumn: the column to check
--    checkValue : the value to check
--    colValues  : a table containing family1, column1, value, family2, column2, value ... they 
--                 must be in triples, e.g.
--                 {[1]=family1, [2]=column1, [3]=value1, [4]=family2, [5]=column2, [6]=value2, ... }
--                 if it's nil or an empty table, the call will succeeded but nothing will be done.
--    ttl        : ttl in milli-seconds. Notice that the ttl is only set in HBase. In LCache and RCache,
--                 this ttl is not set, because the row has expiration-time in LCache and RCache. Normally, 
--                 the expiration-time is much shorter than ttl.
--Return: innercode, ok
function _M.checkAndPut(self, table_name, key, checkFamily, checkColumn, checkValue, colValues, ttl)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", self.storName, " Enter checkAndPut() table_name=", table_name, 
               " key=", key, " checkFamily=", checkFamily, " checkColumn=", checkColumn, " checkValue=", checkValue, " colValues=", stringify(colValues))

    if not self.enabled then
        self:update_backstore_checkAndPut()
        return self.backstore:checkAndPut(table_name, key, checkFamily, checkColumn, checkValue, colValues, ttl)
    end

    if nil == colValues or nil == next(colValues) then
        return "0000", true
    end

    local in_cacheset,_ = self:sort_colVals(table_name, colValues)

    --'stats' values are 'COUNTER values' of HBase, so they cannot do 'checkAndPut' operation
    for policy, col_vals in pairs(in_cacheset) do
        if policy == _M.POLICY.STATS then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", self.storName, " Invalid arguments: cannot 'checkAndPut' stats columns: ", stringify(in_cacheset[policy]))
            return "1001",false
        end
    end

    self:update_backstore_checkAndPut()

    --all of 'colValues' should be put into backstore
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", self.storName, " checkAndPut into backstore:", stringify(colValues))
    local innercode,ok = self.backstore:checkAndPut(table_name, key, checkFamily, checkColumn, checkValue, colValues, ttl)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", self.storName, " backstore:checkAndPut() failed: innercode=", innercode)
        return innercode,false
    end

    if nil ~= next(in_cacheset) then
        self:update_cache_checkAndPut()
        --in_cacheset are column families that should be cached in 'this cache'. So 'this cache'
        --should process them;
        for policy, col_vals in pairs(in_cacheset) do
            local innercode,ok = self:process_checkAndPut(policy, table_name, key, col_vals)
            if not ok then
                --failed to put into cache, not a big problem, because the outdated values
                --in cache will expire, and updated/new values will be cached;
                --So, we just log WARN
                ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ", self.storName, " process_checkAndPut() failed: innercode=", innercode)
            end
        end
    end

    return "0000", true
end

--Params:
--    table_name: the name of the table
--    key       : the row key
--    columns   : the names of columns to delete in format of {"family:column", "family:column", ...}
--                if it's nil or an empty table, the entire row will be deleted.
--Return: innercode, ok
function _M.delete(self, table_name, key, columns)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", self.storName, " Enter delete() table_name=", table_name, " key=", key, " columns=", stringify(columns))

    if not self.enabled then
        self:update_backstore_delete()
        return self.backstore:delete(table_name, key, columns)
    end

    if nil == columns or nil == next(columns) then
        --delete the entire row from backstore
        self:update_backstore_delete()
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", self.storName, " delete entire row from backstore: key=", table_name.."_"..key)
        local innercode,ok = self.backstore:delete(table_name, key)
        if not ok then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", self.storName, " backstore:delete() failed: innercode=", innercode)
            return innercode, false
        end
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", self.storName, " delete entire row: key=", table_name.."_"..key)

        if not self.cacheset[table_name] then
            return "0000", true
        end

        self:update_cache_delete()
        local innercode,ok = self:process_delete_row(table_name, key)  --delete the entire row from 'this cache'
        if not ok then
            --failed to delete entire row from 'this cache', there are 2 types of columns
            --  1. non-stats columns: not a big problem. they are in a single 'redis hash stucture', and there is
            --     an expiration time;
            --  2. stats columns: not important data; moreover, the user/bucket has been deleted, so you have no
            --     way to see its stats columns, unless you scan redis;
            --So, we just log WARN;
            ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ", self.storName, " process_delete_row() failed: innercode=", innercode)
        end
        return "0000", true
    end

    local in_cacheset,_ = self:sort_columns(table_name, columns)

    --delete specified columns from backstore
    self:update_backstore_delete()
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", self.storName, " delete columns from backstore: key=", table_name.."_"..key, " columns=", stringify(columns))
    local innercode,ok = self.backstore:delete(table_name, key, columns)
    if not ok then 
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", self.storName, " backstore:delete() failed: innercode=", innercode)
        return innercode,false
    end

    if nil ~= next(in_cacheset) then
        self:update_cache_delete()
        for policy, cols in pairs(in_cacheset) do
            --delete specified columns from 'this cache'
            local innercode,ok = self:process_delete(policy, table_name, key, cols)
            if not ok then
                ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ", self.storName, " process_delete() failed: innercode=", innercode)
            end
        end
    end

    return "0000", true
end

--For the given row, do the delete operation only if the value of 'checkFamily':'checkColumn' matches 'checkValue'
--Params:
--    table_name : the name of the table
--    key        : the key of the given row
--    checkFamily: the family to check
--    checkColumn: the column to check
--    checkValue : the value to check
--    columns    : the names of columns to delete in format of {"family:column", "family:column", ...}
--                 if it's nil or an empty table and the check-condition holds, the entire row will be deleted.
--Return: innercode, ok
function _M.checkAndDelete(self, table_name, key, checkFamily, checkColumn, checkValue, columns)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", self.storName, " Enter checkAndDelete() table_name=", table_name, 
                      " key=", key, " checkFamily=", checkFamily, "checkColumn=", checkColumn, " checkValue=", checkValue, " columns=", stringify(columns))

    if not self.enabled then
        self:update_backstore_checkAndDelete()
        return self.backstore:checkAndDelete(table_name, key, checkFamily, checkColumn, checkValue, columns)
    end

    if nil == columns or nil == next(columns) then
        --checkAndDelete the entire row from backstore
        self:update_backstore_checkAndDelete()
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", self.storName, " checkAndDelete entire row from backstore: key=", table_name.."_"..key, " checkFamily=", checkFamily, "checkColumn=", checkColumn, " checkValue=", checkValue)
        local innercode,ok = self.backstore:checkAndDelete(table_name, key, checkFamily, checkColumn, checkValue)
        if not ok then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", self.storName, " backstore:checkAndDelete() failed: innercode=", innercode)
            return innercode, false
        end

        if not self.cacheset[table_name] then
            return "0000", true
        end

        --delete the entire row from 'this cache'
        self:update_cache_checkAndDelete()
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", self.storName, " checkAndDelete entire row: key=", table_name.."_"..key)
        local innercode,ok = self:process_delete_row(table_name, key)
        if not ok then
            --failed to delete entire row from 'this cache', there are 2 types of columns
            --  1. non-stats columns: not a big problem. they are in a single 'redis hash stucture', and there is
            --     an expiration time;
            --  2. stats columns: not important data; moreover, the user/bucket has been deleted, so you have no
            --     way to see its stats columns, unless you scan redis;
            --So, we just log WARN;
            ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ", self.storName, " process_delete_row() failed: innercode=", innercode)
        end
        return "0000", true
    end

    local in_cacheset,_ = self:sort_columns(table_name, columns)

    self:update_backstore_checkAndDelete()
    --checkAndDelete specified columns from backstore
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", self.storName, " checkAndDelete columns from backstore: key=", table_name.."_"..key, " checkFamily=", checkFamily, "checkColumn=", checkColumn, " checkValue=", checkValue, "columns=", stringify(columns))
    local innercode,ok = self.backstore:checkAndDelete(table_name, key, checkFamily, checkColumn, checkValue, columns)
    if not ok then 
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", self.storName, " backstore:checkAndDelete() failed: innercode=", innercode)
        return innercode,false
    end

    if nil ~= next(in_cacheset) then
        self:update_cache_checkAndDelete()
        for policy, cols in pairs(in_cacheset) do
            --delete specified columns from 'this cache'
            local innercode,ok = self:process_checkAndDelete(policy, table_name, key, cols) 
            if not ok then
                ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ", self.storName, " process_checkAndDelete() failed: innercode=", innercode)
            end
        end
    end

    return "0000", true
end

--For the given row, do the put and/or delete operations atomically only if  
--    compareOp(current-value-of-checkFamily:checkColumn, checkValue) = true
--Params:
--    table_name : the name of the table
--    key        : the key of the given row
--    checkFamily: the family to check
--    checkColumn: the column to check
--    compareOp  : the compare op. it should be one of
--                     LESS             = 0
--                     LESS_OR_EQUAL    = 1
--                     EQUAL            = 2
--                     NOT_EQUAL        = 3
--                     GREATER_OR_EQUAL = 4
--                     GREATER          = 5
--                     NO_OP            = 6
--    checkValue : the value to check
--    put        : an array with the family:column:value to put, it must be like this:
--                         {[1]=family1, [2]=column1, [3]=value1, [4]=family2, [5]=column2, [6]=value2, ... }
--    del        : an array containing an optional list of family[:column]; e.g.
--                         {"family1:column1", "family2", "family3:column3", ...}
--                 if del is nil or an empty table, no data is deleted; else, 
--                 the specified family and/or columns are deleted;
--    ttl        : ttl in milli-seconds
--Note:
--    if you want to put only (no delete), you may pass nil to del or omit it.
--    if you want to delete only (no put), you must pass nil or empty table to put.
--Return: innercode, ok
--           innercode : "0000" if ok is true;
--                       "1121" if ok is false and the failure is caused by condition-check-failure;
--                       "xxxx" if ok is false and the failure is caused by other reasons;
--           ok        : true   if the 'MutateRow' operation has been done; 
--                       false  if the 'MutateRow' operation has NOT been done; if this is because of 
--                              condition-check-failure, innercode must be "1121";
function _M.checkAndMutateRow(self, table_name, key, checkFamily, checkColumn, compareOp, checkValue, put, del, ttl)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", self.storName, " Enter checkAndMutateRow() table_name=", table_name, 
                      " key=", key, " checkFamily=", checkFamily, " checkColumn=", checkColumn, " compareOp=", compareOp, 
                      " checkValue=", checkValue, " \nput=", stringify(put), " \ndel=", stringify(del), " \nttl=", ttl)

    if not self.enabled then
        return self.backstore:checkAndMutateRow(table_name, key, checkFamily, checkColumn, compareOp, checkValue, put, del, ttl)
    end

    local in_cacheset_put = nil
    local in_cacheset_del = nil

    if nil ~= put and nil ~= next(put) then
        in_cacheset_put,_ = self:sort_colVals(table_name, put)
    end
    if nil ~= del and nil ~= next(del) then
        in_cacheset_del,_ = self:sort_columns(table_name, del)
    end

    local innercode,ok = self.backstore:checkAndMutateRow(table_name, key, checkFamily, checkColumn, compareOp, checkValue, put, del, ttl) 
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", self.storName, " backstore:checkAndMutateRow() failed: innercode=", innercode)
        return innercode,false
    end

    if nil ~= in_cacheset_put and nil ~= next(in_cacheset_put) then
        --in_cacheset_put are column families that should be cached in 'this cache'. So 'this cache'
        --should process them;
        for policy, col_vals in pairs(in_cacheset_put) do
            local innercode,ok = self:process_put(policy, table_name, key, col_vals)
            if not ok then
                --failed to put into cache, not a big problem, because the outdated values
                --in cache will expire, and updated/new values will be cached;
                --So, we just log WARN
                ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ", self.storName, " process_put() failed: innercode=", innercode)
            end
        end
    end

    if nil ~= in_cacheset_del and nil ~= next(in_cacheset_del) then
        for policy, cols in pairs(in_cacheset_del) do
            --delete specified columns from 'this cache'
            local innercode,ok = self:process_delete(policy, table_name, key, cols)
            if not ok then
                ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ", self.storName, " process_delete() failed: innercode=", innercode)
            end
        end
    end

    return "0000", true
end


--For the given row, do the put and/or delete operations atomically only if  
--    compareOp(current-value-of-checkFamily:checkColumn, checkValue) = true
--    and return the original values;
--Params:
--    table_name : the name of the table
--    key        : the key of the given row
--    checkFamily: the family to check
--    checkColumn: the column to check
--    compareOp  : the compare op. it should be one of
--                     LESS             = 0
--                     LESS_OR_EQUAL    = 1
--                     EQUAL            = 2
--                     NOT_EQUAL        = 3
--                     GREATER_OR_EQUAL = 4
--                     GREATER          = 5
--                     NO_OP            = 6
--    checkValue : the value to check
--    put        : an array with the family:column:value to put, it must be like this:
--                         {[1]=family1, [2]=column1, [3]=value1, [4]=family2, [5]=column2, [6]=value2, ... }
--    del        : an array containing an optional list of family[:column]; e.g.
--                         {"family1:column1", "family2", "family3:column3", ...}
--                 if del is nil or an empty table, no data is deleted; else, 
--                 the specified family and/or columns are deleted;
--    ttl        : ttl in milli-seconds
--Note:
--    if you want to put only (no delete), you may pass nil to del or omit it.
--    if you want to delete only (no put), you must pass nil or empty table to put.
--Return: innercode, ok, result
--           innercode : "0000" if ok is true;
--                       "1121" if ok is false and the failure is caused by condition-check-failure;
--                       "xxxx" if ok is false and the failure is caused by other reasons;
--           ok        : true   if the 'MutateRow' operation has been done; 
--                       false  if the 'MutateRow' operation has NOT been done; if this is because of 
--                              condition-check-failure, innercode must be "1121";
--           result    : the original row. it is meaningful ONLY WHEN innercode is "0000" or "1121"; other failures
--                       may be caused by invalid argument, program crash, timeout and the like, thus 'result' is
--                       meanless; When meaningful, the result is a table like this:
--                              {
--                                  [family:column] = value
--                                  [family:column] = value
--                                  ...
--                              }
function _M.checkAndMutateAndGetRow(self, table_name, key, checkFamily, checkColumn, compareOp, checkValue, put, del, ttl)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", self.storName, " Enter checkAndMutateAndGetRow() table_name=", table_name, 
                      " key=", key, " checkFamily=", checkFamily, " checkColumn=", checkColumn, " compareOp=", compareOp, 
                      " checkValue=", checkValue, " \nput=", stringify(put), " \ndel=", stringify(del), " \nttl=", ttl)

    if not self.enabled then
        return self.backstore:checkAndMutateAndGetRow(table_name, key, checkFamily, checkColumn, compareOp, checkValue, put, del, ttl)
    end

    local in_cacheset_put = nil
    local in_cacheset_del = nil

    if nil ~= put and nil ~= next(put) then
        in_cacheset_put,_ = self:sort_colVals(table_name, put)
    end
    if nil ~= del and nil ~= next(del) then
        in_cacheset_del,_ = self:sort_columns(table_name, del)
    end

    local innercode,ok,origin = self.backstore:checkAndMutateAndGetRow(table_name, key, checkFamily, checkColumn, compareOp, checkValue, put, del, ttl) 
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", self.storName, " backstore:checkAndMutateAndGetRow() failed: innercode=", innercode)
        return innercode,false,origin
    end

    if nil ~= in_cacheset_put and nil ~= next(in_cacheset_put) then
        --in_cacheset_put are column families that should be cached in 'this cache'. So 'this cache'
        --should process them;
        for policy, col_vals in pairs(in_cacheset_put) do
            local innercode,ok = self:process_put(policy, table_name, key, col_vals)
            if not ok then
                --failed to put into cache, not a big problem, because the outdated values
                --in cache will expire, and updated/new values will be cached;
                --So, we just log WARN
                ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ", self.storName, " process_put() failed: innercode=", innercode)
            end
        end
    end

    if nil ~= in_cacheset_del and nil ~= next(in_cacheset_del) then
        for policy, cols in pairs(in_cacheset_del) do
            --delete specified columns from 'this cache'
            local innercode,ok = self:process_delete(policy, table_name, key, cols)
            if not ok then
                ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ", self.storName, " process_delete() failed: innercode=", innercode)
            end
        end
    end

    return "0000", true, origin
end

return _M
