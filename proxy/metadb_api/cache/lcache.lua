-- By Yuanguo, 21/04/2017

local ok, new_tab = pcall(require, "table.new")
if not ok or type(new_tab) ~= "function" then
    new_tab = function (narr, nrec) return {} end
end

local ok, lrucache = pcall(require, "resty.lrucache")
if not ok or not lrucache then
    error("failed to local resty.lrucache: " .. (lrucache or "nil"))
end

local ok, cache = pcall(require, "cache.cache")
if not ok or not cache then
    error("failed to load cache.cache:" .. (cache or "nil"))
end

local ok, utils = pcall(require, "common.utils")
if not ok or not utils then
    error("failed to load common.utils:" .. (utils or "nil"))
end

local ok, RCache = pcall(require, "cache.rcache")
if not ok or not RCache then
    error("failed to load cache.rcache:" .. (RCache or "nil"))
end

local ok,stor_conf = pcall(require, "storageproxy_conf")
if not ok or not stor_conf then
    error("failed to load storageproxy_conf. err="..(stor_conf or "nil"))
end

local ok, misshit_stats = pcall(require, "stats.stats")
if not ok or not misshit_stats then
    error("failed to load stats.stats:" .. (misshit_stats or "nil"))
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

local lcache_cacheset = 
{
     ["user"] = 
     {
         [cache.POLICY.VERY_STABLE] =
         {
             ["info"]         = true,
             ["quota"]        = true,
             ["ver"]          = true,
             ["exattrs"]      = true, 
             ["bucket_quota"] = true,
         },
     },
}

local rcache = RCache:new()

local EXPIRE_SECONDS = stor_conf["config"]["cache_config"]["lcache"]["expire"]
local FACTOR = stor_conf["config"]["cache_config"]["lcache"]["factor"]
local LRU_ITEMS = stor_conf["config"]["cache_config"]["lcache"]["items"]

local _M = cache:new("LCache", lcache_cacheset, rcache)  --my backstore is rcache
_M.lrucache = lrucache.new(LRU_ITEMS)

function _M.new(self, lenabled)
    local enabled = lenabled
    if nil == enabled then
        enabled = stor_conf["config"]["cache_config"]["lcache"]["enabled"]
    end
    local newobj = {enabled = enabled}
    setmetatable(newobj, {__index = self})
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " Create LCache")
    return newobj
end

local function gen_lcache_key(table_name, key)
    return table_name .. "_" .. key
end

--Params:
--    policy     : the cache policy
--    table_name : the name of the hbase table
--    key        : the row key
--    to_cache   : column values to be put into 'this cache' in format of
--                 {
--                     [family:column] = value
--                     [family:column] = value
--                     ...
--                 }
--Return: innercode, ok;
function _M.update_cache(self, policy, table_name, key, to_cache) 
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", self.storName, " Enter update_cache() policy=", policy, " table_name=", table_name, " key=", key, " to_cache=", stringify(to_cache))

    if nil == to_cache or nil == next(to_cache) then
        return "0000", true
    end

    local lcache_key = gen_lcache_key(table_name, key)

    if cache.POLICY.STATS == policy then
        assert(false)  -- 'this cache' doesn't cache stats values
    elseif cache.POLICY.STABLE == policy or cache.POLICY.VERY_STABLE == policy then
        local lcache_tab = self.lrucache:get(lcache_key)
        if nil == lcache_tab then
            lcache_tab = new_tab(0,6)

            local expiration = EXPIRE_SECONDS
            if cache.POLICY.VERY_STABLE == policy then
                expiration = EXPIRE_SECONDS * FACTOR
            end

            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", self.storName, " cache row ", lcache_key)
            self.lrucache:set(lcache_key, lcache_tab, expiration)
        end

        for k,v in pairs(to_cache) do
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", self.storName, " set row ", lcache_key, " ", k, "=", v)
            lcache_tab[k] = v
        end
    else
        return "1001", false, nil, nil
    end
    return "0000",true
end

--Params:
--    policy     : the cache policy
--    table_name : the name of the hbase table
--    key        : the row key
--    columns    : the column names in format of {"family:column", "family:column", ...}
--Return: innercode, ok, body, missed; 
--        body   : a table as shown below in the case of success or nil in the case of failure:
--                 {
--                      [family:column] = value
--                      [family:column] = value
--                      ...
--                 }
--        missed : an array of missed columns in format of {"family:column", "family:column, ...}
function _M.process_get(self, policy, table_name, key, columns)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", self.storName, " Enter process_get() policy=", policy, " table_name=", table_name, " key=", key, " columns=", stringify(columns))

    local result = new_tab(0,6)
    local missed = new_tab(64,0)

    if nil == columns or nil == next(columns) then
        return "0000", true, result, missed
    end

    local lcache_key = gen_lcache_key(table_name, key)

    if cache.POLICY.STATS == policy then
        assert(false)  -- 'this cache' doesn't cache stats values
    elseif cache.POLICY.STABLE == policy or cache.POLICY.VERY_STABLE == policy then
        local lcache_tab = self.lrucache:get(lcache_key)
        if nil == lcache_tab or nil == next(lcache_tab) then
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", self.storName, " row ", lcache_key, " missed")
            return "0000", true, result, columns
        end

        for _,fc in ipairs(columns) do 
            if nil ~= lcache_tab[fc] then
                ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", self.storName, " row ", lcache_key, " key ", fc, " hit")
                result[fc] = lcache_tab[fc]
            else
                ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", self.storName, " row ", lcache_key, " key ", fc, " missed")
                table.insert(missed, fc)
            end
        end
    else
        return "1001", false, nil, nil
    end

    return "0000", true, result, missed
end

--Params:
--    policy     : the cache policy
--    table_name: the name of the table
--    key       : the row key
--    colValues : an array containing family1, column1, amount, family2, column2, amount ... they 
--                must be in triples. e.g.
--                {[1]=family1, [2]=column1, [3]=value1, [4]=family2, [5]=column2, [6]=value2, ... }
--Return: innercode, ok, current, need_flush
--               current: the current values of the incremented columns; it's a table like this:
--               {
--                   [family:column] = value
--                   [family:column] = value
--                   ...
--               }
--               need_flush: an array containing family1, column1, value, family2, column2, value ... that need
--               to be incremented in backstore. they must be in triples, e.g:
--               {[1]=family1, [2]=column1, [3]=incremental-1, [4]=family2, [5]=column2, [6]=incremental-2, ... }
function _M.process_increment(self, policy, table_name, key, colValues)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", self.storName, " Enter process_increment() policy=", policy, " table_name=", table_name, " key=", key, " colValues=", stringify(colValues))
    assert(policy == cache.POLICY.STATS)
    assert(false)  -- 'this cache' doesn't cache stats values
end

--new values have been put into backstore, 'this cache' should process them;
--Params:
--    policy     : the cache policy
--    table_name : the name of the hbase table
--    key        : the row key
--    colValues  : an array containing family1, column1, value, family2, column2, value ... they 
--                 must be in triples, e.g:
--                 {[1]=family1, [2]=column1, [3]=value1, [4]=family2, [5]=column2, [6]=value2, ... }
--Return: innercode, ok
function _M.process_put(self, policy, table_name, key, colValues)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", self.storName, " Enter process_put() policy=", policy, " table_name=", table_name, " key=", key, " colValues=", stringify(colValues))

    assert(policy ~= cache.POLICY.STATS)

    local to_cache = new_tab(0,6)
    for i=1,#colValues-2,3 do
        local fam,col,val = colValues[i],colValues[i+1],colValues[i+2]
        to_cache[fam..":"..col] = val
    end

    local innercode,ok = _M.update_cache(self, policy, table_name, key, to_cache)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", self.storName," update_cache() failed: innercode=", innercode)
        return innercode,false
    end

    return "0000", true
end

--new values have been checkAndPut into backstore, 'this cache' should process them;
--Params:
--    policy     : the cache policy
--    table_name : the name of the hbase table
--    key        : the row key
--    colValues  : an array containing family1, column1, value, family2, column2, value ... they 
--                 must be in triples, e.g:
--                 {[1]=family1, [2]=column1, [3]=value1, [4]=family2, [5]=column2, [6]=value2, ... }
--Return: innercode, ok
function _M.process_checkAndPut(self, policy, table_name, key, colValues)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", self.storName, " Enter process_checkAndPut() policy=", policy, " table_name=", table_name, " key=", key, " colValues=", stringify(colValues))

    assert(policy ~= cache.POLICY.STATS)

    return _M.process_put(self, policy, table_name, key, colValues)
end

--delete entire row from 'this cache'
--Params:
--    table_name  : the name of the table
--    key         : the row key
--Return: innercode, ok
function _M.process_delete_row(self, table_name, key)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", self.storName, " Enter process_delete_row() table_name=", table_name, " key=", key)

    local lcache_key = gen_lcache_key(table_name, key)
    self.lrucache:delete(lcache_key)

    return "0000", true
end

--delete columns of a row from 'this cache'
--Params:
--    policy    : the cache policy
--    table_name: the name of the table
--    key       : the row key
--    columns   : the column names in format:  {"family:column", "family:column", ...}
--Return: innercode, ok
function _M.process_delete(self, policy, table_name, key, columns)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", self.storName, " Enter process_delete() policy=", policy, " table_name=", table_name, " key=", key, " columns=", stringify(columns))

    local lcache_key = gen_lcache_key(table_name, key)

    if cache.POLICY.STATS == policy then
        assert(false)  -- 'this cache' doesn't cache stats values
    elseif cache.POLICY.STABLE == policy or cache.POLICY.VERY_STABLE == policy then
        local lcache_tab = self.lrucache:get(lcache_key)
        if nil == lcache_tab or nil == next(lcache_tab) then
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", self.storName, " row ", lcache_key, " not cached")
            return "0000", true
        end

        for _,fc in ipairs(columns) do
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", self.storName, " row ", lcache_key, " delete ", fc)
            lcache_tab[fc] = nil
        end

        return "0000", true
    else
        return "1001", false
    end

    --never reach here!!
    assert(false)
    return "0000", true
end

function _M.process_checkAndDelete(self, policy, table_name, key, columns)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", self.storName, " Enter process_checkAndDelete() policy=", policy, " table_name=", table_name, " key=", key, " columns=", stringify(columns))

    return _M.process_delete(self, policy, table_name, key, columns)
end

function _M.update_miss(self)
    misshit_stats:update("lcache_miss")
end

function _M.update_hit(self)
    misshit_stats:update("lcache_hit")
end

function _M.update_backstore_increment(self)
    misshit_stats:update("lcache_back_increment")
end

function _M.update_cache_increment(self)
    misshit_stats:update("lcache_cache_increment")
end

function _M.update_backstore_put(self)
    misshit_stats:update("lcache_back_put")
end

function _M.update_cache_put(self)
    misshit_stats:update("lcache_cache_put")
end

function _M.update_backstore_checkAndPut(self)
    misshit_stats:update("lcache_back_checkAndPut")
end

function _M.update_cache_checkAndPut(self)
    misshit_stats:update("lcache_cache_checkAndPut")
end

function _M.update_backstore_delete(self)
    misshit_stats:update("lcache_back_delete")
end

function _M.update_cache_delete(self)
    misshit_stats:update("lcache_cache_delete")
end

function _M.update_backstore_checkAndDelete(self)
    misshit_stats:update("lcache_back_checkAndDelete")
end

function _M.update_cache_checkAndDelete(self)
    misshit_stats:update("lcache_cache_checkAndDelete")
end

return _M
