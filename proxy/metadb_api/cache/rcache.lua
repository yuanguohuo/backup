-- By Yuanguo, 14/04/2017

local ok, new_tab = pcall(require, "table.new")
if not ok or type(new_tab) ~= "function" then
    new_tab = function (narr, nrec) return {} end
end

local ok, cache = pcall(require, "cache.cache")
if not ok or not cache then
    error("failed to load cache.cache:" .. (cache or "nil"))
end

local ok, utils = pcall(require, "common.utils")
if not ok or not utils then
    error("failed to load common.utils:" .. (utils or "nil"))
end

local ok, hbase = pcall(require, "hbase.hbase-instance")
if not ok or not hbase then
    error("failed to load hbase.hbase-instance:" .. (hbase or "nil"))
end

local ok,redis = pcall(require, "redis.cluster-instance")
if not ok or not redis then
    error("failed to load redis.cluster-instance. err="..(redis or "nil"))
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

local rcache_cacheset = 
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
         [cache.POLICY.STATS] =
         {
             ["stats"] = true,
         }
     },

     ["bucket"] = 
     {
         [cache.POLICY.VERY_STABLE] =
         {
             ["info"] = true,
             ["quota"] = true,
             ["ver"] = true,
             ["exattrs"] = true
         },

         [cache.POLICY.STATS] =
         {
             ["stats"] = true
         },
     },
}

local STATS_FLUSH_THRESHOLD = 
{
    ["user"] = 
    {
        ["stats:objects"] = 40,
        ["stats:size_bytes"] = 163840,  --160MB
    },

    ["bucket"] = 
    {
        ["stats:objects"] = 20,
        ["stats:size_bytes"] = 81920,  --80MB
    },
}

local EXPIRE_SECONDS = stor_conf["config"]["cache_config"]["rcache"]["expire"]
local FACTOR = stor_conf["config"]["cache_config"]["rcache"]["factor"]

local _M = cache:new("RCache", rcache_cacheset, hbase)  --my backstore is hbase

function _M.new(self)
    local enabled = stor_conf["config"]["cache_config"]["rcache"]["enabled"]
    local newobj = {enabled = enabled}
    setmetatable(newobj, {__index = self})
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " Create RCache")
    return newobj
end


local function gen_redis_hkey(table_name, key)
    return table_name .. "_" .. key
end

--Params:
--    hkey    : tablename_rowkey
--    columns : columns of stats family, such as {"stats:objects", "stats:size_bytes", ...}
--Return: 
--    a table containing 2 elements for each column:
--        {tablename_rowkey}_stats:col_base
--        {tablename_rowkey}_stats:col_incr
--    it will be used as parameters of redis 'mget' cmd, which will get stats values: base_of_stats1, 
--        incr_of_stats1, base_of_stats2, incr_of_stats2, ...
--    for example, khey="bucket_b1234", columns={"stats:objects", "stats:size_bytes"}, then return table is:
--        {
--            {bucket_b1234}_stats:objects_base,
--            {bucket_b1234}_stats:objects_incr,
--            {bucket_b1234}_stats:size_bytes_base,
--            {bucket_b1234}_stats:size_bytes_incr,
--        }
--    Note: the order must be kept!
--    Note: "{tablename_rowkey}" makes sure all these keys are hashed to the same slot;
local function make_stats_mget(hkey, columns)
    local stats_keys = new_tab(64,0)
    local braced_hkey = "{" .. hkey .. "}"
    for _,col in ipairs(columns) do
        local stat_base_key = braced_hkey .. "_" .. col .. "_base"
        local stat_incr_key = braced_hkey .. "_" .. col .. "_incr"
        table.insert(stats_keys,stat_base_key)
        table.insert(stats_keys,stat_incr_key)
    end
    return stats_keys
end

--Params:
--    hkey      : tablename_rowkey
--    stat_vals : stats column values, such as 
--                    {
--                        [stats:objects] = value1,
--                        [stats:size_bytes] = value2, 
--                        ...
--                    }
--Return: 
--    a table containing 2 elements for each column:
--       {tablename_rowkey}_stats:col_base 
--       value
--    it will be used as parameters of redis 'mset' cmd, which will update stats values: base_of_stats1=xx, 
--        base_of_stats2=yy, ...
--    for example, khey="bucket_b1234", columns=
--        {
--            [stats:objects] = value1,
--            [stats:size_bytes] = value2, 
--        },
--    then return table is:
--       {
--           {bucket_b1234}_stats:objects_base, value1, 
--           {bucket_b1234}_stats:size_bytes_base, value2,
--       }
--    Note: "{tablename_rowkey}" makes sure all these keys are hashed to the same slot;
--    Note: we don't update the 'incr' part;
local function make_stats_mset(hkey, stat_vals)
    local updates = new_tab(64,0)
    local braced_hkey = "{" .. hkey .. "}"
    for col,val in pairs(stat_vals) do
        local stat_base_key = braced_hkey .. "_" .. col .. "_base"
        table.insert(updates, stat_base_key)
        table.insert(updates, val)
    end
    return updates
end

--Params:
--    vals : column values, in format of
--               {
--                   [family:column] = value,
--                   [family:column] = value,
--                   ...
--               }
--Return: 
--    a table used as parameters of redis 'hmset' cmd.
--    for example, columns=
--        {
--            [info:name] = value1,
--            [quota:objects] = value2, 
--        },
--    then return table is: {"info:name", value1, "quota:objects", value2}
local function make_hmset(vals)
    local updates = new_tab(64,0)
    for k,v in pairs(vals) do
        table.insert(updates, k)
        table.insert(updates, v)
    end
    return updates
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

    local hkey = gen_redis_hkey(table_name, key)

    if cache.POLICY.STATS == policy then
        local updates = make_stats_mset(hkey, to_cache)
        local ret,err = redis:do_cmd_quick("mset", unpack(updates))
        if not ret then
            --because mset is atomic, we don't need to rollback (such as delete those keys) 
            --in the case of failure;
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", self.storName, ": redis 'mset' failed: err=", err, " updates=", stringify(updates))
            return "5008", false
        end
        --stats will not expire, the "timer" will make them valid/updated periodically
    elseif cache.POLICY.STABLE == policy or cache.POLICY.VERY_STABLE == policy then
        local updates = make_hmset(to_cache)
        local ret,err = redis:do_cmd_quick("hmset", hkey, unpack(updates))
        if not ret then
            --because hmset is atomic (see https://groups.google.com/forum/#!topic/redis-db/_Tm9DZOrw9s), 
            --we don't need to rollback (such as delete hkey) in the case of failure;
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", self.storName, ": redis 'hmset' failed: err=", err, " hkey=", hkey, " updates=", stringify(updates))
            return "5009", false
        end

        local expiration = EXPIRE_SECONDS
        if cache.POLICY.VERY_STABLE == policy then
            expiration = EXPIRE_SECONDS * FACTOR
        end

        --update the expire time
        local ret,err = redis:do_cmd_quick("expire", hkey, expiration)
        if not ret then
            ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ", self.storName, ": redis 'expire' failed: err=", err, " hkey=", hkey, " expiration=", expiration)
            --if failed to set expire time, we try to delete hkey, because the values just updated
            --might be outdated (backstore was updated just now), and if set expire time failed, they 
            --will reside in redis forever if there's no more updates;
            local ret1,err1 = redis:do_cmd_quick("del", hkey)
            if not ret1 then
                --we just log WARN here. because, as long as there's updates later, the expire time
                --will be set then.
                ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ", self.storName, ": redis 'del' failed: err=", err, " hkey=", hkey)
            end
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

    local hkey = gen_redis_hkey(table_name, key)

    if cache.POLICY.STATS == policy then
        local stats_keys = make_stats_mget(hkey, columns)
        local ret,err = redis:do_cmd_quick("mget", unpack(stats_keys))
        if not ret then
            return "5006", false, nil, nil
        end
        for i=1,#columns do
            local base = ret[2*i-1]
            local incr = ret[2*i]
            if ngx.null ~= base and ngx.null ~= incr then  --hit
                result[columns[i]] = base + incr
            elseif ngx.null ~= base and ngx.null == incr then --hit
                result[columns[i]] = base
            elseif ngx.null == base and ngx.null ~= incr then --missing
                table.insert(missed, columns[i])
            else --missing
                table.insert(missed, columns[i])
            end
        end
    elseif cache.POLICY.STABLE == policy or cache.POLICY.VERY_STABLE == policy then
        local ret,err = redis:do_cmd_quick("hmget", hkey, unpack(columns))
        if not ret then
            return "5007", false, nil, nil
        end
        for i=1,#ret do
            if ngx.null ~= ret[i] then  --hit
                result[columns[i]] = ret[i]
            else  --missing
                table.insert(missed, columns[i])
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
    local current = new_tab(0,6)

    local hkey = gen_redis_hkey(table_name, key)
    local braced_hkey = "{" .. hkey .. "}"

    local need_flush = new_tab(64,0)

    for i=1,#colValues-2,3 do
        local fam,col,val = colValues[i],colValues[i+1],colValues[i+2]
        if val == 0 then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", self.storName, ": skip ", fam, ":", col ," because val=0")
        else
            local fc = fam .. ":" .. col
            local stat_base_key = braced_hkey .. "_" .. fc .. "_base"
            local stat_incr_key = braced_hkey .. "_" .. fc .. "_incr"
            local incr,err = redis:do_cmd_quick("incrby", stat_incr_key, val)
            if not incr then
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", self.storName, ": redis 'incrby' failed: err=", err, " stat_incr_key=", stat_incr_key, " val=", val)
                return "5011", false, nil
            end

            local base,err = redis:do_cmd_quick("get", stat_base_key)
            if not base then
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", self.storName, ": redis 'get' failed: err=", err, " stat_base_key=", stat_base_key)
                return "5000", false, nil
            end

            local thresholds = STATS_FLUSH_THRESHOLD[table_name]

            --there're 3 cases now:
            --  1) 'base' is cached and 'incr' has not reached flush threshold: do nothing;
            --  2) 'base' is cached and 'incr' has reached flush threshold: flush it;
            --  3) 'base' is not cached: flush it, in order to retrieve 'base' from backstore and cache it;
            if thresholds and incr > thresholds[fc] or ngx.null == base then   -- case 2) and 3)
                local incremental, err = redis:do_cmd_quick("getset", stat_incr_key, 0)
                if not incremental then
                    --case 2): failed to 'getset' incremental, not a big problem: not flush it this time
                    --and it will be flushed next time;
                    --So, we just log WARN;
                    ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ", self.storName, ": redis 'getset' failed: err=", err, " stat_incr_key=", stat_incr_key)

                    --case 3): 'base' is not cached yet, we also flush it, just in order to retrieve 'base' from backstore and 
                    --cache it; in this case, we set incremental=0;
                    if ngx.null == base then
                        table.insert(need_flush, fam)
                        table.insert(need_flush, col)
                        table.insert(need_flush, 0)
                    end
                else
                    table.insert(need_flush, fam)
                    table.insert(need_flush, col)
                    table.insert(need_flush, incremental)
                end
            else  -- case 1)
                current[fc] = base + incr
            end
        end
    end

    return "0000", true, current, need_flush
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

    local del_keys = new_tab(64,0)

    local hkey = gen_redis_hkey(table_name, key)
    table.insert(del_keys, hkey)

    --we make use of STATS_FLUSH_THRESHOLD to figure out the 'stats columns' of this table;
    local stats_columns = STATS_FLUSH_THRESHOLD[table_name]
    if stats_columns then
        local braced_hkey = "{" .. hkey .. "}"
        for fc,_ in pairs(stats_columns) do
            --every stats column has 2 keys in redis
            local stat_base_key = braced_hkey .. "_" .. fc .. "_base"
            local stat_incr_key = braced_hkey .. "_" .. fc .. "_incr"
            table.insert(del_keys, stat_base_key)
            table.insert(del_keys, stat_incr_key)
        end
    end

    --delete the keys from redis; and retry in the case of failure;
    local ret,err = nil,nil
    for retry=1,3 do
        ret,err = redis:do_cmd_quick("del", unpack(del_keys))
        if ret then
            break
        end
    end

    if not ret then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", self.storName, ": redis 'del' failed: err=", err, " del_keys=", stringify(del_keys))
        return "5004", false
    end

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

    local hkey = gen_redis_hkey(table_name, key)

    if cache.POLICY.STATS == policy then
        local del_keys = new_tab(64,0)

        local braced_hkey = "{" .. hkey .. "}"
        for _,fc in ipairs(columns) do
            --every stats column has 2 keys in redis
            local stat_base_key = braced_hkey .. "_" .. fc .. "_base"
            local stat_incr_key = braced_hkey .. "_" .. fc .. "_incr"
            table.insert(del_keys, stat_base_key)
            table.insert(del_keys, stat_incr_key)
        end

        --delete the keys from redis; and retry in the case of failure;
        local ret,err = nil,nil
        for retry=1,3 do
            ret,err = redis:do_cmd_quick("del", unpack(del_keys))
            if ret then
                break
            end
        end

        if not ret then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", self.storName, ": redis 'del' failed: err=", err, " del_keys=", stringify(del_keys))
            return "5004", false
        end

        return "0000", true
    elseif cache.POLICY.STABLE == policy or cache.POLICY.VERY_STABLE == policy then
        --delete the fields from redis hash structure; and retry in the case of failure;
        local ret,err = nil,nil
        for retry=1,3 do
            ret,err = redis:do_cmd_quick("hdel", hkey, unpack(columns))
            if ret then
                break
            end
        end

        if not ret then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", self.storName, ": redis 'hdel' failed: err=", err, " hkey=", hkey, " columns=", stringify(columns))
            return "5005", false
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
    misshit_stats:update("rcache_miss")
end

function _M.update_hit(self)
    misshit_stats:update("rcache_hit")
end

function _M.update_backstore_increment(self)
    misshit_stats:update("rcache_back_increment")
end

function _M.update_cache_increment(self)
    misshit_stats:update("rcache_cache_increment")
end

function _M.update_backstore_put(self)
    misshit_stats:update("rcache_back_put")
end

function _M.update_cache_put(self)
    misshit_stats:update("rcache_cache_put")
end

function _M.update_backstore_checkAndPut(self)
    misshit_stats:update("rcache_back_checkAndPut")
end

function _M.update_cache_checkAndPut(self)
    misshit_stats:update("rcache_cache_checkAndPut")
end

function _M.update_backstore_delete(self)
    misshit_stats:update("rcache_back_delete")
end

function _M.update_cache_delete(self)
    misshit_stats:update("rcache_cache_delete")
end

function _M.update_backstore_checkAndDelete(self)
    misshit_stats:update("rcache_back_checkAndDelete")
end

function _M.update_cache_checkAndDelete(self)
    misshit_stats:update("rcache_cache_checkAndDelete")
end

return _M
