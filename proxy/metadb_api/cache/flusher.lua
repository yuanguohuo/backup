-- By Yuanguo, 04/05/2017

local ok, new_tab = pcall(require, "table.new")
if not ok or type(new_tab) ~= "function" then
    new_tab = function (narr, nrec) return {} end
end

local ok,redis = pcall(require, "redis.cluster-instance")
if not ok or not redis then
    error("failed to load redis.cluster-instance. err="..(redis or "nil"))
end

local ok, hbase = pcall(require, "hbase.hbase-instance")
if not ok or not hbase then
    error("failed to load hbase.hbase-instance:" .. (hbase or "nil"))
end

local ok, sp_conf = pcall(require, "storageproxy_conf")
if not ok or not sp_conf then
    error("failed to load storageproxy_conf:" .. (sp_conf or "nil"))
end

local ok, utils = pcall(require, "common.utils")
if not ok or not utils then
    error("failed to load common.utils:" .. (utils or "nil"))
end

local stringify = utils.stringify

local _M = new_tab(0, 7)
_M._VERSION = '0.1'


local STATS_COLUMNS = 
{
    ["user"] = 
    {
        {"stats", "objects"},
        {"stats", "size_bytes"},
    },

    ["bucket"] = 
    {
        {"stats", "objects"},
        {"stats", "size_bytes"},
    },
}

local function flush_rcache_stats(table_name)
    local code,ok,scanner = hbase:openScanner(table_name, hbase.MIN_CHAR, hbase.MAX_CHAR, {"ver:tag"}, 1000)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " HBase failed to open scanner for table ", table_name, ". innercode=", code)
        return false
    end

    local batch = sp_conf.config["hbase_config"]["max_scan_once"] or 1000

    while true do
        local code,ok,rows = hbase:scan(table_name, scanner, batch)
     	if not ok then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " HBase failed to scan table ", table_name, ". innercode=", code)
            break
     	end	

    	local count = #rows

        for _,row in ipairs(rows) do
            local braced_hkey = "{" .. table_name .. "_" .. row.key .. "}"

            local need_flush = new_tab(64,0)

            for _,fc in ipairs(STATS_COLUMNS[table_name]) do
                local fam = fc[1]
                local col = fc[2]
                local fam_col = fam .. ":" .. col

                local redis_stat_incr_key = braced_hkey .. "_" .. fam_col .. "_incr"

                local incremental, err = redis:do_cmd_quick("getset", redis_stat_incr_key, 0)

                if not incremental then
                    --failed to 'getset' incremental, not flush it this time, and will try next time;
                    ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " redis 'getset' failed: err=", err, "key=", redis_stat_incr_key)
                else
                    if ngx.null == incremental then
                        --this stats has not been cached. do nothing
                        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " no need to flush ", redis_stat_incr_key, ", not cached")
                    else
                        if 0 == tonumber(incremental) then
                            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " no need to flush ", redis_stat_incr_key, ", incremental=", incremental)
                        else
                            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " need to flush ", redis_stat_incr_key, ", incremental=", incremental)
                            table.insert(need_flush, fam)
                            table.insert(need_flush, col)
                            table.insert(need_flush, incremental)
                        end
                    end
                end
            end

            if(nil == next(need_flush)) then
                ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " nothing to flush for row=", row.key)
            else
                ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " start to flush for row=", row.key, " column-values=", stringify(need_flush))
                local code,ok,current = hbase:increment(table_name, row.key, need_flush)
                if not ok then
                    ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " failed to put incremental into HBase. innercode=", code, ". Put them back into redis!")
                    --we failed to increment hbase. then we try to put them back into redis, so that they will be
                    --flushed next time.
                    for i=1,#need_flush-2,3 do
                        local fam = need_flush[1]
                        local col = need_flush[2]
                        local inc = need_flush[3] 

                        local fam_col = fam .. ":" .. col
                        local redis_stat_incr_key = braced_hkey .. "_" .. fam_col .. "_incr"

                        local incr,err = redis:do_cmd_quick("incrby", redis_stat_incr_key, inc)
                        if not incr then
                            --We have tried our best and we can do nothing here except printing a log!
                            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " redis 'incrby' failed: err=", err, " we may loose ", inc, " on ", redis_stat_incr_key)
                        end
                    end
                else
                    local cur_bases = new_tab(64,0)
                    for fam_col, cur_base in pairs(current) do
                        local redis_stat_base_key = braced_hkey .. "_" .. fam_col .. "_base"
                        table.insert(cur_bases, redis_stat_base_key)
                        table.insert(cur_bases, cur_base)
                    end

                    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " redis 'mset' kv-pairs=", stringify(cur_bases))
                    local ret,err = redis:do_cmd_quick("mset", unpack(cur_bases))
                    if not ret then
                        --not a big problem except that the stats are outdated until next flush.
                        ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " redis 'mset' failed: err=", err)
                    end
                end
            end
        end

        if count < batch then  --scan is over
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " finished scanning table ", table_name)
            break
        else
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " continue to scan table ", table_name)
        end
    end

    local code,ok = hbase:closeScanner(scanner)
    if not ok then
        ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " HBase failed to close scanneri of table ", table_name, ". innercode=", code)
    end

    return true
end

function _M.flush()
    local ok1 = flush_rcache_stats("bucket")
    local ok2 = flush_rcache_stats("user")
    return ok1 and ok2
end

return _M
