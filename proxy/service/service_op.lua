local reqmonit = require("reqmonit")

local ok,inspect = pcall(require, "inspect")
if not ok or not inspect then
    error("failed to load inspect:" .. (inspect or "nil"))
end

local ok, json = pcall(require, "cjson")
if not ok or not json then
    error("failed to load cjson:" .. (cjson or "nil"))
end

local sp_conf = require("storageproxy_conf")

local ok,utils = pcall(require, "common.utils")
if not ok or not utils then
    error("failed to load common.utils:" .. (utils or "nil"))
end

local ok, lock_mgr = pcall(require, "common.lock")
if not ok or not lock_mgr then
    error("failed to load common.lock:" .. (lock_mgr or ""))
end

local ok, sync_mgr = pcall(require, "sync_manager")
if not ok or not sync_mgr then
    error("failed to load sync_manager: " .. (sync_mgr or ""))
end

local ok, http = pcall(require, "resty.http")
if not ok or not http then
    error("failed to load resty.http:" .. (http or "nil"))
end

local ok, metadb = pcall(require, "metadb-instance")
if not ok or not metadb then
    error("failed to load metadb-instance:" .. (metadb or "nil"))
end

local ok, objstore = pcall(require, "storageproxy_objstore")
if not ok or not objstore then
    error("failed to load storageproxy_objstore:" .. (objstore or "nil"))
end

local ok, sp_s3op = pcall(require, "storageproxy_s3_op")
if not ok or not sp_s3op then
    error("failed to load storageproxy_s3_op:" .. (sp_s3op or "nil"))
end

local ok,uuid = pcall(require, "resty.uuid")
if not ok or not uuid then
    error("failed to load resty.uuid:" .. (uuid or "nil"))
end

local ok, new_tab = pcall(require, "table.new")
if not ok or type(new_tab) ~= "function" then
    new_tab = function (narr, nrec) return {} end
end

local ok, oindex = pcall(require, "storageproxy_oindex")
if not ok or not oindex or type(oindex) ~= "table" then
    error("failed to load storageproxy_oindex:" .. (oindex or "nil"))
end

local ok, sync_meta = pcall(require, "sync_meta")
if not ok or not sync_meta or type(sync_meta) ~= "table" then
    error("failed to load sync_meta:" .. (sync_meta or "nil"))
end

local ngx_now = ngx.now
local log = ngx.log
local ERR = ngx.ERR
local INFO = ngx.INFO
local sync_cfg = sp_conf.config["sync_config"]

local DELETE_OBJ_NAME = sp_conf.config["deleteobj"]["name"]
local DELETE_OBJ_MAX = sp_conf.config["deleteobj"]["max"]

local _M = {
    _VERSION = '0.01',
}

--description
-- Scan the objects within the specified range and move them from object table to the delete table
-- params:
--     start_key    : range start key 
--     end_key      : range end key
--     lockkey      : redis lock key 
--     sig          : redis lock sig
--     btag         : bucket tag
-- return:
--     completed    : if all expired objs are cleaned out, return true, otherwise return false
local function move_expire_objects(start_key, end_key, lockkey, sig, ubo_info)
    local completed = true
    local btag = ubo_info.buck_vtag
    local bucket = ubo_info.buck_name
    local bkey = ubo_info.buck_key
    -- clusterid
    local local_clusterid = sync_cfg["clusterid"]

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", " start move expire obj start_key:", start_key, " end_key:", end_key, " req_info:", inspect(req_info))
    repeat
        while reqmonit.check_system_busy() do
            local ok,ret = lock_mgr.extend_lock(lockkey, sig, 300)
            if not ok  then
                ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ", "extend lock_mgr faile lockkey:", lockkey, " sig:", sig)
                return false
            end
            ngx.sleep(3)
        end

        local ok, result, common_prefixes, next_marker, truncated = sp_s3op.list_entries(metadb, "expire_object", nil, start_key, end_key, "", "", 1001)
        if not ok then
            log(ERR, "metadb failed to scan rule.")
            return false
        end
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", " list entries next_marker:", next_marker, " truncated:", truncated)
        local ok,ret = lock_mgr.extend_lock(lockkey, sig, 1000)
        if not ok  then
            ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ", "extend lock_mgr faile lockkey:", lockkey, " sig:", sig)
            return false
        end

        for i, v_tab in ipairs(result) do
            -- key for example: expire.."_"..cdate.."_"..btag.."_"..obj.."_"..seqno
            local key = v_tab.key
            local s,e = ngx.re.find(key, btag, "jo")
            local okey = string.sub(key, s, -1)
            local seqno = v_tab.values["info:seqno"]
            local ul_pos = string.find(okey, "_")
            assert(ul_pos)
            local object = string.sub(okey, ul_pos+1, -#seqno-2)
            local create_time = v_tab.values["info:ctime"]

            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", " expire delete object:", object, " bkey:", bkey)
            -- ubo_info: user-bucket-object info
            local errCode,succ,obj_key,dirkeys = oindex.parse_objname1(btag, object, ubo_info.user_type)
            if not succ then
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to parse objectname. objectname=", object, " errCode=", errCode)
                return  false
            end
            ubo_info.obj_key            = obj_key
            ubo_info.obj_name           = object
            ubo_info.obj_vtag           = nil
            ubo_info.obj_dirkeys        = dirkeys
    
            local delete_time = string.format("%0.3f", ngx.now())
            local seq_no = utils.gen_seq_no(delete_time)
            local code, incode, ok = sp_s3op.delete_obj(metadb, local_clusterid, ubo_info, seq_no, delete_time, seqno)
            if not ok and "1121" ~= incode then -- incode 1121 seqno ~= seqno
                ngx.log(ngx.ERR, "delete object failed")
                return false
            end
            local code,ok = metadb:delete("expire_object", key)
            if not ok then
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "failed delete object from metadb")
            end
        end
        
        start_key = next_marker
        if not truncated or not next_marker then
            return true
        end
    until false

    return completed
end

local function compare_rules(rule_x, rule_y)
    local date_x = rule_x["days"]
    local date_y = rule_y["days"]

    return date_x > date_y
end

local EXPIRE_MAX = sp_conf.config["expire"]["max"]
local EXPIRE_NAME = sp_conf.config["expire"]["name"]
local function get_expire_shard_lock(tag, i)
    local shard = utils.get_shard(EXPIRE_NAME, EXPIRE_MAX, i)
    local lockkey = tag..shard
    local ok,ret = lock_mgr.lock(lockkey, 600)
    if ok then
        return true, ret, shard
    end
    return false, nil, nil
end


local function get_expire_mark(markkey)
    local mark = nil
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", "get expire mark  markkey:", markkey)
    local code, ok, result = metadb:get("log_marker", markkey, {"info:marked", "info:timestamp"})
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", "get expire mark faile markkey:", markkey)
        return mark
    end

    if result and next(result) then
        mark = result["info:marked"]
    end
    
    if mark then
        return mark
    end

    -- get clusters create time 
    -- user root accessid 1WYCCJZ9JRLWZU8JTDQJ
    local code, ok, result = metadb:get("user", "1WYCCJZ9JRLWZU8JTDQJ", {"info:mtime"})
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", "get expire mark get cluster create time :", markkey)
        return mark
    end

    local date = nil
    if result and next(result) then
        local mtime = ngx.parse_http_time(result["info:mtime"])
        date = utils.toDATE(mtime)
    end
    
    mark = date

    return mark
end
--description
-- Scan all eligible expired objects in the object table and move them to the delete table
-- params:
-- return:
--     completed    : if all objs are cleaned out, return true, otherwise return false
--     count        : number of objects successfully deleted
function _M.expire_objects(self)
    --1. get all lifecycle rules
    local start_key = metadb.MIN_CHAR
    local rules = {}
    local count = 1
    ngx.log(ngx.INFO, "start scan expire rules")

    repeat
        local ok, result, common_prefixes, next_marker, truncated = sp_s3op.list_entries(metadb, "lifecycle", {"info"}, start_key, metadb.MAX_CHAR, "", "", 1000)
        if not ok then
            ngx.log(ngx.ERR, "RequestID=", "metadb failed to scan rule :")
            return false, 0
        end 

        for i, v in ipairs(result) do
            local ok, rtable = pcall(json.decode, v.values["info:rules"])
            if not ok or "table" ~= type(rtable) then
                --ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ","json.decode failed, so we skip this rule:", inspect(v))
            else
                local enabled_rules = {}
                for _, v in pairs(rtable) do
                    if "Enabled" == v["status"] then
                        enabled_rules[#enabled_rules + 1] = v 
                    end
                end
                --sort rules by expire days
                table.sort(enabled_rules, compare_rules)

                v.values["info:rules"] = enabled_rules
                rules[#rules + 1] = v 
            end
        end

        start_key = next_marker
        if not truncated or not next_marker then
            break
        end 
    until false

    if not next(rules) then
        ngx.log(ngx.INFO, "no expire rules")
        return true
    end 

    log(INFO, inspect(rules))

    for _, rule in ipairs(rules) do
        local bucket = rule.values["info:bname"]
        local bkey = rule.values["info:bkey"]
        local btag = string.sub(rule.key, #bkey+2)
        local user_type = rule.values["info:user_type"]
        local accessid = rule.values["info:accessid"]
        local enabled_rules = rule.values["info:rules"]

        for _, item in ipairs(enabled_rules) do
            local prefix = item["prefix"]
            local expire_time = item["days"] * 86400
            local tag = item["tag"]
            local now = ngx_now()
            assert(now > expire_time)
            local expire_clock = tostring(math.ceil(now - expire_time))
            ngx.log(ngx.INFO, "now is ", now, ", expire_time is ", expire_time, " expire clock is ", expire_clock)

            -- ubo_info: user-bucket-object info
            local ubo_info = new_tab(0, 6)

            ubo_info.user_key           = accessid
            ubo_info.user_vtag          = nil
            ubo_info.user_displayname   = nil
            ubo_info.user_uid           = nil
            ubo_info.user_type          = user_type

            ubo_info.buck_key           = bkey
            ubo_info.buck_name          = bucket
            ubo_info.buck_vtag          = btag

            for i=1,EXPIRE_MAX do
                local ok,sig,shard = get_expire_shard_lock(tag, i)
                if ok then
                    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", " expire shard lock_mgr i:", i, " shard:", shard)
                    local lockkey = tag..shard
                    local markkey = lockkey
                    local mark = get_expire_mark(markkey)
                    if nil == mark then
                        lock_mgr.unlock(lockkey, sig)
                        return false
                    end

                    local last_timestamp = tonumber(utils.date_to_sec(mark))
                    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", " expire  markkey:", markkey, " mark:", mark, " last_timestamp:", last_timestamp)
                    local current = expire_clock
                    local finished = false
                    while last_timestamp <= (current - 86400) do
                        current = current - 86400 --move one day later
                        local date = utils.toDATE(tostring(current))

                        local lower_key = shard.."_"..date.."_"..btag.."_"..prefix..metadb.MIN_CHAR
                        local upper_key = shard.."_"..date.."_"..btag.."_"..prefix..metadb.MAX_CHAR
                        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", " expire  lower_key:", lower_key, " upper_key:", upper_key)
                        repeat
                            local busy = reqmonit.check_system_busy()
                            if busy then
                                ngx.sleep(5)
                            else
                                finished = move_expire_objects(lower_key, upper_key, lockkey, sig, ubo_info)
                                break
                            end
                            local ok,ret = lock_mgr.extend_lock(lockkey, sig, 1000)
                            if not ok  then
                                ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ", "extend lock_mgr faile lockkey:", lockkey, " sig:", sig)
                                finished = false
                            end
                        until false

                        if not finished then
                            break
                        end
                    end

                    if finished then
                        local mark = utils.toDATE(expire_clock)
                        local code, ok, result = metadb:put("log_marker", markkey, {"info", "marked", mark, "info", "timestamp", ngx.now()})
                        if not ok then
                            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", "put expire mark faile markkey:", markkey, " mark", mark)
                        end
                    end
                    lock_mgr.unlock(lockkey, sig)
                end
            end
        end
    end
end

-- params:
--     object   : object name
--     tag      : object instance name in ceph
--     size     : object total size
--     hsize    : object head data length in hbase
--     psize    : object part size 
--     chksize  : object chunk size
--     loc      : where is the obj stored
-- return:
--     clean    : if all objs are cleaned out, return true, otherwise return false
local function delete_ceph_objs(object, tag, size, hsize, psize, chksize, loc)
    ngx.log(ngx.INFO, "Enter delete_ceph_objs, object="..object..", tag="..tag)
    
    local clean = true
    local concurrent = sp_conf.config["chunk_config"]["rconcurrent"]
    -- low layer objects to read. we will find out all such objects
    -- first, and read them at last.
    local objs = sp_s3op.calc_objs(hsize, psize, chksize, 1, size, tag)
    if objs and next(objs) then
        local ceph = objstore:get_store(loc)
        assert(ceph)

        local batch = {}
        local bindex = 1

        for i=1, #objs do
            batch[bindex] = objs[i]
            bindex = bindex + 1
            
            if i%concurrent == 0 or i == #objs then
                ngx.sleep(1)
                ngx.log(ngx.INFO, "delete objects from ceph: \n", utils.stringify(batch))
                local code,ok,res = ceph:delete_obj(object, batch, false)
                if not ok then
                    ngx.log(ngx.ERR, "deleting ceph objects failed"..", code="..(code or "nil")..", res="..utils.stringify(res))
                    clean = false
                end
                
                batch = {}
                bindex = 1
            end
        end
    end
    ngx.log(ngx.INFO, "delete objects from ceph succeeded.")

    return clean
end

local function get_gc_object_shard(i)
    local shard = utils.get_shard(DELETE_OBJ_NAME, DELETE_OBJ_MAX, i)
    local ok,ret = lock_mgr.lock(shard, 600)
    if ok then
        return true, ret, shard
    end
    return false, nil, nil
end
-- params:
-- return:
--     completed    : if all objs are cleaned out, return true, otherwise return false
--     count        : number of objects successfully deleted
function _M.gc_objects(self)
    log(INFO, "new start gc_objects")
    local completed = true
    local count = 0
    for i=1,DELETE_OBJ_MAX do
        local ok,sig,shard = get_gc_object_shard(i)
        if ok then
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", " gc objects shard lock_mgr i:", i, " shard:", shard)
            local lockkey = shard
            local startkey = shard..metadb.MIN_CHAR
            local endkey = shard..metadb.MAX_CHAR
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", " gc objects startkey=", startkey, " endkey=", endkey)
            local circle = 0
            local result_counts = 0
            repeat
                while reqmonit.check_system_busy() do
                    ngx.sleep(10)
                end

                local ok,ret = lock_mgr.extend_lock(lockkey, sig, 600)
                if not ok  then
                    ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ", "gc object extend lock_mgr faile lockkey:", lockkey, " sig:", sig)
                    completed = false
                    break
                end
                ngx.sleep(1)
                local ok, result, common_prefixes, next_marker, truncated = sp_s3op.list_entries(metadb, "delete_object", {"info:oname", "info:size", "info:hsize", "info:link", "mfest:chksize", "mfest:loc", "mfest:psize", "ver:tag"}, startkey, endkey, "", "", 1001)
                if not ok then
                    ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ", "metadb failed to scan delete_object")
                    break
                end
                circle = circle + 1
                ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", "gc_objects circle="..circle..", count="..#result)
                result_counts = result_counts + #result
                for i, v_tab in ipairs(result) do
                    local key = v_tab.key
                    local oname = v_tab.values["info:oname"]
                    local size = tonumber(v_tab.values["info:size"])
                    local hsize = tonumber(v_tab.values["info:hsize"])
                    local chksize = tonumber(v_tab.values["mfest:chksize"])
                    local psize = tonumber(v_tab.values["mfest:psize"])
                    local tag = v_tab.values["ver:tag"]

                    local loc = v_tab.values["mfest:loc"]
                    assert(loc)
                    
                    local ceph_objtag = tag
                    local ceph_objname = oname
                    local link = v_tab.values["info:link"]
                    if nil ~= link then
                        local s,e = string.find(link, "_")
                        ceph_objtag  = string.sub(link, 1, s-1) 
                        ceph_objname = string.sub(link, e+1, -1)
                    end
                    
                    local ok,ret = lock_mgr.extend_lock(lockkey, sig, 900)
                    if not ok  then
                        ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ", "gc object extend lock_mgr faile lockkey:", lockkey, " sig:", sig)
                        completed = false
                        break
                    end
                    
                    local clean = delete_ceph_objs(ceph_objname, ceph_objtag, size, hsize, psize, chksize, loc)
                    if clean then
                        local code,ok = metadb:delete("delete_object", key)
                        if ok then
                            count = count + 1
                        else
                            completed = false
                            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ","failed delete object from metadb")
                        end
                    else
                        completed = false
                        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", "delete ceph objects failed, obj="..oname..", tag="..tag)
                    end
                end
            
                startkey = next_marker
                if not truncated or not next_marker then
                    break
                end
            until false
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", "gc_objects circle="..circle..", delete results count="..result_counts)
            lock_mgr.unlock(lockkey, sig)
        end
    end
    
    return completed, count
end

-- params:
-- return:
--     completed    : if all objs are cleaned out, return true, otherwise return false
--     count        : number of objects successfully deleted
function _M.sync_objects(self)
    log(INFO, "new start sync_objects")

    local sharding_num = sync_cfg["datalog"]["max"]
    local lock_ttl = sync_cfg["lock_ttl"]
    local max_logs = sync_cfg["datalog"]["maxkeys"]["default"]
    local peers = sync_cfg["peers"]

    if not peers or not next(peers) then
        return true
    end

    for i, peer in ipairs(peers) do
        log(ngx.INFO, "new peers, i="..i)
        local sync_mode = tonumber(peer.sync_mode)
        --sync data
        if 3 == sync_mode then
            local cluster_id = peer.id
            assert(peer.ip and peer.log_port and peer.data_port)
            local sync_op  = sync_mgr:new(peer.ip, peer.log_port, peer.data_port)
            local start_id = math.random(1, sharding_num)

            for j = start_id, sharding_num do
                log(ngx.INFO, "new sharding, j="..j)
                while reqmonit.check_system_busy() do
                    ngx.sleep(1)
                end

                local lock_key = cluster_id .. "dataloglock" .. j
                local ok, signature = lock_mgr.lock(lock_key, lock_ttl)
                if not ok or not signature then
                    log(ngx.WARN, "lock failed. lock_key = " .. lock_key)
                    ngx.sleep(1)
                else
                    log(ngx.INFO, "lock successful. lock_key = " .. lock_key)
                    -- read log marker from hbase
                    local marker_key = cluster_id .. "datalog_marker" .. j
                    local code, ok, res = metadb:get("log_marker", marker_key, {"info:marker"})
                    if not ok then
                        log(ERR, "metadb get log marker failed. code = " .. code)
                        lock_mgr.unlock(lock_key, signature)
                        return false
                    elseif not res then
                        log(ngx.INFO, "metadb get log marker nil ")
                    end

                    local marker = ""
                    if next(res) then
                        marker = res["info:marker"] or ""
                    end

                    local skip_cur = false
                    repeat
                        while reqmonit.check_system_busy() do
                            ngx.sleep(1)
                        end

                        local ok, err = lock_mgr.extend_lock(lock_key, signature, lock_ttl)
                        if not ok then
                            log(ERR, "extend_lock failed, err=" .. err .. ", lock_key=" .. lock_key)
                            break
                        end
                        local ok, logs = sync_op:fetch_remote_logs("data", j, marker, max_logs)
                        if not ok or not logs then
                            log(ERR, "fetch_remote_logs failed or no new logs")
                            break
                        end

                        local totol_logs = logs.total
                        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", "fetch_remote_logs success, totol_logs="..totol_logs)
                        if 0 == totol_logs then
                            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", "no new logs")
                            break
                        end
                        local cur_marker
                        local cur_timestamp 
                        local marker_step = 0
                        for k = 1, totol_logs do
                            local datalog = logs[tostring(k)]
                            local lock_info = {
                                start = ngx_now(),
                                key = lock_key,
                                signature = signature,
                                ttl = lock_ttl,
                            }
                            local ok = sync_op:process_datalog(datalog, lock_info)
                            if not ok then
                                skip_cur = true
                                break
                            end

                            local ok, err = lock_mgr.extend_lock(lock_key, signature, lock_ttl)
                            if not ok then
                                log(ERR, "extend_lock failed, err=" .. err .. ", lock_key=" .. lock_key)
                                skip_cur = true
                                break
                            end

                            cur_marker = datalog.marker
                            cur_timestamp = datalog.timestamp
                            marker_step = marker_step + 1
                            if marker_step >= 10 then
                                local hbase_body = {
                                    "info", "marker", cur_marker, 
                                    "info", "timestamp", cur_timestamp
                                }
                                local code, ok = metadb:put("log_marker", marker_key, hbase_body)
                                if not ok then
                                    log(ERR, "metadb put log maker failed. code = " .. code)
                                    lock_mgr.unlock(lock_key, signature)
                                    return false
                                end
                                marker_step = 0
                            end
                        end

                        if marker_step > 0 then
                            local hbase_body = {
                                "info", "marker", cur_marker, 
                                "info", "timestamp", cur_timestamp
                            }
                            local code, ok = metadb:put("log_marker", marker_key, hbase_body)
                            if not ok then
                                log(ERR, "metadb put log maker failed. code = " .. code)
                                lock_mgr.unlock(lock_key, signature)
                                return false
                            end
                        end
                        if skip_cur then
                            break
                        end
                        marker = cur_marker
                    until false
                    lock_mgr.unlock(lock_key, signature)
                end
            end
        end
    end

    return true
end

--[[  delete 
--description
-- Scan the objects within the specified range and move them from object table to the delete table
-- params:
--     start_key    : range start key 
--     end_key      : range end key
--     lockkey      : redis lock key 
--     sig          : redis lock sig
--     btag         : bucket tag
-- return:
--     completed    : if all expired objs are cleaned out, return true, otherwise return false
local function move_expire_placeholder_objects(start_key, end_key, lockkey, sig)
    local completed = true
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", " start move expire placeholder  obj start_key:", start_key, " end_key:", end_key)
    repeat
        while reqmonit.check_system_busy() do
            local ok,ret = lock_mgr.extend_lock(lockkey, sig, 300)
            if not ok  then
                ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ", "extend lock_mgr faile lockkey:", lockkey, " sig:", sig)
                return false
            end
            ngx.sleep(3)
        end

        local ok, result, common_prefixes, next_marker, truncated = sp_s3op.list_entries(metadb, "expire_object_placeholder", nil, start_key, end_key, "", "", 1001)
        if not ok then
            log(ERR, "metadb failed to scan rule.")
            return false
        end
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", " list entries next_marker:", next_marker, " truncated:", truncated)
        local ok,ret = lock_mgr.extend_lock(lockkey, sig, 1000)
        if not ok  then
            ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ", "extend lock_mgr faile lockkey:", lockkey, " sig:", sig)
            return false
        end

        for i, v_tab in ipairs(result) do
            local key = v_tab.key --3d1f1b6aef_20170514_30b2fde56ad948d2b1bc4e4262168f8d_test_a553600266d5454e8485daf38c9e934d
            local s,e = ngx.re.find(key, "_", "jo")
            local noshardkey = string.sub(key, e+1, -1)--20170514_30b2fde56ad948d2b1bc4e4262168f8d_test_a553600266d5454e8485daf38c9e934d
            
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", " expire placeholder  delete noshardkey:", noshardkey, " ", s, e)
            local s,e = ngx.re.find(noshardkey, "_", "jo")
            local nodatekey = string.sub(noshardkey, e+1, -1)--30b2fde56ad948d2b1bc4e4262168f8d_test_a553600266d5454e8485daf38c9e934d
 
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", " expire placeholder  delete nodatekey:", nodatekey, " ", s, e)
            local otag = v_tab.values["info:tag"]
            local okey = string.sub(nodatekey, 1, -#otag-2)--30b2fde56ad948d2b1bc4e4262168f8d_test

            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", " expire placeholder  delete okey:", okey)
            -- todo checkAndDelete return value
            local code, incode, ok = metadb:checkAndDelete("object", okey, "ver", "tag", otag)
            local code,ok = metadb:delete("expire_object_placeholder", key)
            if not ok then
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "failed delete expire placeholder object from metadb")
            end
        end
        
        start_key = next_marker
        if not truncated or not next_marker then
            return true
        end
    until false

    return completed
end
local function get_expire_placeholder_shard_lock(i)
    local shard = utils.get_shard(EXPIRE_NAME, EXPIRE_MAX, i)
    local ok,ret = lock_mgr.lock(shard, 600)
    if ok then
        return true, ret, shard
    end
    return false, nil, nil
end

--description
-- Scan all eligible expired placeholder objects in the object table and  delete table
-- params:
-- return:
--     completed    : if all objs are cleaned out, return true, otherwise return false
--     count        : number of objects successfully deleted
function _M.expire_placeholder_objects(self)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", "entry expire placeholder object ")
    for i=1,EXPIRE_MAX do
        local ok,sig,shard = get_expire_placeholder_shard_lock(i)
        if ok then
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", " expire placeholder shard lock_mgr i:", i, " shard:", shard)
            local lockkey = shard
            local markkey = shard.."_placeholder"
            local mark = get_expire_mark(markkey)
            local last_timestamp = tonumber(utils.date_to_sec(mark))
            local expire_time = expire_placeholder_days * 86400
            local now = ngx_now()
            assert(now > expire_time)
            local expire_clock = tostring(math.ceil(now - expire_time))
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", "now is ", now, ", expire placeholder time is ", expire_time, " expire placeholder clock is ", expire_clock, " last_timestamp:", last_timestamp)
            local current = expire_clock
            local finished = false
            while last_timestamp <= (current - 86400) do
                current = current - 86400 --move one day later
                local date = utils.toDATE(tostring(current))
                
                local lower_key = shard.."_"..date.."_"..metadb.MIN_CHAR
                local upper_key = shard.."_"..date.."_"..metadb.MAX_CHAR
                ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", " expire  lower_key:", lower_key, " upper_key:", upper_key)
                repeat
                    local busy = reqmonit.check_system_busy()
                    if busy then
                        ngx.sleep(5)
                    else
                        finished = move_expire_placeholder_objects(lower_key, upper_key, lockkey, sig)
                        break
                    end
                    local ok,ret = lock_mgr.extend_lock(lockkey, sig, 1000)
                    if not ok  then
                        ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ", "extend lock_mgr faile lockkey:", lockkey, " sig:", sig)
                        finished = false
                    end
                until false
                
                if not finished then
                    break
                end
            end

            if finished then
                local mark = utils.toDATE(expire_clock)
                local code, ok, result = metadb:put("log_marker", markkey, {"info", "marked", mark, "info", "timestamp", ngx.now()})
                if not ok then
                    ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", "put expire mark faile markkey:", markkey, " mark", mark)
                end
            end
            lock_mgr.unlock(lockkey, sig)
        end
    end
end
--]]

local function get_full()
    local code, ok, res = metadb:get("full_marker", "fullsync", {"info:id", "info:status"})
    if not ok then
        ngx.log(ngx.ERR, "metadb get full marker failed. code = ", code)
        return false
    end

    local fullid = res["info:id"]
    local status = res["info:status"]

    if nil == fullid or '' == fullid then
        return false
    end

    return true, fullid, status
end

local function get_fullid_info(fullid)
    local code, ok, res = metadb:get("full_marker", fullid, {"info:status", "info:accessid", "info:ctime", "info:remote_ip", "info:remote_port", "info:remote_data", "info:sync_type", "info:force"})
    if not ok then
        ngx.log(ngx.ERR, "metadb get full marker failed. code = ", code)
        return false
    end

    return true, res
end

-- single shard lock
local function get_full_shard_lock(shard)
    local ok,ret = lock_mgr.lock(shard, 600)
    if ok then
        return true, ret
    end
    return false, nil
end

local function get_full_shard(fullid, marker)
    local completed = true
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", " get full shard id:", fullid)

    local start_key = nil
    if nil == marker then
        start_key = fullid.."_"
    else
        start_key = marker
    end

    local end_key = fullid.."_"..metadb.MAX_CHAR
    repeat
        local filter = "SingleColumnValueFilter('info','status',!=, 'binary:complete2')"
        local ok, result, common_prefixes, next_marker, truncated = sp_s3op.list_entries(metadb, "full_marker", nil, start_key, end_key, "", "", 1001, filter)
        if not ok then
            ngx.log(ngx.ERR, "metadb failed to scan rule.")
            return false
        end
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", " list entries next_marker:", next_marker, " truncated:", truncated, " result:", inspect(result))
        
        return true, result, next_marker      
    until false

    return false
end


local function get_user_fsindex(accessid)
    local code, ok, res = metadb:get("user", accessid, {"info:isindex"})
    if not ok then
        ngx.log(ngx.ERR, "metadb get user fsindex failed. code = ", code)
        return false
    end

    local fsindex = res["info:isindex"]
    return true, fsindex
end

local function get_signle_shardid_info(shardid)
    local code, ok, res = metadb:get("full_marker", shardid, {"info:status", "info:marker"})
    if not ok then
        ngx.log(ngx.ERR, "metadb get user fsindex failed. code = ", code)
        return false
    end

    return true, res
end

local function full_check_system_busy(lockid, sig)
    while (true)
    do
        local ok,ret = lock_mgr.extend_lock(lockid, sig, 1800)
        if not ok then
            ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ", "extend lock_mgr faile lockid:", lockid, " sig:", sig)
            return false
        end
        
        local busy = reqmonit.check_system_busy()
        if busy then
            ngx.sleep(5)
        else
            return true
        end
    end
    return false
end

local function full_insert_marker(accessid, btag, uid, res)
    
    --1. encode resps body to string
    local ok, sres = pcall(json.encode, res)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", " json.encode error ", inspect(res))
        return false
    end
    
    local rowkey = accessid.."_"..btag.."_"..uid
    local code, ok = metadb:put("full_marker", rowkey, {"info", "data", sres})
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", " full  marker put db failed rowkey :", rowkey, " sres:", sres)
        return false
    end
    return true
end

local function full_delete_marker(rowkey)
    
    local code, ok = metadb:delete("full_marker", rowkey)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", " full  marker delete db failed rowkey :", rowkey)
        return false
    end
    return true
end
local function full_handle_shard(sync_op, accessid, lockid, sig, res)
    local total = res["total"] 
    for k = 1, total do
        local objinfo = res[tostring(k)]
        objinfo["user"] = accessid
        local bkey = objinfo["bkey"]
        local s,e = ngx.re.find(bkey, "_", "jo")
        local bucket = string.sub(bkey, e+1, -1)
        objinfo["bucket"] = bucket
        local lock_info = {
            start = ngx_now(),
            key = lockid,
            signature = sig,
            ttl = 1800,
        }
        local ok = sync_op:process_datalog(objinfo, lock_info)
        if not ok then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", " full sync object failed!!!  object:", objinfo["obj"], " seqno:", objinfo["seqno"])
            return false
        end
    end
    return true
end

local function full_get_local_shard(shardid, marker)
    local completed = true
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", " get full shard id:", shardid)

    local start_key = nil
    if nil == marker then
        start_key = shardid..metadb.MIN_CHAR
    else
        start_key = marker
    end

    local end_key = shardid..metadb.MAX_CHAR
    repeat
        local ok, result, common_prefixes, next_marker, truncated = sp_s3op.list_entries(metadb, "full_marker", nil, start_key, end_key, "", "", 1001, filter)
        if not ok then
            ngx.log(ngx.ERR, "metadb failed to scan rule.")
            return false
        end
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", " list entries next_marker:", next_marker, " truncated:", truncated)
        
        return true, result, next_marker      
    until false

    return false
end
local function full_handle_local_shard(sync_op, accessid, btag, lockid, sig, shardid)

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", " full handle local shard  shardid :", shardid)
    --local marker
    local id = accessid.."_"..btag.."_"
    local shard_marker = nil
    local prve_marker = nil
    local empty = true
    while(true)
    do
        prve_marker = shard_marker 
        local ok, result, next_marker = full_get_local_shard(id, shard_marker)
        if ok then
            if result and next(result) then
                empty = false
                for i, v in pairs(result) do
                    local key = v.key
                    local value = v.values
                    local data = value["info:data"]

                    local new_lockid = key
                    local ok,new_sig = get_full_shard_lock(new_lockid)
                    if ok then
                        --unlock fullid lock
                        lock_mgr.unlock(lockid, sig)
                    
                        local ok = full_check_system_busy(new_lockid, new_sig)
                        if not ok then
                            return ok
                        end
                    
                        --1. encode resps body to string
                        local ok, res = pcall(json.decode, data)
                        if not ok then
                            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", " json.decode error ", data)
                            lock_mgr.unlock(new_lockid, new_sig)
                            return false
                        end

                        local ok = full_handle_shard(sync_op, accessid, new_lockid, new_sig, res)
                        if not ok then 
                            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", " full handle shard failed. res:", data)
                            lock_mgr.unlock(new_lockid, new_sig)
                            return false
                        end
                        full_delete_marker(key)
                        lock_mgr.unlock(new_lockid, new_sig)
                        return true
                    end    
                end
            else
                if empty then
                    --full complete
                    local code, ok = metadb:put("full_marker", shardid, {"info", "status", "complete2", "info", "mtime", ngx.now()})
                    if not ok then
                        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", " full local shards complete put db failed !!!  shardid :", shardid)
                    else
                        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", " full local shards complete put db OK ! shardid :", shardid)
                    end
                end
                return true
            end
            shard_marker = next_marker
        else
            return false
        end
    end
    return true
end

local function full_handle_remote_shard(sync_op, accessid, btag, lockid, sig, shardid, obj_marker)
     
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", " full handle remote shard  shardid :", shardid, " obj_marker:", obj_marker)
    
    local ok, res =  sync_op:full_fetch_remote_objects(obj_marker)
    if ok then
        local total = res["total"] 
        if 0 == tonumber(total)  then
            -- single shard complete
            local code, ok = metadb:put("full_marker", shardid, {"info", "status", "complete1"})
            if not ok then
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", " full remote shard objects complete put db failed !!!  shardid :", shardid)
            else
                ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", " full remote shard objects complete put db OK ! shardid :", shardid)
            end
            return ok,1
        else
            -- full shard objects
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", " full remote shard objects marker :", obj_marker, " total:", total)
            local uid = uuid()
            local new_lockid = accessid.."_"..btag.."_"..uid
            local ok,new_sig = get_full_shard_lock(new_lockid)
            if not ok then
                return false,0
            end

            local ok = full_insert_marker(accessid, btag, uid, res)
            if ok then
                local objinfo = res[tostring(total)]
                local last_marker = objinfo["marker"] 
                local code, ok = metadb:put("full_marker", shardid, {"info", "marker", last_marker})
                if not ok then
                    ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", " full remote shard objects marker put db failed  shardid :", shardid, " marker:", last_marker)
                    return false,0
                end
                lock_mgr.unlock(lockid, sig)
            end
            
            local ok = full_handle_shard(sync_op, accessid, new_lockid, new_sig, res)
            if not ok then 
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", " full handle shard failed. res:", data)
                return false,0
            end
            full_delete_marker(new_lockid)
            lock_mgr.unlock(new_lockid, new_sig)
        end --full shard objects end
    else
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", " full shard fetch remote objects failed. shardid:", shardid)
        return false,0
    end

    return true,2
end

local function full_single_shard(sync_op, shardid, lockid, sig)
    
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", " full single shard  shardid :", shardid)
    -- shardid: full_a6f6f4bf5b8c4231b73a5c723caa28a7_QKXNB86K5P0PGSJ4ZJAP_da289f5d41bd42889d4252ae1cac4daa
    local s,e = ngx.re.find(shardid, "_", "jo")
    local surplus = string.sub(shardid, e+1, -1)
    
    local s,e = ngx.re.find(surplus, "_", "jo")
    local fullid = string.sub(surplus, 1, s-1)
    local surplus1 = string.sub(surplus, e+1, -1)
    
    local s,e = ngx.re.find(surplus1, "_", "jo")
    local accessid = string.sub(surplus1, 1, s-1)
    local btag = string.sub(surplus1, e+1, -1)
    
    local ok,fsindex = get_user_fsindex(accessid)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", " get user fsindex failed")
        return ok,0
    end
    
    local ok, shardid_info =  get_signle_shardid_info(shardid)
    if not ok then 
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", " get shardid info failed")
        return ok,0
    end
    
    local shardid_status = shardid_info["info:status"]
    if "complete2" == shardid_status then
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", " full single shardid status:", shardid_status)
        return true, 1
    elseif "complete1" == shardid_status then
        local ok = full_handle_local_shard(sync_op, accessid, btag, lockid, sig, shardid)
        return ok,1
    else
        local obj_marker = shardid_info["info:marker"] and shardid_info["info:marker"] or btag.."_"
        local ok,code = full_handle_remote_shard(sync_op, accessid, btag, lockid, sig, shardid, obj_marker) 
        return ok,code
    end
end


-- fullid lock
local function get_full_sync_lock(key)
    local ok,ret = lock_mgr.lock(key, 600)
    if ok then
        return true, ret
    end
    return false, nil
end

local FULL_SYNC_LOCK_KEY = "fullsync"

local function updata_full_sync_id()
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", " updata full sync id")

    local start_key = "full_"
    local end_key = "full_"..metadb.MAX_CHAR
    local filter = "SingleColumnValueFilter('info','status',=, 'binary:ready')"

    local ok, result, common_prefixes, next_marker, truncated = sp_s3op.list_entries(metadb, "full_marker", nil, start_key, end_key, "", "", 1, filter)
    if not ok then
        ngx.log(ngx.ERR, "metadb failed to scan rule.")
        return false
    end
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", " list full sync id result:", inspect(result))

    if result and next(result) then
        local key = nil
        local value = nil
        for i, v in pairs(result) do
            key = v.key
            value = v.values
        end

        --update full sync id
        local code, ok = metadb:put("full_marker", "fullsync", {"info", "status", "ready","info", "mtime", ngx.now(), "info", "id", key})
        if not ok then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", " put db  full sync id falied !")
        end

        return ok, key, value
   end

   return false
end

local function get_syncflag(accessid, bucket)
    if not accessid then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "accessid is nil")
        return false, nil
    end

    local hbase_body = {"info:syncflag", "ver:tag"}
    local code, ok, user_info = metadb:get("user", accessid, hbase_body)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "get user_info failed(not client.get err and client.get error), code is:" .. code)
        return false, nil
    end

    ngx.log(ngx.DEBUG, "get userinfo by accessid,result: \n", inspect(user_info))
    if not user_info or not next(user_info) then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "get user_info failed non-exist")
        return false, nil
    end

    local sync_flag = user_info["info:syncflag"]
    if bucket then
        local rowkey = user_info["ver:tag"] .. "_" .. bucket
        local code, ok, rebody1 = metadb:get("bucket", rowkey, hbase_body)
        if not ok then
            ngx.log(ngx.ERR,  "metadb failed to get bucket :" .. code)
            return false, nil
        end

        if not rebody1 or not next(rebody1) then
            ngx.log(ngx.ERR,"bucket inexistent :" .. bucket)
            return false, nil
        end
        sync_flag = sync_flag or rebody1["info:syncflag"]
    end
    
    return true, sync_flag
end

-- full remote data
function _M.full_objects(self)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", " start full  rsyncing !")
    
    --get redis lock
    local ok, full_sig = get_full_sync_lock(FULL_SYNC_LOCK_KEY)
    if not ok then
        return false
    end
    
    local ok, fullid, status = get_full()
    if not ok then
        lock_mgr.unlock(FULL_SYNC_LOCK_KEY, full_sig)
        return false
    end

    local idinfo = nil
    if "complete" == status then
        -- sync next id
        ok, fullid, idinfo = updata_full_sync_id()
        if not ok then
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", "updata full sync id failed. mybe not sync!")
            lock_mgr.unlock(FULL_SYNC_LOCK_KEY, full_sig)
            return true
        end
    else
        ok, idinfo = get_fullid_info(fullid)
        if not ok then
            lock_mgr.unlock(FULL_SYNC_LOCK_KEY, full_sig)
            return false
        end
    end

    local id_status = idinfo["info:status"]
    if "complete" == id_status then
        local code, ok, result = metadb:put("full_marker", "fullsync", {"info", "status", "complete","info", "mtime", ngx.now()})
        if not ok then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", " put db  full remote status rsyncing falied !")
        end
        lock_mgr.unlock(FULL_SYNC_LOCK_KEY, full_sig)
        return ok
    end
    
    local remote_ip = idinfo["info:remote_ip"]
    local remote_port = tonumber(idinfo["info:remote_port"])
    local remote_data = tonumber(idinfo["info:remote_data"]) 
    local accessid = idinfo["info:accessid"]
    local bucket = idinfo["info:bucket"]
    local force = idinfo["info:force"]
    -- sync_type: 1 sync all user. but unsync bucket,data.
    --            2 sync user,bucket. but unsync data.
    --            3 sync user,bucket,data.
    --            4 sync specify user. but unsync bucket,data.
    --            5 sync specify user, all bucket. but unsync data.
    --            6 sync specify user, all bucket data.
    --            7 sync specify user, specify bucket. but unsync data.
    --            8 sync specify user, specify bucket all data.
    if not force then
        local ok, syncflag = get_syncflag(accessid, bucket)
        if not ok then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", " get_syncflag failed !")
            return false
        end

        if not syncflag then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", " an unforced fullsync can't begin for no syncflag configed!")
            return true
        end
    end

    local sync_type = tonumber(idinfo["info:sync_type"])
    
    local sync_op  = sync_mgr:new(remote_ip, remote_port, remote_data)
    if "ready" == id_status then
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", " full remote  ready fullid: ", fullid)
        
        local ok = full_check_system_busy(FULL_SYNC_LOCK_KEY, full_sig)
        if not ok then
            return ok
        end

        local ok = sync_op:fetch_remote_user(fullid, sync_type, accessid, bucket)
        if ok then 
            if 3 == sync_type or 
                6 == sync_type or
                8 == sync_type  then -- sync data
                local code = nil 
                code, ok = metadb:put("full_marker", fullid, {"info", "status", "rsyncing",})
                if not ok then
                    ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", " put db  full remote status rsyncing falied !  fullid: ", fullid)
                end
            else
                -- unsync data
                --full complete
                local code = nil
                code, ok = metadb:put("full_marker", fullid, {"info", "status", "complete", "info", "mtime", ngx.now()})
                if not ok then
                    ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", " full metadata complete put db failed !!!  fullid :", fullid)
                else
                    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", " full metadata complete put db OK ! fullid :", fullid)
                end
            end 
        end
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", " full remote user bucket ok!")
        
        lock_mgr.unlock(FULL_SYNC_LOCK_KEY, full_sig)
        return ok
    elseif "rsyncing" == id_status then 
        lock_mgr.unlock(FULL_SYNC_LOCK_KEY, full_sig)
        
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", " full remote rsyncing fullid: ", fullid)
        local shard_marker = nil
        local prve_marker = nil
        while(true)
        do
            prve_marker = shard_marker
            local ok, result, next_marker =  get_full_shard(fullid, shard_marker)
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", " get full shard  next_marker:", next_marker,  " result:", inspect(result))
            if ok then
                if nil == shard_marker and nil == next_marker then
                    --full complete
                    local code, ok = metadb:put("full_marker", fullid, {"info", "status", "complete", "info", "mtime", ngx.now()})
                    if not ok then
                        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", " full shards complete put db failed !!!  fullid :", fullid)
                    else
                        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", " full shards complete put db OK ! fullid :", fullid)
                    end
                    return ok;
                else
                    if result and next(result) then
                        for i, v in pairs(result) do
                            local key = v.key
                            local lockid = key
                            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", " full shards lockid :", lockid, " i:", i, " key:", key)
                            while(true)
                            do
                                local ok,sig = get_full_shard_lock(lockid)
                                if ok then
                                    -- full single shard 
                                    local ok,code = full_single_shard(sync_op, key, lockid, sig)
                                    if not ok or 2 ~= code then
                                        lock_mgr.unlock(lockid, sig)
                                        break
                                    end
                                    
                                    local ok = full_check_system_busy(lockid, sig)
                                    if not ok then
                                        lock_mgr.unlock(lockid, sig)
                                        return ok
                                    end
                                    lock_mgr.unlock(lockid, sig)
                                else
                                    break
                                end
                            end
                        end
                    else
                        return true   
                    end
                end

                shard_marker = next_marker
            else
                return false
            end -- if  get full shard end

            if prve_marker == shard_marker then
                return false
            end
        end
    end
    return true
end

local function delete_bucket_objects(btag, lockkey, sig)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", " delete bucket objects. key:", lockkey)
    local oldkey   = btag..metadb.MIN_CHAR
    local startkey = btag..metadb.MIN_CHAR
    local endkey   = btag..metadb.MAX_CHAR
    repeat
        while reqmonit.check_system_busy() do
            ngx.sleep(10)
        end
        
        local ok,ret = lock_mgr.extend_lock(lockkey, sig, 1800)
        if not ok then
            ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ", "extend lock_mgr faile lockkey:", lockkey, " sig:", sig)
            return false
        end

        local ok, result, common_prefixes, next_marker, truncated = sp_s3op.list_entries(metadb, "object", nil, startkey, endkey, "", "", 1000)
        if not ok then
            ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ", " metadb failed to scan object.")
            break
        end
        
        for i, v_tab in ipairs(result) do
            local okey = v_tab.key
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", " delete bucket service okey:", okey)
            
            local dir = v_tab.values["info:dir"]
            if nil == dir then
                -- is file
                local hsize      = tonumber(v_tab.values["info:hsize"])
                local size       = tonumber(v_tab.values["info:size"])

                if size > hsize then
                    local hbase_body = utils.out_db_to_in(v_tab.values)
                    local object     = v_tab.values["info:oname"]
                    local shard      = sp_s3op.get_delete_obj_shard(object)
                    local new_okey   = shard.."_"..okey.."_"..uuid()
                    local code,ok    = metadb:put("delete_object", new_okey, hbase_body)
                    if not ok then
                        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "metadb put failed")
                        return false
                    end
                end
            end

            local code,ok = metadb:delete("object",okey) -- hdata is also deleted
            if not ok then
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "failed delete object from metadb")
                return false
            end
        end
        if not truncated or not next_marker then
            break
        end
        startkey = next_marker
    until false
    if startkey == oldkey then
        local code,ok = metadb:delete("delete_bucket", lockkey) 
        if not ok then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "failed delete bucket from metadb")
            return false
        end
    end

    return true
end

function _M.gc_bucket(self)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ","new delete bucket service start ")
    local startkey = metadb.MIN_CHAR
    repeat
        while reqmonit.check_system_busy() do
            ngx.sleep(10)
        end

        local ok, result, common_prefixes, next_marker, truncated = sp_s3op.list_entries(metadb, "delete_bucket", {"ver:tag"}, startkey, nil, "", "", 1001)
        if not ok then
            ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ", "metadb failed to scan delete_bucket")
            break
        end
        
        for i, v_tab in ipairs(result) do
            local key = v_tab.key
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", " delete bucket service btag:", key)
            local ok,sig = lock_mgr.lock(key, 600)
            if ok then
                local btag = v_tab.values["ver:tag"]
                delete_bucket_objects(btag, key, sig)
                lock_mgr.unlock(key, sig)
            end
        end
        if not truncated or not next_marker then
            break
        end
        startkey = next_marker
    until false
    return true
end

function _M.sync_meta(self)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", "new meta sync service start ")

    local lock_ttl = 300 
    local lock_key = "metaloglock"
    local ok, signature = lock_mgr.lock(lock_key, lock_ttl)
    if not ok or not signature then
        log(ngx.WARN, "lock failed. lock_key = " .. lock_key)
        return false
    end

    log(ngx.INFO, "lock successful. lock_key = " .. lock_key)
    -- read log marker from hbase
    local marker_key = "metalog_marker"
    local code, ok, res = metadb:get("log_marker", marker_key, {"info:marker"})
    if not ok then
        log(ERR, "metadb get log marker failed. code = " .. code)
        lock_mgr.unlock(lock_key, signature)
        return false
    end

    local marker  = res["info:marker"] or metadb.MIN_CHAR
    repeat
        local code, ok, logs_table = metadb:quickScan("metalog", marker, metadb.MAX_CHAR, nil, 101)
        if not ok then 
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "metadb failed to scan metalog :")
            lock_mgr.unlock(lock_key, signature)
            return "4011", code, false
        end  

        if not next(logs_table) then
            lock_mgr.unlock(lock_key, signature)
            return true
        end

        local steps = 0
        for i, metalog in pairs(logs_table) do
            local json_body = metalog.values["info:json"]
            local ok, meta_info = pcall(json.decode, json_body)
            if not ok then 
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to decode json_body. json_body=", stringify(json_body))
                break
            end

            local req_info = meta_info["req_info"]
            local seq_no = meta_info["seq_no"]
            local body = meta_info["body"]

            local ok = sync_meta.do_sync(req_info, seq_no, body)
            if not ok then
                break
            end
            marker = metalog.key
            steps = steps + 1
        end

        local ok, err = lock_mgr.extend_lock(lock_key, signature, lock_ttl)
        if not ok then
            log(ERR, "extend_lock failed, err=" .. err .. ", lock_key=" .. lock_key)
            break
        end

        if steps > 0 then
            local hbase_body = {
                "info", "marker", marker, 
                "info", "timestamp", cur_timestamp
            }
            local code, ok = metadb:put("log_marker", marker_key, hbase_body)
            if not ok then
                log(ERR, "metadb put log maker failed. code = " .. code)
                lock_mgr.unlock(lock_key, signature)
                return false
            end
        end

        ngx.sleep(1)
    until false
    lock_mgr.unlock(lock_key, signature)

    return true
end

function _M.delete_mu()
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " enter delete_mu")
    local startkey = metadb.MIN_CHAR

    local ok, result, common_prefixes, next_marker, truncated = sp_s3op.list_entries(metadb, "temp_object", {"flag:flag"}, startkey, nil, "", "", 1001)
    if not ok then
        ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " metadb failed to scan temp_object")
        return false
    end

    for i, v_tab in ipairs(result) do
        local obj_key = v_tab.key

        local flag = v_tab.values["flag:flag"]
        if "1" == flag or "2" == flag then
            local lock_key = "delete_multi_up_"..obj_key
            local ok,sig = lock_mgr.lock(lock_key, 1200)
            if ok then
                ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " succeeded to lock key: ", lock_key)

                local upload_id = nil
                local len = #obj_key
                local obj_key_bytes = {string.byte(obj_key,1,-1)}
                local pos
                for pos = len, 1, -1 do
                    if obj_key_bytes[pos] == 95 then    -- 95 is ASCII code of '_'
                        upload_id = string.sub(obj_key, pos+1, -1)
                        break
                    end
                end

                assert(upload_id)

                --get all part info
                local status,ok,uploaded_parts = metadb:quickScanHash("temp_part", upload_id, upload_id..metadb.MAX_CHAR, nil, 10001)
                if not ok then
                    ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to scan uploaded parts from metadb. status=", status)
                    lock_mgr.unlock(lock_key, sig)
                else
                    local succ = true
                    --delete uploaded parts
                    for k,v in pairs(uploaded_parts) do
                        local code,ok = metadb:delete("temp_part", k)
                        if not ok then
                            ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " failed to delete temp part: ", k)
                            succ = false
                        end
                    end

                    --delete the temp_object if all uploaded parts were deleted successfully;
                    if succ then
                        local code,ok = metadb:delete("temp_object", obj_key)
                        if not ok then
                            ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " failed to delete temp obj: ", obj_key)
                        end
                    end

                    lock_mgr.unlock(lock_key, sig)
                end
            end
        end
    end
    return true
end

return _M
