local _M = {
    _VERSION = '0.01',
}

local utils = require('common.utils')
local cjson = require('cjson')
local sp_conf = require('storageproxy_conf')
local sync_cfg = sp_conf.config["sync_config"]
local inspect = require("inspect")
local ok,utils = pcall(require, "common.utils")
if not ok or not utils then
    error("failed to load common.utils:" .. (utils or "nil"))
end

local ok,uuid = pcall(require, "resty.uuid")
if not ok or not uuid then
        error("failed to load resty.uuid:" .. (uuid or "nil"))
end

local DATALOG_MAX = sync_cfg["datalog"]["max"]
local DATALOG_NAME = sync_cfg["datalog"]["name"]

local DIR_MARKER = string.char(1)
local function last_slash_dec(src)
    local dest = ngx.re.gsub(src, DIR_MARKER, "")
    ngx.log(ngx.DEBUG, string.format("src=%s, dest=%s", src, dest))
    return dest
end

local function get_datalog(metadb, req_info)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### Enter get_datalog")
    local query_args = req_info["args"]

    local id = query_args["id"]
    if not id or DATALOG_MAX < tonumber(id)then
        ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ", " get datalog id is nil")
        return "5100", "0000", false
    end

    local shardid = utils.get_shard(DATALOG_NAME, DATALOG_MAX, tonumber(id))
    local startkey = shardid
    local endkey = shardid..metadb.MAX_CHAR
    local marker = query_args["marker"]
    if nil ~= marker and '' ~= marker then
        startkey = marker .. metadb.MIN_CHAR 
    end

    local maxkeys = nil
    if nil ~= query_args["max-keys"] and '' ~= query_args["max-keys"] then
        maxkeys = tonumber(query_args["max-keys"])
        if nil ~= maxkeys then
            if maxkeys > sync_cfg["datalog"]["maxkeys"]["max"] then
                maxkeys = sync_cfg["datalog"]["maxkeys"]["max"]
            end
        else
           ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ",  "request max-keys is warn!!! max-key is :", query_args["max-keys"])
        end
    end
    if nil == maxkeys then
        maxkeys = sync_cfg["datalog"]["maxkeys"]["default"]
    end

    local clusterid = query_args["clusterid"]
    if nil == clusterid or "string" ~= type(clusterid) or '' == clusterid then
        ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ", " get datalog clusterid is nil")
        return "5100", "0000", false
    end
    local filter = "SingleColumnValueFilter('info','clusterid',!=, 'binary:"..clusterid.."')"
    local status,ok,result = metadb:quickScan("datalog", startkey, endkey, nil, maxkeys, 0, filter)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "metadb failed to scan datalog :"..inspect(result))
        return "5101", status, false
    end
    
    local j_result = {}
    local index = 0
    for i,v_tab in pairs(result) do
        index = index + 1
        local part = {}
        part["marker"] = v_tab.key
        part["user"] =  v_tab.values["info:user"]
        part["bucket"] =  v_tab.values["info:bucket"]
        part["obj"] =  v_tab.values["info:obj"]
        part["type"] =  v_tab.values["info:type"]
        part["timestamp"] =  v_tab.values["info:timestamp"]
        part["clusterid"] = v_tab.values["info:clusterid"]
        part["vertag"] = v_tab.values["info:vertag"]
        if "copy" == v_tab.values["info:type"] then
            part["sbucket"] = v_tab.values["info:sbucket"]
            part["sobj"] = v_tab.values["info:sobj"]
        end
        if nil ~= v_tab.values["info:seqno"] then
            part["versionid"] = v_tab.values["info:seqno"]
        end

        j_result[tostring(index)] = part
    end
    
    j_result["total"] = index
    local jsonstr = cjson.encode(j_result)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", " get datalog id:", id, " result:", jsonstr)
    
    ngx.print(jsonstr)
    return "0000","0000",true
end

local function get_users(metadb, req_info)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### Enter get_users")

    local query_args = req_info["args"]
    local accessid = query_args["accessid"] -- specify user accessid
    local userAccId = req_info["auth_info"]["accessid"] -- root user accessid

    local filter = "RowFilter(!=, 'binary:"..userAccId.."')"
    local start_key = metadb.MIN_CHAR
    local end_key = metadb.MAX_CHAR

    if '' == accessid then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", " sync specify user is '' ")
        return "5100", "0000", false
    end

    if nil ~= accessid then
        start_key = accessid
        end_key = accessid..metadb.MAX_CHAR
    end

    local status,ok,result = metadb:quickScan("user", start_key, end_key, {"info","quota","ver","exattrs","bucket_quota"}, 10000, nil, filter)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "metadb failed to scan users")
        return "5101", status, false
    end
    
    local jsonstr = cjson.encode(result)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", " get users result:", jsonstr)
    
    ngx.print(jsonstr)
    return "0000","0000",true
end

local function get_buckets(metadb, req_info)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### Enter get_buckets")
    local query_args = req_info["args"]

    local rowkey = nil
    local usertag = query_args["usertag"]
    if nil == usertag or "string" ~= type(usertag) or '' == usertag then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", " get buckets usertag is nil")
        return "5100", "0000", false
    end
    local bucket = query_args["bucket"]
    if '' == bucket then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", " get buckets specify is '' ")
        return "5100", "0000", false
    end
    
    local start_key = usertag..metadb.MIN_CHAR
    local end_key = usertag..metadb.MAX_CHAR
    if nil ~= bucket then
        start_key = usertag.."_"..bucket
        end_key = usertag.."_"..bucket..metadb.MAX_CHAR
    end

    local status,ok,result = metadb:quickScan("bucket", start_key, end_key, {"info","quota","ver","exattrs"}, 10001)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "metadb failed to scan buckets usertag:", usertag)
        return "5101", status, false
    end
    
    local jsonstr = cjson.encode(result)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", " get buckets result:", jsonstr)
    
    ngx.print(jsonstr)
    return "0000","0000",true
end


local function get_objcet_bkey(metadb, btag)
    local filter = "SingleColumnValueFilter('ver','tag',=,'binary:"..btag.."')"
    local status,ok,result = metadb:quickScan("bucket", nil, nil, nil, 1, 0, filter)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "metadb failed to scan bucket bkey :"..inspect(result))
        return "5101", status, false
    end
    
    local bkey = nil
    for i,v_tab in pairs(result) do
        bkey = v_tab.key
    end
    
    if nil == bkey then 
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "metadb failed to scan bucket bkey is nil.")
        return "5101", status, false
    end
    return "0000","0000",true,bkey
end

local function get_objects(metadb, req_info)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### Enter get_objects")
    local query_args = req_info["args"]

    local start_key = nil
    local end_key = nil
    local marker = query_args["marker"]
    if nil ~= marker and '' ~= marker then
        start_key = marker .. metadb.MIN_CHAR
        
        local s,e = ngx.re.find(marker, "_", "jo")
        end_key = string.sub(marker, 1, s+1)
        end_key = end_key..metadb.MAX_CHAR
    else
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", " get objects marker is nil")
        return "5100", "0000", false
    end

    local maxkeys = nil
    if nil ~= query_args["max-keys"] and '' ~= query_args["max-keys"] then
        maxkeys = tonumber(query_args["max-keys"])
        if nil ~= maxkeys then
            if maxkeys > sync_cfg["datalog"]["maxkeys"]["max"] then
                maxkeys = sync_cfg["datalog"]["maxkeys"]["max"]
            end
        else
           ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ",  "request max-keys is warn!!! max-key is :", query_args["max-keys"])
        end
    end
    if nil == maxkeys then
        maxkeys = sync_cfg["datalog"]["maxkeys"]["default"]
    end 

    local status,ok,result = metadb:quickScan("object", start_key, end_key, nil, maxkeys, 0, filter)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "metadb failed to scan objects :"..inspect(result))
        return "5101", status, false
    end
    
    local bkey = nil
    for i,v_tab in pairs(result) do
        local file = v_tab.values["info:file"]
        if nil ~= file and "true" == file then
            bkey = v_tab.values["info:bucket"]
            break
        end
    end
    
    local j_result = {}
    local index = 0
    for i,v_tab in pairs(result) do
        index = index + 1
        local part = {}
        local dir = v_tab.values["info:dir"]
        if nil ~= dir and "true" == dir then
            part["dir"] = "true"
            local okey = last_slash_dec(v_tab.key)
            local s,e = ngx.re.find(okey, "_/", "jo")
            local obj = string.sub(okey, e+1, -1)
            if nil == bkey then
                local btag = string.sub(okey, 1, s-1)
                local code,incode,ok,key = get_objcet_bkey(metadb, btag)
                if not ok then
                    return code,incode,ok
                end
                bkey = key
            end
	    if nil ~= v_tab.values["ver:seq"] then
                part["seqno"] =  v_tab.values["ver:seq"]
	    end
            part["obj"] = obj
            part["bkey"] = bkey
        else
            part["bkey"] = v_tab.values["info:bucket"]
            part["obj"] =  v_tab.values["info:oname"]
            part["timestamp"] =  v_tab.values["info:time"]
            part["etag"] =  v_tab.values["info:etag"]
            part["seqno"] =  v_tab.values["ver:seq"]
            part["vertag"] = v_tab.values["ver:tag"]
            if 0 == tonumber(v_tab.values["mfest:psize"]) then
                --put
                part["type"] = "put"
            else
                --mulit
                part["type"] = "multput"
            end
        end
        part["marker"] = v_tab.key
        part["clusterid"] = sync_cfg["clusterid"]
        j_result[tostring(index)] = part
    end
    
    j_result["total"] = index
    local jsonstr = cjson.encode(j_result)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", " get objects result:", jsonstr)
    
    ngx.print(jsonstr)
    return "0000","0000",true
end

local function put_full_rsync(metadb, rtable)

    local remote_ip = rtable["remote_ip"]
    local remote_port = rtable["remote_port"]
    local remote_data_port = rtable["remote_data_port"]
    local accessid = rtable["accesskeyid"]
    local bucket = rtable["bucket"]
    local force_sync = rtable["force"]
    -- sync_type: 1 sync all user. but unsync bucket,data.
    --            2 sync user,bucket. but unsync data.
    --            3 sync user,bucket,data.
    --            4 sync specify user. but unsync bucket,data.
    --            5 sync specify user, all bucket. but unsync data.
    --            6 sync specify user, all bucket data.
    --            7 sync specify user, specify bucket. but unsync data.
    --            8 sync specify user, specify bucket all data.
    local sync_type = rtable["sync_type"]

    if nil == remote_ip or '' == remote_ip or
        nil == remote_port or '' == remote_port or
        nil == remote_data_port or '' == remote_data_port or
        nil == sync_type or '' == sync_type then
        
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ","Post full sync parameter nil. remote_ip:", remote_ip, " remote_port:", remote_port, " remote_data_port:", remote_data_port, " sync_type:", sync_type)
        return "0010", "0000", false
    end
    if tonumber(sync_type) < 1 or tonumber(sync_type) > 8 then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ","Post full sync parameter error.  sync_type:", sync_type)
        return "0010", "0000", false 
    end

    local id = "full_"..uuid() 
    local hbase_body = {
        "info", "id", id,
        "info", "ctime", ngx.now(),
        "info", "status", "ready",
        "info", "remote_ip", remote_ip,
        "info", "remote_port", remote_port,
        "info", "remote_data", remote_data_port,
        "info", "sync_type", sync_type,
    }
    local index = #hbase_body

    if tonumber(sync_type) > 3 then
        if nil == accessid or '' == accessid then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ","Post full sync accessid nil")
            return "0010", "0000", false
        end
        hbase_body[index+1] = "info"
        hbase_body[index+2] = "accessid"
        hbase_body[index+3] = accessid
        index = index + 3
        if tonumber(sync_type) > 6 then
            if nil == bucket or '' == bucket then
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ","Post full sync bucket nil")
                return "0010", "0000", false
            end
            hbase_body[index+1] = "info"
            hbase_body[index+2] = "bucket"
            hbase_body[index+3] = bucket
            index = index + 3

        end
    end
    if force_sync then
        hbase_body[index+1] = "info"
        hbase_body[index+2] = "force"
        hbase_body[index+3] = force_sync
        index = index + 3 
    end

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", " post full sync hbase body:", inspect(hbase_body))
    
    local code,ok = metadb:put("full_marker", id, hbase_body)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ","Post full sync put hbase failed. code:", code)
        return "0011","0000",false
    end
    
    return "0000","0000",true
end
local function post_full_rsync(metadb, req_info)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### Enter put full rsync")
    
    ngx.req.read_body()
    local request_body = ngx.var.request_body
    local ok, rtable = pcall(cjson.decode, request_body)
    if not ok or "table" ~= type(rtable) then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ","Response body error when invoke json.decode. body=", inspect(request_body))
        return "0010", "0000", false
    end

    return put_full_rsync(metadb, rtable)
end
function _M.process_rsync(self, metadb, req_info)

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### Enter process_rsync :", req_info["method"], req_info["uri"])
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### args is ", inspect(req_info["args"]))
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### headers is ", inspect(req_info["headers"]))

    local statcode = "0018" -- not supported
    local innercode = "0000"
    local ok = false
    local op_type = req_info["op_type"]

    if "GET_DATALOG" == op_type then
        statcode, innercode, ok = get_datalog(metadb, req_info)
    elseif "GET_USERS" == op_type then
        statcode, innercode, ok = get_users(metadb, req_info)
    elseif "GET_BUCKETS" == op_type then
        statcode, innercode, ok = get_buckets(metadb, req_info)
    elseif "GET_OBJECTS" == op_type then
        statcode, innercode, ok = get_objects(metadb, req_info)
    elseif "POST_FULL_RSYNC" == op_type then
        statcode, innercode, ok = post_full_rsync(metadb, req_info)
    else
        --method not supported
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### operation not supported:"..req_info["method"].." "..req_info["uri"].." "..op_type)
    end
    
    return statcode, innercode, ok
end

function _M.set_new_fullsync(self, metadb, accessid, bucket)
    if not accessid then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", " accessid is nil")
        return "0010", "0000", false
    end
    -- sync_type:
    -- 6 sync specify user, all bucket data.
    -- 8 sync specify user, specify bucket, all data.
    local sync_type = 6
    if bucket then
        sync_type = 8
    end

    local sync_peers = sync_cfg["peers"]
    for _, p in ipairs(sync_peers) do
        local rtable = {
            sync_type = sync_type,
            remote_ip = p["ip"],
            remote_port = 6080,
            remote_data_port = 6081,
            accesskeyid = accessid,
            bucket = bucket,
        }

        local code, incode, ok = put_full_rsync(metadb, rtable)
        if not ok then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", " get datalog id is nil")
            return code, incode, ok
        end
    end

    return "0000", "0000", true
end

return _M
