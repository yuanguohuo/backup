local sp_conf = require("storageproxy_conf")
local json = require('cjson')
local inspect = require("inspect")
local auth = require("v2auth")

local ok, metadb = pcall(require, "metadb-instance-nolcache")
if not ok or not metadb then
    error("failed to load metadb-instance:" .. (metadb or "nil"))
end

local mfloor = math.floor
local ngx_re = require "ngx.re"
local http = require "resty.http"

local ffi = require("ffi")
ffi.cdef[[
]]

local _M = table.new(0, 155)

function _M.write_metalog(req_info, seq_no, body)
    --seq_no in args means the request is from peer
    if req_info["args"]["seq_no"] then
        return "0000", true
    end

    local rowkey = "metalog_"..seq_no

    local meta_info = {
        req_info  = req_info,
        seq_no    = seq_no,
        body      = body,
    }

    local ok, metadata = pcall(json.encode, meta_info)
    if not ok then 
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to encode headers into sattrs. headers=", stringify(headers))
        return "0025", false
    end
    local log_body = {"info", "json", metadata}
    local code, ok = metadb:checkAndPut("metalog", rowkey, "info", "json", nil, log_body)
    if not ok then 
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " write metalog failed. code=", code)
        return code, false
    end  
    return "0000", true
end

local function delete_metalog(seq_no)
    local rowkey = "metalog_"..seq_no
    local code, ok = metadb:delete("metalog", rowkey)
    if not ok then 
        return "0001", false
    end 
    return "0000", true
end

local function query_bucket_done_seqno(req_info)
    local bucket = req_info["op_info"]["bucketname"]
    local rowkey = req_info["user_info"]["ver:tag"] .. "_" .. bucket 

    local code, ok, rebody = metadb:get("bucket", rowkey, {"ver:tag", "ver:seq"})
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "metadb get bucket failed:" .. code)
        return false
    end

    ngx.log(ngx.DEBUG, "get bucket tag and seq: ", inspect(rebody))
    if not rebody or not next(rebody) then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "bucket non-exist: ", rowkey)
        return true
    end

    if req_info["args"]["lifecycle"] then
        local rowkey = rowkey .. "_" .. rebody["ver:tag"]
        code, ok, rebody = metadb:get("lifecycle", rowkey, {"ver:seq"})
        if not ok then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "metadb get bucket failed:" .. code)
            return false
        end

        ngx.log(ngx.DEBUG, "get bucket lifecycle seq: ", inspect(rebody))
        if not rebody or not next(rebody) then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "bucket lifecycle non-exist")
            return true
        end
    end

    local done_seq = rebody["ver:seq"]

    return true, done_seq
end

local function query_done_seqno(req_info)
    if req_info["op_info"] then
        return query_bucket_done_seqno(req_info)
    end

    local uid = req_info["args"]["uid"]
    local code, ok, rebody = metadb:get("userid", uid, {"info:accessid"})
    if not ok then 
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to query userid table, code", code)
        return false
    end  

    if not next(rebody) then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " uid not existed in userid table: " .. uid)
        return true
    end

    local accessid = rebody["info:accessid"]
    local hbase_body = {"ver:tag", "ver:seq"}
    local code, ok, user_info = metadb:get("user", accessid, hbase_body)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "get user_info failed(not client.get err and client.get error), code is:" .. code)
        return false
    end

    ngx.log(ngx.DEBUG, "get userinfo by accessid,result: \n", inspect(user_info))
    if not user_info or not next(user_info) then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "get user_info failed non-exist")
        return true
    end

    local done_seq = user_info["ver:seq"]
    local bucket = req_info["args"]["bucket"]
    if bucket then
        local rowkey = user_info["ver:tag"] .. "_" .. bucket
        local code, ok, rebody1 = metadb:get("bucket", rowkey, hbase_body)
        if not ok then
            ngx.log(ngx.ERR,  "metadb failed to get bucket :" .. code)
            return false
        end

        if not rebody1 or not next(rebody1) then
            ngx.log(ngx.ERR,"bucket inexistent :" .. bucket)
            return true
        end
        done_seq = rebody1["ver:seq"]
    end

    return true, done_seq
end

-- query existent seqno in hbase, if the result is inexistent or unequal to the log seqno,
-- means the op not executed successfully or the resource changed, so we just
-- delete the metalog and do not execute the metalog
local function check_req_valid(req_info, seq_no)
    local ok, done_seq = query_done_seqno(req_info)
    if not ok then
        return false, "0001"
    end

    if not done_seq then
        if "DELETE_USER" == req_info["op_type"] then
            return true
        end

        if req_info["op_info"] then
            local op_type = req_info["op_info"]["type"]
            if sp_conf.get_s3_op_rev("DELETE_BUCKET") == op_type or sp_conf.get_s3_op_rev("DELETE_BUCKET_LIFECYCLE") == op_type then
                return true
            end
        end

    end

    if done_seq ~= seq_no then
        ngx.log(ngx.ERR, "donw_seq mismatch seq_no: ", done_seq, ", ", seq_no)
        return false, "0000"
    end

    return true
end

local function get_peer_port(req_info)
    if req_info["op_info"] then
        return ":6081"
    end
    return ":6080"
end

local function do_sync(req_info, seq_no, body)
    ngx.log(ngx.DEBUG, "req_info is: ", inspect(req_info))

    local ok, code = check_req_valid(req_info, seq_no)
    if not ok then
        if "0000" == code then
            delete_metalog(seq_no)
        end
        return false, code
    end

    local headers = req_info["headers"] 
    local args = req_info["args"]
    args["seq_no"] = seq_no
    local port = get_peer_port(req_info)

    local sync_peers = sp_conf.config["sync_config"]["peers"]
    for _, p in ipairs(sync_peers) do
        local date = ngx.http_time(ngx.time())
        headers["Date"] = date

        local ok, authorization = auth.generate_AuthString(req_info)
        if not ok then
            ngx.log(ngx.ERR, "calc_signature failed")
            return false, "0001"
        end

        headers["Host"] = p["ip"] .. port
        headers["Content-Length"] = 0
        headers["Authorization"] = authorization

        local params = {
            method = req_info["method"],
            headers = headers, 
            query = args,
        }

        if body then
            params.body = body
            headers["Content-Length"] = #body
        end

        local httpc = http.new()
        local full_uri = "http://" .. p["ip"] .. port .. req_info["uri"]
        local res, err = httpc:request_uri(full_uri, params)
        if err or not res or res.status ~= ngx.HTTP_OK then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "error occurred from peer ", res.status)
            return false, "0002"
        end
    end

    --after doing sync meta successfully, we delete the metalog
    delete_metalog(seq_no)

    return true, "0000"
end

local handler
handler = function(premature, arg1, arg2, arg3)
    if not premature then
        ngx.log(ngx.INFO, os.date("%Y-%m-%d %H:%M:%S", ngx.time()).." : create timer success")
        local ok, err = do_sync(arg1, arg2, arg3)
        if not ok then
            ngx.log(ngx.ERR, "do_sync failed, err:", err)
        end
    end
end

function _M.sync_req(req_info, seq_no, body)
    if req_info["args"]["seq_no"] then
        return "0000", true
    end

    local sync_peers = sp_conf.config["sync_config"]["peers"]
    if sync_peers then
        local ok, err = ngx.timer.at(0, handler, req_info, seq_no, body)
        if not ok then
            ngx.log(ngx.ERR, "failed to create timer: ", err)
            return
        end
    end
end

return _M
