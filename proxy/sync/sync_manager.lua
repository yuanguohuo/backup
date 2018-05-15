local sp_conf = require("storageproxy_conf")

local ok,utils = pcall(require, "common.utils")
if not ok or not utils then
    error("failed to load common.utils:" .. (utils or "nil"))
end

local ok, new_tab = pcall(require, "table.new")
if not ok or type(new_tab) ~= "function" then
    new_tab = function (narr, nrec) return {} end
end

local ok, http = pcall(require, "resty.http")
if not ok or not http then
    error("failed to load resty.http:" .. (http or "nil"))
end

local ok, inspect = pcall(require, "inspect")
if not ok or not inspect then
    error("failed to load inspect:" .. (inspect or "nil"))
end

local ok, sp_s3op = pcall(require, "storageproxy_s3_op")
if not ok or not sp_s3op then
    error("failed to load storageproxy_s3_op:" .. (sp_s3op or "nil"))
end

local ok, metadb = pcall(require, "metadb-instance")
if not ok or not metadb or type(metadb) ~= "table" then
    error("failed to load metadb-instance:" .. (metadb or "nil"))
end

local ok, lock_mgr = pcall(require, "common.lock")
if not ok or not lock_mgr then
    error("failed to load common.lock:" .. (lock_mgr or ""))
end

local ok, oindex = pcall(require, "storageproxy_oindex")
if not ok or not oindex or type(oindex) ~= "table" then
    error("failed to load storageproxy_oindex:" .. (oindex or "nil"))
end

local ok, auth = pcall(require, "v2auth")
if not ok or not auth or type(auth) ~= "table" then
    error("failed to load auth:" .. (oindex or "nil"))
end

local str = require "resty.string"
local ngx = ngx
local ngx_now = ngx.now
local sync_cfg = sp_conf.config["sync_config"]

local json = require('cjson')

local root_uid = sp_conf.config["root_uid"]
local root_accid = nil
local root_seckey = nil

local setmetatable = setmetatable
local _M = new_tab(0, 155)
_M._VERSION = '0.1'

function _M.new(self, server, log_port, data_port, timeout)
    if not root_accid or not root_seckey then
        local  status, ok, user_info = metadb:get("userid", root_uid, {"info:accessid"})
        if not ok or not user_info or not next(user_info) then
            ngx.log(ngx.ERR, "get root accessid failed or non-exist, status is:" .. status)
            return nil
        end
        root_accid = user_info["info:accessid"]
        local  status, ok, user_info = metadb:get("user", root_accid, {"info:secretkey"})
        if not ok or not user_info or not next(user_info) then
           ngx.log(ngx.ERR, "get user_info failed or non-exist, status is:" .. status)
           return nil
        end
        root_seckey = user_info["info:secretkey"]
    end
    return setmetatable(
               {server = server, log_port = log_port, data_port = data_port},
               {__index = self}
           )
end

local function create_httpc(server, port)
    local httpc = http.new()
    if not httpc then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "failed to create resty.http for "..op)
        return nil, "2011"
    end

    local ok, err = httpc:connect(server, port)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "resty.http failed to connect to "..server..":"..port..". err="..(err or "nil"))
        return nil, "2001"
    end

    return httpc, "0000"
end

local function calc_awsv2_signature(params)
    local http_verb = params["http_verb"]
    if not http_verb then
        ngx.log(ngx.ERR, "http_verb is nil")
        return false, nil
    end

    local cont_md5 = params["content-md5"] or ""

    local cont_type = params["content-type"] or ""

    local date = params["Date"] or params["x-amz-date"]
    if not date then
        ngx.log(ngx.ERR, "Date is nil")
        return false, nil
    end

    local canonicalizedAmzHeaders = params["CanonicalizedAmzHeaders"] or ""

    local canonicalizedResource = params["request_uri"] .. (params["subresource"] or "")

    local stringToSign = http_verb .. "\n" ..
        cont_md5 .. "\n" ..
        cont_type .. "\n" ..
        date .. "\n" ..
        canonicalizedAmzHeaders ..
        canonicalizedResource

    local signature = ngx.encode_base64(ngx.hmac_sha1(params["secretkey"], stringToSign));

    local authorization = "AWS" .. " " .. params["accessid"] .. ":" .. signature;

    return true, authorization
end

local function sync_delete_obj(ubo, seqNo, clusterid, delete_time, versionid)
    local statcode, innercode, ok = sp_s3op.delete_obj(metadb, clusterid, ubo, seqNo, delete_time, versionid)
    if not ok then
        if "1121" == innercode then
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, "sync delete operation is outdated, obj="..ubo.obj_name)
            return true
        end
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, "delete_obj failed, obj="..ubo.obj_name..", statcode="..statcode..", innercode="..innercode)
    end

    return ok
end

local function sync_copy_obj(accessid, seqNo, user_info, bucket, object, s_bucket, s_obj)
    local reqinfo = {
        user_info = user_info,
        auth_info = {
            accessid = accessid,
        },
    }
    local headers = {}
    headers["x-amz-copy-source"] = "/" .. s_bucket .. "/" .. s_obj
    headers["Proxy-Object-SeqNo"] = seqNo

    local stat_code, inner_code, ok, etag = sp_s3op.copy_obj(metadb, headers, bucket, object, reqinfo)
    if not ok then 
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, "copy_obj failed ", " bucket:", bucket, " obj:", object, " source:", headers["x-amz-copy-source"])
        return false 
    end

    return true
end

function _M.head_remote_obj(self, accessid, seqNo, user_info, bucket, obj)
    local uri_obj = ngx.escape_uri(obj)
    ngx.log(ngx.INFO, " head remote obj uri_obj:", uri_obj)
    local params = {
        http_verb = "HEAD",
        accessid = accessid,
        secretkey = user_info["info:secretkey"],
        Date = ngx.http_time(ngx.time()), 
        request_uri = "/" .. bucket .. "/" .. uri_obj,
    }

    local ok, authorization = calc_awsv2_signature(params)
    if not ok or not authorization then
        ngx.log(ngx.ERR, "calc_awsv2_signature failed")
        return false
    end

    local params_table = {
        path = params["request_uri"],
        method = params["http_verb"],
        headers = {
            Authorization = authorization,
            Date = params["Date"],
        },
    }

    local query = {}
    query["seqno"] = seqNo
    params_table["query"] = query

    local httpc,errcode = create_httpc(self.server, self.data_port)
    if not httpc then
        ngx.log(ngx.ERR, "failed to get resty.http ")
        return false, errcode
    end

    local res, err = httpc:request(params_table)
    if err or not res then
        ngx.log(ngx.ERR, "http request failed. err="..err)
        return false, "2004"
    end

    if 404 == res.status then
        ngx.log(ngx.ERR, "http response return 404, maybe remote object is removed")
        httpc:set_keepalive()
        return false, "0026"
    elseif 200 ~= res.status then
        ngx.log(ngx.ERR, "http response has no body or code="..res.status)
        httpc:set_keepalive()
        return false, "2004"
    end

    ngx.log(ngx.INFO, "head remote obj, request_uri="..params["request_uri"]..", res="..inspect(res))
    --[[ 
    local size = tonumber(res.headers["Content_Length"])
    assert(size)
    local part_size = tonumber(res.headers["part_size"])]]

    httpc:set_keepalive()
    return true, "0000", res.headers
end

function _M.sync_put_obj(self, accessid, seqNo, user_info, bucket, obj, part_info, ubo, create_time)
    local uri_obj = ngx.escape_uri(obj)
    local params = {
        http_verb = "GET",
        accessid = accessid,
        secretkey = user_info["info:secretkey"],
        Date = ngx.http_time(ngx.time()), 
        request_uri = "/" .. bucket .. "/" .. uri_obj,
    }

    local ok, authorization = calc_awsv2_signature(params)
    if not ok or not authorization then
        ngx.log(ngx.ERR, "calc_awsv2_signature failed")
        return false
    end

    local params_table = {
        path = params["request_uri"],
        method = params["http_verb"],
        headers = {
            Authorization = authorization,
            Date = params["Date"],
            Range= part_info and part_info["range"]
        },
    }

    local query = {}
    if not part_info then
        query["prepend-metadata"] = true
    end
    query["seqno"] = seqNo
    params_table["query"] = query

    local httpc,errcode = create_httpc(self.server, self.data_port)
    if not httpc then
        ngx.log(ngx.ERR, "failed to get resty.http for ")
        return false, errcode
    end

    local res, err = httpc:request(params_table)
    if err or not res then
        ngx.log(ngx.ERR, "http request failed. err="..err)
        return false, "2004"
    end

    local ok = false
    local code = nil

    if 404 == res.status then
        ngx.log(ngx.ERR, "http response return 404, maybe remote object is removed: " .. params_table["path"])
        httpc:set_keepalive()
        return true
    end

    if not res.has_body or (200 ~= res.status and 206 ~= res.status)then
        ngx.log(ngx.ERR, "http response has no body or code="..res.status)
        httpc:set_keepalive()
        return false, "2004"
    end

    ngx.log(ngx.INFO, "fetch remote obj, request_uri="..params["request_uri"]..", res="..inspect(res))
    local headers = res.headers
    local clusterid = user_info["clusterid"]
    headers["Proxy-Object-SeqNo"] = headers["Proxy-Object-SeqNo"] or seqNo
    local stat_code, inner_code, ok, etag
    if part_info then
        local uploadId = part_info["uploadId"]
        local partNumber = part_info["partNumber"]
        stat_code, inner_code, ok, etag = sp_s3op.put_part_obj(metadb, headers, ubo, res.body_reader, uploadId, partNumber)
    else
        stat_code, inner_code, ok, etag = sp_s3op.put_obj(metadb, headers, clusterid, ubo, seqNo, create_time, res.body_reader, false, true)
    end
    if not ok then
        -- stat_code  delete obj
        -- inner_code check obj seqno
        if "0026" == stat_code  or "1121" == inner_code then
            ngx.log(ngx.ERR, "sync put operation is outdated, obj="..obj)
            httpc:set_keepalive()
            return true
        else
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "put data failed: code="..stat_code..", ".."inner_code")
            httpc:set_keepalive()
            return false, stat_code
        end
    end

    httpc:set_keepalive()
    return ok, code, etag
end

function _M.lock_holding(lock_info)
    local start = lock_info.start
    local key = lock_info.key
    local sig = lock_info.signature
    local ttl = lock_info.ttl
    local now = ngx_now()

    if now - start < ttl/2 then
        return true
    end

    local ok,ret = lock_mgr.extend_lock(key, sig, ttl)
    if not ok then
        ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ", string.format("extend lock failed key=%s,sig=%s,ret=%s", key, sig, ret))
    end

    lock_info.start = now

    return ok, ret
end

function _M.sync_multiput_obj(self, accessid, seqNo, user_info, bucket, obj, lock_info, ubo, create_time)
    local ok, code, headers = self:head_remote_obj(accessid, seqNo, user_info, bucket, obj)
    if not ok then
        if "0026" == code then
            ngx.log(ngx.ERR, "head obj operation is outdated, obj="..obj)
            return true
        end
        ngx.log(ngx.ERR, "http response has no body or code="..code)
        return false, code
    end
    
    local uploadId = headers["Proxy-Sync-Uploadid"]

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", " uploadId =", uploadId)
    local req_info = {
        user_info = user_info,
        auth_info = {
            accessid = accessid,
        },
        args = {
            uploadId = uploadId,
        },
    }

    local code, incode, ok, finished_parts = sp_s3op.list_finished_parts(metadb, headers, bucket, obj, req_info)
    if not ok and "4005" ~= code then
        ngx.log(ngx.ERR, "list_finished_parts failed: code="..code..", incode="..incode)
        return false, code
    end

    if "4005" == code then
        --1. init multupload
        code, incode, ok = sp_s3op.create_temp_obj(metadb, headers, ubo, uploadId)
        if not ok then
            ngx.log(ngx.ERR, string.format("new_uploadid failed, code=%s, incode=%s", code, incode))
            return false, code
        end
    end

    --2. loop upload slice
    local total_size = tonumber(headers["Content_Length"])
    local psize = tonumber(headers["Proxy-Sync-Partsize"])
    local pstart = 0
    local pend = 0
    local etag_array = {}
    local id = 1

    while pstart < total_size  do
        local ok, err = self.lock_holding(lock_info)
        if not ok then
            ngx.log(ngx.ERR, "extend_lock failed, err=" .. err .. ", lock_key=" .. lock_info.key)
            return false, err
        end

        pend = math.min(total_size, pstart + psize) - 1 

        local ok, code, etag
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", " uploadId =", uploadId)
        local part = finished_parts[uploadId.."_"..id]
        etag = part and part["info:etag"] and str.to_hex(part["info:etag"]) 

        if not etag then
            local part_info = {
                range = string.format("bytes=%d-%d", pstart, pend),
                uploadId = uploadId,
                partNumber = id,
            }
            ok, code, etag = self:sync_put_obj(accessid, seqNo, user_info, bucket, obj, part_info, ubo, timestamp)
            if not ok then
                if "0026" == code  then
                    ngx.log(ngx.WARN, "fetch_remote_obj_data obj operation is outdated, obj="..obj)
                    return true
                end
                ngx.log(ngx.ERR, "http response has no body or code="..code)
                return false, code
            else
                if not etag then
                    ngx.log(ngx.WARN, "sync put obj return ok, but fetch_remote_obj_data obj operation is outdated, obj="..obj)
                    return true
                end
            end
        end

        etag_array[id] = '"'..etag..'"'
        id = id + 1
        pstart = pend + 1
    end
    --3. complete upload
    local clusterid = user_info["clusterid"]
    local code, incode, ok = sp_s3op.cmu_result(metadb, headers, clusterid, ubo, seqNo, create_time, etag_array, uploadId, true)
    if not ok then
        -- stat_code  delete obj
        -- inner_code check obj seqno
        if "0026" == code  or "1121" == incode then
            ngx.log(ngx.WARN, "sync put operation is outdated, obj="..obj)
            return true
        end
        return false, code
    end

    return true
end

function _M.sync_put_dir(self, accessid, seqNo, user_info, bucket, obj, part_info, ubo, create_time)
    local headers = {}
    headers["content-length"] ="0"
    local stat_code, inner_code, ok, etag = sp_s3op.put_obj(metadb, headers, clusterid, ubo, seqNo, create_time, nil, false, true)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "put data failed: code="..stat_code..", ".."inner_code")
        return false, stat_code
    end

    return ok
end

-- params:
--     sharding_id  : data log sharding id
--     log_marker   : marker in data log
--     log_counts   : max logs for each fetch
-- return:
--     ok           : fetching logs successful return true, otherwise return false 
--     logs_table   : fetching logs stored in talbe
function _M.fetch_remote_logs(self, log_type, sharding_id, log_marker, log_counts)
    ngx.log(ngx.INFO, "fetch logs, id="..sharding_id..", marker="..log_marker)
    local params = {
        http_verb = "GET",
        accessid = root_accid,
        secretkey = root_seckey,
        Date = ngx.http_time(ngx.time()), 
        request_uri = "/admin/log",
    }

    local ok, authorization = calc_awsv2_signature(params)
    if not ok or not authorization then
        ngx.log(ngx.ERR, "calc_awsv2_signature failed")
        return false
    end

    local params_table = {
        path = params["request_uri"],
        method = params["http_verb"],
        headers = {
            Authorization = authorization,
            Date = params["Date"],
        },
        query = {
            id = sharding_id,
            marker = log_marker,
            max_logs = log_counts,
            clusterid = sync_cfg["clusterid"],
        },
    }

    params_table.query["type"] = log_type

    local httpc,errcode = create_httpc(self.server, self.log_port)
    if not httpc then
        ngx.log(ngx.ERR, "failed to get resty.http errcode="..errcode)
        return false
    end

    local res, err = httpc:request(params_table)
    if err or not res then
        ngx.log(ngx.ERR, "http request failed. err="..err)
        return false
    end
   
    local ok = false
    local logs_table = nil

    repeat
        if 200 ~= res.status then
            ngx.log(ngx.ERR, "http response code not 200, status="..res.status)
            break
        end

        local rbody = res:read_body()
        if not rbody then
            ngx.log(ngx.ERR, "read_body return nil")
            break
        end

        ok, logs_table = pcall(json.decode, rbody)
        if not ok or "table" ~= type(logs_table) then
            ngx.log(ngx.ERR, "Response body error when invoke json.decode. body=", inspect(rbody))
            break
        end

        ngx.log(ngx.INFO, "fetch logs, logs_table="..inspect(logs_table))

    until true

    httpc:set_keepalive()
    return ok, logs_table
end

function _M.check_local_seqno(self, obj_key, remote_seqno)
    local obj_columns = {
        "info:file",
        "info:dir",
        "ver:seq",
    }
    
    local code,ok,result = metadb:get("object", obj_key, obj_columns)
    if not ok then
        return false
    end
    if nil == next(result) then
        -- file or dir doesn't exist
        -- need sync
        return true,true
    end
    if nil == result["info:dir"] then
        -- is file
        local seq = result["ver:seq"]
        if remote_seqno > seq then
            -- need sync
            return true,true
        end
    end

    -- info:dir is true, no need sync
    -- remote_seqno less-than or equal local seq, no need sync
    return true,false
end

function _M.process_datalog(self, datalog, lock_info)
    local op_type = datalog["type"]
    local accessid = datalog["user"]
    local bucket = datalog["bucket"]
    local objectname = datalog["obj"]
    local clusterid = datalog["clusterid"]
    local cur_key = datalog["marker"]
    local object_vertag = datalog["vertag"]
    local timestamp = datalog["timestamp"]
    local dir = datalog["dir"]

    local columns = {"info:uid", "info:type", "info:secretkey", "info:displayname", "ver:tag"}
    local status, ok, user_info = metadb:get("user", accessid, columns)
    if not ok then
        ngx.log(ngx.ERR, "get user_info failed, stop sync opertions!!! error status is:" .. status)
        return false
    elseif not user_info or not next(user_info) then
        ngx.log(ngx.WARN, "get user info nonexist accessid=" .. accessid)
        return true
    end
    --sync op for obj need clusterid
    user_info["clusterid"] = clusterid

    -- get bucket info from metadb;
    local buck_key  = user_info["ver:tag"].."_"..bucket
    local errCode,succ,buck = metadb:get("bucket", buck_key, {"ver:tag"})
    if not succ then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to get bucket from metadb. buck_key=", buck_key, " errCode=", errCode)
        return false
    end

    if nil == buck["ver:tag"] then
        ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " bucket ", buck_key, " does not exist")
        return true
    end
    local is_fs_user = false
    local errCode,succ,obj_key,dirkeys = oindex.parse_objname1(buck["ver:tag"], objectname, is_fs_user)
    if not succ then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to parse objectname. objectname=", objectname, " errCode=", errCode)
        return false
    end

    -- ubo_info: user-bucket-object info
    local ubo_info = new_tab(0, 6)

    ubo_info.user_key           = accessid
    ubo_info.user_vtag          = user_info["ver:tag"]
    ubo_info.user_displayname   = user_info["info:displayname"]
    ubo_info.user_uid           = user_info["info:uid"]
    ubo_info.user_type          = user_info["info:type"]

    ubo_info.buck_key           = buck_key
    ubo_info.buck_name          = bucket
    ubo_info.buck_vtag          = buck["ver:tag"]

    ubo_info.obj_key            = obj_key
    ubo_info.obj_name           = objectname
    ubo_info.obj_vtag           = object_vertag
    ubo_info.obj_dirkeys        = dirkeys 

    local seqNo = nil
    if nil == dir  then
        seqNo = datalog["seqno"] --full sync
        if nil == seqNo then
            --datalog sync
            local seqNo_index = string.find(cur_key, "_") + 1
            assert(seqNo_index < #cur_key)
            seqNo = string.sub(cur_key, seqNo_index)
        end
    else
        obj_key = dirkeys[#dirkeys]
    end
    
    local ok,need = self:check_local_seqno(obj_key, seqNo)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " get local obj seqNo failed. obj_key:", obj_key)
        return false
    end

    if not need then
        -- remote_seqno less-than or equal local seq, no need sync
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " remote_seqno less-than or equal local seq, no need sync.")
        return true
    end

    if nil ~= dir and "true" == dir then
        local dir_seqno = datalog["seqno"]
        if nil == dir_seqno then
            local create_time = string.format("%0.3f", ngx.now())
            dir_seqno = utils.gen_seq_no(create_time)
        end
        return self:sync_put_dir(accessid, dir_seqno, user_info, bucket, objectname, nil, ubo_info, timestamp) 
    end
    
    --note: the copy operation involves two objects, it is possible that the source object of the remote cluster changes before the the synchronous
    --copy operation, causing the dest object inconsistent among the clusters, so we put a copy operation as a write operation
    if "put" == op_type or "copy" == op_type then
        ok = self:sync_put_obj(accessid, seqNo, user_info, bucket, objectname, nil, ubo_info, timestamp)
    elseif "multput" == op_type then
        ok = self:sync_multiput_obj(accessid, seqNo, user_info, bucket, objectname, lock_info, ubo_info, timestamp)
    elseif "delete" == op_type then
        local versionid = datalog["versionid"]
        ok = sync_delete_obj(ubo_info, seqNo, clusterid, timestamp, versionid)
    else
        ngx.log(ngx.ERR, "unsupported sync type:" .. op_type)
        return false
    end

    return ok
end


local function fetch_remote(self, req_query)
    ngx.log(ngx.INFO, "fetch remote ")
    local params = {
        http_verb = "GET",
        accessid = root_accid,
        secretkey = root_seckey,
        Date = ngx.http_time(ngx.time()), 
        request_uri = "/admin/log",
    }

    local ok, authorization = calc_awsv2_signature(params)
    if not ok or not authorization then
        ngx.log(ngx.ERR, "calc_awsv2_signature failed")
        return false
    end

    local params_table = {
        path = params["request_uri"],
        method = params["http_verb"],
        headers = {
            Authorization = authorization,
            Date = params["Date"],
        },
        query = req_query
    }

    local httpc,errcode = create_httpc(self.server, self.log_port)
    if not httpc then
        ngx.log(ngx.ERR, "failed to get resty.http errcode="..errcode)
        return false
    end

    local res, err = httpc:request(params_table)
    if err or not res then
        ngx.log(ngx.ERR, "http request failed. err="..err)
        return false
    end
    
    if 200 ~= res.status then
        ngx.log(ngx.ERR, "http response code not 200, status="..res.status)
        return false
    end

    local rbody = res:read_body()
    if not rbody then
        ngx.log(ngx.ERR, "read_body return nil")
        return false
    end
    --1. decode resps body to table
    local ok, rtable = pcall(json.decode, rbody)
    if not ok or "table" ~= type(rtable) then
        ngx.log(ERR, "Response body error when invoke json.decode. body=", inspect(rbody))
        return false
    end

    ngx.log(ngx.INFO, "fetch logs, logs_table="..inspect(rtable))

    return true, rtable
end

local function metadata_decode(src)
    local dest = {}
    local index = 1
    for k, v in pairs(src) do
        local s,e = ngx.re.find(k, ":", "jo")
        local family = string.sub(k, 1, s-1)
        local column = string.sub(k, e+1, -1)
    
        dest[index] = family
        index = index + 1
        dest[index] = column
        index = index + 1
        dest[index] = v
        index = index + 1
    end
    return dest
end

local function put_bucket_mark(self, fullid, accessid, btag)
    local key = fullid.."_"..accessid.."_"..btag
    local code,ok = metadb:put("full_marker", key, {"info", "status", "ready"})
    if not ok then
        return false
    end
    return true
end

local function fetch_remote_user_bucket(self, fullid, usertag, accessid, sync_type, bucket) 
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", " fetch remote user bucket start ")

    -- sync_type: 1 sync all user. but unsync bucket,data.
    --            2 sync user,bucket. but unsync data.
    --            3 sync user,bucket,data.
    --            4 sync specify user. but unsync bucket,data.
    --            5 sync specify user, all bucket. but unsync data.
    --            6 sync specify user, all bucket data.
    --            7 sync specify user, specify bucket. but unsync data.
    --            8 sync specify user, specify bucket all data.
    local query = {
        type = "metadata",
        id = "buckets",
        usertag = usertag
    }
   
    if 7 == sync_type or 8 == sync_type then
        query["bucket"] = bucket
    end

    local ok, res_table = fetch_remote(self, query)
    if not ok then
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", " fetch remote user bucket failed ")
        return false
    end
    
    for i, v in ipairs(res_table) do
        local key = v["key"]
        local value = v["values"]
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", " metadata decode v : ", inspect(value))
        local hbase_body = metadata_decode(value)
        
        local btag = value["ver:tag"]
        
        local code,ok,ret = metadb:checkAndPut("bucket", key, "ver", "tag", nil, hbase_body)
        if not ok and "1121" ~= code then
            return false
        end
        
        --sync data
        if 3 == sync_type or 
            6 == sync_type or
            8 == sync_type then
            local ok = put_bucket_mark(self, fullid, accessid, btag)
            if not ok then
                return false
            end
        end
        
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", " metadata decode: ", inspect(hbase_body))
    end

    return true
end

function _M.fetch_remote_user(self, fullid, sync_type, accessid, bucket) 
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", " fetch remote user start port: ", self.log_port)

    -- sync_type: 1 sync all user. but unsync bucket,data.
    --            2 sync user,bucket. but unsync data.
    --            3 sync user,bucket,data.
    --            4 sync specify user. but unsync bucket,data.
    --            5 sync specify user, all bucket. but unsync data.
    --            6 sync specify user, all bucket data.
    --            7 sync specify user, specify bucket. but unsync data.
    --            8 sync specify user, specify bucket all data.
    
    local query = {
        type = "metadata",
        id = "users",
    }
    if 3 < sync_type then
        -- specify user
        query["accessid"] = accessid
    end
    
    local ok, res_table = fetch_remote(self, query)
    if not ok then
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", " fetch remote user failed ")
        return false
    end
    
    for i, v in ipairs(res_table) do
        local key = v["key"]
        local value = v["values"]
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", " metadata decode v : ", inspect(value))
        local hbase_body = metadata_decode(value)

        local uid = value["info:uid"]
        local usertag = value["ver:tag"]

        local code,ok,ret = metadb:checkAndPut("user", key, "ver", "tag", nil, hbase_body)
        if not ok and "1121" ~= code then
            return false
        end
        local userid_body = {
                            "info", "accessid", key,
        }
        local code,ok,ret = metadb:checkAndPut("userid", uid, "info", "accessid", nil, userid_body)
        if not ok and "1121" ~= code then
            return false
        end
        if 2 == sync_type or 3 == sync_type or
            5 == sync_type or 6 == sync_type or
            7 == sync_type or 8 == sync_type then
            local ok = fetch_remote_user_bucket(self, fullid, usertag, key, sync_type, bucket)
            if not ok then
                return false
            end
        end
        
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", " metadata decode: ", inspect(hbase_body))
    end

    return true
end

function _M.full_fetch_remote_objects(self, marker) 
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", " full fetch remote objects start ")

    local query = {
        type = "metadata",
        id = "objects",
        marker = marker
    }
    
    local ok, res_table = fetch_remote(self, query)
    if not ok then
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", " full  fetch remote objects  failed ")
        return false
    end
    
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", " full fetch remote objects : ", inspect(res_table))

    return true, res_table
end

return _M
