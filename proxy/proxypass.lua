local ok, new_tab = pcall(require, "table.new")
if not ok or type(new_tab) ~= "function" then 
    new_tab = function (narr, nrec) return {} end
end

local ok,utils = pcall(require, "common.utils")
if not ok or not utils then
    error("failed to load common.utils:" .. (utils or "nil"))
end

local ok,md5 = pcall(require, "resty.md5")
if not ok or not md5 then
    error("failed to load resty.md5:" .. (md5 or "nil"))
end

local ok,uuid = pcall(require, "resty.uuid")
if not ok or not uuid then
    error("failed to load resty.uuid:" .. (uuid or "nil"))
end

local ok,objstore = pcall(require, "storageproxy_objstore")
if not ok or not objstore then
    error("failed to load storageproxy_objstore:" .. (objstore or "nil"))
end
local mark_event  = utils.mark_event
local dump_events = utils.dump_events
local ngx_re = require "ngx.re"
local codelist = require("storageproxy_codelist")
local sp_s3op = require("storageproxy_s3_op")
local sp_conf = require("storageproxy_conf")
local sync_cfg = sp_conf.config["sync_config"]
local ngx_re_gsub = ngx.re.gsub

local inspect = require("inspect")
local stringify   = utils.stringify
local gen_seq_no  = utils.gen_seq_no
local restystr = require "resty.string"
local lock_ttl = 600
local ngx_md5 = ngx.md5
local ngx_now = ngx.now

local ok, metadb = pcall(require, "metadb-instance")
if not ok or not metadb or type(metadb) ~= "table" then
    error("failed to load metadb-instance:" .. (metadb or "nil"))
end

local ok, oindex = pcall(require, "storageproxy_oindex")
if not ok or not oindex or type(oindex) ~= "table" then
    error("failed to load storageproxy_oindex:" .. (oindex or "nil"))
end

local ok, http = pcall(require, "resty.http")
if not ok or not http then
    error("failed to load resty.http:" .. (http or "nil"))
end

local ok, lock_mgr = pcall(require, "common.lock")
if not ok or not lock_mgr then
    error("failed to load common.lock:" .. (lock_mgr or ""))
end

local function sendErrorResp(proxy_code)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### SendErrorRespToS3Client proxy_code is: ", proxy_code)

    local incode = proxy_code or "0000"
    
    local errcode = "0017"
    if codelist.ProxyError[incode] and codelist.ProxyError[incode][1] then
        errcode = codelist.ProxyError[incode][1]
    end 
    
    local httpcode = 500 
    if codelist.S3Error[errcode] and codelist.S3Error[errcode][3] then
        httpcode = tonumber(codelist.S3Error[errcode][3])
    end

    ngx.exit(httpcode)
end

local function repair_insertobj(metadb, ubo, etag, size, meta)
    local etaghex = restystr.to_hex(etag) or "notknown"
    local create_time = meta["info:time"] or string.format("%0.3f", ngx.now()) 
    local seq_no = meta["ver:seq"] or gen_seq_no(create_time) 
    local vtag = meta["ver:tag"]
    local loc = meta["mfest:loc"]
    local psize = meta["mfest:psize"]
    local chunksize = sp_conf.config["chunk_config"]["size"]

    local hbase_body =
    {
        "info",   "file",         "true",
        "info",   "oname",        ubo.obj_name,
        "info",   "bucket",       ubo.buck_key,
        "info",   "time",         create_time,
        "info",   "size",         size,
        "info",   "hsize",        0,
        "info",   "etag",         etaghex,
        "ver",    "tag",          vtag,
        "ver",    "seq",          seq_no,
        "mfest",  "loc",          loc,
        "mfest",  "psize",        psize,
        "mfest",  "chksize",      chunksize,
    }

    if nil ~= ubo.infolink then
        local index = #hbase_body
        hbase_body[index+1] = "info"
        hbase_body[index+2] = "link"
        hbase_body[index+3] = ubo.infolink
    end

    local clusterid = sync_cfg["clusterid"]

    local datalog_body =
    {
        "info",   "clusterid",   clusterid,
        "info",   "user",        ubo.user_key,
        "info",   "bucket",      ubo.buck_name,
        "info",   "obj",         ubo.obj_name,
        "info",   "type",        "put",
        "info",   "timestamp",   create_time,
        "info",   "vertag",      ubo.obj_vtag,
    }

    local code, incode, ok = sp_s3op.insert_object(metadb, ubo, seq_no, create_time, size, hbase_body, datalog_body)
    if not ok then
        if "1121" == incode then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " insert_object failed because of a newer concurrent competitive write code=", code, " incode=", incode)
            return "0000", "0000", true, etaghex
        end
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " insert_object failed, code=", code, " incode=", incode)
        return code, incode, ok
    end
end

-- params:
--     headers    : the request headers
--     ubo        : user-buck-object info
-- return:
--     proxy_code, inner_code, ok
local function get_and_repair_obj(method, headers, ubo)
    local bucketname = ubo.buck_name
    local objectname = ubo.obj_name
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "Enter get_and_repair_obj: bucket=" .. bucketname .. ",object=" .. objectname)

    local obj_columns =                                                                             
    {                                                                                             
        "info:file",                                                                              
        "info:oname",                                                                             
        "info:bucket",                                                                            
        "info:time",                                                                              
        "info:size",                                                                              
        "info:hsize",                                                                             
        "info:etag",                                                                              
        "info:lock",                                                                              
        "info:link",                                                                              
        "ver:tag",                                                                                
        "ver:seq",                                                                                
        "mfest:loc",                                                                              
        "mfest:psize",                                                                            
        "mfest:chksize",                                                                          
        "hdata:data",                                                                             
        "sattrs:json",                                                                            
    }

    local code, ok, res = metadb:get("mergedobject", objectname, obj_columns)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to get object from metadb. obj_key=", objectname)
        return "3002", stat, false
    end

    if nil == res or nil == res["ver:tag"] then
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " object ", objectname, " does not exist!")
        ngx.status = 404
        return "0000", "0000", true
    end

    local size = tonumber(res["info:size"])
    if "HEAD" == method then
        ngx.header["Content-Length"] = size
        ngx.status = 200
        return "0000", "0000", true
    end
    local hsize = tonumber(res["info:hsize"])
    if hsize and hsize > 0 then
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, "repaired object ", objectname, " should not have hdata, so we treat it as nonexistent!")
        return "3001","0000",false
    end

    local etag = res["info:etag"]
    local objtag = res["ver:tag"]
    ubo.obj_vtag = objtag
    local psize = tonumber(res["mfest:psize"])
    local range_str = headers["range"]
    
    local ceph_objtag = objtag
    local ceph_objname = objectname
    local link = res["info:link"]
    if nil ~= link then
        ubo.infolink = link
        local s,e = string.find(link, "_")
        ceph_objtag  = string.sub(link, 1, s-1) 
        ceph_objname = string.sub(link, e+1, -1)
    end

    local ok, partial, posStart, posEnd = sp_s3op.parse_range(size, range_str)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " request range is invalid, range=", range_str)
        return "0019", "0000", false
    end

    local read = 0  -- how many bytes we have ready read?
    local toRead = posEnd - posStart + 1
    ngx.header["Content-Length"] = toRead
    
    local calc_md5 = true
    if partial then
        ngx.status = 206
        calc_md5 = false  --we check md5 only when this is not a range request;
        ngx.header["Content-Range"] = "bytes "..tostring(posStart-1).."-"..tostring(posEnd-1).."/"..tostring(size)
    end

    local md5sum = nil
    if calc_md5 then
        md5sum = md5:new()
    end

    local chunksize = sp_conf.config["chunk_config"]["size"]
    local concurrent = sp_conf.config["chunk_config"]["rconcurrent"]
    local objs = sp_s3op.calc_objs(0, psize, chunksize, posStart, posEnd, ceph_objtag)
    ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ceph_objtag: ", ceph_objtag)
    
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ceph objects to read: ", stringify(objs))
    if nil ~= next(objs) then
        local ceph = objstore:get_store(res["mfest:loc"])
        assert(ceph)

        local batch = {}
        local bindex = 1

        for i=1,#objs do
            batch[bindex] = objs[i]
            bindex = bindex + 1

            if i%concurrent == 0 or i == #objs then
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " reading objects from ceph: ", stringify(batch))
                local code,ok,res = ceph:get_obj(ceph_objname, batch)
                if not ok or "table" ~= type(res) or #res ~= bindex-1 then
                    ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " Error occurred when reading objects from ceph. code=", code, " res=", stringify(res))
                    return "3009", code, false
                end

                ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " reading objects from ceph succeeded.")

                for k=1, #res do
                    local len = string.len(res[k])
                    read = read + len
                    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " read=", read, " len=", len)
                    ngx.print(res[k])
                    local ok,err = ngx.flush(true)
                    if not ok then
                        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ngx print failed. err=", err)
                        return "3019", "0000", false
                    end
                    if calc_md5 then
                        md5sum:update(res[k])
                    end
                end

                batch = {}
                bindex = 1
            end
        end
    end

    if toRead ~= read then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " size read (", read, ") does not match size requested (", toRead, ")")
        return "3010", "0000", false
    end

    if calc_md5 then
        local etag_new = md5sum:final()
        md5sum:reset()
        if etag and etag ~= etag_new then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " etag we've calculated '", etag_new, "' does not match info:etag '", etag, "'")
            return "3011", "0000", false
        end
    end
    
    repair_insertobj(metadb, ubo, etag, size, res)

    return "0000", "0000", true
end

local function preprocess_request(method, uri, args, headers)
    if "HEAD" ~= method and "GET" ~= method then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "method not allowed: " .. method)
        return "0018", "0000", false 
    end
    --analyse uri to get bucketname, objectname
    local objectname = ""
    local bucketname = ""

    --eg. uri is /xxx//yyy/zzz/ or xxx/yyy///zzz, then first = xxx, second = yyy/zzz
    local splittab, err = ngx_re.split(uri, "/")
    for i,v in ipairs(splittab) do
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", "##### uri split to ", i .. ":" .. v)
        if "" == bucketname then
            bucketname = v
        elseif "" == objectname then
            objectname = v
        else
            objectname = objectname .. "/" .. v
        end
    end

    if "" == bucketname or "" == objectname then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "uri does not have valid bucketname and objectname: " .. uri)
        return "0023", "0000", false 
    end

    local accessid = headers["accessid"]
    if not accessid then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "headers does not have Accessid.")
        return "0023", "0000", false 
    end
    
    local status, ok, user_info = metadb:get("user", accessid, {"ver:tag"})
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "get user_info from hbase failed: " .. status)
        return "0011", status, false
    elseif not user_info or not next(user_info) then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "user accessid  non-exist: " .. accessid)
        return "1004", status, false
    end

    -- get bucket info from metadb;
    local buck_key  = user_info["ver:tag"].."_"..bucketname
    local errCode, succ, buck = metadb:get("bucket", buck_key, {"ver:tag"})
    if not succ then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to get bucket from metadb. buck_key=", buck_key, " errCode=", errCode)
        return "2004", errCode, false
    end

    if nil == buck["ver:tag"] then
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " bucket ", buck_key, " does not exist")
        return "0012","0000",false
    end

    local obj_key = buck["ver:tag"] .. "_/" .. objectname

    local errCode, succ, obj_key, dirkeys = oindex.parse_objname1(buck["ver:tag"], objectname, "fs")
    if not succ then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to parse objectname. objectname=", objectname, " errCode=", errCode)
        return "3017", errCode, false
    end

    local ubo_info = new_tab(0, 6)

    ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " req_info is ", inspect(req_info))

    ubo_info.user_key           = accessid 
    ubo_info.buck_key           = buck_key
    ubo_info.buck_name          = bucketname
    ubo_info.buck_vtag          = buck["ver:tag"]

    ubo_info.obj_key            = obj_key
    ubo_info.obj_name           = objectname
    ubo_info.obj_dirkeys        = dirkeys

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "ubo_info is \n",inspect(ubo_info))

    return "0000", "0000", true, ubo_info
end

local function lock_holding(lock_info)
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

local function handle_response(lock_info, body_reader)
    return function(max_chunk_size)
        local ok, err = lock_holding(lock_info)
        if not ok then
            ngx.log(ngx.ERR, "extend_lock failed, err=" .. err .. ", lock_key=" .. lock_info.key)
            return function(max_chunk_size)
                return nil
            end
        end

        return body_reader(max_chunk_size)
    end
end

local function proxy_store_obj(upstream, uri, headers, ubo)
    local lock_key = ngx_md5(uri) .. "_proxystore_lock"
    local ok, signature = lock_mgr.lock(lock_key, lock_ttl)
    if not ok or not signature then
        ngx.log(ngx.ERR, "lock failed, maybe an store process already exists. lock_key=" .. lock_key .. ", uri=" .. uri)
        return false
    end
    
    ngx.log(ngx.ERR, "proxy store lock success. lock_key=" .. lock_key .. ",signature=" .. signature .. ",uri = " .. uri)
    local lock_info = {
        start = ngx_now(),
        key = lock_key,
        signature = signature,
        ttl = lock_ttl,
    }
    local httpc = http.new()

    httpc:set_timeout(500)
    local ok, err = httpc:connect(upstream, 80)
    if not ok then
        ngx.log(ngx.ERR, err)
        lock_mgr.unlock(lock_key, signature)
        return false
    end

    local new_headers = {}
    for k, v in pairs(headers) do
        if "range" ~= k then
            new_headers[k] = v 
        end 
    end 
    local params_table = {
        method = "GET",
        path = ngx_re_gsub(uri, "\\s", "%20", "jo"),
        headers = new_headers,
    }

    httpc:set_timeout(2000)
    local res, err = httpc:request(params_table)
    if err or not res then
        ngx.log(ngx.ERR, "http request failed. err="..err)
        lock_mgr.unlock(lock_key, signature)
        return false, "2004"
    end

    if not res.has_body or (200 ~= res.status and 206 ~= res.status)then
        ngx.log(ngx.ERR, "http response has no body or code="..res.status)
        httpc:set_keepalive()
        lock_mgr.unlock(lock_key, signature)
        return false, "2004"
    end

    local headers = res.headers
    local clusterid = sync_cfg["clusterid"]
    local create_time = string.format("%0.3f", ngx.now())
    local seq_no = gen_seq_no(create_time)
    ubo.obj_vtag = uuid()
    ubo.user_type = "fs"
    local body_handle = handle_response(lock_info, res.body_reader) 

    local stat_code, inner_code, ok, etag = sp_s3op.put_obj(metadb, headers, clusterid, ubo, seq_no, create_time, body_handle, true, false)
    if not ok then
        if "0026" == stat_code  or "1121" == inner_code then
            ngx.log(ngx.ERR, "sync put operation is outdated, obj=" .. ubo_info.obj_key)
        else
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "put data failed: code="..stat_code..", ".."inner_code")
        end
    end
    ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "put data: code="..stat_code..", ".."inner_code")
    httpc:set_keepalive()
    lock_mgr.unlock(lock_key, signature)

    return true
end

local store_handler
store_handler = function(premature, upstream, uri, headers, ubo_info)
    if not premature then
        ngx.log(ngx.INFO, os.date("%Y-%m-%d %H:%M:%S", ngx.time()).." : create timer success")
        local ok, err = proxy_store_obj(upstream, uri, headers, ubo_info)
        if not ok then
            ngx.log(ngx.ERR, "proxy_store_obj failed, err:", err)
        end
    end
end

local function async_proxy_store(upstream, uri, headers, ubo_info)
    local ok, err = ngx.timer.at(0, store_handler, upstream, uri, headers, ubo_info)
    if not ok then
        ngx.log(ngx.ERR, "failed to create timer: ", err)
        return
    end
end

local function proxy_pass_obj(upstream)
    local httpc = http.new()

    httpc:set_timeout(500)
    local ok, err = httpc:connect(upstream, 80)
    if not ok then
        ngx.log(ngx.ERR, err)
        return false
    end

    httpc:set_timeout(2000)
    ngx.req.set_header("connection", nil)
    local ok, status = httpc:proxy_response(httpc:proxy_request(), 131072)
    httpc:set_keepalive()
    return ok, status
end

local function get_obj(method, headers, ubo_info)
    if "HEAD" == method then
        local statcode, innercode, ok = sp_s3op.head_obj(metadb, headers, ubo_info)
        if 404 == ngx.status then
            return "3001", "0000", false
        end
        return statcode, innercode, ok
    end

    local statcode, innercode, ok = sp_s3op.get_obj(metadb, headers, ubo_info, false)
    return statcode, innercode, ok
end

local function handleGetObject()
    local headers = ngx.req.get_headers()
    local uri = headers["s3uri"] or ngx.var.uri
    local args = ngx.req.get_uri_args()
    local method = ngx.var.request_method

    ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "##### Enter handleGetObject, current uri is ", uri)

    mark_event("RequestStart")
    local proxy_code, innercode, ok, ubo_info = preprocess_request(method, uri, args, headers)
    if ok then
        proxy_code, innercode, ok = get_obj(method, headers, ubo_info)
        if ok then
            mark_event("RequestDone")
            return
        end
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " get obj failed, objectname=", ubo_info.obj_key, " errCode=", proxy_code)
    end

    local upstream = headers["upstream"]
    if not upstream then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to get obj and no upstream, errCode=", proxy_code)
        mark_event("RequestDone")
        sendErrorResp(proxy_code)
        return
    end

    if "3010" == proxy_code then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to get obj, 3010")
        if headers["store"] then
            async_proxy_store(upstream, ngx.var.uri, headers, ubo_info)
        end
        sendErrorResp(proxy_code)
    else
        local ok, status = proxy_pass_obj(upstream)
        if not ok then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to proxy_pass obj=", ubo_info.obj_key, " status=", status)
            mark_event("RequestDone")
            return
        end
    end
    if headers["store"] then
        async_proxy_store(upstream, ngx.var.uri, headers, ubo_info)
    end

    mark_event("RequestDone")
    dump_events()
end

handleGetObject()
