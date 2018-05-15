--[[
存储代理逻辑v1
Author:      杨婷
Mail:        yangting@dnion.com
Version:     1.0
Doc:
Modify：
2016-07-15  杨婷  初始版本
]]
-- collectgarbage()
local _M = {
    _VERSION = '0.01',
}
local ok, new_tab = pcall(require, "table.new")
if not ok or type(new_tab) ~= "function" then
    new_tab = function (narr, nrec) return {} end
end

local ok,utils = pcall(require, "common.utils")
if not ok or not utils then
    error("failed to load common.utils:" .. (utils or "nil"))
end
local mark_event  = utils.mark_event
local dump_events = utils.dump_events

local ok, fs = pcall(require, "storageproxy_fs")
if not ok or not fs then
    error("failed to load storageproxy_fs:" .. (fs or "nil"))
end

local access = require('storageproxy_access')
local accessobj = access.new()

local codelist = require("storageproxy_codelist")
local sp_s3op = require("storageproxy_s3_op")
local sp_conf = require("storageproxy_conf")
local ngxprint = require("printinfo")

local json = require('cjson')
local inspect = require("inspect")

local ok, metadb = pcall(require, "metadb-instance")
if not ok or not metadb or type(metadb) ~= "table" then
    error("failed to load metadb-instance:" .. (metadb or "nil"))
end

--local ok,resty_uuid = pcall(require, "resty.uuid")
--if not ok or not resty_uuid then
--    error("failed to load resty.uuid:" .. (resty_uuid or "nil"))
--end

function _M.SendErrorRespToS3Client(innercode, Resource, RequestId)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### SendErrorRespToS3Client innercode is: ", innercode)

    local incode = "0000"
    if nil ~= innercode then
        incode = innercode
    end
    
    --recode exception log
    ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "proxy error: ",codelist.ProxyError[incode][1])
    
    local errcode = "0017"
    if nil ~= codelist.ProxyError[incode] then
        if nil ~= codelist.ProxyError[incode][1] and '' ~= codelist.ProxyError[incode][1] then
            errcode = codelist.ProxyError[incode][1]
        end
    end
    
    --return error response to S3Client
    local error_body = {}
    local httpcode = 404
    local ok, res_body

    if nil ~= errcode then
        if nil ~= codelist.S3Error[errcode] then
            local s3code = codelist.S3Error[errcode][1]
            local message = codelist.S3Error[errcode][2]
            httpcode = codelist.S3Error[errcode][3]

            res_body = string.format([[<?xml version="1.0" encoding="UTF-8"?><Error><Resource>%s</Resource><Message>%s</Message><Code>%s</Code><RequestId>%s</RequestId></Error>]], Resource, message, s3code, RequestId)
        end
    else
        res_body = "storeproxy exceptional quit"
    end

    if "444" == httpcode then
        ngx.exit(ngx.HTTP_CLOSE)
    end
    
    ngx.header["Content-Type"] = "application/xml"
    ngx.header["x-amz-request-id"] = RequestId
    ngx.header["Content_Length"] = #res_body
    ngx.status = httpcode
    ngx.print(res_body)
    ngx.exit(ngx.OK)
end

local function process_storeproxy(metadb, req_info)
	--if S3_OP.LIST_BUCKETS == req_info["op_info"]["type"] then
    if "LIST_BUCKETS" == sp_conf.get_s3_op(req_info["op_info"]["type"]) then
        -- this is a interface to S3 service op
        return sp_s3op:process_service(metadb, req_info)
    elseif req_info["op_info"]["objectname"] and "" ~= req_info["op_info"]["objectname"] then
        -- this is a interface to S3 object op
        return sp_s3op:process_object(metadb, req_info)
    elseif req_info["op_info"]["bucketname"] and "" ~= req_info["op_info"]["bucketname"] then
        -- this is a interface to S3 bucket op
        return sp_s3op:process_bucket(metadb, req_info)
    else
        --method not supported
        return "0018", "0000", false
    end
end

--存储代理主逻辑
function _M.handleStoreProxy(uri)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### Enter handleStoreProxy, current uri is ", uri)

    mark_event("RequestStart")

    local headers = ngx.req.get_headers()
    local args = ngx.req.get_uri_args()
    local method = ngx.var.request_method

    -- rewrite the request if it comes from goofys;
    local ok, real_uri, proxy_code = fs.rewrite_request(method, uri, args, headers, metadb)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " unsupported operation \"", method, "\" in dir \"/\" or \"/uesrname/\"")
        -- Yuanguo: just return 404 now; for HEAD operation, it is OK;  for other operation, need test!
        ngx.status = 404
        return 
    end
    if ok and code == 2 then
        _M.SendErrorRespToS3Client("0021", "", ngx.ctx.reqid)
    end

    if real_uri then
        uri = real_uri
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " uri is overwritten, new uri: ", uri)
    end

    local s3code, code, innercode
    local req_info
    local proxy_code, ok
    proxy_code, innercode, ok, req_info = accessobj:get_req_info(method, uri, args, headers, metadb)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "get_req_info failed, proxy_code = " .. proxy_code .. "innercode = " .. innercode)
        _M.SendErrorRespToS3Client(proxy_code, "", ngx.ctx.reqid)
    end

    -- ngx.var.userid = req_info["user_info"]["info:uid"] or "-"
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", "userid: " .. req_info["user_info"]["info:uid"])

    mark_event("GotReqInfo")

    --代理身份验证流程处理
    local s3code, innercode, ok = accessobj:access_authentication(req_info)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "s3code is ", s3code, "innercode is", innercode," req_info is \n",inspect(req_info))
        _M.SendErrorRespToS3Client(s3code, "", ngx.ctx.reqid)
    end

    mark_event("Authenticated")
   
    local s3code, innercode, ok = accessobj:verify_permission(req_info)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "s3code is ", s3code, "innercode is", innercode," req_info is \n",inspect(req_info))
        _M.SendErrorRespToS3Client(s3code, "", ngx.ctx.reqid)
    end

    mark_event("Verify_Permission")

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### ######baseinfo is ########")
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  inspect(accessobj.baseinfo))
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### ######AWS_userinfo is ########")
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  inspect(accessobj.AWS_userinfo))
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### #######query_uri_args ###############")
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  inspect(accessobj.query_uri_args))


    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### #######now entering process_storeproxy ###############")
    --代理接口分析处理
    --身份验证通过后，可获得分析处理后的"body\header\args"
    --直接通过accessobj.body/accessobj.header/accessobj.uri_args/accessobj.query_uri_args访问
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "req_info is \n",inspect(req_info))
    local s3innercode, innercode, ok = process_storeproxy(metadb, req_info)

    mark_event("RequestDone")

    local resource
    if not ok then
        --ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "code:" .. code .. " code:" .. innercode, ", s3code is ", s3code)
        if not accessobj.baseinfo["service"] then
            if accessobj.baseinfo["objectname"] then
                resource = "/" .. req_info["op_info"]["bucketname"] .. "/" .. accessobj.baseinfo["objectname"]
            else
                resource = "/" .. req_info["op_info"]["bucketname"]
            end
            _M.SendErrorRespToS3Client(s3innercode, resource, ngx.ctx.reqid)
        end
    end

    dump_events()
    --ngx.exit(0)
end
return _M
