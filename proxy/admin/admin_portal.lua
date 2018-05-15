local json = require('cjson')
local codelist = require('storageproxy_codelist')
local inspect = require("inspect")
local parser = require('admin_parse')
local auth = require('v2auth')
local op = require('admin_op')

local function reply_err(innercode, Resource, RequestId)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "#####SendErrorResp innercode is: ", innercode)

    local incode = innercode or "0000"
    
    ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "proxy error: ", codelist.ProxyError[incode][1])
    
    local errcode = "0017"
    if nil ~= codelist.ProxyError[incode] then
        if nil ~= codelist.ProxyError[incode][1] and '' ~= codelist.ProxyError[incode][1] then
            errcode = codelist.ProxyError[incode][1]
        end
    end
    
    --return error response to S3Client
    local httpcode = 404
    local res_body = nil
    local j_res_body = "\"error\":\"admin exceptional quit\""

    if nil ~= errcode then
        if nil ~= codelist.S3Error[errcode] then
            local s3code = codelist.S3Error[errcode][1]
            local message = codelist.S3Error[errcode][2]
            httpcode = codelist.S3Error[errcode][3]
            res_body = {}
            
            res_body["Code"] = s3code
            res_body["Message"] = message
            res_body["Resource"] = Resource
            res_body["RequestId"] = RequestId
            j_res_body = json.encode(res_body)     
        end
    end
    
    ngx.header["Content-Type"] = "application/json"
    ngx.header["x-amz-request-id"] = RequestId
    ngx.header["Content_Length"] = #j_res_body
    ngx.status = httpcode
    ngx.print(j_res_body)
    ngx.exit(ngx.OK)
end

local function handleadmin()
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### Enter handleadmin, current uri is ", ngx.var.uri)

    local method = ngx.var.request_method
    local uri = ngx.var.uri
    local headers = ngx.req.get_headers()
    local args = ngx.req.get_uri_args()

    local proxy_code, innercode, ok, req_info = parser.parse(method, uri, headers, args)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "get_req_info failed, proxy_code = " .. proxy_code .. "innercode = " .. innercode)
        reply_err(proxy_code, "", ngx.ctx.reqid)
        return
    end

    ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " ",  "req_info is",inspect(req_info))
    
    local proxy_code, innercode, ok = auth.authorize(req_info)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "proxy_code is ", proxy_code, "innercode is", innercode," req_info is \n",inspect(req_info))
        reply_err(proxy_code, "", ngx.ctx.reqid)
        return
    end

    local proxy_code, innercode, ok = op.process(req_info)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "proxy_code is ", proxy_code, "innercode is", innercode," req_info is \n",inspect(req_info))
        reply_err(proxy_code, "", ngx.ctx.reqid)
        return
    end
end

--admin op entry
handleadmin()
