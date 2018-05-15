--[[
storage proxy auth and parse module  
Author:      yangting
Mail:        yangting@dnion.com
Version:     1.0
Doc:
Modify:
    2016-07-19  yangting initial
Modify:
    2016-08-31  chentao refactor
]]

local _M = {
    _VERSION = '0.01',
}

local mt = { __index = _M }
function _M.new(self)
    return setmetatable({}, mt)
end

local utils = require('common.utils')

local json = require('cjson')
local ngxprint = require("printinfo")
local ok,inspect = pcall(require, "inspect")
if not ok or not inspect then
    error("failed to load inspect:" .. (inspect or "nil"))
end

local sp_conf = require("storageproxy_conf")
local ngx_re = require "ngx.re"

-- return timestamp, 0, true
-- return nil, 1, false  -- parse data error
-- return timestamp, 2, false  -- request didn't later current time 
local function _opt_valid_request(auth_type, date, date_name)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### Enter _opt_valid_request/")
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### protocal is ", auth_type, ", date_name is ", date_name, ", date is ", date)
    --按协议进行消息转换，获取消息体时间戳

    local timestamp = ngx.parse_http_time(date)
    if nil == timestamp then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "request data parse error!!! data is :", date)
        return nil, 1, false
    end
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### body timestamp is ", timestamp)
    
    local bodytime = tonumber(timestamp)
    if bodytime <= 0 then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "request data parse error!!! data is :", date)
        return nil, 1, false
    end
    
    if sp_conf.config["AUTH_DIFFTIME"] then
        local nowtime = ngx.time()
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### now timestamp is ", nowtime)
        local difftime = nowtime - bodytime
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### difftime is ", difftime)

        local verifyprotocal = "aws2"
        local allowtimediff = sp_conf.config[verifyprotocal .. "_timediff"] * 60
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### allowtimediff to ", verifyprotocal .. "_timediff is ", allowtimediff)

        if difftime > allowtimediff or difftime < -allowtimediff then
             ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "Current request invalid, s3_", verifyprotocal, "require request didn't later current time than ", allowtimediff, "s")
             return timestamp, 2, false
        end
    end
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### Current request is in valid time gap")
    return timestamp, 0, true
end

-- return 0 true  ok
-- return 1 false date is empty
-- return 2 false date error
-- return 3 false date difference error
local function check_time_info(req_info)
    local headers = req_info["headers"]
    local auth_type = req_info["auth_info"]["type"]
    local date, date_name
    --根据S3要求，只有通过Authorization头进行验证的需要进行时间验证
    if sp_conf.auth("HEADER_V2") == auth_type then
        if nil ~= headers["x-amz-date"] then
            date = headers["x-amz-date"]
            date_name = "x-amz-date"
        elseif nil ~= headers["Date"] then
            date = headers["Date"]
            date_name = "Date"
        else
            --Yuanguo: MissingSecurityHeader, Your request is missing a required header. 400 Bad Request
            return 1, false
        end
        
        -- return timestamp, 0, true
        -- return nil, 1, false  -- parse data error
        -- return timestamp, 2, false  -- request didn't later current time 
        local timestamp,code,ok = _opt_valid_request(auth_type, date, date_name)
        if not ok then
            if 2 == code then
                --Yuanguo: RequestTimeTooSkewed, The difference between the request time and the server's time is too large, 403 Forbidden
                return 3, false
            else -- code 1
                return 2, false
            end
        end
        req_info["timestamp"] = timestamp
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### req_info timestamp is ", timestamp)
    end

    --s3 protocol:When an x-amz-date header is present in a request, the
    --system will ignore any Date header when computing the request signature.
    --Therefore, if you include the x-amz-date header, use the empty string for
    --the Date when constructing the StringToSign
    --todo date and x-amz-date both exsit ,wo shoudl process this case.
    if date_name ~= "x-amz-date" then
        req_info["original_date_name"] = date_name
    end
    return 0,true
end

local function _analyse_sub_resource(args)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### Enter _analyse_sub_resource")

    local sub_resource = {}
    if nil ~= next(args) then
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### args is not nil, process args parameter")
        local op_args = sp_conf.config["sub_resource"]
        local i = 1
        for k, v in pairs(args) do
            if nil ~= op_args[k] then
                if tostring(v) == "true" or tostring(v) == "" then
                    sub_resource[i] = k
                else
                    sub_resource[i] = k.."="..v
                end
                i = i + 1
            end
        end
    end

    table.sort(sub_resource) --sort by alphabet order

    local sub_str = ""

    for i, v in ipairs(sub_resource) do
        if "" == sub_str then
            sub_str = v
        else
            sub_str = sub_str .. "&" .. v
        end
    end

    return sub_str
end

local function _get_CanonicalizedResource(uri, args)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### Enter _get_CanonicalizedResource")

    local CanonicalizedResource = uri or ""

    local sub_resource = _analyse_sub_resource(args) 
    if "" ~= sub_resource then
        CanonicalizedResource = CanonicalizedResource .. "?" .. sub_resource
    end
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", "Resource is ",inspect(CanonicalizedResource))
    return CanonicalizedResource
end

local function _header_format(headerinfo, header)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### Enter _header_format----", header)
    local format_header
    if "table" == type(headerinfo) then
        local tmpstr = headerinfo[1]
        for i,v in pairs(headerinfo) do
            if 1 ~=i then
                tmpstr = tmpstr .. v
            end
            if i ~= #headerinfo then
                tmpstr = tmpstr .. ","
            end
        end
        format_header = header .. ":" .. tmpstr
    else
        format_header = header ..":"..headerinfo
    end

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### This format_header is ", format_header)
    return format_header
end

local function _get_AWS2_CanonicalizedAmzHeaders(headers)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### Enter _get_AWS2_CanonicalizedAmzHeaders")
    -- since we use ngx.req.get_headers to get header, this api has convert every header to lowercase
    -- and combine same header's value to one table
    local tmptable = {}
    local headertable = {}
    for k,v in pairs(headers) do
        if type(v) == "table" then
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### ", k, ": ", table.concat(v, ", "))
        else
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### ", k, ": ", v)
        end
        if ngx.re.find(k, "^x-amz-\\S") then
            table.insert(headertable, k)
            if string.lower(k) == "x-amz-copy-source" then
                tmptable[k] = headers["root-x-amz-copy-source"] or v
            else
                tmptable[k] = v
            end
        end
    end

    if nil == next(headertable) then
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "current quest didn't have any header to match \"x-amz-\" ")
        return "" 
    end

    table.sort(headertable)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "headertable is ", inspect(headertable))

    local CanonicalizedAmzHeaders = _header_format(tmptable[headertable[1]], headertable[1])
    for i,v in ipairs(headertable) do
        if 1 ~= i then
            CanonicalizedAmzHeaders = CanonicalizedAmzHeaders .. _header_format(tmptable[v], v)
        end
        CanonicalizedAmzHeaders = CanonicalizedAmzHeaders .. '\n'
    end

    return CanonicalizedAmzHeaders
end

local function _get_AWS2_md5(req_info)
    local content_md5 = req_info["headers"]["Content-MD5"] or ""
    return content_md5 .. "\n"
end

local function _get_content_type(req_info)
    local content_md5 = req_info["headers"]["Content-Type"] or ""
    return content_md5 .. "\n"
end

local function _get_date(req_info)
    local date = ""
    if sp_conf.auth("ARGS_V2") == req_info["auth_info"]["type"] then
        date = req_info["auth_info"]["Expires"]
    elseif nil ~= req_info["original_date_name"] then
        date = req_info["headers"][req_info["original_date_name"]]
    end

    return date .. "\n"
end

local function _get_secretkey(req_info)
    if not req_info["user_info"] or not req_info["user_info"]["info:secretkey"] then
        ngx.log(ngx.ERR, "user_info or info:secretkey is nil")
        return false, nil
    end

    local secretkey = req_info["user_info"]["info:secretkey"]

    if req_info["headers"] and req_info["headers"]["root_secretkey"] then
        secretkey = req_info["headers"]["root_secretkey"]
    end

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### secretkey is ", secretkey)

    return true, secretkey
end

local function _get_StringToSign(req_info)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### Enter _get_StringToSign")

    local http_verb = req_info["method"]
    if not http_verb then
        ngx.log(ngx.ERR, "http_verb is nil")
        return false, nil
    end

    local headers = req_info["headers"]
    if not headers or not next(headers) then
        ngx.log(ngx.ERR, "headers is nil")
        return false, nil
    end

    local cont_md5 = headers["content-md5"] or ""

    local cont_type = headers["content-type"] or ""

    local date = headers["Date"] or headers["x-amz-date"]
    if not date then
        ngx.log(ngx.ERR, "Date is nil")
        return false, nil
    end

    local canonicalizedAmzHeaders = _get_AWS2_CanonicalizedAmzHeaders(headers)

    local args = req_info["args"]
    if not args then
        ngx.log(ngx.ERR, "args is nil")
        return false, nil
    end

    local origin_uri = headers["root_uri"] or req_info["uri"]
    local canonicalizedResource = _get_CanonicalizedResource(origin_uri, args)

    local stringToSign = http_verb .. "\n" ..
        cont_md5 .. "\n" ..
        cont_type .. "\n" ..
        date .. "\n" ..
        canonicalizedAmzHeaders ..
        canonicalizedResource

    ngx.log(ngx.DEBUG, "stringToSign is " .. stringToSign)
    return true, stringToSign
end

local function calc_signature(req_info)
    local ok, stringToSign = _get_StringToSign(req_info)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "_get_StringToSign failed!")
        return false, "0010"
    end 
    
    local ok, secretkey = _get_secretkey(req_info)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "_get_secretkey failed!")
        return false, "0011"
    end 

    local signature = ngx.encode_base64(ngx.hmac_sha1(secretkey, stringToSign))

    return true, "0000", signature 
end

function _M.authorize(req_info)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### Enter authorize")

    -- return 0 true  ok
    -- return 1 false date is empty
    -- return 2 false date error
    -- return 3 false date difference error
    local code, ok = check_time_info(req_info)
    if not ok then 
        if 1 == code then 
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "request date is nil!")
            return "0021", "0000", false
        elseif 3 == code then 
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "request date difference error")
            return "1001", "0000", false
        else --code 2
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "request date is error!")
            return "0021", "0000", false
        end  
    end
 
    local ok, code, signature = calc_signature(req_info)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "calc_signature failed!")
        return code, "0000", false
    end 

    local req_signature = req_info["auth_info"]["signature"]

    if signature ~= req_signature then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  string.format("##### signature unmatch: %s, %s", req_signature, signature))
        return "1002", "0000", false
    end

    return "0000","0000", true
end

function _M.generate_AuthString(req_info)
    local ok, code, signature = calc_signature(req_info)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "calc_signature failed, code:" .. code)
        return false, nil
    end 

    if not req_info["auth_info"] or not req_info["auth_info"]["accessid"] then
        ngx.log(ngx.ERR, "auth_info or accessid is nil")
        return false, nil
    end

    local accessid = req_info["auth_info"]["accessid"]

    local authorization = "AWS" .. " " .. accessid .. ":" .. signature

    return true, authorization
end

return _M
