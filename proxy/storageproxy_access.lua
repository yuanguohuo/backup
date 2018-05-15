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
local s3v4 = require("v4auth.aws.AwsV4Signature")
local ok,inspect = pcall(require, "inspect")
if not ok or not inspect then
    error("failed to load inspect:" .. (inspect or "nil"))
end

local sp_conf = require("storageproxy_conf")
local permission = require("permission")
local ngx_re = require "ngx.re"
local assert = assert
local ipairs = ipairs
_M.query_uri_args = {}
-- original url args, uri_args is the args needed for auth
--保存从hbase获取的用户信息
_M.AWS_userinfo = {}
--保存分析的s3接口相关信息
_M.baseinfo = {
    -- "service"
    -- "bucketname"
    -- "objectname"
    -- "operationtype"

    -- "method"
    -- "uri"
    -- "sub_request_uri"

    -- "headers"
    -- "body"

    -- "api_access_mode"
    -- "is_auth_args"
}

-- get Authorization info
--return 0 ok
--return 1 Authorization is empty
--return 2 Authorization error
local function get_auth_info(headers, args)
    local auth_info = {}
    if nil ~= headers["Authorization"] and "" ~= headers["Authorization"] then   --auth info is in HTTP headers
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "header Authorization is: ", headers["Authorization"])
        --authorization_info is a string

        if sp_conf.config["AWS_v4"] then
            --AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/iam/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7
            --local cp, err = ngx.re.match(headers["Authorization"], "^AWS4-HMAC-SHA256\sCredential=(\\w+)(\\S+),\\s*SignedHeaders=(\\S+),\\s*Signature=(\\S+)", "jo")
            --local cp, err = ngx.re.match(headers["Authorization"], "^AWS4-HMAC-SHA256\\sCredential=([0-9A-Z]+)\\/\\d{8}\\/(\\S+)\\/(\\S+)\\/(\\S+)," .. "SignedHeaders=(((\\S+);)*)(\\S+),Signature=([a-f0-9]+)", "jo")

            local cp, err = ngx.re.match(headers["Authorization"], "^AWS4-HMAC-SHA256\\sCredential=([0-9A-Z]+)\\/(\\d{8})\\/(\\S+)\\/(\\S+)\\/(\\S+),SignedHeaders=(\\S+),Signature=([a-f0-9]+)", "jo")
                ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### authv4 re ")
            if nil ~= cp and nil ~= next(cp) then  --AWS version 4

                ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### authv4 " .. cp[1])
                if nil == cp[1] or nil == cp[2]  or nil == cp[3]  or nil == cp[4] then -- todo
                    return nil, 2, false
                end
                ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### match---accessinfo[\"AWSAccessKeyId\"] is ", cp[1])
                ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### match---accessinfo[\"AWSDateshort\"] is ", cp[2])
                ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### match---accessinfo[\"AWSRegion\"] is ", cp[3])
                ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### match---accessinfo[\"AWSService\"] is ", cp[4])
                ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### match---accessinfo[\"SignedHeaders\"] is ", cp[6])
                ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### match---accessinfo[\"original_signature\"] is ", cp[7])
                -- auth_info["type"] = AWS_AUTH.HEADER_V4
                auth_info["type"] = sp_conf.auth("HEADER_V4")
                auth_info["accessid"] = cp[1]
                auth_info["dateshort"] = cp[2]
                auth_info["region"] = cp[3]
                auth_info["service"] = cp[4]
                auth_info["signedheaders"] = cp[6]
                auth_info["signature"] = cp[7]
                ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " ", "v4 auth info  is ",inspect(auth_info))
                return auth_info, 0, true
            end
        end

        --local cp, err = ngx.re.match(headers["Authorization"], "(\\S+)\\s*:\\s*(\\S+)", "jo")
        local cp, err = ngx.re.match(headers["Authorization"], "^AWS(\\s+)([0-9A-Za-z]+)\\s*:\\s*(\\S+)", "jo")
        if nil ~= cp and nil ~= next(cp) and nil ~= cp[2] and nil ~= cp[3] then  --AWS version 2
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### match---accessid is ", cp[2])
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### match---original_signature is ", cp[3])
           -- auth_info["type"] = AWS_AUTH.HEADER_V2
            auth_info["type"] = sp_conf.auth("HEADER_V2")
            auth_info["accessid"] = cp[2]
            auth_info["signature"] = cp[3]
            return auth_info, 0, true
        end
        local firstb = string.sub(headers["Authorization"], 1, 3)
        if firstb == "AWS" then
            return nil,2,false
        end


        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "headers Authorization is not valid: ", headers["Authorization"])
        return nil, 1, false
    elseif nil ~= next(args) then   --try to find auth info in args
        --now only support aws v2
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "args is : ", inspect(args))

        if sp_conf.config["AWS_v4"] then
            --Action=ListUsers&Version=2010-05-08&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIDEXAMPLE%2F20150830%2Fus-east-1%2Fiam%2Faws4_request&X-Amz-Date=20150830T123600Z&X-Amz-Expires=60&X-Amz-SignedHeaders=content-type%3Bhost&X-Amz-Signature=37ac2f4fde00b0ac9bd9eadeb459b1bbee224158d66e7ae5fcadb70b2d181d02
            if "AWS4-HMAC-SHA256" == args["X-Amz-Algorithm"]    --AWS version 4
                and nil ~= args["X-Amz-SignedHeaders"]
                and nil ~= args["X-Amz-Signature"]
                and nil ~= args["X-Amz-Credential"] then

                --auth_info["type"] = AWS_AUTH.ARGS_V4
                auth_info["type"] = sp_conf.auth("ARGS_V4")
                auth_info["Action"] = args["Action"]
                auth_info["Version"] = args["Version"]
                auth_info["Date"] = args["X-Amz-Date"]
                auth_info["Expires"] = args["X-Amz-Expires"]

                auth_info["SignedHeaders"] = args["X-Amz-SignedHeaders"]
                auth_info["original_signature"] = args["X-Amz-Signature"]
                local credential = args["X-Amz-Credential"]
                local from, to = string.find(credential, '/')
                auth_info["accessid"] = string.sub(credential, 1, from-1)
                auth_info["surplus_Credential"] = string.sub(credential, to)
                return auth_info, 0, true
            end
        end

        --AWSAccessKeyId=AKIAIOSFODNN7EXAMPLE&Expires=1141889120&Signature=vjbyPxybdZaNmGa%2ByT272YEAiv4%3D
        if nil ~= args["AWSAccessKeyId"]
            and nil ~= args["Signature"]
            and nil ~= args["Expires"] then   --AWS version 2

            --auth_info["type"] = AWS_AUTH.ARGS_V2
            auth_info["type"] = sp_conf.auth("ARGS_V2")
            auth_info["accessid"] = args["AWSAccessKeyId"]
            auth_info["signature"] = args["Signature"]
            auth_info["Expires"] = args["Expires"]
            return auth_info, 0, true
        end

        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "args have no valid auth info: ", inspect(args))
        return nil, 2, false
    end

    ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "request has no valid auth info, maybe a anonymous request")
    return nil, 1, false
end

-- todo global function
-- return timestamp, 0, true
-- return nil, 1, false  -- parse data error
-- return timestamp, 2, false  -- request didn't later current time
local function _opt_valid_request(auth_type, date, date_name)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### Enter _opt_valid_request")
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### protocal is ", auth_type, ", date_name is ", date_name, ", date is ", date)
    --按协议进行消息转换，获取消息体时间戳

    local timestamp = ngx.parse_http_time(date)
    if nil == timestamp then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "request date parse error!!! date is :", date)
        return nil, 1, false
    end
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### body timestamp is ", timestamp)

    local bodytime = tonumber(timestamp)
    if bodytime <= 0 then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "request date parse error!!! date is :", date)
        return nil, 1, false
    end

    if sp_conf.config["AUTH_DIFFTIME"] then
        local nowtime = ngx.time()
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### now timestamp is ", nowtime)
        local difftime = nowtime - bodytime
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### difftime is ", difftime)

        local verifyprotocal
        --if AWS_AUTH.HEADER_V4 == auth_type or AWS_AUTH.ARGS_V4 == auth_type then
        if sp_conf.auth("HEADER_V4") == auth_type or sp_conf.auth("ARGS_V4") == auth_type then
            verifyprotocal = "aws4"
        else
            verifyprotocal = "aws2"
        end
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
local function check_time_info(headers, auth_type, req_info)
    ----c) get date and verify valid request
    local date, date_name
    --根据S3要求，只有通过Authorization头进行验证的需要进行时间验证
    --Yuanguo: in AWS4, query string params, there is X-Amz-Date and
    --         X-Amz-Expires.
    --         in AWS2, there is Expires.
    --         they are used to validate the time;
   -- if AWS_AUTH.HEADER_V2 == auth_type or AWS_AUTH.HEADER_V4 == auth_type then
    if sp_conf.auth("HEADER_V2") == auth_type or sp_conf.auth("HEADER_V4") == auth_type then
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

local function valid_list_params(args)
    for k, v in pairs(args) do
        if nil == sp_conf.config["list_params"][k] then
            return false
        end
    end
    return true
end

local function get_op_info(method, uri, args, headers)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### Enter get_op_info:", method, uri) --, args, headers)
    --bucket info in request_uri
    -- "/" -- list bucket
    -- "/bucket{?optype}" --bucket op
    -- "/bucket/object{?optype}" --object op

    --bucket info in host
    -- "/" -- list bucket
    -- "/{?optype}" --bucket op
    -- "/object{?optype}" --object op

    local bucket_mode = "uri" --default
    local bucketname = ""
    local objectname = ""

    local host = headers["Host"]
    if nil == host then
        return nil
    end
    local defhost = sp_conf.config["default_hostdomain"]
    local from, to, err = ngx.re.find(host, defhost, "jo")
    if nil ~= from and 1 ~= from then
        bucket_mode = "host"
        bucketname = string.sub(host, 1, from-2)
    elseif err then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "Failed run ngx.re.find , and err is ", err)
        return nil
    else
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "bucketname may in uri, host: " .. host .. ", uri: " .. uri)
        if host == defhost then
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", "using the s3 normal domain and path mode")
        else
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", "maybe using host  including port ")
            --[[local m,err = ngx.re.match(host,[=[([^:]*):(\d+)]=])
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", "m[1] is ",m[1],"m[2] is ", m[2])
            if defhost ~= m[1] then
                ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", "using server ip or wrong domain ","defhost is ",defhost, "supplied host is ",m[1])
                --return nil
            else
                --for now dosenot support server ip and port check,maybe place in the access block?
            end--]]
        end

    end

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", "uri is ",uri)
    if nil ~= ngx.var.portal then
       local uri1, n, err =  ngx.re.sub(uri, "/t","")
       if uri1 then
         uri = uri1
       else
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "error: ", err)
       end
    end

    --analyse uri to get bucketname, objectname
--    local splittab = utils.strsplit(uri, "/")
    local splittab, err = ngx_re.split(uri, "/")
    local first = ""
    local second = ""

    --eg. uri is /xxx//yyy/zzz/ or xxx/yyy///zzz, then first = xxx, second = yyy/zzz
    for i,v in ipairs(splittab) do
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", "##### uri split to ", i .. ":" .. v)
        if "" == first then
            first = v
        elseif "" == second then
            second = v
        else
            second = second .. "/" .. v
        end
    end



    if "uri" == bucket_mode then
        assert("" == bucketname)
        bucketname = first
        objectname = second
    else
        assert("" ~= bucketname)
        if "" == first or "" == second then
            objectname = first .. second
        else
            objectname = first .. "/" .. second
        end
    end

    -- start get src bucket object name
    local src_bucketname = ""
    local src_objectname = ""
    local src_uri = ""
    local req_uri = ngx.var.request_uri
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### request_uri is  ", req_uri)
   -- local request_uri_tab = utils.strsplit(ngx.var.request_uri, "?")
    local request_uri_tab, err = ngx_re.split(req_uri, "\\?")
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### request_uri split table:errmsg--- ", inspect(request_uri_tab)  .. (err or ""))
    if err then
        src_uri = req_uri
    else
            for i,v in ipairs(request_uri_tab) do
               src_uri = v
               break
            end
    end
    --analyse uri to get bucketname, objectname
    -- local splittab = utils.strsplit(src_uri, "/")
    local splittab = ngx_re.split(src_uri, "/")
    local first = ""
    local second = ""
    --eg. uri is /xxx//yyy/zzz/ or xxx/yyy///zzz, then first = xxx, second = yyy/zzz
    local skip_first_slash = true
    for i,v in ipairs(splittab) do
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### uri split to ", i .. ":" .. v)
        if "" == first then
            first = v
        elseif "" == second and skip_first_slash then
            second = v
            skip_first_slash = false
        else
            second = second .. "/" .. v
        end
    end
    if "uri" == bucket_mode then
        assert("" == src_bucketname)
        src_bucketname = first
        src_objectname = second
    else
        src_bucketname = bucketname
        if "" == first or "" == second then
            src_objectname = first .. second
        else
            src_objectname = first .. "/" .. second
        end
    end
    -- end get src bucket object name

    local op_type
    -- op_type is index for the op ,we can get index)using
    -- get_s3_op_rev function,
    -- then,when somtime ,we judge the index(op_type) is a specifed type,
    -- we use "LIST BUCKET" == get_s3_op(index)
    -- so we avoid the global varial.
    -- we using the module level per worker  var,may we should use the shardict
    -- or database for the per master var

    --new op_types depend on args to be developed later
    if "" == bucketname and "" == objectname then
        if "GET" == method then
            op_type = sp_conf.get_s3_op_rev("LIST_BUCKETS")
        end
    elseif "" == objectname then
        -- is bucket
        if "GET" == method then
            if  nil == next(args) then
                op_type = sp_conf.get_s3_op_rev("LIST_OBJECTS")
            elseif args["accelerate"] then
                op_type = sp_conf.get_s3_op_rev("GET_BUCKET_ACCELERATE")
            elseif args["acl"] then
                op_type = sp_conf.get_s3_op_rev("GET_BUCKET_ACL")
            elseif args["cors"] then
                op_type = sp_conf.get_s3_op_rev("GET_BUCKET_CORS")
            elseif args["lifecycle"] then
                op_type = sp_conf.get_s3_op_rev("GET_BUCKET_LIFECYCLE")
            elseif args["policy"] then
                op_type = sp_conf.get_s3_op_rev("GET_BUCKET_POLICY")
            elseif args["location"] then
                op_type = sp_conf.get_s3_op_rev("GET_BUCKET_LOCATION")
            elseif args["logging"] then
                op_type = sp_conf.get_s3_op_rev("GET_BUCKET_LOGGING")
            elseif args["notification"] then
                op_type = sp_conf.get_s3_op_rev("GET_BUCKET_NOTIFICATION")
            elseif args["replication"] then
                op_type = sp_conf.get_s3_op_rev("GET_BUCKET_REPLICATION")
            elseif args["tagging"] then
                op_type = sp_conf.get_s3_op_rev("GET_BUCKET_TAGGING")
            elseif args["versions"] then
                op_type = sp_conf.get_s3_op_rev("GET_BUCKET_OBJECT_VERSIONS")
            elseif args["requestPayment"] then
                op_type = sp_conf.get_s3_op_rev("GET_BUCKET_REQUESTPAYMENT")
            elseif args["versioning"] then
                op_type = sp_conf.get_s3_op_rev("GET_BUCKET_VERSIONING")
            elseif args["website"] then
                op_type = sp_conf.get_s3_op_rev("GET_BUCKET_WEBSITE")
            elseif args["uploads"] then
                op_type = sp_conf.get_s3_op_rev("LIST_MULTIPART_UPLOADS")
            else
                --op_type = S3_OP.LIST_OBJECTS
                op_type = sp_conf.get_s3_op_rev("LIST_OBJECTS")
            end
        elseif "PUT" == method  then
            if nil == next(args) then
                -- op_type = S3_OP.CREATE_BUCKET
                op_type = sp_conf.get_s3_op_rev("CREATE_BUCKET")
            elseif args["accelerate"] then
                op_type = sp_conf.get_s3_op_rev("PUT_BUCKET_ACCELERATE")
            elseif args["acl"] then
                op_type = sp_conf.get_s3_op_rev("PUT_BUCKET_ACL")
            elseif args["cors"] then
                op_type = sp_conf.get_s3_op_rev("PUT_BUCKET_CORS")
            elseif args["lifecycle"] then
                op_type = sp_conf.get_s3_op_rev("PUT_BUCKET_LIFECYCLE")
            elseif args["policy"] then
                op_type = sp_conf.get_s3_op_rev("PUT_BUCKET_POLICY")
            elseif args["logging"] then
                op_type = sp_conf.get_s3_op_rev("PUT_BUCKET_LOGGING")
            elseif args["notification"] then
                op_type = sp_conf.get_s3_op_rev("PUT_BUCKET_NOTIFICATION")
            elseif args["replication"] then
                op_type = sp_conf.get_s3_op_rev("PUT_BUCKET_REPLICATION")
            elseif args["tagging"] then
                op_type = sp_conf.get_s3_op_rev("PUT_BUCKET_TAGGING")
            elseif args["requestPayment"] then
                op_type = sp_conf.get_s3_op_rev("PUT_BUCKET_REQUESTPAYMENT")
            elseif args["versioning"] then
                op_type = sp_conf.get_s3_op_rev("PUT_BUCKET_VERSIONING")
            elseif args["website"] then
                op_type = sp_conf.get_s3_op_rev("PUT_BUCKET_WEBSITE")
            else
                op_type = sp_conf.get_s3_op_rev("CREATE_BUCKET")
            end
        elseif "DELETE" == method then
            if nil == next(args) then
                -- op_type = S3_OP.DELETE_BUCKET
                op_type = sp_conf.get_s3_op_rev("DELETE_BUCKET")
            elseif args["cors"] then
                op_type = sp_conf.get_s3_op_rev("DELETE_BUCKET_CORS")
            elseif args["lifecycle"] then
                op_type = sp_conf.get_s3_op_rev("DELETE_BUCKET_LIFECYCLE")
            elseif args["policy"] then
                op_type = sp_conf.get_s3_op_rev("DELETE_BUCKET_POLICY")
            elseif args["replication"] then
                op_type = sp_conf.get_s3_op_rev("DELETE_BUCKET_REPLICATION")
            elseif args["tagging"] then
                op_type = sp_conf.get_s3_op_rev("DELETE_BUCKET_TAGGING")
            elseif args["website"] then
                op_type = sp_conf.get_s3_op_rev("DELETE_BUCKET_WEBSITE")
            else
                op_type = sp_conf.get_s3_op_rev("DELETE_BUCKET")
            end
        elseif "HEAD" == method then
            op_type = sp_conf.get_s3_op_rev("HEAD_BUCKET")
        else
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "unrecognized request: " .. method .. "," .. uri)
            return nil
        end
    else
        -- is object
        if "HEAD" == method then
            --op_type = S3_OP.HEAD_OBJ
            op_type = sp_conf.get_s3_op_rev("HEAD_OBJECT")
        elseif "GET" == method then
            if nil == next(args) then
                op_type = sp_conf.get_s3_op_rev("GET_OBJECT")
            elseif args["acl"] then
                op_type = sp_conf.get_s3_op_rev("GET_OBJECT_ACL")
            elseif args["torrent"] then
                op_type = sp_conf.get_s3_op_rev("GET_OBJECT_TORRENT")
            elseif args["uploadId"] then
                op_type = sp_conf.get_s3_op_rev("LIST_PARTS")
            else
                op_type = sp_conf.get_s3_op_rev("GET_OBJECT")
            end
        elseif "PUT" == method  then
            if headers["x-amz-copy-source"] then
                if args["uploadId"] and args["partNumber"] then
                    op_type = sp_conf.get_s3_op_rev("UPLOAD_PART_COPY")
                else
                    op_type = sp_conf.get_s3_op_rev("PUT_OBJECT_COPY")
                end
            elseif nil == next(args) then
                op_type = sp_conf.get_s3_op_rev("PUT_OBJECT")
            elseif args["acl"] then
                op_type = sp_conf.get_s3_op_rev("PUT_OBJECT_ACL")
            elseif args["uploadId"] and args["partNumber"] then
                op_type = sp_conf.get_s3_op_rev("UPLOAD_PART")
            else
                op_type = sp_conf.get_s3_op_rev("PUT_OBJECT")
            end
        elseif "DELETE" == method then
            if nil == next(args) then
                op_type = sp_conf.get_s3_op_rev("DELETE_OBJECT")
            elseif args["delete"] then
                op_type = sp_conf.get_s3_op_rev("DELETE_MULTIPLE_OBJECTS")
            elseif args["uploadId"] then
                op_type = sp_conf.get_s3_op_rev("ABORT_MULTIPART_UPLOAD")
            else
                op_type = sp_conf.get_s3_op_rev("DELETE_OBJECT")
            end
        elseif "POST" == method  then
            if nil == next(args) then
                op_type = sp_conf.get_s3_op_rev("POST_OBJECT")
            elseif args["restore"] then
                op_type = sp_conf.get_s3_op_rev("POST_OBJECT_RESTORE")
            elseif args["uploads"] then
                op_type = sp_conf.get_s3_op_rev("INITIATE_MULTIPART_UPLOAD")
            elseif args["uploadId"] then
                op_type = sp_conf.get_s3_op_rev("COMPLETE_MULTIPART_UPLOAD")
            else
                op_type = sp_conf.get_s3_op_rev("POST_OBJECT")
            end
        elseif "OPTIONS" == method then
            op_type = sp_conf.get_s3_op_rev("OPTIONS_OBJECT")
        else
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "unrecognized request ", uri, method)
            return nil
        end
    end

    if not op_type then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "unrecognized request:", uri, " method:", method)
        return nil
    end
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### this request type is:", op_type)

    local op_info = {}
    op_info["type"] = op_type
    op_info["bucketmode"] = bucket_mode
    op_info["bucketname"] = bucketname
    op_info["objectname"] = objectname
    op_info["src_bucketname"] = src_bucketname
    op_info["src_objectname"] = src_objectname
    op_info["trueuri"] = uri
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", "op_info is \n",inspect(op_info))
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", "op_info  type is ",sp_conf.get_s3_op(op_type), "\n")
    return op_info
end

local function check_head_md5(headers)
    local reqmd5 = headers["content-md5"]
    if reqmd5 then
        ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " ",  "The Content-MD5 invalid. MD5:"..reqmd5)
        if '' == reqmd5 then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "The Content-MD5 invalid. MD5 is nil")
            return false, "1005"
        end
        local hmd5 = ngx.decode_base64(reqmd5)
        if nil ~= hmd5  then
            if 16 ~= #hmd5 then
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "The Content-MD5 invalid. MD5:"..hmd5)
                return  false, "1005"
            end
        else
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "The Content-MD5 invalid. MD5 decode base64 is nil")
            return false, "1005"
        end
    end
    return true, "0000"
end
function _M.get_req_info(self, method, uri, args, headers, metadb)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### Enter get_req_info:", method, uri)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### args is ", inspect(args))
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### headers is ", inspect(headers))

    local req_info = {}

    --get op info
    local op_info = get_op_info(method, uri, args, headers)
    if nil == op_info or nil == next(op_info) then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "get_op_info parameters error, 400 Bad Request")
        return "0018", "0000", false, nil ---- not supported
    end

    --get authorization information
    local auth_info,code,ok = get_auth_info(headers, args)

    if not ok then
        if 1 == code then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "get_auth_info parameters is empty")
            return "0021", "0000", false, nil
        elseif 2 == code then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "get_auth_info parameters error")
            return "0010", "0000", false, nil
        else
            return "0010", "0000", false, nil

        end
    end

    local user_columns = {
        "info:uid", 
        "info:secretkey", 
        "info:displayname", 
        "info:type",
        "info:parent",
        "info:permission",
        "info:mode",
        "info:syncflag",
        "ver:tag",
        "quota:enabled",
        "quota:objects",
        "quota:size_kb",
        "bucket_quota:enabled",
        "bucket_quota:objects",
        "bucket_quota:size_kb",
        "stats:objects",
        "stats:size_bytes",
    }
    
    local accessid = auth_info["accessid"]
    local  status, ok, user_info = metadb:get("user", accessid, user_columns)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "get user_info from hbase failed: " .. status)
        return "0011",status,false,nil
    else
        if nil == user_info or nil == next(user_info) then
            --todo why non accessid enter here
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "user accessid  non-exist: " .. accessid)
            return "1004", status, false, nil
        end
    end

    -- return 0 true  ok
    -- return 1 false date is empty
    -- return 2 false date error
    -- return 3 false date difference error
    local code,ok = check_time_info(headers, auth_info["type"], req_info)
    if not ok then
        if 1 == code then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "request date is nil!")
            --return "0010", "0000", false, nil
            return "0021", "0000", false, nil
        elseif 3 == code then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "request date difference error")
            return "1001", "0000", false, nil
        else --code 2
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "request date is error!")
            return "0021", "0000", false, nil
        end
    end

    --check md5
    local ok,code = check_head_md5(headers)
    if not ok then
        return code, "0000", false, nil
    end

--  todo we should fix the error code above
    local operationtype = ""
    local arg
    for k,v in pairs(args) do
        if tostring(v) == "true" then
            arg = k
        else
            arg = k.."="..v
        end
        if "" == operationtype then
            operationtype = arg
         else
            operationtype = operationtype .. "&" .. arg
        end
    end
    if headers then
       req_info["bktjson"] = headers["d_bucket"] --from peer,bucket hbase kv json
       -- maybe objectjson
    end



    req_info["uri"] = op_info["trueuri"]
    req_info["headers"] = headers
    req_info["method"] = method
    req_info["args"] = args
    req_info["op_info"] = op_info
    req_info["auth_info"] = auth_info
    req_info["user_info"] = user_info
   -- req_info["timestamp"] have set by the check_time_info
    return "0000", "0000", true, req_info
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

local function _get_AWS2_CanonicalizedResource(req_info)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### Enter _get_AWS2_CanonicalizedResource")

    local CanonicalizedResource = ""
    if "host" == req_info["op_info"]["bucketmode"] then
        CanonicalizedResource = "/" .. req_info["op_info"]["src_bucketname"]
    end

    local root_uri = req_info["headers"]["root_uri"]
    ngx.log(ngx.INFO, "root_uri="..(root_uri or "nil"))
    if "LIST_BUCKETS" == sp_conf.get_s3_op(req_info["op_info"]["type"]) then
        CanonicalizedResource = root_uri or "/"
    else
        if req_info["op_info"]["src_objectname"] ~= "" then
            CanonicalizedResource = "/"..req_info["op_info"]["src_bucketname"]
            local object = req_info["op_info"]["src_objectname"]
            CanonicalizedResource = CanonicalizedResource.."/"..object
        else
            CanonicalizedResource = CanonicalizedResource .. (root_uri or req_info["uri"])
        end

    end

    local sub_resource = _analyse_sub_resource(req_info["args"])
    if "" ~= sub_resource then
        CanonicalizedResource = CanonicalizedResource .. "?" .. sub_resource
    end
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", "ReSource is ",inspect(CanonicalizedResource))
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

local function _get_AWS2_CanonicalizedAmzHeaders(req_info)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### Enter _get_AWS2_CanonicalizedAmzHeaders")
    -- since we use ngx.req.get_headers to get header, this api has convert every header to lowercase
    -- and combine same header's value to one table
    local tmptable = {}
    local headertable = {}
    for k,v in pairs(req_info["headers"]) do
        if type(v) == "table" then
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### ", k, ": ", table.concat(v, ", "))
        else
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### ", k, ": ", v)
        end
        if ngx.re.find(k, "^x-amz-\\S") then
            table.insert(headertable, k)
            if string.lower(k) == "x-amz-copy-source" then
                tmptable[k] = req_info["headers"]["root-x-amz-copy-source"] or v
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

local function _get_AWS2_StringToSign(req_info)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### Enter _get_AWS2_StringToSign")
    local CanonicalizedResource = _get_AWS2_CanonicalizedResource(req_info)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### CanonicalizedResource is ", CanonicalizedResource)

    local CanonicalizedAmzHeaders = _get_AWS2_CanonicalizedAmzHeaders(req_info)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### CanonicalizedAmzHeaders is ", CanonicalizedAmzHeaders)

    local content_md5 = req_info["headers"]["Content-MD5"] or ""
    content_md5 = content_md5 .. "\n"
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### content_md5 is ", content_md5)

    local content_type = req_info["headers"]["Content-Type"] or ""
    content_type = content_type .. "\n"
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### content_type is ", content_type)

    local date
    --if AWS_AUTH.ARGS_V2 == req_info["auth_info"]["type"] or AWS_AUTH.ARGS_V4 == req_info["auth_info"]["type"] then
    if sp_conf.auth("ARGS_V2") == req_info["auth_info"]["type"]  or sp_conf.auth("ARGS_V4") == req_info["auth_info"]["type"]  then

        date = req_info["auth_info"]["Expires"] .. "\n"
    elseif nil ~= req_info["original_date_name"] then
        date = req_info["headers"][req_info["original_date_name"]] .. "\n"
    else
        date = "\n"
    end

    local StringToSign = req_info["method"] .. "\n" ..  content_md5 .. content_type .. date .. CanonicalizedAmzHeaders .. CanonicalizedResource
    ngx.log(ngx.DEBUG, "stringToSign is " .. StringToSign)
    return StringToSign
end

local function _AWS2_authorization(req_info)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### Enter _AWS2_authorization")
    local StringToSign = _get_AWS2_StringToSign(req_info)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### StringToSign is ", StringToSign)

    local secretkey = req_info["headers"]["root_secretkey"] or req_info["user_info"]["info:secretkey"]
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### secretkey is ", secretkey)
    local digest = ngx.hmac_sha1(secretkey, StringToSign)
    local new_signature = ngx.encode_base64(digest)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### new_signature is ", new_signature)

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### original_signature is ", req_info["auth_info"]["signature"])
    if new_signature == req_info["auth_info"]["signature"] then
        return "0000","0000",true
    else
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "#####  signature is not correct! ", req_info["auth_info"]["signature"],new_signature)
        return "1002", "0000", false
    end
end

local function _AWS4_authorization(req_info)
  -- stringtosing is
  --
  -- "AWS4-HMAC-SHA256" + "\n" +
  -- timeStampISO8601Format + "\n" +
  -- <Scope> + "\n" +
  -- Hex(SHA256Hash(<CanonicalRequest>))
  --
  -- where scope is
  -- date.Format(<YYYYMMDD>) + "/" + <region> + "/" + <service> + "/aws4_request"
  --
  -- where CanonicalRequest is
  --
  -- <HTTPMethod>\n
  -- <CanonicalURI>\n
  -- <CanonicalQueryString>\n
  -- <CanonicalHeaders>\n
  -- <SignedHeaders>\n
  -- <HashedPayload>
  --
  --    wheres CanonicalQueryString is
  --    http://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
  --    http://docs.aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html
  -- so ...
  --


--
--    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### Enter _AWSV4_authorization")
--    local StringToSign = _get_AWS2_StringToSign(req_info)
--    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### StringToSign is ", StringToSign)
--
--    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### secretkey is ", inspect(req_info["user_info"]["info:secretkey"]))
--    local digest = ngx.hmac_sha1(req_info["user_info"]["info:secretkey"], StringToSign)
--    local new_signature = ngx.encode_base64(digest)
--    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### new_signature is ", new_signature)
--
   --local sign_computed = s3v4:getSignature(req_info["method"], req_info["uri"], req_info["args"],
--	                                    req_info["HashedPayload"])

--    local sign_computed = s3v4:getSignature(req_info["method"], req_info["uri"], req_info["args"],
--	                                    nil)
    ngx.req.read_body()  -- explicitly read the req body
    local data = ngx.req.get_body_data()
    if data then
        req_info["body"] = data
    end

--    -- body may get buffered in a temp file:
--    local file = ngx.req.get_body_file()
--    if file then
--        ngx.say("body is in file ", file)
--    else
--        ngx.say("no body found")
--        end
    local sign_computed = s3v4:getSignature(req_info)
    ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " ",  "\n##### original_signature is ", req_info["auth_info"]["signature"],"\n##### sign_computed is ",sign_computed)
    if sign_computed == req_info["auth_info"]["signature"] then
        return "0000","0000",true
    else
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "\n##### original_signature is ", req_info["auth_info"]["signature"],"\n##### sign_computed is ",sign_computed)
        return "1002", "0000", false
    end
    --    return "1003", "0000", false
end

function _M.access_authentication(self, req_info)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### Enter _M.access_authentication")
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### method is ", req_info["method"])
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### uri is ", req_info["uri"])
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### args is ", inspect(req_info["args"]))
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### headers is ", inspect(req_info["headers"]))

    local auth_type = req_info["auth_info"]["type"]

    if not sp_conf.config["AUTH_OPEN"] then
         ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " ",  "##### Enter noaccess_authentication")
        return "0000", "0000", true
--    elseif AWS_AUTH.HEADER_V2 == auth_type or AWS_AUTH.ARGS_V2 == auth_type then
    elseif sp_conf.auth("HEADER_V2") == auth_type or sp_conf.auth("ARGS_V2") == auth_type then
         ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " ",  "##### Enter AWSV2 access_authentication")
        return _AWS2_authorization(req_info)
--    elseif AWS_AUTH.HEADER_V4 == auth_type or AWS_AUTH.ARGS_V4 == auth_type then
    elseif sp_conf.auth("HEADER_V4") == auth_type or sp_conf.auth("ARGS_V4") == auth_type then
         ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " ",  "##### Enter AWSV4 access_authentication")
        return _AWS4_authorization(req_info)
    end

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### other noaccess_authentication")
    return "0010","0000", false
end

function _M.verify_permission(self, req_info)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### Enter verify_permission")

    local user_info = req_info["user_info"]
    if not user_info["info:parent"] then
        return "0000", "0000", true
    end

    local permission_json = user_info["info:permission"]
    if not permission_json then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", "no permission for this user=", user_info["info:uid"])
        return "0021", "0000", false
    end

    local ok, ptable = pcall(json.decode, permission_json)
    if not ok or "table" ~= type(ptable) then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ","json.decode permission failed, =", permission_json)
        return "0021", "0000", false
    end

    local op_type = req_info["op_info"]["type"]
    local op_name = sp_conf.get_s3_op(op_type)
    local ok, permission_prefixes = permission:verify_permission(req_info["uri"], op_name, ptable)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ","op permission denied, =", op_name)
        return "0021", "0000", false
    end

    req_info["permission_prefixes"] = permission_prefixes
    return "0000", "0000", true
end

return _M
