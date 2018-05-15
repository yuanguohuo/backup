--admin request parse module  

local _M = {
    _VERSION = '0.01',
}

local utils = require('common.utils')
local json = require('cjson')
local inspect = require("inspect")
local sp_conf = require("storageproxy_conf")
local ngx_re = require "ngx.re"

local ngx_re_gsub   = ngx.re.gsub

local ok, metadb = pcall(require, "metadb-instance")
if not ok or not metadb then
    error("failed to load metadb-instance:" .. (metadb or "nil"))
end

local mt = { __index = _M }
function _M.new(self)
    return setmetatable({}, mt)
end

local function uri_normalization(uri)
    local new_uri,_,err = ngx_re_gsub(uri, "/+", "/", "jo")
    if(string.byte(new_uri,1,1) == 47) then  -- 47 is '/'
        new_uri = string.sub(new_uri,2,-1)
    end
    if(string.byte(new_uri,-1,-1) == 47) then  -- 47 is '/'
        new_uri = string.sub(new_uri,1,-2)
    end
    return new_uri
end

local function get_op_type(method, uri, args)
    local uri = uri_normalization(uri)

    local op_type

    if "admin/user" == uri and "PUT" == method then
        op_type = "CREATE_USER"
    elseif "admin/user" == uri and "GET" == method then
        op_type = "QUERY_USER"
    elseif "admin/user" == uri and "DELETE" == method then
        op_type = "DELETE_USER"
    elseif "admin/permission" == uri and "PUT" == method then
        op_type = "SET_PERMISSION"
    elseif "admin/permission" == uri and "GET" == method then
        op_type = "GET_PERMISSION"
    elseif "admin/permission" == uri and "DELETE" == method then
        op_type = "DELETE_PERMISSION"
    elseif "admin/sync" == uri and "PUT" == method then
        op_type = "SET_SYNC"
    elseif "admin/sync" == uri and "GET" == method then
        op_type = "GET_SYNC"
    elseif "admin/sync" == uri and "DELETE" == method then
        op_type = "DELETE_SYNC"
    elseif "admin/stats" == uri and "GET" == method then
        op_type = "GET_STATS"
    elseif "admin/quota" == uri and "PUT" == method then
        op_type = "SET_QUOTA"
    elseif "sync/full" == uri and "POST" == method then
        op_type = "POST_FULL_RSYNC"
    elseif "admin/log" == uri and "GET" == method then
        if "data" == args["type"] then
            op_type = "GET_DATALOG"
        elseif "metadata" == args["type"] and args["id"] and "users" == args["id"] then
            op_type = "GET_USERS"
        elseif "metadata" == args["type"] and args["id"] and "buckets" == args["id"] then
            op_type = "GET_BUCKETS"
        elseif "metadata" == args["type"] and args["id"] and "objects" == args["id"] then
            op_type = "GET_OBJECTS"
        end
    elseif "admin/ceph/new" == uri and "PUT" == method then
        op_type = "NEW_CEPH"
    elseif "admin/ceph/get" == uri and "GET" == method then
        op_type = "GET_CEPH"
    elseif "admin/ceph/set" == uri and "PUT" == method then
        op_type = "SET_CEPH"
    elseif "admin/rgw/add" == uri and "PUT" == method then
        op_type = "ADD_RGW"
    elseif "admin/rgw/del" == uri and "DELETE" == method then
        op_type = "DEL_RGW"
    end

    return op_type
end

-- get Authorization info
local function get_auth_info(headers, args)
    local auth_info = {}
    if headers["Authorization"] then   --auth info is in HTTP headers
        local cp, err = ngx.re.match(headers["Authorization"], "^AWS(\\s+)([0-9A-Za-z]+)\\s*:\\s*(\\S+)", "jo")
        if nil ~= cp and nil ~= next(cp) and nil ~= cp[2] and nil ~= cp[3] then  --AWS version 2
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### match---accessid is ", cp[2])
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### match---original_signature is ", cp[3])
            auth_info["type"] = sp_conf.auth("HEADER_V2")
            auth_info["accessid"] = cp[2]
            auth_info["signature"] = cp[3]
            return auth_info
        end

        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "header authorization invalid: " .. headers["Authorization"])
        return nil
    end

    --AWSAccessKeyId=AKIAIOSFODNN7EXAMPLE&Expires=1141889120&Signature=vjbyPxybdZaNmGa%2ByT272YEAiv4%3D
    if args["AWSAccessKeyId"] and args["Signature"] and args["Expires"] then   --AWS version 2
        auth_info["type"] = sp_conf.auth("ARGS_V2")
        auth_info["accessid"] = args["AWSAccessKeyId"]
        auth_info["signature"] = args["Signature"]
        auth_info["Expires"] = args["Expires"]
        return auth_info
    end

    ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "request has no valid auth info, maybe a anonymous request")
    return nil
end

local function get_user_info(accessid)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### Enter get_user_info")

    local columns = {
        "info:uid",
        "info:secretkey",
        "info:displayname",
        "info:isindex",
        "ver:tag",
    }
    
    local status, ok, user_info = metadb:get("user", accessid, columns)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "get user_info failed(not client.get err and client.get error),status is:" .. status)
        return "0011",status,false,nil 
    end

    if nil == user_info or nil == next(user_info) then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "get user_info failed non-exist")
        return "1004", status, false, nil
    end

    user_info["accessid"] = accessid
      
    return "0000", "0000", true, user_info
end

function _M.parse(method, uri, headers, args)
    local req_info = {}

    local op_type = get_op_type(method, uri, args)
    if not op_type then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "unsupported op, uri: ", uri, ", args: ", inspect(args))
        return "0018", "0000", false
    end

    local auth_info = get_auth_info(headers, args)
    if not auth_info then
        return "", "", false
    end

    local proxy_code, incode, ok, user_info = get_user_info(auth_info["accessid"])
    if not user_info then
        --return "", "", false
        return proxy_code, incode, false
    end
    
    req_info["op_type"] = op_type
    req_info["auth_info"] = auth_info
    req_info["user_info"] = user_info
    req_info["uri"] = uri
    req_info["method"] = method
    req_info["headers"] = headers
    req_info["args"] = args

    return "0000", "0000", true, req_info
end
return _M
