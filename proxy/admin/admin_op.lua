local sp_conf = require("storageproxy_conf")
local json = require('cjson')
local inspect = require("inspect")
local rsync_op = require('rsync_op')
local sync_meta = require("sync_meta")
local permission = require("permission")
local utils = require("common.utils")

local ok,objstore = pcall(require, "storageproxy_objstore")
if not ok or not objstore then
    error("failed to load storageproxy_objstore:" .. (objstore or "nil"))
end

local ok, metadb = pcall(require, "metadb-instance-nolcache")
if not ok or not metadb then
    error("failed to load metadb-instance:" .. (metadb or "nil"))
end

local mfloor = math.floor
local ngx_re = require "ngx.re"
local stringify = utils.stringify
local gen_seq_no  = utils.gen_seq_no

local _M = table.new(0, 155)

local function get_user_stats(uid)
    ngx.log(ngx.DEBUG,"bucket: " .. (bucket or "null"), ";uid: " .. (uid or "null"))
    local result
    local statcode, innercode, ok, accessid = getaccessid(uid)
    if not ok then
        return nil, statcode, false,nil
    end

    local status, ok, user_info = metadb:get("user", accessid, {"stats:size_bytes","stats:objects"})
    if not ok then
        ngx.log(ngx.ERR, "get user_info failed(not client.get err and client.get error),status is:" .. status)
        return nil,"4030",status,false
    else
        if nil == user_info or nil == next(user_info) then
            ngx.log(ngx.ERR,"get user_info failed non-exist")
            return nil,"4021", status, true
        end
    end

    ngx.log(ngx.DEBUG,"get user stats result: ", inspect(user_info))

    if not user_info["stats:size_bytes"] and not user_info["stats:objects"]then
        user_info["stats:size_bytes"] = 0
        user_info["stats:objects"] = 0
    else
        if not user_info["stats:size_bytes"]  or  not user_info["stats:objects"] then
            ngx.log(ngx.WARN,  "some user stat is absent: ".. uid)
        end
    end
    user_info["stats:mb_rounded"] = user_info["stats:size_bytes"] and mfloor(user_info["stats:size_bytes"]/1024/1024) or 0
    result = user_info

    return result,"0000","0000",true
end

local function get_bucket_stats(bucket, b_prefix)
    local bucketrowkey = b_prefix.."_"..bucket

    ngx.log(ngx.DEBUG,"entering get bucketrowkeys stats: ", bucketrowkey)

    local code, ok, rebody1 = metadb:get("bucket", bucketrowkey, {"info:mtime", "stats:size_bytes", "stats:objects"})
    if not ok then
        ngx.log(ngx.ERR,  "metadb failed to get bucket stats:"..code)
        return "4030", code, false
    end

    if nil == rebody1 or nil == next(rebody1) then
        ngx.log(ngx.ERR,"metadb failed to get bucket stats,empty:"..code)
        return "4031", code, false
    end

    local result = {
        ["bucket"] = bucket,
        ["usage"] = {
            ["stats:size_bytes"] = rebody1["stats:size_bytes"] or 0,
            ["stats:objects"] = rebody1["stats:objects"] or 0,
        },
        ["mtime"] = rebody1["info:mtime"] or string.format("%0.3f", ngx.now()),
    }

    ngx.log(ngx.DEBUG,"stats result return : ", inspect(result))

    return "0000", "0000", true, result
end

local function create_user(req_info)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, ", Enter create user")
    local args = req_info["args"]
    local uid = args["uid"]
    local accessid = args["accessid"]
    local secretkey = args["secretkey"]
    local p_uid = args["parent_uid"]

    if not uid or not accessid or not secretkey then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " essential args absent")
        return "0010", "0000", false
    end

    if "root" == uid or "root" == p_uid then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " invalid uid or p_uid")
        return "0010", "0000", false
    end

    local cap, _ = ngx.re.match(accessid, "^[0-9A-Z]{20}$", "jo")
    if not cap then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "invalid accessid: ", accessid)
        return "0010", "0000", false
    end

    local cap, _ = ngx.re.match(secretkey, "^[0-9a-zA-Z]{40}$", "jo")
    if not cap then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "invalid secretkey: ", secretkey)
        return "0010", "0000", false
    end

    local code, ok, rebody = metadb:get("userid", uid, {"info:accessid"})
    if not ok then 
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to query userid table, code", code)
        return "3002", code, false
    end  

    local uid_existed = false
    if rebody["info:accessid"] then
        if accessid ~= rebody["info:accessid"] then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " uid already existed and info unmatch.")
            return "4027", "0000", false
        end
        uid_existed = true
    end

    local code, ok, rebody = metadb:get("user", accessid, {"info:uid", "info:secretkey", "info:parent"})
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to query user table, code=", code)
        return "3002", code, false
    end  

    local user_existed = false
    if rebody["info:uid"] or rebody["info:secretkey"] or rebody["info:parent"] then 
        if uid ~= rebody["info:uid"] or secretkey ~= rebody["info:secretkey"] or p_uid ~= rebody["info:parent"] then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " accessid already existed and info unmatch.")
            return "4027", "0000", false
        end
        user_existed = true
    end

    local ver_tag = accessid
    if not user_existed and p_uid then
        local code, ok, rebody = metadb:get("userid", p_uid, {"info:accessid"})
        if not ok then 
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to query userid table, codei=", code)
            return "3002", code, false
        end  
        if not rebody or not next(rebody) then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " parent uid not existed, p_uid=" .. p_uid)
            return "4027", code, false
        end

        local code, ok, rebody = metadb:get("user", rebody["info:accessid"], {"ver:tag"})
        if not ok then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to query user table, code=", code)
            return "3002", code, false
        end  

        if not rebody or not next(rebody) then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " parent accessid not existed, p_uid=" .. rebody["info:accessid"])
            return "4027", code, false
        end
        ver_tag = rebody["ver:tag"]
    end

    local displayname = args["displayname"]
    local mode = args["mode"]
    local maxbuckets = tonumber(args["maxbuckets"])
    local size_kb = tonumber(args["max-size-kb"])
    local objects = tonumber(args["max-objects"])
    local quota_enable = "false"
    if size_kb and objects then
        quota_enable = "true"
    end

    local mtime = string.format("%0.3f", ngx.now())
    local seq_no = args["seq_no"] or gen_seq_no(mtime)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " seq_no generated for user ", uid, ". seq_no=", seq_no)

    local code, ok = sync_meta.write_metalog(req_info, seq_no)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " write_metalog failed. code=", code) 
        return "4041", code, false
    end

    if not uid_existed then
        local hbase_body ={
            "info", "accessid", accessid,
        }

        local code, ok = metadb:put("userid", uid, hbase_body)
        if not ok then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " metadb put failed. code=", code, " accessid=", accessid)
            return "4028", code, false
        end
    end

    local hbase_body = {}
    if not user_existed then
        hbase_body = {
            "info", "uid", uid,
            "info", "type", "fs",
            "info", "secretkey", secretkey,
            "info", "displayname", displayname or uid,
            "info", "mtime", ngx.http_time(mtime),
            "info", "maxbuckets", maxbuckets or 1000,
            "ver",  "tag", ver_tag,
            "ver",  "seq", seq_no,
            "quota", "enabled", quota_enable,
            "quota", "size_kb", size_kb or -1,
            "quota", "objects", objects or -1,
        }

        if p_uid then
            hbase_body[#hbase_body + 1] = "info"
            hbase_body[#hbase_body + 1] = "parent"
            hbase_body[#hbase_body + 1] = p_uid
        end

        if mode then
            hbase_body[#hbase_body + 1] = "info"
            hbase_body[#hbase_body + 1] = "mode"
            hbase_body[#hbase_body + 1] = mode
        end
    else
        hbase_body = {
            "info", "mtime", ngx.http_time(mtime),
            "ver",  "seq", seq_no,
        }

        if displayname then
            hbase_body[#hbase_body + 1] = "info"
            hbase_body[#hbase_body + 1] = "displayname"
            hbase_body[#hbase_body + 1] = displayname
        end

        if maxbuckets then
            hbase_body[#hbase_body + 1] = "info"
            hbase_body[#hbase_body + 1] = "maxbuckets"
            hbase_body[#hbase_body + 1] = maxbuckets
        end

        if size_kb or objects then
            hbase_body[#hbase_body + 1] = "quota"
            hbase_body[#hbase_body + 1] = "enabled"
            hbase_body[#hbase_body + 1] = "true"
        end

        if size_kb then
            hbase_body[#hbase_body + 1] = "quota"
            hbase_body[#hbase_body + 1] = "size_kb"
            hbase_body[#hbase_body + 1] = size_kb
        end

        if objects then
            hbase_body[#hbase_body + 1] = "quota"
            hbase_body[#hbase_body + 1] = "objects"
            hbase_body[#hbase_body + 1] = objects
        end

        if mode then
            hbase_body[#hbase_body + 1] = "info"
            hbase_body[#hbase_body + 1] = "mode"
            hbase_body[#hbase_body + 1] = mode
        end
    end

    local compare_op = metadb.CompareOp.GREATER
    local code, ok = metadb:checkAndMutateRow("user", accessid, "ver", "seq", compare_op, seq_no, hbase_body, nil)
    if not ok then
        if "1121" == code then --failed because of condition-check-failure
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " checkandmutate outdated. accessid=", accessid, " seq_no=", seq_no) 
        else
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " checkandmutate failed. code=", code, " accessid=", accessid, " seq_no=", seq_no)
            -- new put user failed, so we delete userid created
            if not uid_existed then
                local code, ok = metadb:delete("userid", uid)
                if not ok then
                    ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "delete user failed :", accessid, ",uid:", uid)
                    return "4026", false
                end
            end
        end
        
        return "4028", code, false
    end
    
    sync_meta.sync_req(req_info, seq_no)

    ngx.status = ngx.HTTP_OK
    return "0000", "0000", true
end

local function delete_user(req_info)
    local args = req_info["args"]
    local uid = args["uid"]

    local code, ok, rebody = metadb:get("userid", uid, {"info:accessid"})
    if not ok then 
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to query userid table, code", code)
        return "3002", code, false
    end  

    if not rebody or not next(rebody) then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " inexistent uid: " .. uid)
        ngx.status = ngx.HTTP_NO_CONTENT
        return "0000", "0000", true
    end

    local accessid = rebody["info:accessid"]

    local mtime = string.format("%0.3f", ngx.now())
    local seq_no = args["seq_no"] or gen_seq_no(mtime)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " seq_no generated for user ", uid, ". seq_no=", seq_no)

    local code, ok = sync_meta.write_metalog(req_info, seq_no)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " write_metalog failed. code=", code) 
        return "4041", code, false
    end

    local compare_op = metadb.CompareOp.GREATER
    local delete_families = {
        "info:uid",
        "info:type", 
        "info:secretkey", 
        "info:displayname", 
        "info:mtime", 
        "info:maxbuckets", 
        "info:permission", 
        "info:syncflag", 
        "info:parent", 
        "ver:tag", 
        "ver:seq", 
        "quota:enabled", 
        "quota:size_kb", 
        "quota:objects", 
        "stats:size_bytes",
        "stats:objects",
    }
    local code, ok = metadb:checkAndMutateRow("user", accessid, "ver", "seq", compare_op, seq_no, nil, delete_families)
    if not ok then
        if "1121" == code then --failed because of condition-check-failure
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " checkandmutate outdated. accessid=", accessid, " seq_no=", seq_no)
        else
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " checkandmutate failed. code=", code, " accessid=", accessid, " seq_no=", seq_no)
        end
        return "4026", code, false
    end

    local code, ok = metadb:delete("userid", uid)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "delete user failed :", accessid, ",uid:", uid)
        return "4026", false
    end
    
    sync_meta.sync_req(req_info, seq_no)

    ngx.status = ngx.HTTP_NO_CONTENT
    return "0000", "0000", true
end

local function set_quota(req_info)
    local args = req_info["args"]
    local uid = args.uid
    local bucket = args.bucket
    local size_kb = tonumber(args.size_kb)
    if not uid or not size_kb then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "args uid or size_kb invalid!")
        return "4023", "", false
    end

    local code, ok, rebody = metadb:get("userid", uid, {"info:accessid"})
    if not ok then 
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to query userid table, code", code)
        return "3002", code, false
    end  

    if not rebody or not rebody["info:acessid"] then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " inexistent uid " .. uid)
        return "3001", code, false
    end

    local accessid = rebody["info:acessid"]

    local code, ok, rebody = metadb:get("user", accessid, {"ver:tag"})
    if not ok then 
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to query user table, code=", code)
        return "3002", code, false
    end  

    if not rebody or not rebody["ver:tag"] then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " inexistent accessid " .. accessid)
        return "3001", code, false
    end
    
    local b_prefix = rebody["ver:tag"]
    local bucket = args["bucket"]
    if bucket then --bucketname
        local time = string.format("%0.3f", ngx.now())
        local rowkey = b_prefix .. "_" .. bucket
        local hbase_body = {
            "info", "mtime", time,
            "quota", "enabled", "1",
            "quota", "size_kb", size_kb
        }

        local code, ok = metadb:checkAndPut("bucket", rowkey, "info", "accessid", accessid, hbase_body)
        if not ok then 
            if code == "1121" then 
                ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " bucket not exists: " .. bucket)
            else
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " put bucket failed (put request is from client)")
            end  
            return "2003", code, false
        end

        return "0000", "0000", true
    end

    local time = string.format("%0.3f", ngx.now())
    local hbase_body = {
        "info", "mtime", time,
        "quota", "enabled", "1",
        "quota", "size_kb", size_kb
    }

    local code, ok = metadb:checkAndPut("user", accessid, "info", "accessid", accessid, hbase_body)
    if not ok then 
        if code == "1121" then 
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " user not exists: " .. bucket)
        else
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " put bucket failed (put request is from client)")
        end  
        return "2003", code, false
    end

    return "0000", "0000", true
end

local function get_userinfo(req_info)
    ngx.log(ngx.DEBUG,"get userinfo by accessid: \n", inspect(req_info))

    local uid = req_info["args"]["uid"]
    if not uid then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " you should supply the uid to get user_info")
        return "4024", "0000", false
    end

    local code, ok, rebody = metadb:get("userid", uid, {"info:accessid"})
    if not ok then 
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to query userid table, code", code)
        return "3002", code, false
    end  

    if not next(rebody) then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " uid not existed in userid table: " .. uid)
        return "4023", "0000", false
    end

    local accessid = rebody["info:accessid"]
    local hbase_body = {
        "info:uid", 
        "info:displayname", 
        "info:email", 
        "info:maxbuckets", 
        "info:secretkey", 
        "info:op_mask",
        "bucket_quota:enabled", 
        "bucket_quota:size_kb", 
        "bucket_quota:objects",
        "quota:enabled", 
        "quota:size_kb", 
        "quota:objects",
    }

    local code, ok, user_info = metadb:get("user", accessid, hbase_body)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "get user_info failed(not client.get err and client.get error), code is:" .. code)
        return "4030", code, false
    end

    ngx.log(ngx.DEBUG, "get userinfo by accessid,result: \n", inspect(user_info))
    if not user_info or not next(user_info) then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "get user_info failed non-exist")
        return "4021", "0000", false
    end

    local jstb = {
        [1] = {
            ["user_id"] = user_info["info:uid"],
            ["display_name"] = user_info["info:displayname"],
            ["email"] = user_info["info:email"] or "",
            ["suspended"] = 0,
            ["max_buckets"] = user_info["info:maxbuckets"],

            ["keys"]= {
                ["user"]= uid,
                ["access_key"]= accessid,
                ["secret_key"]= user_info["info:secretkey"],
            },

            ["caps"]= "",
            ["op_mask"] = user_info["info:op_mask"] or  {"read","write","delete"},

            ["user_quota"] = {
                ["enabled"] = user_info["quota:enabled"] or -1,
                ["max_size_kb"]= user_info["quota:size_kb"] or -1,
                ["max_objects"] = user_info["quota:objects"] or -1,
            },
        }
    }

    local ok, jsoninfo = pcall(json.encode, jstb)
    if not ok then
        ngx.log(ngx.ERR, "failed to encode userinfo for get user")
        return "4022", "0000", false
    end

    ngx.say(jsoninfo)

    return "0000", "0000", true
end

local function set_permission(req_info)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, ", Enter set_permission")
    local args = req_info["args"]
    local uid = args["uid"]

    if not uid then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " essential args absent")
        return "0010", "0000", false
    end

    ngx.req.read_body()
    local request_body = ngx.var.request_body

    local ok, rtable = pcall(json.decode, request_body)
    if not ok or "table" ~= type(rtable) then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ","json.decode body failed. body=", request_body)
        return "0028", "0000", false
    end

    local ok, _ = permission:check_permission(rtable)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ","permisson json invalid. body=", request_body)
        return "0028", "0000", false
    end

    local code, ok, rebody = metadb:get("userid", uid, {"info:accessid"})
    if not ok then 
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to query userid table, code", code)
        return "3002", code, false
    end  

    if not next(rebody) then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " uid inexistent.")
        return "4027", "0000", false
    end

    local accessid = rebody["info:accessid"]
    local code, ok, rebody = metadb:get("user", accessid, {"ver:tag"})
    if not ok then 
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to query user table, code=", code)
        return "3002", code, false
    end  

    if not next(rebody) then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " accessid inexistent.")
        return "4027", "0000", false
    end

    local mtime = string.format("%0.3f", ngx.now())
    local seq_no = args["seq_no"] or gen_seq_no(mtime)
    ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " seq_no generated for user ", uid, ". seq_no=", seq_no)

    local code, ok = sync_meta.write_metalog(req_info, seq_no, request_body)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " write_metalog failed. code=", code) 
        return "4041", code, false
    end

    local hbase_body = {
        "info", "permission", request_body,
        "ver",  "seq", seq_no,
    }

    local compare_op = metadb.CompareOp.GREATER
    local code, ok = metadb:checkAndMutateRow("user", accessid, "ver", "seq", compare_op, seq_no, hbase_body, nil)
    if not ok then
        if "1121" == code then --failed because of condition-check-failure
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " checkandmutate outdated. accessid=", accessid, " seq_no=", seq_no) 
        else
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " checkandmutate failed. code=", code, " accessid=", accessid, " seq_no=", seq_no)
        end
        
        return "4028", code, false
    end

    sync_meta.sync_req(req_info, seq_no, request_body)

    ngx.status = ngx.HTTP_OK
    return "0000", "0000", true
end

local function get_permission(req_info)
    ngx.log(ngx.DEBUG,"get permission by uid: \n", inspect(req_info))

    local uid = req_info["args"]["uid"]
    if not uid then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " you should supply the uid to get user_info")
        return "4024", "0000", false
    end

    local code, ok, rebody = metadb:get("userid", uid, {"info:accessid"})
    if not ok then 
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to query userid table, code", code)
        return "3002", code, false
    end  

    if not next(rebody) then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " uid not existed in userid table: " .. uid)
        return "4023", "0000", false
    end

    local accessid = rebody["info:accessid"]

    local hbase_body = {"info:uid", "info:permission"}
    local code, ok, user_info = metadb:get("user", accessid, hbase_body)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "get user_info failed(not client.get err and client.get error), code is:" .. code)
        return "4030", code, false
    end

    ngx.log(ngx.DEBUG, "get userinfo by accessid,result: \n", inspect(user_info))
    if not user_info or not next(user_info) then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "get user_info failed non-exist")
        return "4021", "0000", false
    end

    local permission_json = user_info["info:permission"]
    ngx.header["Content-Length"] = 0
    if permission_json then
        ngx.header["Content-Length"] = #permission_json
        ngx.header["Content-Type"] = "application/json"
        ngx.print(permission_json)
    end

    return "0000", "0000", true
end

local function delete_permission(req_info)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, ", Enter delete_permission")
    local args = req_info["args"]
    local uid = args["uid"]

    if not uid then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " essential args absent")
        return "0010", "0000", false
    end

    local code, ok, rebody = metadb:get("userid", uid, {"info:accessid"})
    if not ok then 
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to query userid table, code", code)
        return "3002", code, false
    end  

    if not next(rebody) then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " uid inexistent.")
        return "4023", "0000", false
    end

    local accessid = rebody["info:accessid"]
    local code, ok, rebody = metadb:get("user", accessid, {"ver:tag"})
    if not ok then 
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to query user table, code=", code)
        return "3002", code, false
    end  

    if not next(rebody) then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " accessid inexistent.")
        return "4021", "0000", false
    end

    local mtime = string.format("%0.3f", ngx.now())
    local seq_no = args["seq_no"] or gen_seq_no(mtime)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " seq_no generated for user ", uid, ". seq_no=", seq_no)

    local code, ok = sync_meta.write_metalog(req_info, seq_no)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " write_metalog failed. code=", code) 
        return "4041", code, false
    end

    local hbase_body = {"ver", "seq", seq_no}
    local delete_columns = {"info:permission"}
    local compare_op = metadb.CompareOp.GREATER
    local code, ok = metadb:checkAndMutateRow("user", accessid, "ver", "seq", compare_op, seq_no, hbase_body, delete_columns)
    if not ok then
        if "1121" == code then --failed because of condition-check-failure
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " checkandmutate outdated. accessid=", accessid, " seq_no=", seq_no) 
        else
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " checkandmutate failed. code=", code, " accessid=", accessid, " seq_no=", seq_no)
        end
        
        return "4028", code, false
    end

    sync_meta.sync_req(req_info, seq_no)

    ngx.status = ngx.HTTP_NO_CONTENT
    return "0000", "0000", true
end

local function set_sync(req_info)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, ", Enter set_sync")
    local args = req_info["args"]
    local uid = args["uid"]
    local bucket = args["bucket"]

    if not uid then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " essential args absent")
        return "0010", "0000", false
    end

    local code, ok, rebody = metadb:get("userid", uid, {"info:accessid"})
    if not ok then 
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to query userid table, code", code)
        return "3002", code, false
    end  

    if not next(rebody) then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " uid inexistent.")
        return "4023", "0000", false
    end

    local accessid = rebody["info:accessid"]
    local code, ok, rebody = metadb:get("user", accessid, {"ver:tag"})
    if not ok then 
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to query user table, code=", code)
        return "3002", code, false
    end  

    if not next(rebody) then 
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " accessid inexistent." .. accessid)
        return "4021", "0000", false
    end

    local table_name = "user"
    local rowkey = accessid
    if bucket then
        table_name = "bucket"
        rowkey = rebody["ver:tag"] .. "_" .. bucket

        local code, ok, rebody1 = metadb:get("bucket", rowkey, {"ver:tag"})
        if not ok then
            ngx.log(ngx.ERR,  "metadb failed to get bucket :" .. code)
            return "4030", code, false
        end

        if not rebody1 or not next(rebody1) then
            ngx.log(ngx.ERR,"bucket inexistent :" .. bucket)
            return "4031", code, false
        end
    end

    local code, incode, ok = rsync_op:set_new_fullsync(metadb, accessid, bucket)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " set_new_fullsync failed. code=", code, " incode", incode)
        return code, incode, ok
    end

    local mtime = string.format("%0.3f", ngx.now())
    local seq_no = args["seq_no"] or gen_seq_no(mtime)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " seq_no generated for user ", uid, ". seq_no=", seq_no)

    local code, ok = sync_meta.write_metalog(req_info, seq_no)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " write_metalog failed. code=", code) 
        return "4041", code, false
    end

    local hbase_body = {
        "info", "syncflag", "true",
        "ver",  "seq", seq_no,
    }

    local compare_op = metadb.CompareOp.GREATER
    local code, ok = metadb:checkAndMutateRow(table_name, rowkey, "ver", "seq", compare_op, seq_no, hbase_body, nil)
    if not ok then
        if "1121" == code then --failed because of condition-check-failure
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " checkandmutate outdated. rowkey=", rowkey, " seq_no=", seq_no) 
        else
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " checkandmutate failed. code=", code, " rowkey=", rowkey, " seq_no=", seq_no)
        end

        return "4028", code, false
    end

    sync_meta.sync_req(req_info, seq_no)

    ngx.status = ngx.HTTP_OK
    return "0000", "0000", true
end

local function get_sync(req_info)
    ngx.log(ngx.DEBUG,"get sync by uid: \n", inspect(req_info))
    local args = req_info["args"]
    local uid = args["uid"]
    local bucket = args["bucket"]

    if not uid then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " you should supply the uid to get user_info")
        return "4024", "0000", false
    end

    local code, ok, rebody = metadb:get("userid", uid, {"info:accessid"})
    if not ok then 
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to query userid table, code", code)
        return "3002", code, false
    end  

    if not next(rebody) then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " uid not existed in userid table: " .. uid)
        return "4027", "0000", false
    end

    local accessid = rebody["info:accessid"]
    local hbase_body = {"info:syncflag", "ver:tag"}
    local code, ok, user_info = metadb:get("user", accessid, hbase_body)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "get user_info failed(not client.get err and client.get error), code is:" .. code)
        return "4030", code, false
    end

    ngx.log(ngx.DEBUG, "get userinfo by accessid,result: \n", inspect(user_info))
    if not user_info or not next(user_info) then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "get user_info failed non-exist")
        return "4021", "0000", false
    end

    local sync_flag = user_info["info:syncflag"]
    if bucket then
        local rowkey = user_info["ver:tag"] .. "_" .. bucket
        local code, ok, rebody1 = metadb:get("bucket", rowkey, hbase_body)
        if not ok then
            ngx.log(ngx.ERR,  "metadb failed to get bucket :" .. code)
            return "4030", code, false
        end

        if not rebody1 or not next(rebody1) then
            ngx.log(ngx.ERR,"bucket inexistent :" .. bucket)
            return "4031", code, false
        end
        sync_flag = sync_flag or rebody1["info:syncflag"]
    end

    sync_flag = sync_flag or "false"
    local ok, jsoninfo = pcall(json.encode, {["sync"] = sync_flag})
    if not ok then
        ngx.log(ngx.ERR, "failed to encode userinfo for get user")
        return "4022", "0000", false
    end

    ngx.header["Content-Length"] = #jsoninfo
    ngx.header["Content-Type"] = "application/json"
    ngx.print(jsoninfo)

    return "0000", "0000", true
end

local function delete_sync(req_info)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, ", Enter delete_sync")
    local args = req_info["args"]
    local uid = args["uid"]
    local bucket = args["bucket"]

    if not uid then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " essential args absent")
        return "0010", "0000", false
    end

    local code, ok, rebody = metadb:get("userid", uid, {"info:accessid"})
    if not ok then 
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to query userid table, code", code)
        return "3002", code, false
    end  

    if not next(rebody) then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " uid inexistent.")
        return "4027", "0000", false
    end

    local accessid = rebody["info:accessid"]
    local code, ok, rebody = metadb:get("user", accessid, {"ver:tag"})
    if not ok then 
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to query user table, code=", code)
        return "3002", code, false
    end  

    if not next(rebody) then 
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " accessid inexistent." .. accessid)
        return "4027", "0000", false
    end

    local table_name = "user"
    local rowkey = accessid
    if bucket then
        table_name = "bucket"
        rowkey = rebody["ver:tag"] .. "_" .. bucket
        local code, ok, rebody1 = metadb:get("bucket", rowkey, {"ver:tag"})
        if not ok then
            ngx.log(ngx.ERR,  "metadb failed to get bucket :" .. code)
            return "4030", code, false
        end

        if not rebody1 or not next(rebody1) then
            ngx.log(ngx.ERR,"bucket inexistent :" .. bucket)
            return "4031", code, false
        end
    end

    local mtime = string.format("%0.3f", ngx.now())
    local seq_no = args["seq_no"] or gen_seq_no(mtime)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " seq_no generated for user ", uid, ". seq_no=", seq_no)

    local code, ok = sync_meta.write_metalog(req_info, seq_no)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " write_metalog failed. code=", code) 
        return "4041", code, false
    end

    local hbase_body = {"ver", "seq", seq_no}
    local delete_columns = {"info:syncflag"}
    local compare_op = metadb.CompareOp.GREATER
    local code, ok = metadb:checkAndMutateRow(table_name, rowkey, "ver", "seq", compare_op, seq_no, hbase_body, delete_columns)
    if not ok then
        if "1121" == code then --failed because of condition-check-failure
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " checkandmutate outdated. rowkey=", rowkey, " seq_no=", seq_no) 
        else
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " checkandmutate failed. code=", code, " rowkey=", rowkey, " seq_no=", seq_no)
        end

        return "4028", code, false
    end

    sync_meta.sync_req(req_info, seq_no)

    ngx.status = ngx.HTTP_OK
    return "0000", "0000", true
end

local function get_total_stats()
    local columns = {
        "info:uid",
        "stats",
    }

    local code, ok, scanner = metadb:openScanner("user", metadb.MIN_CHAR, metadb.MAX_CHAR, columns, 1000)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "HBase failed to open scanner, innercode=", code)
        return "2004", code, false
    end

    local total_usage = {
        size_bytes = 0,
        objects = 0,
    }

    local user_usage = {}

    local batch = sp_conf.config["hbase_config"]["max_scan_once"] or 1000
    while true do
        local code, ok, ret = metadb:scan("user", scanner, batch)
        if not ok then
            local code, ok = metadb:closeScanner(scanner)
            if not ok then
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "HBase failed to close scanner, innercode=", code)
            end
            return "2005", code, false
        end

        if 0 == #ret then
            break
        end

        for _, v in ipairs(ret) do
            local uid = v.values["info:uid"]
            if uid then
                local size_bytes = v.values["stats:size_bytes"] or 0
                local objects = v.values["stats:objects"] or 0
                local user_info = {
                    owner = uid,
                    usage = {
                        size_bytes = size_bytes,
                        objects = objects,
                    },
                }
                user_usage[#user_usage + 1] = user_info
                total_usage.size_bytes = total_usage.size_bytes + size_bytes
                total_usage.objects = total_usage.objects + objects
            else
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "invalid user having unfound uid ", v.key)
            end
        end
    end

    local code, ok = metadb:closeScanner(scanner)
    if not ok then
        ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ",  "HBase failed to close scanner, innercode=", code)
    end

    local stats_table = {
        total_usage = total_usage,
        user_usage = user_usage,
    }

    local ok, jsoninfo = pcall(json.encode, stats_table)
    if not ok then
        ngx.log(ngx.ERR, "failed to encode stats table")
        return "4022","0000", false
    end

    ngx.header["Content-Length"] = #jsoninfo
    ngx.header["Content-Type"] = "application/json"
    ngx.print(jsoninfo)

    return "0000", "0000", true

end

local function get_stats(req_info)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, ", Enter get_stats")
    local args = req_info["args"]
    local uid = args["uid"]
    local bucket = args["bucket"]

    if not uid then
        return get_total_stats()
    end

    local code, ok, rebody = metadb:get("userid", uid, {"info:accessid"})
    if not ok then 
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to query userid table, code", code)
        return "3002", code, false
    end  

    if not next(rebody) then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " uid not existed in userid table: " .. uid)
        return "4027", "0000", false
    end

    local accessid = rebody["info:accessid"]
    local hbase_body = {
        "stats:size_bytes", 
        "stats:objects", 
        "ver:tag",
    }
    local code, ok, rebody = metadb:get("user", accessid, hbase_body)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "get user_info failed(not client.get err and client.get error), code is:" .. code)
        return "4030", code, false
    end

    ngx.log(ngx.ERR, "get userinfo by accessid,result: \n", inspect(rebody))
    if not rebody or not next(rebody) then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "get user_info non-exist")
        return "4021", "0000", false
    end

    if bucket then
        local rowkey = rebody["ver:tag"] .. "_" .. bucket
        code, ok, rebody = metadb:get("bucket", rowkey, hbase_body)
        if not ok then
            ngx.log(ngx.ERR,  "metadb failed to get bucket :" .. code)
            return "4030", code, false
        end

        if not rebody or not next(rebody) then
            ngx.log(ngx.ERR,"bucket inexistent :" .. bucket)
            return "4031", code, false
        end
    end

    local stats_table = {
        owner = uid,
        bucket = bucket,
        usage = {
            size_bytes = rebody["stats:size_bytes"] or 0,
            objects = rebody["stats:objects"] or 0,
        },
    }

    local ok, jsoninfo = pcall(json.encode, stats_table)
    if not ok then
        ngx.log(ngx.ERR, "failed to encode stats table")
        return "4022", "0000", false
    end

    ngx.header["Content-Length"] = #jsoninfo
    ngx.header["Content-Type"] = "application/json"
    ngx.print(jsoninfo)

    return "0000", "0000", true
end

local function check_ceph_args(args)
    if not args["id"] or args["id"] == "" then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " id is nil")
        return "6200"
    end

    local state = args["state"]
    if state and state ~= "" and state ~= "OK" and state ~= "WARN" and state ~= "ERR" then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " state ", state," is invalid. it must be OK, WARN or ERR")
        return "6201"
    end

    local weight = args["weight"]
    if weight and weight ~= "" then
        local num_weight = tonumber(weight)
        if num_weight < 0 then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " weight ", num_weight, " is invalid, it must be 0 or positive number")
            return "6202"
        end
    end

    return "0000"
end

local function create_ceph(req_info)
    local args = req_info["args"]
    local code = check_ceph_args(args)
    if code ~= "0000" then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " args invalid for create_ceph")
        return code, "0000", false
    end

    if not args["state"] or args["state"] == "" then
        args["state"] = "OK"
    end
    if not args["weight"] or args["weight"] == "" then
        args["weight"] = "100"
    end

    local body = {
        "info", "state", args["state"],
        "info", "weight", args["weight"],
    }
    local code,ok = metadb:checkAndPut("ceph", args["id"], "info", "state", nil, body)
    if not ok then
        if "1121" == code then
            ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ceph ", args["id"], " already exists, you cannot create it.")
            ngx.status = ngx.HTTP_CONFLICT
        else
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to create ceph ", args["id"], " code=", code)
            return "6204", code, false
        end
    end

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " succeeded to create ceph ", args["id"])
    return "0000", "0000", true 
end

local function query_ceph(req_info)
    local args = req_info["args"]

    local from = args["from"]

    if not from or from == "" then
        from = "hbase"    --by default, query from hbase
    end

    if from ~= "hbase" and from ~= "memory" then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " from ", from, " is invalid, only 'hbase' or 'memory' is allowed")
        return "6211", "0000", false
    end

    if from == "hbase" then
        --there should not be too many ceph clusters, so just scan all of them
        local code,ok,ret = metadb:quickScanHash("ceph", metadb.MIN_CHAR, metadb.MAX_CHAR, nil, 1000)
        if not ok then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to scan table 'ceph'. code=", code)
            return "6205", code, false
        end

        local id = args["id"]
        if id and id ~= "" then --if 'id' is specified
            if not ret[id] then
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ceph ", id, " does not exist")
                return "6206", "0000", false
            end
            --remove all except 'id'
            for i,_ in pairs(ret) do
                if i ~= id then
                    ret[i] = nil
                end
            end
        end

        local ret_table = {}
        for i,c in pairs(ret) do
            local ceph = {}
            ceph["id"] = i
            ceph["state"] = c["info:state"]
            ceph["weight"] = c["info:weight"]
            ceph["gateways"] = {}
            local code,ok,rgws = metadb:quickScan("rgw", i.."-"..metadb.MIN_CHAR, i.."-"..metadb.MAX_CHAR, nil, 1000)
            if not ok then
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to scan table 'rgw'. code=", code)
                return "6207", code, false
            end
            for _,r in ipairs(rgws) do
                local rkey = r.key
                local rval = r.values

                local rstate = rval["info:state"]
                local server_port = string.sub(rkey, #i+2, -1)
                table.insert(ceph["gateways"], server_port .. " - " .. rstate)
            end
            table.insert(ret_table, ceph)
        end

        local ret_str = stringify(ret_table, false)
        ngx.print(ret_str)
    else  -- from "memory"
        ngx.print(stringify(objstore, false))
    end

    return "0000", "0000", true
end

local function modify_ceph(req_info)
    local args = req_info["args"]
    local code = check_ceph_args(args)
    if code ~= "0000" then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " args invalid for modify_ceph")
        return code, "0000", false
    end

    local body = {}
    if args["state"] and args["state"] ~= "" then
        table.insert(body, "info")
        table.insert(body, "state")
        table.insert(body, args["state"])
    end
    if args["weight"] and args["weight"] ~= "" then
        table.insert(body, "info")
        table.insert(body, "weight")
        table.insert(body, args["weight"])
    end

    if nil == next(body) then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " nothing to modify. you must modify at least one of 'state' and 'weight'")
        return "6210", "0000", false
    end

    local code,ok = metadb:checkAndMutateRow("ceph", args["id"], "info", "state", metadb.CompareOp.NOT_EQUAL, "", body, nil)
    if not ok then
        if "1121" == code then
            ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ceph ", args["id"], " does not exist, you cannot modify it.")
            return "6208", "0000", false
        else
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to modify ceph ", args["id"], " code=", code)
            return "6209", code, false
        end
    end

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " succeeded to modify ceph ", args["id"])
    return "0000", "0000", true 
end

local function add_rgw(req_info)
    local args = req_info["args"]

    local ceph_id = args["cid"]
    if not ceph_id or ceph_id == "" then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " 'cid' is missing for add_rgw")
        return "6212", "0000", false
    end

    local server = args["server"]
    if not server or server == "" then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " 'server' is missing for add_rgw")
        return "6213", "0000", false
    end

    local port = args["port"]
    if not port or port == "" then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " 'port' is missing for add_rgw")
        return "6214", "0000", false
    end

    local code,ok,ret = metadb:get("ceph", ceph_id, {"info:state"})
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " cannot check if the ceph ", ceph_id, " exists or not, due to hbase failure. code=", code)
        return "6203", code, false
    end

    local rowkey = ceph_id .. "-" .. server .. ":" .. port

    if nil == ret["info:state"] then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " cannot add rgw ", rowkey, " because ceph ", ceph_id, " does not exist")
        return "6215", "0000", false
    end

    local code,ok = metadb:checkAndPut("rgw", rowkey, "info", "state", nil, {"info", "state", "OK"})
    if not ok then
        if code == "1121" then
            ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " rgw ", rowkey, " already exists, cannot add it")
            ngx.status = ngx.HTTP_CONFLICT
        else
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to add rgw ", rowkey, " due to hbase failure, code=", code)
            return "6216", code, false
        end
    end

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " succeeded to add rgw ", rowkey)
    return "0000", "0000", true
end

local function del_rgw(req_info)
    local args = req_info["args"]

    local ceph_id = args["cid"]
    if not ceph_id or ceph_id == "" then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " 'cid' is missing for del_rgw")
        return "6212", "0000", false
    end

    local server = args["server"]
    if not server or server == "" then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " 'server' is missing for del_rgw")
        return "6213", "0000", false
    end

    local port = args["port"]
    if not port or port == "" then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " 'port' is missing for del_rgw")
        return "6214", "0000", false
    end

    local rowkey = ceph_id .. "-" .. server .. ":" .. port
    local code,ok = metadb:checkAndMutateRow("rgw", rowkey, "info", "state", metadb.CompareOp.NOT_EQUAL, nil, nil, {"info:state"})
    if not ok then 
        if code == "1121" then 
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " rgw ", rowkey, " doesn't exist")
            ngx.status = ngx.HTTP_NOT_FOUND
        else
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to delete rgw ", rowkey, " due to hbase failure, code=", code)
            return "6217", code, false
        end
    else
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " succeeded to delete rgw ", rowkey)
        ngx.status = ngx.HTTP_NO_CONTENT
    end

    return "0000", "0000", true
end

local function do_default(req_info)
    --todo: unsupported op
    return "4033", "0000", false
end

local op_table = {
    CREATE_USER = create_user,
    QUERY_USER = get_userinfo,
    DELETE_USER = delete_user,

    SET_PERMISSION = set_permission,
    GET_PERMISSION = get_permission,
    DELETE_PERMISSION = delete_permission,

    SET_SYNC = set_sync,
    GET_SYNC = get_sync,
    DELETE_SYNC = delete_sync,
    
    SET_QUOTA = set_quota,
    GET_QUOTA = get_quota,

    GET_STATS = get_stats,

    NEW_CEPH = create_ceph,
    GET_CEPH = query_ceph,
    SET_CEPH = modify_ceph,
    ADD_RGW  = add_rgw,
    DEL_RGW  = del_rgw,

    __default = do_default,
}

function _M.process(req_info)
    local op_func = op_table[req_info.op_type]
    if op_func then
        return op_func(req_info)
    else
        return rsync_op:process_rsync(metadb, req_info)
    end

end

return _M
