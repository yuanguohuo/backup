local _M = {
    _VERSION = '0.01',
}

local sp_conf = require("storageproxy_conf")
local json = require('cjson')
local ngxprint = require("printinfo")
local ngx_re_gsub   = ngx.re.gsub

local ngx_re = require "ngx.re"

local xmlsimple = require("xmlSimple")
local XmlParser = xmlsimple.XmlParser

local ok, new_tab = pcall(require, "table.new")
if not ok or type(new_tab) ~= "function" then
    new_tab = function (narr, nrec) return {} end
end

local ok,utils = pcall(require, "common.utils")
if not ok or not utils then
    error("failed to load common.utils:" .. (utils or "nil"))
end

local ok,objstore = pcall(require, "storageproxy_objstore")
if not ok or not objstore then
    error("failed to load storageproxy_objstore:" .. (objstore or "nil"))
end

local ok,inspect = pcall(require, "inspect")
if not ok or not inspect then
    error("failed to load inspect:" .. (inspect or "nil"))
end

local ok,uuid = pcall(require, "resty.uuid")
if not ok or not uuid then
    error("failed to load resty.uuid:" .. (uuid or "nil"))
end

local ok,md5 = pcall(require, "resty.md5")
if not ok or not md5 then
    error("failed to load resty.md5:" .. (md5 or "nil"))
end

local ok, http = pcall(require, "resty.http")
if not ok or not http then
    error("failed to load resty.http:" .. (http or "nil"))
end

local ok, math = pcall(require, "math")
if not ok or not math then
    error("failed to load math:" .. (math or "nil"))
end

local ok, oindex = pcall(require, "storageproxy_oindex")
if not ok or not oindex or type(oindex) ~= "table" then
    error("failed to load storageproxy_oindex:" .. (oindex or "nil"))
end

local ok, fs = pcall(require, "storageproxy_fs")
if not ok or not fs then
    error("failed to load storageproxy_fs:" .. (fs or "nil"))
end

local ok, hash = pcall(require, "hash.hash")
if not ok or not hash then
    error("failed to load hash.lua:" .. (hash or "nil"))
end

local ok, iconv = pcall(require, "resty.iconv")
if not ok or not iconv then
    error("failed to load resty.iconv:" .. (iconv or "nil"))
end

local sync_meta = require("sync_meta")

local stringify   = utils.stringify
local mark_event  = utils.mark_event
local gen_seq_no  = utils.gen_seq_no

local str = require "resty.string"
local mfloor = math.floor
--[[
local ok, rados = pcall(require, "rados_op")
if not ok or not rados then
    error("failed to load rados_op :" .. (rados or "nil"))
end
]]

local EMPTY_MD5 = sp_conf.config["constants"]["empty_md5"]
local EMPTY_MD5_HEX = sp_conf.config["constants"]["empty_md5_hex"]
local EMPTY_MD5_BASE64 = sp_conf.config["constants"]["empty_md5_base64"]

local sync_cfg = sp_conf.config["sync_config"]
local DATALOG_MAX = sync_cfg["datalog"]["max"]
local DATALOG_NAME = sync_cfg["datalog"]["name"]
local dict = ngx.shared.statics_dict

local EXPIRE_NAME = sp_conf.config["expire"]["name"]
local EXPIRE_MAX = sp_conf.config["expire"]["max"]

local DELETE_OBJ_NAME = sp_conf.config["deleteobj"]["name"]
local DELETE_OBJ_MAX = sp_conf.config["deleteobj"]["max"]
local SLASH = "/"
local DIR_MARKER = string.char(1)
local BIGGER_THAN_DIR_MARKER = string.char(2)

local FS_USER_AGENT = "aws%-sdk%-go" -- user-agent of goofys is: "aws-sdk-go/1.0.0 (go1.6.3; linux; amd64)"

local function get_client_type(user_agent)
    if not user_agent or #user_agent == 0 then
        return "s3"
    end

    local i,j = string.find(user_agent, FS_USER_AGENT)
    if i then
        return "fs"
    end

    return "s3"
end

--return: ok
local function write_datalog(metadb, buck_name, obj_name, seq_no, datalog_body)
    local shard_id = hash.hash(buck_name..obj_name, DATALOG_MAX)
    local logname = utils.get_shard(DATALOG_NAME, DATALOG_MAX, shard_id)
    local rowkey = logname.."_"..seq_no

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " buck_name=", buck_name, " obj_name=", obj_name, " seq_no=", seq_no,
                      " shard_id=", shard_id, " logname=", logname, " rowkey=", rowkey)

    local innercode,ok = nil,nil
    for r=1,3 do
        innercode,ok = metadb:put("datalog", rowkey, datalog_body)
        if ok then
            return true
        end
    end

    ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " write datalog failed. innercode=", innercode)
    return false
end

local function incr_stats(metadb, user_key, buck_key, incr_size, incr_objs, is_del_buck)
    local changes = new_tab(8, 0)
    if 0 ~= incr_size then
        table.insert(changes, "stats")
        table.insert(changes, "size_bytes")
        table.insert(changes, incr_size)
    end
    if 0 ~= incr_objs then
        table.insert(changes, "stats")
        table.insert(changes, "objects")
        table.insert(changes, incr_objs)
    end

    local succ = 2
    if #changes > 0 then
        if not is_del_buck then
            local code,ok = metadb:increment("bucket", buck_key, changes)
            if not ok then
                ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " failed to increase bucket stats. buck_key=", buck_key)
                succ = succ - 1
            end
        end

        local code,ok = metadb:increment("user", user_key, changes)
        if not ok then
            ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " failed to increase user stats. user_key=", user_key)
            succ = succ - 1
        end
    end

    if succ ~= 2 then
        ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " incr_stats failed")
        return false
    end

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " incr_stats succeeded. user_key=", user_key, " buck_key=", buck_key,
                      " incr_size=", incr_size, " incr_objs=", incr_objs)
    return true
end

local function insert_placeholder(metadb, obj_key, buck_name, obj_name, delete_time, seq_no)
    local placeholder_body =
    {
        "info",      "bucket",    buck_name,
        "info",      "object",    obj_name,
        "info",      "rmtime",    delete_time,
        "ver",       "seq",       seq_no,
    }

    local innercode,ok = nil,nil
    for r=1,3 do
        innercode,ok = metadb:put("placeholder", obj_key, placeholder_body)
        if ok then
            return true
        end
    end

    ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " write placeholder failed. innercode=", innercode)
    return false
end

--return innercode,ok,allow
local function check_placeholder(metadb, obj_key, seq_no)
    local innercode,ok,placeholder = nil,nil,nil
    for r=1,3 do
        innercode,ok,placeholder = metadb:get("placeholder", obj_key, {"ver:seq"})
        if ok then
            break
        end
    end

    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " get placeholder failed. innercode=", innercode)
        return innercode, false, nil
    end

    if not placeholder["ver:seq"] then
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " no placeholder for ", obj_key)
        return "0000", true, true
    end

    if placeholder["ver:seq"] < seq_no then
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " op seq_no is newer than placeholder. op seq_no=", seq_no, " placeholder seq_no=", placeholder["ver:seq"])
        return "0000", true, true
    end

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " op seq_no is older than placeholder. op seq_no=", seq_no, " placeholder seq_no=", placeholder["ver:seq"])
    return "0000", true, false
end

--expire_info
--  ["btag"] = "bucket tag"
--  ["obj"] = "objectname"
--  ["tag"] = "object tag"
--  ["ctime"] = "create time"

--  return
--  code,false put metadb failed
--  0,ture ok
local function write_expire(metadb ,expire_info)
    local btag  = expire_info["buck_vtag"]
    local obj   = expire_info["obj_name"]
    local seqno = expire_info["seqno"]
    local ctime = expire_info["ctime"]
    local cdate = utils.toDATE(ctime)

    local id = hash.hash(obj, EXPIRE_MAX)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " write expire id:",id)
    local expire = utils.get_shard(EXPIRE_NAME, EXPIRE_MAX, id)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "write expire :", expire)
    local rowkey = expire.."_"..cdate.."_"..btag.."_"..obj.."_"..seqno
    local hbase_body = {
        "info", "ctime", ctime,
        "info", "seqno", seqno,
    }

    local code = nil
    local ok   = nil
    local succ
    for r=1,3 do
        code,ok = metadb:put("expire_object", rowkey, hbase_body)
        if ok then
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " succeeded to put expire table. obj_name=", obj)
            succ = true
            break
        else
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " succeeded to put expire table. obj_name=", obj, " errorcode=", code, " retry=", r)
        end
    end

    if not succ then
        ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " failed to put expire table. obj_name=", obj)
    end

    return code,ok
end

-- object  : object name
--
-- return shard
function _M.get_delete_obj_shard(object)
    local id = hash.hash(object, DELETE_OBJ_MAX)
    local shard = utils.get_shard(DELETE_OBJ_NAME, DELETE_OBJ_MAX, id)
    return shard
end

function _M.process_service(self, metadb, reqinfo)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### Enter process_service")

    local user_info = reqinfo["user_info"]

    local args = reqinfo["args"]
    local marker = args["marker"] or ""
    local delimiter = args["delimiter"]

    local maxkeys = nil
    if nil ~= args["max-keys"] and '' ~= args["max-keys"] then
        maxkeys = tonumber(args["max-keys"])
        if nil ~= maxkeys then
            if maxkeys > sp_conf.config["s3_config"]["maxkeys"]["max"] then
                maxkeys = sp_conf.config["s3_config"]["maxkeys"]["max"]
            end
        else
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "request max-keys is error!!! max-key is :", args["max-keys"])
            return "0010","0000",false
        end
    end
    if nil == maxkeys then
        maxkeys = sp_conf.config["s3_config"]["maxkeys"]["default"]
    end

    if fs.is_root_ls() then
        return fs.root_ls(metadb, marker, delimiter, maxkeys)
    end

    if fs.is_user_ls() then
        return fs.user_ls(metadb, user_info, marker, delimiter, maxkeys)
    end

    local head = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" ..
                 "<ListAllMyBucketsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">" ..
                     "<Owner>" ..
                         "<ID>" ..
                             user_info["info:uid"] ..
                         "</ID>" ..
                         "<DisplayName>" ..
                             user_info["info:displayname"] ..
                         "</DisplayName>" ..
                     "</Owner>" ..
                     "<Buckets>"

    local tail =     "</Buckets>" ..
                 "</ListAllMyBucketsResult>"

    local response_headers = sp_conf.config["s3_GET_Service"]["s3_response_headers"]
    for k,v in pairs(response_headers) do
        ngx.header[k] = v
        -- if "x-amz-request-id" = k then
        -- 	--generate value
        -- end
    end
    ngx.header["Content-Type"] = "application/xml"

    --send the head of the xml
    ngx.print(head)
    --ngx.send_headers()
--    ngx.headers_sent
    local usertag = user_info["ver:tag"]
    local code,ok,scanner = metadb:openScanner("bucket", usertag.."_"..metadb.MIN_CHAR, usertag.."_"..metadb.MAX_CHAR,nil,1000)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "HBase failed to open scanner, innercode=", code)
        return "2004", code, false
    end

    local prefixes = reqinfo["permission_prefixes"]
    local batch = sp_conf.config["hbase_config"]["max_scan_once"] or 1000
    while true do
        local code,ok,ret = metadb:scan("bucket",scanner, batch)
        if not ok then
            return "2005", code, false
        end
        local count = #ret
        local out = {}
        local index = 1
        for k,v in pairs(ret) do
            local name = string.gsub(v.key, user_info["ver:tag"] .. "_", "")
            local cdate = utils.toUTC(v.values["info:ctime"])
            local element = "<Bucket><Name>"..name.."</Name><CreationDate>"..cdate.."</CreationDate></Bucket>"

            if prefixes and next(prefixes) then
                for _, prefix in ipairs(prefixes) do
                    if string.byte(prefix, #prefix) == string.byte("*") then
                        local new_prefix = string.sub(prefix, 1, -2)
                        if new_prefix == string.sub(name, 1, #new_prefix) then
                            out[index] = element
                            index = index + 1
                            break
                        end
                    else
                        if name == prefix then
                            out[index] = element
                            index = index + 1
                            break
                        end
                    end
                end
            else
                out[index] = element
                index = index + 1
            end
        end

        --send current batch
        ngx.print(table.concat(out))

        if count < batch then  --scan is over
            break
        end
    end

    --send the tail of the xml
    ngx.print(tail)

    local code,ok = metadb:closeScanner(scanner)
    if not ok then
        ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ",  "HBase failed to close scanner, innercode=", code)
    end

    return "0000", "0000", true
end

local function put_bucket(metadb, req_info)
    -- Each label must start and end with a lowercase letter or a number.
    -- Bucket names must be at least 3 and no more than 255 characters long
    -- Bucket names can contain lowercase letters, numbers, and hyphens.
    local ok = ngx.re.match(req_info["op_info"]["bucketname"], "^[a-z\\d]([A-Za-z\\d\\.\\-_]{2,254})$")
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " bucket is name error! bucket is name:", req_info["op_info"]["bucketname"])
        return "2007", "0000", false
    end

    -- Bucket names must not be formatted as an IP address
    local ok = ngx.re.match(req_info["op_info"]["bucketname"], "^(\\d{1,3}\\.){3}(\\d{1,3})$")
    if ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " bucket is name error! bucket is name:", req_info["op_info"]["bucketname"])
        return "2007", "0000", false
    end

    local mtime = string.format("%0.3f", ngx.now())
    local args = req_info["args"]
    local seq_no = args["seq_no"] or gen_seq_no(mtime)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " seq_no generated for bucket ", bucket, ". seq_no=", seq_no)

    local code, ok = sync_meta.write_metalog(req_info, seq_no)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " write_metalog failed. code=", code) 
        return "4041", code, false
    end

    local bucket = req_info["op_info"]["bucketname"]
    local rowkey = req_info["user_info"]["ver:tag"] .. "_" .. bucket 
    local vertag = uuid()
    local hbase_body = {
        "info", "ctime", mtime,
        "info", "mtime", mtime,
        "info", "accessid", req_info["auth_info"]["accessid"],
        "quota", "enabled", "false",
        "quota", "size_kb", -1,
        "quota", "objects", -1,
        "ver", "tag", vertag,
        "ver", "seq", seq_no,
    }


    local code, ok, rebody = metadb:get("bucket", rowkey, {"ver:tag"})
    if not ok then 
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to query bucket, code=", code)
        return "3002", code, false
    end  

    if rebody["ver:tag"] then
        hbase_body = {
            "info", "mtime", mtime,
            "ver", "seq", seq_no,
        }
    end

    local compare_op = metadb.CompareOp.GREATER
    local code, ok = metadb:checkAndMutateRow("bucket", rowkey, "ver", "seq", compare_op, seq_no, hbase_body, nil)
    if not ok then
        if "1121" == code then --failed because of condition-check-failure
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " checkandmutate outdated. rowkey=", rowkey, " seq_no=", seq_no) 
        else
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " checkandmutate failed. code=", code, " rowkey=", rowkey, " seq_no=", seq_no)
        end
        
        return "2003", code, false
    end

    sync_meta.sync_req(req_info, seq_no)

    ngx.status = ngx.HTTP_OK
    ngx.header["Content_Length"] = 0
    return "0000", "0000", true
end

local function delete_bucket(metadb, req_info)
    --get bucket tag
    local useretag = req_info["user_info"]["ver:tag"]
    local bucketname = req_info["op_info"]["bucketname"]
    local bucketrowkey = useretag.."_"..bucketname

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " bucketrowkey is ", bucketrowkey)
    local hbase_body = {
        "info:ctime",
        "info:mtime",
        "info:accessid",
        "quota:enabled",
        "quota:size_kb",
        "quota:objects",
        "stats:size_bytes",
        "stats:objects",
        "ver:tag",
    }
    local code,ok,rebody1 = metadb:get("bucket", bucketrowkey, hbase_body)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " metadb failed to get bucket meta accessid :"..code)
        return "2001", code, false
    end

    local existent = true
    if not rebody1 or not next(rebody1) or not rebody1["info:accessid"] or not rebody1["ver:tag"] then
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " bucket does not exist!")
        existent = false
    end

    local mtime = string.format("%0.3f", ngx.now())
    local args = req_info["args"]
    local seq_no = args["seq_no"] or gen_seq_no(mtime)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " seq_no generated for bucket ", bucketname, ". seq_no=", seq_no)

    local code, ok = sync_meta.write_metalog(req_info, seq_no)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " write_metalog failed. code=", code) 
        return "4041", code, false
    end

    if existent then
        local compare_op = metadb.CompareOp.GREATER
        local delete_families = {
            "info:ctime",
            "info:mtime", 
            "info:accessid", 
            "info:syncflag", 
            "quota:enabled", 
            "quota:size_kb", 
            "quota:objects",
            "stats:size_bytes",
            "stats:objects",
            "ver:tag", 
            "ver:seq", 
        }
        local code, ok = metadb:checkAndMutateRow("bucket", bucketrowkey, "ver", "seq", compare_op, seq_no, nil, delete_families)
        if not ok then
            if "1121" == code then --failed because of condition-check-failure
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " checkandmutate outdated. bucketrowkey=", bucketrowkey, " seq_no=", seq_no)
            else
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " checkandmutate failed. code=", code, " bucketrowkey=", bucketrowkey, " seq_no=", seq_no)
            end
            return "0011", code, false
        end

        local userAccId = rebody1["info:accessid"]
        local dec_size  = rebody1["stats:size_bytes"] or 0
        local dec_objs  = rebody1["stats:objects"] or 0

        incr_stats(metadb, userAccId, bucketrowkey, 0-dec_size, 0-dec_objs, true)

        local d_rowkey = bucketrowkey.."_"..rebody1["ver:tag"]
        local hbody = utils.out_db_to_in(rebody1)
        local code,ok = metadb:put("delete_bucket", d_rowkey, hbody)
        if not ok then
            --todo garbage handle
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", "delete bucket, put delete_bucket failed!!! d_rowkey:", d_rowkey)
        end
    end

    sync_meta.sync_req(req_info, seq_no)

    if not existent then -- for s3tests, delete a inexistent bucket should return 404
        return "0012", "0000", false
    end

    ngx.status = ngx.HTTP_NO_CONTENT
    return "0000", "0000", true
end

local function last_slash_enc(src)
    local dest = src
    for i = #src, 1, -1 do
        if string.byte(src, i) == string.byte(SLASH) then
            dest = string.sub(src, 1, i) .. DIR_MARKER .. string.sub(src, i + 1)
            break
        end
    end
    ngx.log(ngx.DEBUG, string.format("src=%s, dest=%s", src, dest))
    return dest
end

local function last_slash_dec(src)
   local dest = ngx.re.gsub(src, DIR_MARKER, "")
   ngx.log(ngx.DEBUG, string.format("src=%s, dest=%s", src, dest))
   return dest
end

-- for a list objects, two steps:
-- 1. scan all files(after prefix, no slash in key)
-- 2. scan all sub directories(after prefix, equal or more than one slash in key)
local function list_objects_no_delimiter(metadb, table_name, columns, marker, prefix, max)
    local s_key, e_key

    if 0 >= max then
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "list objects no delimiter max is less 0, max is:", max)
        return true, {}, {}, nil, false
    end

    local sip, _ = string.find(prefix, SLASH)
    assert(sip)
    local sim, _ = string.find(marker, SLASH, #prefix + 1)

    local scan_file = false
    if not sim then
        scan_file = true
        if marker <= prefix then                               --prefix: tag_/ab, marker: tag_/ab       -> s_key = tag_/#ab, e_key = tag_/#ab(MAX)
            s_key = last_slash_enc(prefix)
        else                                                   --prefix: tag_/ab, marker: tag_/abcd     -> s_key = tag_/#abcd(MIN), e_key = tag_/#ab(MAX)
            s_key = last_slash_enc(marker).. metadb.MIN_CHAR
        end
        e_key = last_slash_enc(prefix) .. metadb.MAX_CHAR
    else                                                       --prefix: tag_/ab, marker: tag_/abc/de   -> s_key = tag_/abc/#de(MIN), e_key = tag_/ab(MAX)
        s_key = last_slash_enc(marker).. metadb.MIN_CHAR
        e_key = prefix .. metadb.MAX_CHAR
    end

    -- add filter info:file = true
    --local filter = "SingleColumnValueFilter('info','dir',!=,'binary:".."true".."')"
    local code, ok, scanner = metadb:openScanner(table_name, s_key, e_key, columns)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "HBase failed to open scanner, innercode=", code)
        return false
    end

    local count = 0
    local truncated = true
    local result = {}
    local common_prefixes = {}
    local batch = sp_conf.config["hbase_config"]["max_scan_once"] or 100
    local r_index = 1
    while count < max and truncated do
        local scan_num = math.min(batch, max - count)
        local code, ok, ret, num = metadb:scan(table_name, scanner, scan_num)
        if not ok then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "HBase failed to scan table="..table_name..", innercode="..code)
            return false
        end

        for i,v_tab in pairs(ret) do
            local key = v_tab.key

            result[r_index] = v_tab
            r_index = r_index + 1
            count = count + 1

            if (count >= max) then
                truncated = true
                local code, ok = metadb:closeScanner(scanner)
                if not ok then
                    ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ",  "HBase failed to close scanner, innercode=", code)
                    return false
                end
                return true, result, {}, nil, truncated
            end
        end

        if num < scan_num then
            -- scan_file = true means we need change prefix and s_key, then close and init a new scan
            if scan_file then
                scan_file = false
                local code, ok = metadb:closeScanner(scanner)
                if not ok then
                    ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ",  "HBase failed to close scanner, innercode=", code)
                    return false
                end
                s_key = prefix .. BIGGER_THAN_DIR_MARKER
                e_key = prefix .. metadb.MAX_CHAR

                code, ok, scanner = metadb:openScanner(table_name, s_key, e_key, columns)
                if not ok then
                    ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "HBase failed to open scanner, innercode=", code)
                    return false
                end
            else
                truncated = false
                break
            end
        end
    end

    local code, ok = metadb:closeScanner(scanner)
    if not ok then
        ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ",  "HBase failed to close scanner, innercode=", code)
        return false
    end
    return true, result, {}, nil, truncated
end

local function list_objects_s3_delimiter(metadb, table_name, columns, marker, prefix, delimiter, max)
    assert(SLASH == delimiter)

    if 0 >= max then
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "list objects s3 delimiter max is less 0, max is:", max)
        return true, {}, {}, nil, false
    end

    local s_key, e_key
    local bigger_than_delim = string.char(string.byte(SLASH) + 1)  --bigger_than_delim = '0'

    local sip, _ = string.find(prefix, SLASH)
    assert(sip)
    local sim, _ = string.find(marker, SLASH, #prefix + 1)

    local scan_file = false
    if not sim then
        scan_file = true
        if marker <= prefix then                               --prefix: tag_/ab, marker: tag_/ab       -> s_key = tag_/#ab, e_key = tag_/#ab(MAX)
            s_key = last_slash_enc(prefix)
        else                                                   --prefix: tag_/ab, marker: tag_/abcd     -> s_key = tag_/#abcd(MIN), e_key = tag_/#ab(MAX)
            s_key = last_slash_enc(marker).. metadb.MIN_CHAR
        end
        --prefix = last_slash_enc(prefix)
        e_key = last_slash_enc(prefix) .. metadb.MAX_CHAR
    else                                                       --prefix: ab/c, marker: ab/cde/   -> s_key = ab/cde0, e_key = ab/c(MAX)
        assert(sim == #marker)
        s_key = string.sub(marker, 1, sim - 1) .. bigger_than_delim
        e_key = prefix .. metadb.MAX_CHAR
    end

    --if marker points at a common prefix, fast forward it into its upperbound string
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "s_key: "..s_key..",e_key: "..e_key)

    --local filter = "SingleColumnValueFilter('info','dir',!=,'binary:".."true".."')"
    local code,ok,scanner = metadb:openScanner(table_name, s_key, e_key, columns)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "HBase failed to open scanner, innercode=", code)
        return false
    end

    local batch = sp_conf.config["hbase_config"]["max_scan_once"] or 100
    local c_index = 1
    local r_index = 1
    local result = {}
    local common_prefixes = {}
    local truncated = true
    local last_cp
    local count = 0
    local next_marker

    while count < max and truncated do
        local scan_num = math.min(batch, max - count)
        local code, ok, ret, num = metadb:scan(table_name, scanner, scan_num)
        if not ok then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "HBase failed to scan table="..table_name..", innercode="..code)
            return false
        end

        for i, v_tab in pairs(ret) do
            local key = v_tab.key

            if scan_file then
                local delim_pos, _ = string.find(key, delimiter, #last_slash_enc(prefix) + 1)
                assert(not delim_pos)
                result[r_index] = v_tab
                r_index = r_index + 1
                count = count + 1
                next_marker = key
                assert(not last_cp)
            else
                local delim_pos, _ = string.find(key, delimiter, #prefix + 1)
                assert(delim_pos)
                local prefix_key = string.sub(key, 1, delim_pos);
                if last_cp ~= prefix_key then
                    common_prefixes[c_index] = {
                        ["key"] = prefix_key,
                        ["versionid"] = v_tab.values["ver:seq"],
                    }
                    c_index = c_index + 1
                    count = count + 1
                    next_marker = prefix_key
                    last_cp = prefix_key
                end
            end

            if (count >= max) then
                truncated = true
                code,ok = metadb:closeScanner(scanner)
                if not ok then
                    ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ",  "HBase failed to close scanner, innercode=", code)
                    return false
                end
                return true, result, common_prefixes, next_marker, truncated
            end
        end

        if num < scan_num then
            if scan_file then
                scan_file = false
                local code, ok = metadb:closeScanner(scanner)
                if not ok then
                    ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ",  "HBase failed to close scanner, innercode=", code)
                    return false
                end
                s_key = prefix .. BIGGER_THAN_DIR_MARKER
                e_key = prefix .. metadb.MAX_CHAR

                assert(not last_cp)
                code, ok, scanner = metadb:openScanner(table_name, s_key, e_key, columns)
                if not ok then
                    ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "HBase failed to open scanner, innercode=", code)
                    return false
                end
            else
                truncated = false
                break
            end
        elseif last_cp then
            local code,ok = metadb:closeScanner(scanner)
            if not ok then
                ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ",  "HBase failed to close scanner, innercode=", code)
                return false
            end

            s_key = string.sub(last_cp, 1, -2) .. bigger_than_delim
            code, ok, scanner = metadb:openScanner(table_name, s_key, e_key, columns, 0, filter)
            if not ok then
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "HBase failed to open scanner, innercode=", code)
                return false
            end
        end
    end

    code,ok = metadb:closeScanner(scanner)
    if not ok then
        ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ",  "HBase failed to close scanner, innercode=", code)
        return false
    end
    return true, result, common_prefixes, next_marker, truncated
end

local function list_objects_fs_delimiter(metadb, table_name, columns, marker, prefix, delimiter, max)
    assert(SLASH == delimiter)

    if 0 >= max then
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "list objects fs delimiter max is less 0, max is:", max)
        return true, {}, {}, nil, false
    end
    local s_key, e_key
    local bigger_than_delim = string.char(string.byte(SLASH) + 1)  --bigger_than_delim = '0'

    local sip, _ = string.find(prefix, SLASH)
    assert(sip)
    local sim, _ = string.find(marker, SLASH, #prefix + 1)

    if not sim then
        if marker <= prefix then                               --prefix: tag_/ab, marker: tag_/ab       -> s_key = tag_/#ab, e_key = tag_/#ab(MAX)
            s_key = last_slash_enc(prefix)
        else                                                   --prefix: tag_/ab, marker: tag_/abcd     -> s_key = tag_/#abcd(MIN), e_key = tag_/#ab(MAX)
            s_key = last_slash_enc(marker).. metadb.MIN_CHAR
        end
        e_key = last_slash_enc(prefix) .. metadb.MAX_CHAR
    else                                                       --prefix: ab/c, marker: ab/cde/   -> s_key = ab/#cde0, e_key = ab/#c(MAX)
        assert(sim == #marker)
        s_key = last_slash_enc(prefix) .. string.sub(marker, #prefix + 1, #marker - 1) .. bigger_than_delim
        e_key = last_slash_enc(prefix) .. metadb.MAX_CHAR
    end

    prefix = last_slash_enc(prefix)      -- for fs delimiter, all scan prefix is last_slash_encoded

    local code, ok, scanner = metadb:openScanner(table_name, s_key, e_key, columns, 0)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "HBase failed to open scanner, innercode=", code)
        return false
    end

    local count = 0
    local truncated = true
    local result = {}
    local common_prefixes = {}
    local next_marker
    local batch = sp_conf.config["hbase_config"]["max_scan_once"] or 100
    local r_index = 1
    local c_index = 1
    while count < max and truncated do
        local scan_num = math.min(batch, max - count)
        local code, ok, ret, num = metadb:scan(table_name, scanner, scan_num)
        if not ok then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "HBase failed to scan table="..table_name..", innercode="..code)
            return false
        end

        for i,v_tab in pairs(ret) do
            local key = v_tab.key

            local delim_pos, _ = string.find(key, delimiter, #prefix + 1)
            if delim_pos then
                assert(delim_pos == #key)
                common_prefixes[c_index] = {
                    ["key"] = key, 
                    ["versionid"] = v_tab.values["ver:seq"],
                }
                c_index = c_index + 1
                count = count + 1
                next_marker = key
            else
                result[r_index] = v_tab
                r_index = r_index + 1
                count = count + 1
                next_marker = key
            end

            if (count >= max) then
                truncated = true
                local code, ok = metadb:closeScanner(scanner)
                if not ok then
                    ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ",  "HBase failed to close scanner, innercode=", code)
                    return false
                end
                return true, result, common_prefixes, next_marker, truncated
            end
        end

        if num < scan_num then
            truncated = false
            break
        end
    end

    local code, ok = metadb:closeScanner(scanner)
    if not ok then
        ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ",  "HBase failed to close scanner, innercode=", code)
        return false
    end
    return true, result, common_prefixes, next_marker, truncated
end

local function list_objects_fs_delimiter_reverse(metadb, table_name, columns, marker, prefix, delimiter, max)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " Enter list_objects_fs_delimiter_reverse, marker=", marker,
                      " prefix=", prefix, " delimiter=", delimiter, " max=", max)

    local result = new_tab(1000,0)
    local common_prefixes = new_tab(1000,0)
    local next_marker = nil
    local truncated = nil

    local ASCII_SLASH = 47  --ascii code of '/' is 47;


    prefix,_,err1 = ngx_re_gsub(prefix, "/+", "/", "jo")
    marker,_,err2 = ngx_re_gsub(marker, "/+", "/", "jo")
    if err1 or err2 then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ngx_re_gsub failed. err=1", err1, " err2=", err2)
        return false
    end

    if string.byte(marker,-1,-1) == ASCII_SLASH then
        marker = string.sub(marker,1,-2)
    end

    local need_scan, key_start, key_end
    if string.byte(prefix,-1,-1) == ASCII_SLASH then
        need_scan = true
        if marker > prefix then
            key_start = last_slash_enc(marker)

            -- to exclude the marker itself from the return-list, we need to dec the last char by 1;
            local dec_last_char = string.char(string.byte(marker,-1,-1) - 1)
            key_start = string.sub(key_start, 1,-2)
            key_start = key_start .. dec_last_char .. metadb.MAX_CHAR
        else
            key_start = prefix .. DIR_MARKER .. metadb.MAX_CHAR
        end
        key_end = prefix .. DIR_MARKER .. metadb.MIN_CHAR
    else
        local prefix1 = last_slash_enc(prefix)
        local innercode,ok,dentry = oindex.stat(metadb, prefix1)
        if not ok then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to stat ", prefix1, " innercode=", innercode)
            return false
        end

        if not dentry or nil == next(dentry) then
            prefix1 = prefix1 .. SLASH
            innercode,ok,dentry = oindex.stat(metadb, prefix1)
            if not ok then
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to stat ", prefix1, " innercode=", innercode)
                return false
            end

            if not dentry then
                need_scan = false
                ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", prefix1, " doesn't exist")
            end
        end

        if dentry and nil ~= next(dentry) then
            if dentry["info:dir"] == "true" then 
                need_scan = true

                if marker > prefix then
                    assert(string.byte(marker, #prefix+1, #prefix+1) == ASCII_SLASH)
                    key_start = last_slash_enc(marker)

                    -- to exclude the marker itself from the return-list, we need to dec the last char by 1;
                    local dec_last_char = string.char(string.byte(marker,-1,-1) - 1)
                    key_start = string.sub(key_start, 1,-2)
                    key_start = key_start .. dec_last_char .. metadb.MAX_CHAR
                else
                    key_start = prefix .. SLASH .. DIR_MARKER .. metadb.MAX_CHAR
                end
                key_end = prefix .. SLASH .. DIR_MARKER .. metadb.MIN_CHAR
            elseif dentry and dentry["info:file"] == "true" and dentry["ver:tag"] then
                need_scan = false
                table.insert(result, {key=prefix1, values=dentry})
            else
                assert(false)
            end
        end
    end

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " need_scan=", need_scan, " key_start=", key_start, " key_end=", key_end)
    
    if need_scan then
        local innercode,ok,scanner = metadb:openScanner("object", key_start, key_end, columns, nil, nil, true)
        if not ok then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to openScanner, innercode=", innercode)
            return false
        end

        local max_once = sp_conf.config["hbase_config"]["max_scan_once"] or 1000

        local got = 0
        while got < max do
            local scan_num = math.min(max_once, max-got)
            local innercode,ok,ret,num = metadb:scan("object", scanner, scan_num)
            if not ok then
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to scan 'object' table, innercode=", innercode)
                return false
            end

            for i,v in ipairs(ret) do
                got = got + 1
                next_marker = v.key
                if v.values["info:dir"] == "true" then
                    table.insert(common_prefixes, {key=v.key, versionid=v.values["ver:seq"]})
                else
                    table.insert(result, v)
                end
            end

            if num < scan_num then
                truncated = false
                break
            else
                truncated = true 
            end
        end

        metadb:closeScanner(scanner)
    end

    return true, result, common_prefixes, next_marker, truncated
end


local function list_objects(metadb, req_info, client_type)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " Enter")

    local headers = req_info["headers"]
    local userinfo = req_info["user_info"]

    -- look up bucket info: key = user ver:tag_bucketname
    local bucket_key = userinfo["ver:tag"] .. "_" .. req_info["op_info"]["bucketname"]

    local code, ok, dbody = metadb:get("bucket", bucket_key, {"ver:tag"})

    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to get bucket from metadb, bucket_key=", bucket_key, " code=", code)
        return "2004", code, false
    end

    if nil == dbody or  nil == dbody["ver:tag"] then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " bucket ", bucket_key, " does not exist!")
        return "0012","0000",false
    end

    --bucket ver:tag is prefix of objectnames
    local tag = dbody["ver:tag"]
    local query_args = req_info["args"]
    local prefix = query_args["prefix"] or ""
    local marker = query_args["marker"] or ""
    local delimiter = query_args["delimiter"] or ""
    local versionid = query_args["versionId"]

    local maxkeys = nil
    if nil ~= query_args["max-keys"] and '' ~= query_args["max-keys"] then
        maxkeys = tonumber(query_args["max-keys"])
        if nil ~= maxkeys then
            if maxkeys > sp_conf.config["s3_config"]["maxkeys"]["max"] then
                maxkeys = sp_conf.config["s3_config"]["maxkeys"]["max"]
            end
        else
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " request max-keys is invalid. max-key is :", query_args["max-keys"])
            return "0010","0000",false
        end
    end
    if nil == maxkeys then
        maxkeys = sp_conf.config["s3_config"]["maxkeys"]["default"]
    end

    if "" ~= delimiter and SLASH ~= delimiter then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " request delimiter is invalid. delimiter is :", delimiter)
        return "0010","0000",false
    end

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " marker:"..marker, " prefix:"..prefix, " delimiter:"..delimiter, " maxkeys:", maxkeys)

    if client_type == "fs" then  -- fs user
        return oindex.fs_ls(metadb, req_info, dbody["ver:tag"], prefix, marker, delimiter, maxkeys)
    end

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " prefix:", prefix, " marker:", marker, " delimiter:", delimiter, " maxkeys:", maxkeys)

    local scan_prefix = tag.."_/"..prefix
    local scan_marker = tag.."_/"..marker

    if scan_marker < scan_prefix then
        scan_marker = scan_prefix
    end

    local columns = {"info"}
    if versionid then
        columns = {"info", "ver"}
    end
    local ok, result, common_prefixes, next_marker, truncated
    if headers["Reverse"] then
        ok, result, common_prefixes, next_marker, truncated = list_objects_fs_delimiter_reverse(metadb, "object", columns, scan_marker, scan_prefix, delimiter, maxkeys)
        if not ok then
            return "2005", "0000", false
        end
    elseif "" == delimiter then
        ok, result, common_prefixes, next_marker, truncated = list_objects_no_delimiter(metadb, "object", columns, scan_marker, scan_prefix, maxkeys)
        if not ok then
            return "2005", "0000", false
        end
    elseif "fs" == userinfo["info:type"] then
        ok, result, common_prefixes, next_marker, truncated = list_objects_fs_delimiter(metadb, "object", columns, scan_marker, scan_prefix, delimiter, maxkeys)
        if not ok then
            return "2005", "0000", false
        end
    else
        ok, result, common_prefixes, next_marker, truncated = list_objects_s3_delimiter(metadb, "object", columns, scan_marker, scan_prefix, delimiter, maxkeys)
        if not ok then
            return "2005", "0000", false
        end
    end

    local out = {}
    local index = 1
    out[index] = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"..
        "<Name>" .. req_info["op_info"]["bucketname"] .. "</Name>"
    if "" ~= prefix then
        out[index] = out[index].."<Prefix>"..utils.xmlencode(prefix).."</Prefix>"
    end

    out[index] = out[index].."<Marker>"..utils.xmlencode(marker).."</Marker>"
    out[index] = out[index].."<MaxKeys>"..maxkeys.."</MaxKeys>"
    if truncated and next_marker and "" ~= next_marker then
        next_marker = last_slash_dec(next_marker)
        next_marker = string.sub(next_marker, #tag + 3)
        out[index] = out[index].."<NextMarker>"..utils.xmlencode(next_marker).."</NextMarker>"
    end

    if "" ~= delimiter then
        out[index] = out[index].."<Delimiter>"..delimiter.."</Delimiter>"
    end

    out[index] = out[index].."<IsTruncated>"..(truncated and "true" or "false").."</IsTruncated>"

    local uid = userinfo["info:uid"]
    local displayname = userinfo["info:displayname"]
    ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, "list result:\n", inspect(result))

    for i,v_tab in pairs(result) do
        if "" == delimiter and not v_tab.values["info:oname"]  then
            -- no_delimiter,not return dir
        else
            index = index + 1
            local key = last_slash_dec(v_tab.key)
            key = string.sub(key, #tag + 3)
            local etag = utils.md5hex((v_tab.values["info:etag"]) or "")

            local mtime = v_tab.values["info:rsyncmtime"]
            if not mtime or mtime == "0" then
                mtime = v_tab.values["info:time"]
            end

            -- assert(key == v_tab.values["info:oname"])
            out[index] = "<Contents><Key>"..utils.xmlencode(key).."</Key>"..
            "<LastModified>"..utils.toUTC(mtime).."</LastModified>"..
            "<ETag>".."&quot;"..etag.."&quot;".."</ETag>"..
            "<Size>"..v_tab.values["info:size"].."</Size>"..
            "<StorageClass>STANDARD</StorageClass>"..
            "<Owner><ID>"..uid.."</ID><DisplayName>"..displayname.."</DisplayName></Owner></Contents>"
        end
    end

    for k, v in pairs(common_prefixes) do
        index = index + 1
        local delim_prefix = last_slash_dec(v.key)
        delim_prefix = utils.xmlencode(string.sub(delim_prefix, #tag + 3))
        if not versionid then
            out[index] = "<CommonPrefixes><Prefix>"..delim_prefix.."</Prefix></CommonPrefixes>"
        else
            out[index] = "<CommonPrefixes>" .. 
            "<Prefix>"..delim_prefix.."</Prefix>" ..
            "<VersionId>"..(v.versionid or "").."</VersionId>"..
            "</CommonPrefixes>"
        end
    end

    index = index + 1
    out[index] = "</ListBucketResult>"
    --according to the demands of S3_bucket to process s3_response_header
    local response_headers = sp_conf.config["s3_GET_Service"]["s3_response_headers"]
    for k,v in pairs(response_headers) do
        ngx.header[k] = v
        -- if "x-amz-request-id" = k then
        -- 	--generate value
        -- end
    end
    ngx.header["Content-Type"] = "application/xml"

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " out=",  stringify(out))

    ngx.print(table.concat(out))
    return "0000", "0000", true
end

local function get_bucket_object_version(metadb, req_info)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### Enter get_bucket_object_version")

    -- look up bucket info: key = user ver:tag_bucketname
    local bucket_key = req_info["user_info"]["ver:tag"] .. "_" .. req_info["op_info"]["bucketname"]
    ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " ",  "bucket key is  ", bucket_key)
    local code, ok, dbody = metadb:get("bucket", bucket_key, {"ver:tag"})
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to get bucket from metadb, code=", code)
        return "2004", code, false
    end

    if nil == dbody or  nil == dbody["ver:tag"] then
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "bucket "..bucket_key.." does not existed!")
        return "0012","0000",false
    end
    --bucket ver:tag is prefix of objectnames
    local tag = dbody["ver:tag"]
    local query_args = req_info["args"]
    local prefix = query_args["prefix"] or ""
    local key_marker = query_args["key-marker"] or ""
    local delimiter = query_args["delimiter"] or ""
    local versionid_marker = query_args["version-id-marker"] or ""

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "prefix: "..prefix..",keymarker: "..key_marker..",versionid_marker: "..versionid_marker..",delimiter: "..delimiter)

    local maxkeys = nil
    if nil ~= query_args["max-keys"] and '' ~= query_args["max-keys"] then
        maxkeys = tonumber(query_args["max-keys"])
        if nil ~= maxkeys then
            if maxkeys > sp_conf.config["s3_config"]["maxkeys"]["max"] then
                maxkeys = sp_conf.config["s3_config"]["maxkeys"]["max"]
            end
        end
    end
    if nil == maxkeys then
        maxkeys = sp_conf.config["s3_config"]["maxkeys"]["default"]
    end



    if client_type == "fs" then  -- fs user
        return oindex.fs_ls(metadb, req_info, dbody["ver:tag"], prefix, key_marker, delimiter, maxkeys)
    end

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " prefix:", prefix, " marker:", key_marker, " delimiter:", delimiter, " maxkeys:", maxkeys)

    local scan_prefix = tag.."_/"..prefix
    local scan_marker = tag.."_/"..key_marker

    if scan_marker < scan_prefix then
        scan_marker = scan_prefix
    end

    if "" ~= versionid_marker then
        scan_marker = scan_marker.."_"..versionid_marker
    end

    local ok, result, common_prefixes, next_marker, truncated
    if "" == delimiter then
        ok, result, common_prefixes, next_marker, truncated = list_objects_no_delimiter(metadb, "object", {"info"}, scan_marker, scan_prefix, maxkeys)
        if not ok then
            return "2005", "0000", false
        end
    elseif "true" == userinfo["info:fsindex"] then
        ok, result, common_prefixes, next_marker, truncated = list_objects_fs_delimiter(metadb, "object", {"info"}, scan_marker, scan_prefix, delimiter, maxkeys)
        if not ok then
            return "2005", "0000", false
        end
    else
        ok, result, common_prefixes, next_marker, truncated = list_objects_s3_delimiter(metadb, "object", {"info"}, scan_marker, scan_prefix, delimiter, maxkeys)
        if not ok then
            return "2005", "0000", false
        end
    end

    local out = {}
    local index = 1
    out[index] = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListVersionsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"..
        "<Name>" .. req_info["op_info"]["bucketname"] .. "</Name>"
    if "" ~= prefix then
        out[index] = out[index].."<Prefix>"..utils.xmlencode(prefix).."</Prefix>"
    end

    --out[index] = out[index].."<KeyCount>"..maxuploads.."</KeyCount>"

    out[index] = out[index].."<KeyMarker>"..utils.xmlencode(key_marker).."</KeyMarker>"
    out[index] = out[index].."<MaxKeys>"..maxkeys.."</MaxKeys>"
    if truncated and next_marker and "" ~= next_marker then
        local cp, err = ngx.re.match(next_marker, "^"..tag.."_".."(\\S+)$", "jo")
        if nil ~= cp and nil ~= next(cp) and nil ~= cp[1] then
            out[index] = out[index].."<NextKeyMarker>"..utils.xmlencode(cp[1]).."</NextKeyMarker>"
        end
    end

    if "" ~= delimiter then
        out[index] = out[index].."<Delimiter>"..delimiter.."</Delimiter>"
    end

    out[index] = out[index].."<IsTruncated>"..(truncated and "true" or "false").."</IsTruncated>"

    local uid = req_info["user_info"]["info:uid"]
    local displayname = req_info["user_info"]["info:displayname"]

    for i,v_tab in pairs(result) do
        if "" == delimiter and not v_tab.values["info:oname"]  then
               -- no_delimiter,not return dir
        else
            index = index + 1
            local key = last_slash_dec(v_tab.key)
            key = string.sub(key, #tag + 3)
            local etag = utils.md5hex((v_tab.values["info:etag"]) or "")

            local mtime = v_tab.values["info:rsyncmtime"]
            if not mtime or mtime == "0" then
                mtime = v_tab.values["info:time"]
            end

            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "  key:", key, "  oname:", v_tab.values["info:oname"])
            -- assert(key == v_tab.values["info:oname"])
            out[index] = "<Version><Key>"..utils.xmlencode(key).."</Key>"..
            "<VersionId></VersionId>"..
            "<IsLatest>true</IsLatest>"..
            "<LastModified>"..utils.toUTC(mtime).."</LastModified>"..
            "<ETag>".."&quot;"..etag.."&quot;".."</ETag>"..
            "<Size>"..v_tab.values["info:size"].."</Size>"..
            "<StorageClass>STANDARD</StorageClass>"..
            "<Owner><ID>"..uid.."</ID><DisplayName>"..displayname.."</DisplayName></Owner></Version>"
        end
    end

    for k,v in pairs(common_prefixes) do
        index = index + 1
        local delim_prefix = last_slash_dec(v.key)
        delim_prefix = utils.xmlencode(string.sub(delim_prefix, #tag + 3))
        out[index] = "<CommonPrefixes><Prefix>"..delim_prefix.."</Prefix></CommonPrefixes>"
    end

    index = index + 1
    out[index] = "</ListVersionsResult>"
    --according to the demands of S3_bucket to process s3_response_header
    local response_headers = sp_conf.config["s3_GET_Service"]["s3_response_headers"]
    for k,v in pairs(response_headers) do
        ngx.header[k] = v
        -- if "x-amz-request-id" = k then
        -- 	--generate value
        -- end
    end
    ngx.header["Content-Type"] = "application/xml"
    ngx.print(table.concat(out))
    return "0000", "0000", true
end

local function list_mult_uploads(metadb, req_info)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### Enter list_mult_uploads")

    --Yuanguo:
    --   for goofys of "root" user, when mount (which will call this function), we
    --   cheat on it!
    local headers = req_info["headers"]
    if fs.is_root_mount() then
        return fs.root_mount()
    end

    -- look up bucket info: key = user ver:tag_bucketname
    local bucket_key = req_info["user_info"]["ver:tag"] .. "_" .. req_info["op_info"]["bucketname"]
    ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " ",  "bucket key is  ", bucket_key)
    local code, ok, dbody = metadb:get("bucket", bucket_key, {"ver:tag"})
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to get bucket from metadb, code=", code)
        return "2004", code, false
    end

    if nil == dbody or  nil == dbody["ver:tag"] then
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "bucket "..bucket_key.." does not existed!")
        return "0012","0000",false
    end
    --bucket ver:tag is prefix of objectnames
    local tag = dbody["ver:tag"]
    local query_args = req_info["args"]
    local prefix = query_args["prefix"] or ""
    local key_marker = query_args["key-marker"] or ""
    local uploadid_marker = query_args["upload-id-marker"] or ""
    local delimiter = query_args["delimiter"] or ""

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "prefix: "..prefix..",keymarker: "..key_marker..",uploadidmarker: "..uploadid_marker.."delimiter: "..delimiter)

    local maxuploads = sp_conf.config["s3_config"]["maxkeys"]["default"]
    if nil ~= query_args["max-uploads"] then
        maxuploads = tonumber(query_args["max-uploads"])
        if maxuploads > sp_conf.config["s3_config"]["maxkeys"]["max"] then
            maxuploads = sp_conf.config["s3_config"]["maxkeys"]["max"]
        end
    end

    --Yuanguo: suppose we have several multi-uploads like this:
    --            key                  uploadId
    --           obj-100:     AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
    --           obj-100:     BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB
    --           obj-100:     CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC
    --           obj-101:     33333333333333333333333333333333
    --           obj-102:     44444444444444444444444444444444
    -- if key-marker=obj-100 and upload-id-marker="", then uploads with key>obj-100 will be returned, they are
    --     obj-101 and obj-102
    -- if key-marker=obj-100 and upload-id-marker=BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB, then uploads with
    -- key>=obj-100 and upload-id>BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB will be returned, they are
    --     obj-100(uploadId=CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC)
    --     obj-101
    --     obj-102

    local scan_key = ""
    if '' ~= key_marker and key_marker >= prefix then
        if uploadid_marker and "" ~= uploadid_marker then
            scan_key = tag.."_/".. DIR_MARKER ..key_marker.."_"..uploadid_marker
        else
            scan_key = tag.."_/".. DIR_MARKER ..key_marker..metadb.MAX_CHAR
        end
    end
    local scan_prefix = tag.."_"..prefix
    local filter = "SingleColumnValueFilter('flag','flag',=, 'binary:0')"
    local ok, result, common_prefixes, next_marker, truncated = _M.list_entries(metadb, "temp_object", {"info","flag"}, scan_key, nil, scan_prefix, delimiter, maxuploads,filter)
    if not ok then
        return "0012","0000",false
    end

    local out = {}
    local index = 1
    out[index] = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListMultipartUploadsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"..
        "<Bucket>" .. req_info["op_info"]["bucketname"] .. "</Bucket>"
    if "" ~= prefix then
        out[index] = out[index].."<ListMultipartUploadsResult.Prefix>"..utils.xmlencode(prefix).."</ListMultipartUploadsResult.Prefix>"
    end

    out[index] = out[index].."<KeyMarker>"..key_marker.."</KeyMarker>"
    if "" ~= uploadid_marker then
        out[index] = out[index].."<UploadIdMarker>"..uploadid_marker.."</UploadIdMarker>"
    end

    if next_marker and "" ~= next_marker then
        local s,e = string.find(next_marker, tag.."_/")
        if e then
            next_marker = string.sub(next_marker, e+1, -1)
        end
        if next_marker ~= "" then
            next_marker = oindex.del_dir_marker(next_marker)
        end

        local cp, err = ngx.re.match(next_marker, "^(\\S+)_(\\S{32})$", "jo")
        if nil ~= cp and nil ~= next(cp) and nil ~= cp[1] and nil ~= cp[2] then
            out[index] = out[index].."<NextKeyMarker>"..cp[1].."</NextKeyMarker>"
            out[index] = out[index].."<NextUploadIdMarker>"..cp[2].."</NextUploadIdMarker>"
        else
            out[index] = out[index].."<NextKeyMarker>"..next_marker.."</NextKeyMarker>"
        end
    end

    out[index] = out[index].."<MaxUploads>"..maxuploads.."</MaxUploads>"

    if "" ~= delimiter then
        out[index] = out[index].."<Delimiter>"..delimiter.."</Delimiter>"
    end

    out[index] = out[index].."<IsTruncated>"..(truncated and "true" or "false").."</IsTruncated>"

    local uid = req_info["user_info"]["info:uid"]
    local displayname = req_info["user_info"]["info:displayname"]

    for i,v_tab in pairs(result) do
        index = index + 1

        local splittab = utils.strsplit(v_tab.key, "_")
        local uploadid = splittab and splittab[#splittab] or ""
        local key = uploadid and string.sub(v_tab.key, #tag + 3, 0-(#uploadid+2)) or ""

        if #key > 0 then
            key = oindex.del_dir_marker(key)
        end

        out[index] = "<Upload><Key>"..utils.xmlencode(key).."</Key>"..
        "<UploadId>"..uploadid.."</UploadId>"..
        "<Initiator><ID>"..uid.."</ID><DisplayName>"..displayname.."</DisplayName></Initiator>"..
        "<Owner><ID>"..uid.."</ID><DisplayName>"..displayname.."</DisplayName></Owner>"..
        "<StorageClass>STANDARD</StorageClass>"..
        "<Initiated>"..utils.toUTC(v_tab.values["info:ctime"]).."</Initiated></Upload>"
    end

    if nil ~= next(common_prefixes) then
        index = index + 1
        out[index] = "<CommonPrefixes>"
        for k,v in pairs(common_prefixes) do
            index = index + 1
            local delim_prefix = string.sub(v, #tag + 2)
            out[index] = "<CommonPrefixes.Prefix>"..utils.xmlencode(delim_prefix).."</CommonPrefixes.Prefix>"
        end
        out[index] = out[index].."</CommonPrefixes>"
    end

    index = index + 1
    out[index] = "</ListMultipartUploadsResult>"
    --according to the demands of S3_bucket to process s3_response_header
    local response_headers = sp_conf.config["s3_GET_Service"]["s3_response_headers"]
    for k,v in pairs(response_headers) do
        ngx.header[k] = v
        -- if "x-amz-request-id" = k then
        -- 	--generate value
        -- end
    end
    ngx.header["Content-Type"] = "application/xml"
    ngx.print(table.concat(out))
    return "0000", "0000", true
end

local function get_bucket_acl(metadb, req_info)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### Enter get_bucket_acl :", req_info["method"], req_info["uri"])

    local user_info = req_info["user_info"]
    -- now only check if the bucket exists, put_bucket_acl need to be implement later
    local status, ok, dbody = metadb:get("bucket", user_info["ver:tag"] .. "_" .. req_info["op_info"]["bucketname"], {"ver:tag"})
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to get bucket from metadb, status=", status)
        return "2004", status, false
    end

    local res_body = string.format([[<AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Owner><ID>%s</ID><DisplayName>%s</DisplayName></Owner><AccessControlList><Grant><Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser"><ID>%s</ID><DisplayName>%s</DisplayName></Grantee><Permission>FULL_CONTROL</Permission></Grant></AccessControlList></AccessControlPolicy>]], user_info["info:uid"], user_info["info:displayname"], user_info["info:uid"], user_info["info:displayname"])

    --print(res_body)
    --local ok, res = pcall(json.encode, response_body)
    --if not ok then
    --	return 200, "20000022"
    --end

    --according to the demands of S3_bucket to process s3_response_header
    local response_headers = sp_conf.config["s3_GET_Service"]["s3_response_headers"]
    for k,v in pairs(response_headers) do
        ngx.header[k] = v
        -- if "x-amz-request-id" = k then
        -- 	--generate value
        -- end
    end
    ngx.header["Content-Type"] = "application/xml"
    ngx.header["Content_Length"] = #res_body

    --发出响应
    ngx.print(res_body)
    return "0000", "0000", true
end

local function head_bucket(metadb, req_info)
   --get bucket tag
    local useretag = req_info["user_info"]["ver:tag"]
    local bucketrowkey = useretag.."_"..req_info["op_info"]["bucketname"]
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "bucketrowkey is ", bucketrowkey)
    local code,ok,rebody1 = metadb:get("bucket", bucketrowkey, {"info:accessid"})
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "metadb failed to get bucket meta accessid code is :"..code.." internal error!")
        ngx.status = 500
        return "2001", code, true
    end

    if nil == rebody1 or nil == rebody1["info:accessid"] then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "metadb failed to get bucket meta accessid code is :"..code.." bucket not exist!")
        ngx.status = 404
        return "0012", "0000", true
    end

    return "0000", "0000", true
end

local function get_bucket_versioning(metadb, req_info)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### Enter get_bucket_versioning :", req_info["method"], req_info["uri"])
    local useretag = req_info["user_info"]["ver:tag"]
    local bucketrowkey = useretag.."_"..req_info["op_info"]["bucketname"]
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "bucketrowkey is ", bucketrowkey)
    local code,ok,rebody1 = metadb:get("bucket", bucketrowkey, {"info:accessid"})
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "metadb failed to get bucket meta accessid code is :"..code.." internal error!")
        ngx.status = 500
        return "2001", code, true
    end

    if nil == rebody1 or nil == rebody1["info:accessid"] then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "metadb failed to get bucket meta accessid code is :"..code.." bucket not exist!")
        ngx.status = 404
        return "0012", "0000", true
    end

    local res_body = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"..
                    "<VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"..
                    "<Status>Suspended</Status></VersioningConfiguration>"
    ngx.header["Content-Type"] = "application/xml"
    ngx.header["Content_Length"] = #res_body

    ngx.print(res_body)

    return "0000", "0000", true
end

local function bucket_location(metadb, req_info)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### Enter bucket_location :", req_info["method"], req_info["uri"])

    local user_info = req_info["user_info"]
    -- now only check if the bucket exists, put_bucket_acl need to be implement later
    local status, ok, dbody = metadb:get("bucket", user_info["ver:tag"] .. "_" .. req_info["op_info"]["bucketname"], {"ver:tag"})
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to get bucket from metadb, status=", status)
        return "2004", status, false
    end

    local res_body = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"..
                    "<LocationConstraint xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"></LocationConstraint>"
    ngx.header["Content-Type"] = "application/xml"
    ngx.header["Content_Length"] = #res_body

    --发出响应
    ngx.print(res_body)
    return "0000", "0000", true
end

local function put_bucket_lifecycle(metadb, req_info)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### Enter put_bucket_lifecycle :", req_info["method"], req_info["uri"])

    local userinfo = req_info["user_info"]
    if not userinfo["ver:tag"] then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "ver:tag in userinfo is nil")
        return "0011", "0000", false
    end

    local accessid = req_info["auth_info"]["accessid"]
    local user_type = userinfo["info:type"]
    local bname = req_info["op_info"]["bucketname"]

    local bkey = userinfo["ver:tag"].."_"..bname

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "metadb get: table=bucket, key="..bkey)
    local code,ok,res = metadb:get("bucket", bkey, {"ver:tag"})
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "metadb get failed, table=bucket, key="..bkey)
        return "2004", code, false
    end

    if not res or not next(res) then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "bucket "..bkey.." does not existed!")
        return "0012","0000",false
    end

    local btag = res["ver:tag"]

    ngx.req.read_body()
    local xml_request_body = ngx.var.request_body

    if nil == xml_request_body or '' == xml_request_body then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "put bucket lifecycle body is nil")
        return "0010", "0000", false
    end

    local xml = XmlParser:new()
    local parsed_xml = xml:ParseXmlText(xml_request_body)
    if nil == parsed_xml or nil == next(parsed_xml) then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "put bucket lifecycle parsed xml failed")
        return "0024", "0000", false
    end

    local lifecycle_xml = parsed_xml.LifecycleConfiguration
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "put bucket lifecycle parsed xml: ", inspect(lifecycle_xml))
    if nil == lifecycle_xml or nil == next(lifecycle_xml) then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "put bucket lifecycle parsed xml failed")
        return "0024", "0000", false
    end

    local newruletable = {}
    local rule = nil
    local rule_index = 1
    --one rule is rule, some rule is rule[x]
    rule =  nil ~= lifecycle_xml.Rule[rule_index] and lifecycle_xml.Rule[rule_index] or lifecycle_xml.Rule
    while (nil ~= rule)
    do
        repeat
            local id = rule.ID:value()
            local prefix = rule.Prefix:value() and rule.Prefix:value() or ""
            local status = rule.Status:value()
            local days = rule.Expiration.Days:value()

            local rkey = id
            if nil ~= newruletable[rkey] then
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "rule id conflict. id: ", rkey)
                return "0024", "0000", false
            end

            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  " prefix: ", prefix)
            for k,v in pairs(newruletable) do
                local old_prefix = v["prefix"]
                local s,e = string.find(old_prefix, prefix)
                if nil ~= s and s == 1 then
                    ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "rule prefix conflict. prefix: ", prefix)
                    return "0024", "0000", false
                end
                local s,e = string.find(prefix, old_prefix)
                if nil ~= s and s == 1 then
                    ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "rule prefix conflict. prefix: ", prefix)
                    return "0024", "0000", false
                end
            end

            newruletable[rkey] = {
                ["prefix"] = prefix,
                ["status"] = status,
                ["days"] = days,
                ["tag"] =  uuid(),
            }
        until true
        rule_index = rule_index + 1
        rule = lifecycle_xml.Rule[rule_index]
    end

    local ok, jsonrule = pcall(json.encode, newruletable)
    if not ok then
        ngx.log(ngx.ERR, "failed to encode rules")
        return "4022",false
    end

    local mtime = string.format("%0.3f", ngx.now())
    local args = req_info["args"]
    local seq_no = args["seq_no"] or gen_seq_no(mtime)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " seq_no generated for bucket ", bucket, ". seq_no=", seq_no)

    local code, ok = sync_meta.write_metalog(req_info, seq_no)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " write_metalog failed. code=", code) 
        return "4041", code, false
    end

    local hbase_body = {
        "info", "ctime",    mtime,
        "info", "accessid", accessid,
        "info", "type",     user_type,
        "info", "bname",    bname,
        "info", "bkey",     bkey,
        "info", "rules",    jsonrule,
        "ver",  "seq",      seq_no,
    }
    local compare_op = metadb.CompareOp.GREATER
    local rowkey = bkey .. "_" .. btag
    local code, ok = metadb:checkAndMutateRow("lifecycle", rowkey, "ver", "seq", compare_op, seq_no, hbase_body, nil)
    if not ok then
        if "1121" == code then --failed because of condition-check-failure
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " checkandmutate outdated. rowkey=", rowkey, " seq_no=", seq_no) 
        else
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " checkandmutate failed. code=", code, " rowkey=", rowkey, " seq_no=", seq_no)
        end
        
        return "2003", code, false
    end

    sync_meta.sync_req(req_info, seq_no, xml_request_body)

    return "0000", "0000", true
end

local function get_bucket_lifecycle(metadb, req_info)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### Enter get_bucket_lifecycle :", req_info["method"], req_info["uri"])

    local userinfo = req_info["user_info"]
    if not userinfo["ver:tag"] then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "ver:tag in userinfo is nil")
        return "0011", "0000", false
    end

    local bkey = userinfo["ver:tag"].."_"..req_info["op_info"]["bucketname"]

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "metadb get: table=bucket, key="..bkey)
    local code, ok, res = metadb:get("bucket", bkey, {"ver:tag"})
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "metadb get failed, table=bucket, key="..bkey)
        return "2004", code, false
    end

    if not res or not next(res) then
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "bucket "..bkey.." does not existed!")
        return "0012","0000",false
    end

    local btag = res["ver:tag"]
    local rowkey = bkey .. "_" .. btag
    local code, ok, db_rule = metadb:get("lifecycle", rowkey, {"info:rules"})
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "metadb failed to scan rule :")
        return "4011", code, false
    end

    if not db_rule or not next(db_rule) then
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "bucket lifecycle "..rowkey.." does not existed!")
        return "0012","0000",false
    end

    ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " ", "lifecycle rule db is :", inspect(db_rule))

    local ok, rtable = pcall(json.decode, db_rule["info:rules"])
    if not ok or "table" ~= type(rtable) then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ","json.decode body failed. body=", request_body)
        return "0010", "0000", false
    end

    local out = {}
    local index = 1
    out[index] = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"

    for k, v in pairs(rtable) do
        if nil ~= v and "table" == type(v) then
            index = index + 1
            out[index] = "<Rule><ID>"..k.."</ID>"..
                        "<Prefix>"..utils.xmlencode(v["prefix"]).."</Prefix>"..
                        "<Status>"..v["status"].."</Status>"..
                        "<Expiration><Days>"..v["days"].."</Days></Expiration>"..
                        "</Rule>"
        end
    end

    index = index + 1
    out[index] = "</LifecycleConfiguration>"

    ngx.header["Content-Type"] = "application/xml"
    local body = table.concat(out)
    ngx.header["Content-Length"] = #body
    ngx.print(body)
    return "0000", "0000", true
end

local function delete_bucket_lifecycle(metadb, req_info)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### Enter delete_bucket_lifecycle :", req_info["method"], req_info["uri"])

    local userinfo = req_info["user_info"]
    if not userinfo["ver:tag"] then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "ver:tag in userinfo is nil")
        return "0011", "0000", false
    end

    local bkey = userinfo["ver:tag"].."_"..req_info["op_info"]["bucketname"]

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "metadb get: table=bucket, key="..bkey)
    local code,ok,res = metadb:get("bucket", bkey, {"ver:tag"})
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "metadb get failed, table=bucket, key="..bkey)
        return "2004", code, false
    end

    local existent = true
    if not res or not next(res) or not res["ver:tag"] then
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "bucket "..bkey.." does not existed!")
        existent = false
    end

    local mtime = string.format("%0.3f", ngx.now())
    local args = req_info["args"]
    local seq_no = args["seq_no"] or gen_seq_no(mtime)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " seq_no generated for bucket ", bucketname, ". seq_no=", seq_no)

    local code, ok = sync_meta.write_metalog(req_info, seq_no)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " write_metalog failed. code=", code) 
        return "4041", code, false
    end

    if existent then
        local btag = res["ver:tag"]
        local rowkey = bkey .. "_" .. btag 
        local compare_op = metadb.CompareOp.GREATER
        local delete_families = {
            "info:ctime", 
            "info:accessid",
            "info:type", 
            "info:bname", 
            "info:bkey", 
            "info:rules", 
            "ver:seq",
        } 
        local code, ok = metadb:checkAndMutateRow("lifecycle", rowkey, "ver", "seq", compare_op, seq_no, nil, delete_families)
        if not ok then
            if "1121" == code then --failed because of condition-check-failure
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " checkandmutate outdated. rowkey=", rowkey, " seq_no=", seq_no)
            else
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " checkandmutate failed. code=", code, " rowkey=", rowkey, " seq_no=", seq_no)
            end
            return "0011", code, false
        end
    end

    sync_meta.sync_req(req_info, seq_no)

    ngx.status = ngx.HTTP_NO_CONTENT
    return "0000", "0000", true
end

function _M.list_entries(metadb, table_name, columns, key_marker, end_key, prefix, delimiter, max, filter)
    ngx.log(ngx.INFO, "prefix: "..prefix..",key_marker: "..key_marker..",delimiter: "..delimiter)
    local count = 0
    local truncated = true
    local result = {}
    local common_prefixes = {}
    local next_marker

    if 0 >= max then
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "list entries max is less 0, max is:", max)
        return true, {}, {}, nil, false
    end

    local start_key = prefix
    if key_marker >= prefix then
        start_key = key_marker..metadb.MIN_CHAR
    end

    if not end_key then
        end_key = prefix..metadb.MAX_CHAR
    end

    if #delimiter > 1 then
        return false
    end
    local escape_delimiter = delimiter
    if delimiter > "z" or delimiter < "0" then
        escape_delimiter = "%"..delimiter
    end

    local bigger_than_delim
    if "" ~= delimiter then
        --to do, maybe need to check the range of delimiter 0 - 255
        bigger_than_delim = string.char(string.byte(delimiter, 1) + 1)
        --if marker points at a common prefix, fast forward it into its upperbound string
        local delim_pos = string.find(start_key, escape_delimiter, #prefix + 1)
        if nil ~= delim_pos then
            local s = string.sub(start_key, 1, delim_pos - 1)
            start_key = s..bigger_than_delim
        end
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "start_key: "..start_key..",bigger_than_delim: "..bigger_than_delim)
    end

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "start_key: "..start_key..",end_key: "..end_key)
    local skip_after_delim = ""
    local code,ok,scanner = metadb:openScanner(table_name, start_key, end_key, columns, 0, filter)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "HBase failed to open scanner, innercode=", code)
        return false
    end

    local batch = sp_conf.config["hbase_config"]["max_scan_once"] or 100
    local c_index = 1
    local r_index = 1
    local last_common_prefix
    while count <= max and truncated do
        if skip_after_delim > start_key then
            code,ok = metadb:closeScanner(scanner)
            if not ok then
                ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ",  "HBase failed to close scanner, innercode=", code)
                return false
            end

            start_key = skip_after_delim
            code,ok,scanner = metadb:openScanner(table_name, start_key, end_key, columns, 0, filter)
            if not ok then
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "HBase failed to open scanner, innercode=", code)
                return false
            end
        end

        local scan_num = math.min(batch, max + 1 - count)
        local code, ok, ret, num = metadb:scan(table_name, scanner, scan_num)
        if not ok then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "HBase failed to scan table="..table_name..", innercode="..code)
            return false
        end

        if 0 == num then
            truncated = false
            break
        end
        for i,v_tab in pairs(ret) do
            local key = v_tab.key
            if count < max then
                next_marker = key
            end

            repeat
                if "" ~= delimiter then
                    local delim_pos, _ = string.find(key, escape_delimiter, #prefix + 1)
                    if nil ~= delim_pos then
                        local prefix_key = string.sub(key, 1, delim_pos);
                        if prefix_key ~= last_common_prefix then
                            if (count >= max) then
                                truncated = true
                                code,ok = metadb:closeScanner(scanner)
                                if not ok then
                                    ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ",  "HBase failed to close scanner, innercode=", code)
                                    return false
                                end
                                return true, result, common_prefixes, next_marker, truncated
                            end
                            next_marker = prefix_key
                            common_prefixes[c_index] = prefix_key
                            c_index = c_index + 1
                            last_common_prefix = prefix_key
                            skip_after_delim = string.sub(key, 1, delim_pos - 1)..bigger_than_delim
                            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "skip_after_delim: ", skip_after_delim)
                            count = count + 1
                        end
                        break
                    end
                end
                if (count >= max) then
                    truncated = true
                    code,ok = metadb:closeScanner(scanner)
                    if not ok then
                        ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ",  "HBase failed to close scanner, innercode=", code)
                        return false
                    end
                    return true, result, common_prefixes, next_marker, truncated
                end

                result[r_index] = v_tab
                r_index = r_index + 1
                count = count + 1
                start_key = key
            until true
        end
    end

    code,ok = metadb:closeScanner(scanner)
    if not ok then
        ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ",  "HBase failed to close scanner, innercode=", code)
        return false
    end
    return true, result, common_prefixes, next_marker, truncated
end

function _M.process_bucket(self, metadb, req_info)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### Enter process_bucket :", req_info["method"], req_info["uri"])
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### args is ", inspect(req_info["args"]))
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### headers is ", inspect(req_info["headers"]))

    local statcode = "0018" -- not supported
    local innercode = "0000"
    local ok = false
    local op_type = req_info["op_info"]["type"]

    if sp_conf.get_s3_op_rev("CREATE_BUCKET") == op_type then
        statcode, innercode, ok = put_bucket(metadb, req_info)
    elseif sp_conf.get_s3_op_rev("LIST_OBJECTS") == op_type then
        local client_type = get_client_type(req_info["headers"]["user-agent"])
        statcode, innercode, ok = list_objects(metadb, req_info, client_type)
    elseif sp_conf.get_s3_op_rev("GET_BUCKET_ACL") == op_type then
        statcode, innercode, ok = get_bucket_acl(metadb, req_info)
    elseif sp_conf.get_s3_op_rev("DELETE_BUCKET") == op_type then
        statcode, innercode, ok = delete_bucket(metadb, req_info)
    elseif sp_conf.get_s3_op_rev("HEAD_BUCKET") == op_type then
        statcode, innercode, ok = head_bucket(metadb, req_info)
    elseif sp_conf.get_s3_op_rev("GET_BUCKET_OBJECT_VERSIONS") == op_type then
        statcode, innercode, ok = get_bucket_object_version(metadb, req_info)
    elseif sp_conf.get_s3_op_rev("GET_BUCKET_LOCATION") == op_type then
        statcode, innercode, ok = bucket_location(metadb, req_info)
    elseif sp_conf.get_s3_op_rev("LIST_MULTIPART_UPLOADS") == op_type then
        statcode, innercode, ok = list_mult_uploads(metadb, req_info)
    elseif sp_conf.get_s3_op_rev("GET_BUCKET_VERSIONING") == op_type then
        statcode, innercode, ok = get_bucket_versioning(metadb, req_info)
    elseif sp_conf.get_s3_op_rev("PUT_BUCKET_LIFECYCLE") == op_type then
        statcode, innercode, ok = put_bucket_lifecycle(metadb, req_info)
    elseif sp_conf.get_s3_op_rev("GET_BUCKET_LIFECYCLE") == op_type then
        statcode, innercode, ok = get_bucket_lifecycle(metadb, req_info)
    elseif sp_conf.get_s3_op_rev("DELETE_BUCKET_LIFECYCLE") == op_type then
        statcode, innercode, ok = delete_bucket_lifecycle(metadb, req_info)
    else
        --method not supported
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### operation not supported:"..req_info["method"].." "..req_info["uri"].." "..op_type)
    end

    return statcode, innercode, ok
end

local function delete_object_backend(metadb, ubo, origin)
    ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " move deleted object into 'delete_object' table. deleted_object =\n", inspect(origin))

    local buck_vtag  = ubo.buck_vtag
    local obj_name   = origin["info:oname"]
    local seqno      = origin["ver:seq"]
    local ctime      = origin["info:ctime"]
    local size       = tonumber(origin["info:size"])
    local hsize      = tonumber(origin["info:hsize"])
    local lock       = origin["info:lock"]

    -- lock ~= nil on copy rename
    if size > hsize and nil == lock then 
        -- try our best to put it into 'delete_object' table; because if failed, garbage will be
        -- left in our storage;
        local delete_obj = utils.out_db_to_in(origin)
        local shard      = _M.get_delete_obj_shard(obj_name)
        local delObjKey  = shard.."_"..buck_vtag.."_"..obj_name.."_"..uuid()
        local succ = false
        for r=1,3 do
            local code,ok = metadb:put("delete_object", delObjKey, delete_obj)
            if ok then
                ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " succeeded to put deleted object into 'delete_object' table. delObjKey=", delObjKey)
                succ = true
                break
            else
                ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " failed to put deleted object into 'delete_object' table. delObjKey=", delObjKey,
                                  " errorcode=", code, " retry=", r)
            end
        end
        if not succ then
            ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " failed to put deleted object into 'delete_object' table. delObjKey=", delObjKey)
        end
    end

    local cdate  = utils.toDATE(ctime)
    local id     = hash.hash(obj_name, EXPIRE_MAX)
    local expire = utils.get_shard(EXPIRE_NAME, EXPIRE_MAX, id)
    local rowkey = expire.."_"..cdate.."_"..buck_vtag.."_"..obj_name.."_"..seqno

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " delete write expire rowkey:", rowkey)
    --delete expire
    local code,ok = metadb:delete("expire_object", rowkey)
    if not ok then
        ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " failed to delete expire_object table. rowkey=", rowkey)
    end

    return succ
end

function _M.insert_object(metadb, ubo, seq_no, create_time, size, obj_metadata, datalog_body, del_metadata)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " Enter. buck_key=", ubo.buck_key, " obj_name=", ubo.obj_name, " obj_key=", ubo.obj_key, " seq_no=", seq_no)

    -- Step-1: create the object metadata
    local innercode,ok,origin = oindex.link(metadb, ubo.obj_key, seq_no, obj_metadata, del_metadata)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " link failed, obj_key=", ubo.obj_key, " seq_no=", seq_no, " innercode=", innercode)
        return "3007", innercode, false
    end

    --create ancestor dirs if necessary;
    if "fs" == ubo.user_type then
        local innercode,ok = oindex.mkdir_p(metadb, ubo.obj_dirkeys, seq_no, "sys")
        if not ok then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to create ancestor dirs for ", ubo.obj_name,
                             " innercode=", innercode, " dirkeys=", stringify(ubo.obj_dirkeys))
            return "3018", innercode, false
        end
    end

    local incr_size = size
    local incr_objs = 1

    -- Step-2: move the old object into 'delete_object' table;
    if nil ~= origin["ver:tag"] then -- origin was an old object
        -- we are replacing an old object, so size-increment is the delta of new and old object; object count-incrment is 0;
        incr_size = incr_size - origin["info:size"]
        incr_objs = 0

        if ubo.obj_vtag ~= origin["ver:tag"] then
            local succ = delete_object_backend(metadb, ubo, origin)
            if not succ then
                ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " failed to put replaced object into 'delete_object' table. obj_key=", ubo.obj_key)
            end
        else -- if ver_tag ~= origin["ver:tag"]
            ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " we're putting the same object with the same ver:tag. obj_key=", ubo.obj_key, " ver:tag=", ubo.obj_vtag)
        end
    else  -- if nil ~= origin["ver:tag"]
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " we're putting a new object. obj_key=", ubo.obj_key)
    end

    -- Step-3: increment stats
    incr_stats(metadb, ubo.user_key, ubo.buck_key, incr_size, incr_objs, false)

    -- Step-4: write 'expire' info;
    local expire_info = new_tab(0,4)
    expire_info["buck_vtag"]  = ubo.buck_vtag
    expire_info["obj_name"]   = ubo.obj_name
    expire_info["seqno"]      = seq_no
    expire_info["ctime"]      = create_time

    local code,ok = write_expire(metadb, expire_info)
    if not ok then
        ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " failed to put expire table. obj_name=", ubo.obj_name, " errorcode=", code)
    end

    -- Step-5: write 'datalog'
    if sync_cfg["enabled"] and ubo.syncflag then
        local succ = write_datalog(metadb, ubo.buck_name, ubo.obj_name, seq_no, datalog_body)
        if not succ then
            ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " write datalog failed. buck_name=", ubo.buck_name, " obj_name=", ubo.obj_name, " seq_no=", seq_no)
        end
    end

    ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " insert object table finished  obj_name=", ubo.obj_name)
    return "0000","0000",true
end

local function remove_object(metadb, clusterid, ubo, seq_no, delete_time, versionid)
    -- Step-1: remove the object from 'object' table;
    local compareop = metadb.CompareOp.GREATER_OR_EQUAL
    local delobj_seq = seq_no
    if nil ~= versionid then
        compareop = metadb.CompareOp.EQUAL
        delobj_seq = versionid
    end

    local innercode,ok,origin = oindex.unlink(metadb, ubo.obj_key, delobj_seq, compareop)
    if not ok then
        if "1121" == innercode then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " unlink failed because of newer seq_no of the obj, code=", code, " incode=", incode)
            return "0000", true
        end
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " unlink failed. innercode=", innercode)
        return innercode,false
    end

    local dec_size = 0
    local dec_objs = 0
    -- Step-2: put the removed object into 'delete_object' table;
    if nil ~= origin["ver:tag"] then
        dec_size = origin["info:size"] or 0
        dec_objs = 1
        ubo.obj_vtag = origin["ver:tag"]

        local succ = delete_object_backend(metadb, ubo, origin)
        if not succ then
            ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " failed to put deleted object into 'delete_object' table. obj_key=", ubo.obj_key)
        end

        -- Step-3: increment stats
        incr_stats(metadb, ubo.user_key, ubo.buck_key, 0-dec_size, 0-dec_objs, false)

    else
        ubo.obj_vtag = "no such object"
    end

    -- Step-4: write datalog;
    local datalog_body =
    {
        "info",   "clusterid",   clusterid,
        "info",   "user",        ubo.user_key,
        "info",   "bucket",      ubo.buck_name,
        "info",   "obj",         ubo.obj_name,
        "info",   "type",        "delete",
        "info",   "timestamp",   delete_time,
        "info",   "vertag",      ubo.obj_vtag,
    }
    
    if nil ~= versionid then
        local index = #datalog_body
        datalog_body[index+1] = "info"
        datalog_body[index+2] = "seqno"
        datalog_body[index+3] = versionid
    end

    if ubo.syncflag then
        local succ = write_datalog(metadb, ubo.buck_name, ubo.obj_name, seq_no, datalog_body)
        if not succ then
            ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " write datalog failed. buck_name=", ubo.buck_name, " obj_name=", ubo.obj_name, " seq_no=", seq_no)
        end
    end

    -- Step-5: create placeholder in 'placeholder' table;
    local succ = insert_placeholder(metadb, ubo.obj_key, ubo.buck_name, ubo.obj_name, delete_time, delobj_seq)
    if not succ then
        ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " failed to create placeholder. obj_key=", ubo.obj_key,
                          " buck_name=", ubo.buck_name, " obj_name=", ubo.obj_name, " seq_no=", seq_no)
    end

    return "0000", true
end

-- params:
--     metadb           : metadb (hbase/redis/lcache)
--     ubo              : user-bucket-object info
--     seq_no           : seq no of this delete op
--     delete_time      : timestamp of this delete op
-- return:
--     proxy_code, inner_code, ok
function _M.delete_obj(metadb, clusterid, ubo, seq_no, delete_time, versionid)
    local innercode,ok = remove_object(metadb, clusterid, ubo, seq_no, delete_time, versionid)
    if not ok then
        return "3012", innercode, false
    end
    return "0000", "0000", true
end

local function s3_delete_obj(metadb, clusterid, ubo, versionid)
    -- goofys removes a dir. such as "rm -fr a/b/c/"
    if nil == ubo.obj_key then
        local dirkeys = ubo.obj_dirkeys
        if nil == next(dirkeys) then -- remove the root dir; do nothing.
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " remove the root dir. do nothing")
            ngx.status = 204
            return "0000", "0000", true
        end

        --for "rm -fr a/b/c/", dirkeys={"#a/", "a/#b/", "a/b/#c/"}, so, the last one is for us to delete;
        local innercode,ok = oindex.rmdir(metadb, dirkeys[#dirkeys], versionid)
        if not ok then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to remove dir. innercode=", innercode)
            return "3012", innercode, false
        end

        ngx.status = 204
        return "0000", "0000", true
    end

    local delete_time = string.format("%0.3f", ngx.now())
    local seq_no = gen_seq_no(delete_time)
    local code, incode, ok = _M.delete_obj(metadb, clusterid, ubo, seq_no, delete_time, versionid)
    if not ok then
        return code, incode, ok
    end

    ngx.status = 204
    return "0000", "0000", true
end

-- description:
--     for large files, all data is stored in ceph
-- params:
--     ceph             : ceph client handle
--     obj_name         : the name of the s3 object
--     ceph_obj_prefix  : the prefix of the ceph objects (each ceph object is a piece of s3 object)
--     cont_len         : total data length
--     stream_read_func : func for read data from stream
--     check            : check == true, then compute the MD5 value
-- return:
--     code, incode, ok, etag
local function put_ceph_data(ceph, obj_name, ceph_obj_prefix, stream_read_func, cont_len, check)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " Enter. obj_name=", obj_name, " ceph_obj_prefix=", ceph_obj_prefix)

    local md5sum = nil
    if check then
        md5sum = md5:new()
    end

    local total = 0
    local chunksize = sp_conf.config["chunk_config"]["size"]
    local concurrent = sp_conf.config["chunk_config"]["wconcurrent"]
    local chunk_num = 1
    local done = false

    while not done do
        local chunk_body_pair = {}
        local index = 1

        for i=1, concurrent do
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", "entering in to readding chunk")
            mark_event("ChunkReadBeg")
            local chunk,err = stream_read_func(chunksize)
            mark_event("ChunkReadEnd")
            if err then
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "read chunk failed. err="..(err or "nil"))
                return "3016", "0000", false
            end
            if not chunk then --done
                done = true
                break
            end

            chunk_body_pair[index] = ceph_obj_prefix.."_"..chunk_num
            chunk_body_pair[index+1] = chunk

            local length = #chunk
            total = total + length
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "got chunk:"..chunk_body_pair[index].." length:"..length)

            if check then
                mark_event("ChunkMd5Beg")
                md5sum:update(chunk)
                mark_event("ChunkMd5End")
            end

            chunk_num = chunk_num + 1
            index = index + 2
        end

        if nil ~= next(chunk_body_pair) then
            mark_event("RgwBatchPutBeg")
            local status,ok,err = ceph:put_obj(obj_name, chunk_body_pair)
            mark_event("RgwBatchPutEnd")
            if not ok then
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ceph put_obj failed, err=", err)
                return "3004", "0000", false
            end
        end
    end -- end while

    local etag = nil
    if check then
        etag = md5sum:final()
        md5sum:reset()
    end

    if total ~= cont_len then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "Data read doesn't have the expected size. expected=".. cont_len..", actual="..total)
        return "3005", "0000", false
    end

    return "0000", "0000", true, etag
end

local function quota_check(user_info)
    local quota_enabled     = user_info["quota:enabled"]       --user quota in user table
    local quota_objects     = tonumber(user_info["quota:objects"]) or -1 -- -1 or normal value
    local quota_size_kb     = tonumber(user_info["quota:size_kb"]) or -1

    -- current user stats
    local size_bytes        = tonumber(user_info["stats:size_bytes"]) or 0 -- empty bucket have not set user/bucket stats
    local objects           = tonumber(user_info["stats:objects"]) or 0

    if not quota_enabled then
        return true
    end

    if -1 ~= quota_objects and quota_objects < objects then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " object num exceeds quota")
        return false
    end

    if -1 ~= quota_size_kb and quota_size_kb * 1024 < size_bytes then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " size_bytes exceeds quota")
        return false
    end

    return true
end

function _M.put_obj(metadb, headers, clusterid, ubo, seq_no, create_time, stream_read_func, check_md5, is_sync_op)
    local ok, cont_len = pcall(tonumber, headers["content-length"])
    if not ok or not cont_len then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " no valid Content-Length in headers")
        return "3013", "0000", false
    end

    -- no object should be created. for example, goofys mkdir;
    if ubo.obj_key == nil then
        if cont_len > 0 then --not a dir;
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " object name cannot end with '/'. obj_name=", ubo.obj_name)
            return "0010", "0000", false
        end

        --create ancestor dirs if necessary;
        if "fs" == ubo.user_type then
            local innercode,ok = oindex.mkdir_p(metadb, ubo.obj_dirkeys, seq_no, "usr")
            if not ok then
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to create ancestor dirs for ", ubo.obj_name,
                " innercode=", innercode, " dirkeys=", stringify(ubo.obj_dirkeys))
                return "3018", innercode, false
            end
        end

        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " no object should be created. obj_name=", ubo.obj_name)
        return "0000", "0000", true, EMPTY_MD5_HEX
    end

    if is_sync_op then
        local innercode,ok,allow = check_placeholder(metadb, ubo.obj_key, seq_no)
        if not ok then
            ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " failed to check placeholder. we allow the op by default.")
        else
            if not allow then
                ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " current op is outdated. seq_no=", seq_no)
                return "0026", "0000", false
            end
        end
    end

    local metadata = nil
    if headers["Proxy-Embedded-Metadata-len"] then
        local ok, metadata_len = pcall(tonumber, headers["Proxy-Embedded-Metadata-len"])
        if not ok or not metadata_len then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " no valid metadata_len in response headers")
            return "0023", "0000", false
        end

	if metadata_len > 0 then
	    local err = nil
	    metadata,err = stream_read_func(metadata_len)
	    if not metadata then
		ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to read metadata from HTTP body. err=", err)
		return "3016", "0000", false
	    end

	    assert(metadata_len <= cont_len)
	    cont_len = cont_len - metadata_len
	end
    else
        local ok = nil
        ok, metadata = pcall(json.encode, headers)
        if not ok then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to encode headers into sattrs. headers=", stringify(headers))
            return "0025", "0000", false
        end
    end

    local loc = nil

    local small_file_thresh = sp_conf.config["chunk_config"]["hsize"]
    local code, incode, etag, hdata, err
    if 0 == cont_len then
        loc = "none"   -- size=0, thus the obj is not stored anywhere;
        local md5sum = md5:new()
        etag = md5sum:final()
        md5sum:reset()
    elseif cont_len < small_file_thresh then
        --small files, that will be stored in hbase;
        loc = "hbase"

        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " write small file, which will be stored in meta_db")
        mark_event("HDataReadBeg")
        hdata, err = stream_read_func(cont_len)
        mark_event("HDataReadEnd")
        if err or not hdata then
            if "timeout" == err then
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " reading small file timed out!!!")
            else
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " reading small file failed, err=", err)
            end
            return "3016", "0000", false
        end

        if check_md5 then
            mark_event("HDataMd5Beg")
            local md5sum = md5:new()
            md5sum:update(hdata)
            etag = md5sum:final()
            md5sum:reset()
            mark_event("HDataMd5End")
        end
    else
        --big files, that will be stored in ceph;
        local ceph = objstore:pick_store(ubo.obj_name)
        assert(ceph)
 
        loc = ceph.id
        local ok = nil
        code, incode, ok, etag = put_ceph_data(ceph, ubo.obj_name, ubo.obj_vtag, stream_read_func, cont_len, check_md5)
        if not ok then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " put_ceph_data failed for ", ubo.obj_name, ". code=", code)
            return code, "0000", false
        end
    end

    local reqmd5 = headers["content-md5"]
    if check_md5 and reqmd5 then
        local base64md5 = ngx.encode_base64(etag)
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " etaghex=", etag, " base64md5=", base64md5)
        if base64md5 ~= reqmd5 then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " The Content-MD5 user specified (", string.lower(reqmd5),
                             ") did not match with the calculated one (", base64md5, ")")
            return "3006", "0000", false
        end
    end

    --we've succeeded to put data into ceph. now, put the object info in metadb ...

    local hsize = hdata and cont_len or 0

    local etaghex = headers["Proxy-Object-Etag"] or str.to_hex(etag)

    local chunksize = sp_conf.config["chunk_config"]["size"]
    local hbase_body =
    {
        "info",   "file",         "true",
        "info",   "oname",        ubo.obj_name,
        "info",   "bucket",       ubo.buck_key,
        "info",   "time",         create_time,
        "info",   "size",         cont_len,
        "info",   "hsize",        hsize,
        "info",   "etag",         etaghex,
        "ver",    "tag",          ubo.obj_vtag,
        "ver",    "seq",          seq_no,
        "mfest",  "loc",          loc,
        "mfest",  "psize",        0,
        "mfest",  "chksize",      chunksize,
    }

    if hdata then
        local index = #hbase_body
        hbase_body[index+1] = "hdata"
        hbase_body[index+2] = "data"
        hbase_body[index+3] = hdata
    end

    if metadata then
        local index = #hbase_body
        hbase_body[index+1] = "sattrs"
        hbase_body[index+2] = "json"
        hbase_body[index+3] = metadata
    end

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

    local del_metadata = {
        "info:link",
        "info:lock",
    }
    mark_event("PutMetaDataBeg")
    local code, incode, ok = _M.insert_object(metadb, ubo, seq_no, create_time, cont_len, hbase_body, datalog_body, del_metadata)
    mark_event("PutMetaDataEnd")
    if not ok then
        if "1121" == incode then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " insert_object failed because of a newer concurrent competitive write code=", code, " incode=", incode)
            return "0000", "0000", true, etaghex
        end
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " insert_object failed, code=", code, " incode=", incode)
        return code, incode, ok
    end

    return "0000", "0000", true, etaghex
end

local function upgrade_put_obj(metadb, ubo, hbasebody)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " Enter upgrade_put_obj. ubo=", stringify(ubo))

    local cont_len = tonumber(hbasebody["info:size"])
    if not cont_len then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " upgrade_put_obj failed. info:size is missing")
        return "0011", false
    end

    local create_time = hbasebody["info:time"]
    local seq_no = gen_seq_no(create_time)
    -- no object should be created. for example, goofys mkdir;
    if ubo.obj_key == nil then
        if cont_len > 0 then --not a dir;
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " upgrade_put_obj failed. object name cannot end with '/'. obj_name=", ubo.obj_name)
            return "0010", false
        end

        --create ancestor dirs if necessary;
        if "fs" == ubo.user_type then
            local innercode,ok = oindex.mkdir_p(metadb, ubo.obj_dirkeys, seq_no, "usr")
            if not ok then
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " upgrade_put_obj failed. failed to create ancestor dirs for ", ubo.obj_name,
                " innercode=", innercode, " dirkeys=", stringify(ubo.obj_dirkeys))
                return innercode, false
            end
        end

        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " upgrade_put_obj. no object should be created. obj_name=", ubo.obj_name)
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " Exit upgrade_put_obj. Success 1")
        return "0000", true
    end

    local hsize = hbasebody["info:hsize"]
    local etaghex = hbasebody["info:etag"]
    local psize = hbasebody["mfest:psize"]
    local chunksize = hbasebody["mfest:chksize"]

    if nil == create_time or nil == hsize or nil == etaghex or nil == psize or nil == chunksize then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " upgrade_put_obj failed. create_time, hsize, etaghex, psize or chunksize is missing")
        return "0010", false
    end

    local hdata = hbasebody ["hdata:data"]
    local metadata = hbasebody["sattrs:json"]

    local hbase_body =
    {
        "info",   "file",         "true",
        "info",   "oname",        ubo.obj_name,
        "info",   "bucket",       ubo.buck_key,
        "info",   "time",         create_time,
        "info",   "size",         cont_len,
        "info",   "hsize",        hsize,
        "info",   "etag",         etaghex,
        "ver",    "tag",          ubo.obj_vtag,
        "ver",    "seq",          seq_no,
        "mfest",  "loc",          "ceph-1",
        "mfest",  "psize",        psize,
        "mfest",  "chksize",      chunksize,
    }

    if hdata then
        local index = #hbase_body
        hbase_body[index+1] = "hdata"
        hbase_body[index+2] = "data"
        hbase_body[index+3] = hdata
    end

    if metadata then
        local index = #hbase_body
        hbase_body[index+1] = "sattrs"
        hbase_body[index+2] = "json"
        hbase_body[index+3] = metadata
    end

    local local_clusterid = sync_cfg["clusterid"]
    if nil == local_clusterid then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " upgrade_put_obj failed. local_clusterid is nil")
        return "0010", false
    end

    local optype = "put"
    if tonumber(psize) > 0 then
        optype = "multput"
    end

    local datalog_body =
    {
        "info",   "clusterid",   local_clusterid,
        "info",   "user",        ubo.user_key,
        "info",   "bucket",      ubo.buck_name,
        "info",   "obj",         ubo.obj_name,
        "info",   "type",        optype,
        "info",   "timestamp",   create_time,
        "info",   "vertag",      ubo.obj_vtag,
    }
    
    local del_metadata = {
        "info:link",
        "info:lock",
    }

    mark_event("PutMetaDataBeg")
    local code, incode, ok = _M.insert_object(metadb, ubo, seq_no, create_time, cont_len, hbase_body, datalog_body, del_metadata)
    mark_event("PutMetaDataEnd")
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " upgrade_put_obj failed. insert_object failed, code=", code, " incode=", incode)
        return incode, false
    end

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " Exit upgrade_put_obj. Success 2")
    return "0000", true
end


function _M.upgrade_obj(metadb, hbasebody)

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " Enter upgrade_obj. hbasebody=", stringify(hbasebody))

    local user_key = hbasebody["accessId"]
    local buck_key = hbasebody["bucketKey"]
    local buck_name = hbasebody["buckname"]
    local legacy_obj_key = hbasebody["rowkey"]
    local obj_vtag = hbasebody["ver:tag"]

    if nil == user_key or nil == buck_key or nil == buck_name or nil == legacy_obj_key or nil == obj_vtag then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " upgrade_obj failed. user_key, buck_key, buck_name, obj_vtag or legacy_obj_key is nil")
        return "0011", "0000", false
    end

    local i,j = string.find(legacy_obj_key, "_")
    if not i then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " upgrade_obj failed. legacy_obj_key is invalid. legacy_obj_key=", legacy_obj_key)
        return "0011", "0000", false
    end

    local buck_tag = string.sub(legacy_obj_key, 1, i-1)
    local obj_name = string.sub(legacy_obj_key, i+1, -1)

    if not buck_tag or not obj_name or "" == obj_name or "" == buck_tag then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " upgrade_obj failed. buck_tag or obj_name is invalid. buck_tag=", buck_tag, " obj_name=", obj_name)
        return "0011", "0000", false
    end


    local ASCII_SLASH = 47  --ascii code of '/' is 47;
    if string.byte(obj_name, -1, -1) ==  ASCII_SLASH then
        local size = hbasebody["info:size"]
        if not size then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " upgrade_obj. size is missing. legacy_obj_key=", legacy_obj_key)
            return "0011", "0000", false
        end

        if tonumber(size) > 0 then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " upgrade_obj. size of a dir is greater than 0. legacy_obj_key=", legacy_obj_key)
            return "0011", "0000", false
        end
    end

    local client_type = "s3"
    local errCode,succ,obj_key,dirkeys = oindex.parse_objname1(buck_tag, obj_name, client_type)
    if not succ then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " upgrade_obj failed. parse_objname1 failed. obj_name=", obj_name, " errCode=", errCode)
        return "0011", errCode, false
    end

    -- ubo_info: user-bucket-object info
    local ubo_info = new_tab(0, 6)

    ubo_info.user_key           = user_key
    ubo_info.user_type          = "fs"
    ubo_info.user_vtag          = nil
    ubo_info.user_displayname   = nil
    ubo_info.user_uid           = nil

    ubo_info.buck_key           = buck_key
    ubo_info.buck_vtag          = buck_tag
    ubo_info.buck_name          = buck_name

    ubo_info.obj_key            = obj_key
    ubo_info.obj_name           = obj_name
    ubo_info.obj_dirkeys        = dirkeys
    ubo_info.obj_vtag           = obj_vtag

    local errCode,succ = upgrade_put_obj(metadb, ubo_info, hbasebody)

    if not succ then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " upgrade_obj failed. upgrade_put_obj failed.")
        return "3017", errCode, false
    end

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " Exit upgrade_obj. Success")
    return "0000", "0000", true
end

local function s3_put_obj(metadb, headers, clusterid, ubo, check_md5)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " Enter. buck_name=", ubo.buck_name, " obj_name=", ubo.obj_name, " obj_key=", ubo.obj_key)

    mark_event("PutObjBeg")

    local ok, cont_len = pcall(tonumber, headers["content-length"])
    if not ok or not cont_len then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " no valid Content-Length in headers")
        return "3013", "0000", false
    end

    local httpc = http.new()
    local req_reader, err = httpc:get_client_body_reader()
    if err or (not req_reader and 0 ~= tonumber(headers["content-length"])) then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to get client body reader: err=", err)
        return "0014", "0000", false
    end
    mark_event("GotBodyReader")


    local create_time = string.format("%0.3f", ngx.now())
    local seq_no = gen_seq_no(create_time)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " seq_no generated for object ", ubo.obj_name, ". create_time=", create_time, " seq_no=", seq_no)

    local stat_code, inner_code, ok, etaghex = _M.put_obj(metadb, headers, clusterid, ubo, seq_no, create_time, req_reader, check_md5, false)
    ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " s3_put_object _M.put_objc: stat_code=", stat_code, " inner_code=", inner_code," ok=",ok)

    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " put_obj failed: stat_code=", stat_code, " inner_code=", inner_code)
        return stat_code, inner_code, ok
    end

    local response_headers = sp_conf.config["s3_GET_Service"]["s3_response_headers"]
    for k,v in pairs(response_headers) do
        ngx.header[k] = v
    end

    if headers["connection"] == "close" then
        ngx.header["Connection"] = "close"
    end
    ngx.header["Content-Length"] = 0

    if check_md5 then
        ngx.header["ETag"] = '"'..etaghex..'"'
    end
    return "0000", "0000", true
end

local function objs_of_part(ptag, psize, chunksize, pStart, pEnd)
    --starting chunk and offset in it
    local chunkStart = 1
    local offsetStart = 1
    if pStart then
        chunkStart = math.ceil(pStart/chunksize)
        offsetStart = pStart % chunksize
        if offsetStart == 0 then
            offsetStart = chunksize
        end
    end

    --ending chunk and offset in it
    local chunkEnd = nil
    local offsetEnd = nil

    if pEnd then
        chunkEnd = math.ceil(pEnd/chunksize)
        offsetEnd = pEnd % chunksize
        if offsetEnd == 0 then
            offsetEnd = chunksize
        end
    else
        chunkEnd = math.ceil(psize/chunksize)
        offsetEnd = psize % chunksize
        if offsetEnd == 0 then
            offsetEnd = chunksize
        end
    end

    local partObjs = {}
    local index = 1

    for i = chunkStart,chunkEnd do
        local obj = {
            name = ptag.."_"..tostring(i),
            --for chunks that are not the first neither the last, the entire chunk should be read
            start_pos = 0,
            end_pos = chunksize - 1,
        }

        --for the 1st chunk, the start pos needs to be modified
        if i == chunkStart then
            obj.start_pos = offsetStart - 1
        end

        --for the last chunk, the end pos needs to be modified
        if i == chunkEnd then
            obj.end_pos = offsetEnd - 1
        end

        partObjs[index] = obj
        index = index + 1
    end

    return partObjs
end

function _M.calc_objs(hsize, psize, chunksize, posStart, posEnd, objtag)
    local objs = {}

    local toRead = posEnd - posStart + 1
    local read = 0  -- how many bytes we have ready read?

    if not hsize or hsize == 0 then
        read = 0
        --not head data. so, for the next chunk, posStart and posEnd are NOT changed;
    elseif hsize >= posStart then
        if posEnd <= hsize then
            return objs
        end
        read = hsize - posStart + 1

        --for the next chunk, posStart and posEnd are changed; start at the 1st byte for next chunk.
        posStart = 1
        posEnd = posEnd - hsize
    else
        read = 0

        --for the next chunk, posStart and posEnd are changed; start at posStart - hsize for the next chunk
        posStart = posStart - hsize
        posEnd = posEnd - hsize
    end


    -- low layer objects to read. we will find out all such objects
    -- first, and read them at last.
    local index = 1

    if not psize or psize == 0 then  -- not multi uploaded file
        --starting chunk and offset in it
        local chunkStart = math.ceil(posStart/chunksize)
        local offsetStart = posStart % chunksize
        if offsetStart == 0 then
            offsetStart = chunksize
        end

        --ending chunk and offset in it
        local chunkEnd = math.ceil(posEnd/chunksize)
        local offsetEnd = posEnd % chunksize
        if offsetEnd == 0 then
            offsetEnd = chunksize
        end

        for i = chunkStart,chunkEnd do
            local obj = {
                name = objtag.."_"..tostring(i),
                --for chunks that are not the first neither the last, the entire chunk should be read
                start_pos = 0,
                end_pos = chunksize - 1,
            }

            --for the first chunk, modify the start pos
            if i==chunkStart then
                obj.start_pos =  offsetStart - 1
            end

            --for the last chunk, modify the end pos
            if i==chunkEnd then
                obj.end_pos = offsetEnd - 1
            end

            objs[index] = obj
            index = index + 1
        end
    else  --multi uploaded file
        --staring part and offset in it
        local partStart = math.ceil(posStart/psize)
        local pOffsetStart = posStart % psize
        if pOffsetStart == 0 then
            pOffsetStart = psize
        end

        --ending part and offset in it
        local partEnd = math.ceil(posEnd/psize)
        local pOffsetEnd = posEnd % psize
        if pOffsetEnd == 0 then
            pOffsetEnd = psize
        end

        for i = partStart,partEnd do
            local partObjs = nil
            local ptag = objtag.."_"..tostring(i)

            if i~=partStart and i~=partEnd then
                --if current is not the first neither the last part, get all low layer objects of it
                partObjs = objs_of_part(ptag, psize, chunksize)
            elseif i==partStart and i==partEnd then
                --if current is the first and the last part (only one part involved), get low layer objects, considering the start and end offsets
                partObjs = objs_of_part(ptag, psize, chunksize, pOffsetStart, pOffsetEnd)
            elseif i==partStart and i~=partEnd then
                --if current is the first but not the last part , get low layer objects, considering the start offset
                partObjs = objs_of_part(ptag, psize, chunksize, pOffsetStart, nil)
            else
                --if current is the last but not the first part , get low layer objects, considering the end offset
                partObjs = objs_of_part(ptag, psize, chunksize, nil, pOffsetEnd)
            end
            for i,o in ipairs(partObjs) do
                objs[index] = o
                index = index + 1
            end
        end
    end

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "ceph objects to read: \n", stringify(objs))
    return objs
end

function _M.rename_obj(metadb, headers, clusterid, ubo, seq_no, copy_time, is_sync_op, rsyncmtime)
    local copy_source = ngx.unescape_uri(headers["x-amz-copy-source"])
    local dst, err = iconv:gbk_to_utf8(copy_source)
    if dst then
        copy_source = dst
    end
    if nil == copy_source or "" == copy_source then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " x-amz-copy-source is nil")
        return "0012", "0000", false
    end

    --analyse uri to get bucketname, objectname
    local splittab = utils.strsplit(copy_source, "/")
    local s_bucket = ""
    local s_object = ""
    --eg. uri is /xxx//yyy/zzz/ or xxx/yyy///zzz, then first = xxx, second = yyy/zzz
    for i,v in ipairs(splittab) do
        if "" == s_bucket then
            s_bucket = v
        elseif "" == s_object then
            s_object = v
        else
            s_object = s_object .. "/" .. v
        end
    end

    if "" == s_bucket or "" == s_object then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " source bucket and object cannot be empty. s_bucket=", s_bucket, " s_object=", s_object)
        return "2004", "0000", false
    end

    if s_bucket == ubo.buck_name and s_object == ubo.obj_name then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " the destination object is the same one as the source")
        return "0023", "0000", false
    end

    --get src bucket info
    local s_bkey = ubo.user_vtag .. "_" .. s_bucket
    local code,ok,s_b_db = metadb:get("bucket", s_bkey, {"ver:tag"})
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to get source bucket. s_bkey=", s_bkey, " errorcode=", code)
        return "2004", code, false
    end
    if not s_b_db or not s_b_db["ver:tag"] then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " source bucket ", s_bkey, " does not exist!")
        return "0012","0000",false
    end

    local s_bvtag = s_b_db["ver:tag"]
    local errCode,succ,s_okey,s_dirkeys = oindex.parse_objname1(s_bvtag, s_object, (headers["root_secretkey"] ~= nil))
    if not succ then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to parse souce object name. s_object=", s_object, " errCode=", errCode)
        return "3017", errCode, false
    end

    local s_ubo_info = new_tab(0, 6)
    s_ubo_info.user_key           = ubo.user_key
    s_ubo_info.user_vtag          = ubo.user_vtag
    s_ubo_info.user_displayname   = ubo.user_displayname
    s_ubo_info.user_uid           = ubo.user_uid
    s_ubo_info.user_type          = ubo.user_type

    s_ubo_info.buck_key           = s_bkey
    s_ubo_info.buck_name          = s_bucket
    s_ubo_info.buck_vtag          = s_bvtag

    s_ubo_info.obj_key            = s_okey
    s_ubo_info.obj_name           = s_object
    s_ubo_info.obj_vtag           = nil
    s_ubo_info.obj_dirkeys        = s_dirkeys
    s_ubo_info.syncflag           = ubo.syncflag
    
    if nil == s_okey then -- we are copying a dir, not an object;
        --create ancestor dirs for the destination object if necessary;
        if "fs" == ubo.user_type then
            local innercode,ok = oindex.mkdir_p(metadb, ubo.obj_dirkeys, seq_no, "usr")
            if not ok then
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to create ancestor dirs for ", ubo.obj_name,
                " innercode=", innercode, " dirkeys=", stringify(ubo.obj_dirkeys))
                return "3018", innercode, false
            end
            local statcode, innercode, ok = s3_delete_obj(metadb, clusterid, s_ubo_info, nil)
            if not ok then 
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to delete ancestor dirs for ", s_ubo_info.obj_name,
                " innercode=", innercode, " dirkeys=", stringify(s_ubo_info.obj_dirkeys))
                return statcode, innercode, false
            end
        end
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " no object should be copied. obj_name=", ubo.obj_name)
        return "0000", "0000", true, EMPTY_MD5_HEX, copy_time
    end

    if nil == ubo.obj_key then --no source obj, but we have dest obj, error;
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " object name cannot end with '/'. obj_name=", ubo.obj_name)
        return "0010", "0000", false
    end

    if is_sync_op then
        local innercode,ok,allow = check_placeholder(metadb, ubo.obj_key, seq_no)
        if not ok then
            ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " failed to check placeholder. we allow the op by default.")
        else
            if not allow then
                ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " current op is outdated. seq_no=", seq_no)
                return "0026", "0000", false
            end
        end
    end

    local innercode,ok,s_o_db = metadb:checkAndMutateAndGetRow("object", s_okey, 
                                                         "ver", "tag", metadb.CompareOp.NOT_EQUAL, nil, 
                                                         {"info", "lock", "true"}, nil, 
                                                         nil)
    if not ok then
        if "1121" == innercode then --failed, because of condition-check-failure
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " source object ", s_okey, " does not exist!")
            return "3001", innercode, false
        else --failed, because of other errors than condition-check-failure
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to get source object metadata, s_okey=", s_okey)
            return "3002", innercode, false
        end
    end

    if nil == s_o_db or nil == s_o_db["ver:tag"] then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " source object ", s_okey, " does not exist!")
        return "3001","0000",false
    end

    local s_size = tonumber(s_o_db["info:size"]) -- the size have exclude the metadata.
    local s_hsize = tonumber(s_o_db["info:hsize"])
    local s_etag = s_o_db["info:etag"] or 0
    local s_objtag = s_o_db["ver:tag"]
    local s_psize = tonumber(s_o_db["mfest:psize"])
    local s_chunksize = tonumber(s_o_db["mfest:chksize"])
    local s_infolink = s_o_db["info:link"]
    local s_oname = s_o_db["info:oname"]
    local s_seqno = s_o_db["ver:seq"]

    if s_size > s_hsize then
        if s_infolink == nil then
            ubo.infolink = s_objtag.."_"..s_oname
        else
            ubo.infolink = s_infolink
        end
    end
    --we've succeeded to put data into ceph. now, put the object info in metadb ...
    local etaghex = utils.md5hex(s_etag)

    local hbase_body =
    {
        "info",   "file",    "true",
        "info",   "oname",   ubo.obj_name,
        "info",   "bucket",  ubo.buck_key,
        "info",   "time",    copy_time,
        "info",   "size",    s_size,
        "info",   "hsize",   s_hsize,
        "info",   "etag",    etaghex,
        "ver",    "tag",     ubo.obj_vtag,
        "ver",    "seq",     seq_no,
        "mfest",  "loc",     s_o_db["mfest:loc"], --Yuanguo: copy to the same ceph-cluster;
        "mfest",  "psize",   s_psize,
        "mfest",  "chksize", s_chunksize,
    }

    table.insert(hbase_body, "info")
    table.insert(hbase_body, "rsyncmtime")
    if rsyncmtime and #rsyncmtime>0 and tonumber(rsyncmtime) then
        table.insert(hbase_body, rsyncmtime)
    else
        table.insert(hbase_body, "0")
    end

    if s_o_db["sattrs:json"] then
        local index = #hbase_body
        hbase_body[index+1] = "sattrs"
        hbase_body[index+2] = "json"
        hbase_body[index+3] =  s_o_db["sattrs:json"]
    end

    if 0 ~= s_hsize then
        local hdata = s_o_db["hdata:data"]
        assert(hdata)

        local index = #hbase_body
        hbase_body[index+1] = "hdata"
        hbase_body[index+2] = "data"
        hbase_body[index+3] = hdata
    end

    if nil ~= ubo.infolink then
        local index = #hbase_body
        hbase_body[index+1] = "info"
        hbase_body[index+2] = "link"
        hbase_body[index+3] = ubo.infolink
    end

    local datalog_body =
    {
        "info",   "clusterid",  clusterid,
        "info",   "user",       ubo.user_key,
        "info",   "bucket",     ubo.buck_name,
        "info",   "obj",        ubo.obj_name,
        "info",   "type",       "copy",
        "info",   "timestamp",  copy_time,
        "info",   "vertag",     ubo.obj_vtag,

        "info",   "sbucket",    s_bucket,
        "info",   "sobj",       s_object,
    }

    local del_metadata = {
        "info:lock",
    }

    local code, incode, ok = _M.insert_object(metadb, ubo, seq_no, copy_time, s_size, hbase_body, datalog_body, del_metadata)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " insert_object failed, code=", code, " incode=", incode)
        return code, incode, ok
    end

    local statcode, innercode, ok = s3_delete_obj(metadb, clusterid, s_ubo_info, s_seqno)
    if not ok then 
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to delete s_obj for ", s_ubo_info.obj_name,
        " innercode=", innercode)
        return statcode, innercode, false
    end
    return "0000", "0000", true, etaghex, copy_time
end

function _M.copy_obj(metadb, headers, clusterid, ubo, seq_no, copy_time, is_sync_op)
    local copy_source = ngx.unescape_uri(headers["x-amz-copy-source"])
    local dst, err = iconv:gbk_to_utf8(copy_source)
    if dst then
        copy_source = dst
    end
    if nil == copy_source or "" == copy_source then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " x-amz-copy-source is nil")
        return "0012", "0000", false
    end

    --analyse uri to get bucketname, objectname
    local splittab = utils.strsplit(copy_source, "/")
    local s_bucket = ""
    local s_object = ""
    --eg. uri is /xxx//yyy/zzz/ or xxx/yyy///zzz, then first = xxx, second = yyy/zzz
    for i,v in ipairs(splittab) do
        if "" == s_bucket then
            s_bucket = v
        elseif "" == s_object then
            s_object = v
        else
            s_object = s_object .. "/" .. v
        end
    end

    if "" == s_bucket or "" == s_object then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " source bucket and object cannot be empty. s_bucket=", s_bucket, " s_object=", s_object)
        return "2004", "0000", false
    end

    if s_bucket == ubo.buck_name and s_object == ubo.obj_name then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " the destination object is the same one as the source")
        return "0023", "0000", false
    end

    --get src bucket info
    local s_bkey = ubo.user_vtag .. "_" .. s_bucket
    local code,ok,s_b_db = metadb:get("bucket", s_bkey, {"ver:tag"})
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to get source bucket. s_bkey=", s_bkey, " errorcode=", code)
        return "2004", code, false
    end
    if not s_b_db or not s_b_db["ver:tag"] then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " source bucket ", s_bkey, " does not exist!")
        return "0012","0000",false
    end

    local s_bvtag = s_b_db["ver:tag"]
    local errCode,succ,s_okey = oindex.parse_objname1(s_bvtag, s_object, (headers["root_secretkey"] ~= nil))
    if not succ then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to parse souce object name. s_object=", s_object, " errCode=", errCode)
        return "3017", errCode, false
    end

    --create ancestor dirs for the destination object if necessary;
    if "fs" == ubo.user_type then
        local innercode,ok = oindex.mkdir_p(metadb, ubo.obj_dirkeys, seq_no, "usr")
        if not ok then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to create ancestor dirs for ", ubo.obj_name,
            " innercode=", innercode, " dirkeys=", stringify(ubo.obj_dirkeys))
            return "3018", innercode, false
        end
    end
    
    if nil == s_okey then -- we are copying a dir, not an object;
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " no object should be copied. obj_name=", ubo.obj_name)
        return "0000", "0000", true, EMPTY_MD5_HEX, copy_time
    end

    if nil == ubo.obj_key then --no source obj, but we have dest obj, error;
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " object name cannot end with '/'. obj_name=", ubo.obj_name)
        return "0010", "0000", false
    end

    if is_sync_op then
        local innercode,ok,allow = check_placeholder(metadb, ubo.obj_key, seq_no)
        if not ok then
            ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " failed to check placeholder. we allow the op by default.")
        else
            if not allow then
                ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " current op is outdated. seq_no=", seq_no)
                return "0026", "0000", false
            end
        end
    end
    
    local obj_columns = 
    {
        "info:size",
        "info:hsize",
        "info:etag",
        "info:time",
        "info:link",
        "ver:tag",
        "mfest:loc",
        "mfest:psize",
        "mfest:chksize",
        "sattrs:json",
        "hdata:data",
    }

    local stat,ok,s_o_db = metadb:get("object", s_okey, obj_columns)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to get source object metadata, s_okey=", s_okey)
        return "3002", stat, false
    end

    if nil == s_o_db or nil == s_o_db["ver:tag"] then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " source object ", s_okey, " does not exist!")
        return "3001","0000",false
    end

    local s_size = tonumber(s_o_db["info:size"]) -- the size have exclude the metadata.
    local s_hsize = tonumber(s_o_db["info:hsize"])
    local s_etag = s_o_db["info:etag"] or 0
    local s_objtag = s_o_db["ver:tag"]
    local s_psize = tonumber(s_o_db["mfest:psize"])
    local s_chunksize = tonumber(s_o_db["mfest:chksize"])
    local s_infolink = s_o_db["info:link"]

    local concurrent = sp_conf.config["chunk_config"]["rconcurrent"]

    local ceph_objtag = s_objtag
    local ceph_objname = s_object
    if nil ~= s_infolink then
        local s,e = string.find(s_infolink, "_")
        ceph_objtag  = string.sub(s_infolink, 1, s-1) 
        ceph_objname = string.sub(s_infolink, e+1, -1)
    end

    --get source objs info
    local objs = _M.calc_objs(s_hsize, s_psize, s_chunksize, 1, s_size, ceph_objtag)
    if nil ~= objs and nil ~= next(objs) then
        local ceph = objstore:get_store(s_o_db["mfest:loc"])    --Yuanguo: copy to the same ceph-cluster;
        assert(ceph)

        local batch = {}
        local bindex = 1

        for i=1,#objs do
            batch[bindex] = objs[i]
            bindex = bindex + 1

            if i%concurrent == 0 or i == #objs then
                ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " reading objects from ceph: \n", stringify(batch))
                local code,ok,res = ceph:get_obj(ceph_objname, batch)
                if not ok or "table" ~= type(res) or #res ~= bindex-1 then
                    ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " Error occurred when reading objects from ceph. code=", code, " res=", stringify(res))
                    return "3009", code, false
                end

                ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " reading objects from ceph succeeded.")

                local chunk_body_pair = {}
                local index = 1
                for k=1, #res do
                    chunk_body_pair[index] = string.gsub(batch[k]["name"], ceph_objtag, ubo.obj_vtag)
                    chunk_body_pair[index+1] = res[k]
                    index = index + 2
                end

                if nil ~= next(chunk_body_pair) then
                    local status,ok,err = ceph:put_obj(ubo.obj_name, chunk_body_pair)
                    if not ok then
                        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ceph put_obj failed, err=", err)
                        return "3004", status, false
                    end
                end

                batch = {}
                bindex = 1
            end
        end
    end

    --we've succeeded to put data into ceph. now, put the object info in metadb ...

    local etaghex = utils.md5hex(s_etag)

    local hbase_body =
    {
        "info",   "file",    "true",
        "info",   "oname",   ubo.obj_name,
        "info",   "bucket",  ubo.buck_key,
        "info",   "time",    copy_time,
        "info",   "size",    s_size,
        "info",   "hsize",   s_hsize,
        "info",   "etag",    etaghex,
        "ver",    "tag",     ubo.obj_vtag,
        "ver",    "seq",     seq_no,
        "mfest",  "loc",     s_o_db["mfest:loc"], --Yuanguo: copy to the same ceph-cluster;
        "mfest",  "psize",   s_psize,
        "mfest",  "chksize", s_chunksize,
    }

    if s_o_db["sattrs:json"] then
        local index = #hbase_body
        hbase_body[index+1] = "sattrs"
        hbase_body[index+2] = "json"
        hbase_body[index+3] =  s_o_db["sattrs:json"]
    end

    if 0 ~= s_hsize then
        local hdata = s_o_db["hdata:data"]
        assert(hdata)

        local index = #hbase_body
        hbase_body[index+1] = "hdata"
        hbase_body[index+2] = "data"
        hbase_body[index+3] = hdata
    end

    local datalog_body =
    {
        "info",   "clusterid",  clusterid,
        "info",   "user",       ubo.user_key,
        "info",   "bucket",     ubo.buck_name,
        "info",   "obj",        ubo.obj_name,
        "info",   "type",       "copy",
        "info",   "timestamp",  copy_time,
        "info",   "vertag",     ubo.obj_vtag,

        "info",   "sbucket",    s_bucket,
        "info",   "sobj",       s_object,
    }

    local del_metadata = {
        "info:lock",
        "info:link",
    }

    local code, incode, ok = _M.insert_object(metadb, ubo, seq_no, copy_time, s_size, hbase_body, datalog_body, del_metadata)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " insert_object failed, code=", code, " incode=", incode)
        return code, incode, ok
    end

    return "0000", "0000", true, etaghex, copy_time
end

local function put_obj_copy(metadb, headers, clusterid, ubo)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " Enter. buck_name=", ubo.buck_name, " obj_name=", ubo.obj_name, " obj_key=", ubo.obj_key,
                      " source=", headers["x-amz-copy-source"])

    mark_event("PutObjCopyBeg")

    local copy_time = string.format("%0.3f", ngx.now())
    local seq_no = gen_seq_no(copy_time)

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " seq_no generated for object ", ubo.obj_name, ". copy_time=", copy_time, " seq_no=", seq_no)

    local stat_code  = nil
    local inner_code = nil
    local ok         = nil
    local etaghex    = nil
    local infotime   = nil
    local is_rename_op = (headers["x-amz-meta-rename"] == "RENAME")
    if is_rename_op then
        stat_code, inner_code, ok, etaghex, infotime = _M.rename_obj(metadb, headers, clusterid, ubo, seq_no, copy_time, false, headers["x-amz-meta-rsyncmtime"])
        if not ok then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " rename_obj failed. buck_name=", ubo.buck_name, " obj_name=", ubo.obj_name,
                             " source=", headers["x-amz-copy-source"])
            return stat_code, inner_code, ok
        end
    else
        stat_code, inner_code, ok, etaghex, infotime = _M.copy_obj(metadb, headers, clusterid, ubo, seq_no, copy_time, false)
        if not ok then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " copy_obj failed. buck_name=", ubo.buck_name, " obj_name=", ubo.obj_name,
                             " source=", headers["x-amz-copy-source"])
            return stat_code, inner_code, ok
        end
    end

    local response_headers = sp_conf.config["s3_GET_Service"]["s3_response_headers"]
    for k,v in pairs(response_headers) do
        ngx.header[k] = v
    end

    if headers["connection"] == "close" then
        ngx.header["Connection"] = "close"
    end

    local rebody = "<CopyObjectResult><LastModified>"..utils.toUTC(infotime).."</LastModified>"..
                    "<ETag>"..etaghex.."</ETag>"..
                    "</CopyObjectResult>"

    ngx.header["Content-Length"] = #rebody

    ngx.print(rebody)
    return "0000", "0000", true
end

local function get_obj_acl(metadb, headers, ubo)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " Enter. obj_key=", ubo.obj_key)

    if nil == ubo.obj_key then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " object name cannot end with '/'. obj_name=", ubo.obj_name)
        return "0010", "0000", false
    end

    local code,ok,res = metadb:get("object", ubo.obj_key, {"info:oname"})
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to get object 'info:name' from metadb. obj_key=", ubo.obj_key)
        return "3002", code, false
    end

    if not res["info:oname"] then
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " objcet does not exist")
        return "3001", "0000", false
    end

    local res_body = string.format([[<?xml version="1.0" encoding="UTF-8"?><AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Owner><ID>%s</ID><DisplayName>%s</DisplayName></Owner><AccessControlList><Grant><Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser"><ID>%s</ID><DisplayName>%s</DisplayName></Grantee><Permission>FULL_CONTROL</Permission></Grant></AccessControlList></AccessControlPolicy>]], ubo.user_uid, ubo.user_displayname, ubo.user_uid, ubo.user_displayname)

    local response_headers = sp_conf.config["s3_GET_Service"]["s3_response_headers"]
    for k,v in pairs(response_headers) do
        ngx.header[k] = v
    end
    ngx.header["Content-Type"] = "application/xml"
    ngx.header["Content_Length"] = #res_body

    ngx.print(res_body)
    return "0000", "0000", true
end

function _M.head_obj(metadb, headers, ubo, req_seq_no)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " Enter. buck_name=", ubo.buck_name, " obj_name=", ubo.obj_name, " obj_key=", ubo.obj_key)

    if not ubo.obj_key then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " obj_key=nil means a head op to a dir, so return 404")
        ngx.status = 404
        return "0000","0000",true
    end

    local innercode,ok,obj_meta = oindex.head(metadb, ubo.obj_key)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " oindex.head failed, obj_key=", ubo.obj_key)
        ngx.status = 500
        return "0000", innercode, true
    end

    if nil == next(obj_meta) then
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " object ", ubo.obj_key, " does not exist!")
        ngx.status = 404
        return "0000","0000",true
    end

    if req_seq_no then
        local obj_seq_no = obj_meta["ver:seq"]
        if obj_seq_no and req_seq_no ~= obj_seq_no then
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " object seq_no doesn't match the requested seq_no. obj_seq_no=", obj_seq_no,
                              " req_seq_no=", req_seq_no)
            ngx.status = 404
            return "0000","0000",true
        end

        ngx.header["Proxy-Sync-Partsize"] = obj_meta["mfest:psize"]
        ngx.header["Proxy-Sync-Uploadid"] = obj_meta["ver:tag"]
        ngx.header["Proxy-Sync-Metadata"] = obj_meta["sattrs:json"]
    end

    local etaghex = utils.md5hex(obj_meta["info:etag"])
    local obj_head = obj_meta["sattrs:json"]

    if nil ~= obj_head and "" ~= obj_head then
        local ok, jbody = pcall(json.decode, obj_head)
        if ok and type(jbody) == "table" then
            for k,v in pairs(jbody) do
                if nil ~= string.find(k, "x%-amz%-meta%-") then
                    ngx.header[k] = v
                elseif nil ~= string.find(k, "content%-type") then
                    ngx.header["Content-Type"] = v
                elseif nil ~= string.find(k, "cache%-control") then
                    ngx.header["Cache-Control"] = v
                elseif nil ~= string.find(k, "expires") then
                    ngx.header["Expires"] = v
                end
            end
        end
    end

    ngx.header["Content_Length"] = tonumber(obj_meta["info:size"])
    ngx.header["ETag"] = '"'..etaghex..'"'

    local mtime = nil
    local rsyncmtime = obj_meta["info:rsyncmtime"]
    if rsyncmtime and tonumber(rsyncmtime) and tonumber(rsyncmtime) > 0 then
        mtime = tonumber(rsyncmtime) 
    else
        mtime = tonumber(obj_meta["info:time"])
    end

    ngx.header["Last-Modified"] = ngx.http_time(mtime)

    return "0000" , "0000", true
end

--returns:
--   ok        : true, if 'bytes' header is valid; false, otherwise;
--   partial   : true, if this is a range request; false, otherwise;
--   start_pos : starting position, counting from 1, inclusive
--   end_pos   : end position, counting from 1, inclusive
function _M.parse_range(size, range_str)
    if not range_str or range_str == "" then
        return true, false, 1, size
    end

    local cp, err = ngx.re.match(range_str, "^\\s*bytes=([0-9]*)-([0-9]*)$", "jo")
    if not cp then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "range is invalid, pattern is incorrect: "..range_str)
        return false,nil,nil,nil
    end

    local posStart = tonumber(cp[1])
    local posEnd = tonumber(cp[2])

    if nil == posStart and nil == posEnd then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "range is invalid, start and end are both nil: "..range_str)
        return false,nil,nil,nil
    end

    if nil ~= posStart and nil ~= posEnd then
        --in HTTP 1.1, the posistions start from 0, in lua index starts from 1, so
        --make them consistent
        posStart = posStart + 1
        posEnd = posEnd + 1

        if posStart < 1 then
            posStart = 1
        end
        if posEnd > size then
            posEnd = size
        end
    elseif posStart ~= nil then
        posStart = posStart + 1
        if posStart < 1 then
            posStart = 1
        end
        posEnd = size
    elseif posEnd ~= nil then  --: -500 means to read the last 500 bytes
        posStart = size - posEnd + 1
        posEnd = size
    end

    if posStart > posEnd then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "range is invalid, start position is greater than end posistion: "..range_str)
        return false,nil,nil,nil
    end

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "posStart=", posStart, " posEnd=", posEnd)
    return true,true,posStart,posEnd
end



-- params:
--     metadb     : metadb (hbase/redis/lcache)
--     headers    : the request headers
--     ubo        : user-buck-object info
--     check_md5  : bool, whether to compute and compare the MD5 value
--     req_seq_no : if not nil, get the object with the given seq_no:
--     is_sync_op : if true, this get-request is origined from a sync-peer. we need to return extra metadata of the object;
-- return:
--     proxy_code, inner_code, ok
function _M.get_obj(metadb, headers, ubo, check_md5, req_seq_no, is_sync_op)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " Enter. buck_name=", ubo.buck_name, " obj_name=", ubo.obj_name, " obj_key=", ubo.obj_key)

    -- "GET a/b/" will goto list_objects;
    if nil == ubo.obj_key then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " object name cannot end with '/'. obj_name=", ubo.obj_name)
        return "0010", "0000", false
    end

    local obj_columns = {
        "info:size",
        "info:hsize",
        "info:etag",
        "info:time",
        "info:rsyncmtime",
        "info:link",
        "ver:tag",
        "ver:seq",
        "mfest:loc",
        "mfest:psize",
        "sattrs:json"
    }
    local stat,ok,res = metadb:get("object", ubo.obj_key, obj_columns)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to get object from metadb. obj_key=", ubo.obj_key)
        return "3002", stat, false
    end

    if nil == res or  nil == res["ver:tag"] then
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " object ", ubo.obj_key, " does not exist!")
        return "3001","0000",false
    end

    local ver_seq = res["ver:seq"]
    if ver_seq and req_seq_no and (ver_seq ~= req_seq_no) then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " the object ver:seq does not match the requested seq_no. ver_seq=", ver_seq, " req_seq_no=", req_seq_no)
        return "3001","0000",false
    end

    local size = tonumber(res["info:size"])
    local hsize = tonumber(res["info:hsize"])
    local etag = res["info:etag"]
    local objtag = res["ver:tag"]
    local psize = tonumber(res["mfest:psize"])
    local range_str = headers["range"]
    
    local ceph_objtag = objtag
    local ceph_objname = ubo.obj_name
    local link = res["info:link"]
    if nil ~= link then
        local s,e = string.find(link, "_")
        ceph_objtag  = string.sub(link, 1, s-1) 
        ceph_objname = string.sub(link, e+1, -1)
    end

    local ok, partial,posStart,posEnd = _M.parse_range(size, range_str)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " request range is invalid, range=", range_str)
        return "0019", "0000", false
    end

    local toRead = posEnd - posStart + 1
    local read = 0  -- how many bytes we have ready read?

    local obj_head = res["sattrs:json"]
    if nil ~= obj_head and "" ~= obj_head then
        local ok, jbody = pcall(json.decode, obj_head)
        if ok and type(jbody) == "table" then
            for k,v in pairs(jbody) do
                if nil ~= string.find(k, "x%-amz%-meta%-") then
                    ngx.header[k] = v
                elseif nil ~= string.find(k, "content%-type") then
                    ngx.header["Content-Type"] = v
                elseif nil ~= string.find(k, "cache%-control") then
                    ngx.header["Cache-Control"] = v
                elseif nil ~= string.find(k, "expires") then
                    ngx.header["Expires"] = v
                end
            end
        end
    end
    local etaghex = utils.md5hex(etag)
    ngx.header["ETag"] = '"'..etaghex..'"'

    local mtime = nil
    local rsyncmtime = res["info:rsyncmtime"]
    if rsyncmtime and tonumber(rsyncmtime) and tonumber(rsyncmtime) > 0 then
        mtime = tonumber(rsyncmtime) 
    else
        mtime = tonumber(res["info:time"])
    end

    ngx.header["Last-Modified"] = ngx.http_time(mtime)
    ngx.header["Content-Length"] = toRead

    if partial then
        ngx.status = 206
        check_md5 = false  --we check md5 only when this is not a range request;
        ngx.header["Content-Range"] = "bytes "..tostring(posStart-1).."-"..tostring(posEnd-1).."/"..tostring(size)
    end

    --rsync get
    if is_sync_op then
        ngx.header["Proxy-Object-Etag"] = utils.md5hex(etag)
        ngx.header["Proxy-Object-Ctime"] = res["info:time"]
        ngx.header["Proxy-Object-SeqNo"] = res["ver:seq"]
	if nil ~= obj_head then
	    ngx.header["Proxy-Embedded-Metadata-len"] = #obj_head
            ngx.header["Content-Length"] = ngx.header["Content-Length"] + #obj_head
	    ngx.print(obj_head)
	end
    end

    local responsed = false
    local md5sum = nil
    if check_md5 then
        md5sum = md5:new()
    end

    if not hsize or hsize == 0 then
        read = 0
        --no head data. so, for the next chunk, posStart and posEnd are NOT changed;
    elseif hsize >= posStart then
        local code,ok,res = metadb:get("object",ubo.obj_key,{"hdata:data"})
        if not ok then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to get object hdata from metadb. obj_key=", ubo.obj_key, " code=", code)
            return "3002", code, false
        end
        local hdata = res["hdata:data"]
        assert(hdata)

        if check_md5 then
            md5sum:update(hdata)
        end

        if posEnd <= hsize then
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " got all requested data from hdata, posStart=", posStart, " posEnd=", posEnd)
            ngx.print(string.sub(hdata, posStart, posEnd))
            return "0000", "0000", true
        end

        ngx.print(string.sub(hdata, posStart, hsize))
        responsed = true

        read = hsize - posStart + 1

        --for the next chunk, posStart and posEnd are changed; start at the 1st byte for next chunk.
        posStart = 1
        posEnd = posEnd - hsize
    else
        read = 0

        --for the next chunk, posStart and posEnd are changed; start at posStart - hsize for the next chunk
        posStart = posStart - hsize
        posEnd = posEnd - hsize
    end

    local chunksize = sp_conf.config["chunk_config"]["size"]

    -- low layer objects to read. we will find out all such objects
    -- first, and read them at last.
    local objs = {}
    local index = 1

    if not psize or psize == 0 then  -- not multi uploaded file
        --starting chunk and offset in it
        local chunkStart = math.ceil(posStart/chunksize)
        local offsetStart = posStart % chunksize
        if offsetStart == 0 then
            offsetStart = chunksize
        end

        --ending chunk and offset in it
        local chunkEnd = math.ceil(posEnd/chunksize)
        local offsetEnd = posEnd % chunksize
        if offsetEnd == 0 then
            offsetEnd = chunksize
        end

        for i = chunkStart,chunkEnd do
            local obj = {
                name = ceph_objtag.."_"..tostring(i),
                --for chunks that are not the first neither the last, the entire chunk should be read
                start_pos = 0,
                end_pos = chunksize - 1,
            }

            --for the first chunk, modify the start pos
            if i==chunkStart then
                obj.start_pos =  offsetStart - 1
            end

            --for the last chunk, modify the end pos
            if i==chunkEnd then
                obj.end_pos = offsetEnd - 1
            end

            objs[index] = obj
            index = index + 1
        end
    else  --multi uploaded file
        --staring part and offset in it
        local partStart = math.ceil(posStart/psize)
        local pOffsetStart = posStart % psize
        if pOffsetStart == 0 then
            pOffsetStart = psize
        end

        --ending part and offset in it
        local partEnd = math.ceil(posEnd/psize)
        local pOffsetEnd = posEnd % psize
        if pOffsetEnd == 0 then
            pOffsetEnd = psize
        end

        for i = partStart,partEnd do
            local partObjs = nil
            local ptag = ceph_objtag.."_"..tostring(i)

            if i~=partStart and i~=partEnd then
                --if current is not the first neither the last part, get all low layer objects of it
                partObjs = objs_of_part(ptag, psize, chunksize)
            elseif i==partStart and i==partEnd then
                --if current is the first and the last part (only one part involved), get low layer objects, considering the start and end offsets
                partObjs = objs_of_part(ptag, psize, chunksize, pOffsetStart, pOffsetEnd)
            elseif i==partStart and i~=partEnd then
                --if current is the first but not the last part , get low layer objects, considering the start offset
                partObjs = objs_of_part(ptag, psize, chunksize, pOffsetStart, nil)
            else
                --if current is the last but not the first part , get low layer objects, considering the end offset
                partObjs = objs_of_part(ptag, psize, chunksize, nil, pOffsetEnd)
            end
            for i,o in ipairs(partObjs) do
                objs[index] = o
                index = index + 1
            end
        end
    end

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ceph objects to read: ", stringify(objs))

    local concurrent = sp_conf.config["chunk_config"]["rconcurrent"]

    if nil ~= next(objs) then
        local ceph = objstore:get_store(res["mfest:loc"])
        assert(ceph)

        local batch = {}
        local bindex = 1

        for i=1,#objs do
            batch[bindex] = objs[i]
            bindex = bindex + 1

            if i%concurrent == 0 or i == #objs then
                ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " reading objects from ceph: ", stringify(batch))
                local code,ok,res = ceph:get_obj(ceph_objname, batch)
                if not ok or "table" ~= type(res) or #res ~= bindex-1 then
                    ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " Error occurred when reading objects from ceph. code=", code, " res=", stringify(res))
                    if responsed then
                        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " size read (", read, ") does not match size requested (", toRead, ")")
                        return "3010", "0000", false
                    end
                    return "3009", code, false
                end

                ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " reading objects from ceph succeeded.")

                for k=1, #res do
                    local len = string.len(res[k])
                    local req_len = batch[k].end_pos - batch[k].start_pos + 1 
                    if len ~= req_len then
                        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " size returned by rgw (", len, ") does not match size requested (", req_len, ")")
                        return "3010", "0000", false
                    end
                    read = read + len
                    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " read=", read, " len=", len)
                    ngx.print(res[k])
                    local ok,err = ngx.flush(true)
                    if not ok then
                        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ngx print failed. err=", err)
                        ngx.exit(ngx.HTTP_OK)
                        return "0000", "0000", true
                    end
                    responsed = true
                    if check_md5 then
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

    if check_md5 then
        local etag_new = md5sum:final()
        md5sum:reset()
        if etag and etag ~= etag_new then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " etag we've calculated '", etag_new, "' does not match info:etag '", etag, "'")
            return "3011", "0000", false
        end
    end

    return "0000", "0000", true
end

function _M.create_temp_obj(metadb, headers, ubo, uploadId)
    if nil == ubo.obj_key then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " object name cannot end with '/'. obj_name=", ubo.obj_name)
        return "0010", "0000", false
    end

    local obj_uploadId = ubo.obj_key .. "_" .. uploadId

    local ok, sattrs
    sattrs = headers["Proxy-Sync-Metadata"]
    if not sattrs then
        ok, sattrs = pcall(json.encode, headers)
        if not ok then
            ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ",  "failed to encode headers into sattrs, thus skip it")
        end
    end

    local ceph = objstore:pick_store(ubo.obj_name)
    assert(ceph)
    local loc = ceph.id

    local hbase_body = {
        "info" ,"bucket", ubo.buck_name,
        "info" ,"ctime", string.format("%0.3f", ngx.now()),
        "mfest",  "loc", loc,
        "flag" ,"flag", "0",
        "sattrs", "json", sattrs
    }
    local status,ok = metadb:put("temp_object", obj_uploadId, hbase_body)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " metadb put init upload failed")
        return "4002", status, false
    end

    return "0000", "0000", true
end

local function process_init_multipart_upload(metadb, headers, ubo)
    local uploadId = uuid()

    local stat_code,incode,ok = _M.create_temp_obj(metadb, headers, ubo, uploadId)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " create_temp_obj failed. stat_code=", stat_code, " incode=", incode)
        return stat_code, incode, false
    end

    local res_body = string.format([[<?xml version="1.0" encoding="UTF-8"?><InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Bucket>%s</Bucket><Key>%s</Key><UploadId>%s</UploadId></InitiateMultipartUploadResult>]], ubo.buck_name, utils.xmlencode(ubo.obj_name), uploadId)
    ngx.header["Content-Type"] = "application/xml"
    ngx.header["Content_Length"] = #res_body

    ngx.print(res_body)
    return "0000", "0000", true
end

local function delete_multipart(metadb, ubo, uploadId_partNum, partinfo, loc)
    local shard = _M.get_delete_obj_shard(uploadId_partNum)
    local del_okey = shard .. "_" .. uploadId_partNum
    local delete_time = string.format("%0.3f", ngx.now())
    local hbase_body = {
                        "info",   "oname",   ubo.obj_name,
                        "info",   "bucket",  ubo.buck_key,
                        "info",   "time",    delete_time,
                        "info",   "size",    partinfo["info:size"],
                        "info",   "hsize",   "0",
                        "info",   "etag",    partinfo["info:etag"],
                        "ver",    "tag",     uploadId_partNum,
                        "mfest",  "loc",     loc,
                        "mfest",  "psize",   0,
                        "mfest",  "chksize", partinfo["info:chksize"],
                       }
    local status,ok = metadb:put("delete_object", del_okey, hbase_body)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to put an uploaded part into 'delete_object' table")
        return status, false
    end

    return "0000", true
end

function _M.put_part_obj(metadb, headers, ubo, data_reader, uploadId, partNumber)
    if nil == ubo.obj_key then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " object name cannot end with '/'. obj_name=", ubo.obj_name)
        return "0010", "0000", false
    end

    if(nil == uploadId or '' == uploadId or nil == partNumber) then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " uploadId and partNumber must be present")
        return "0010", "0000", false
    end

    local cont_len = tonumber(headers["content-length"])
    if not cont_len then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " content-length is missing")
        return "3013", "0000", false
    end

    local p_num_min = sp_conf.config["s3_config"]["multipart_config"]["p_num_min"]
    local p_num_max = sp_conf.config["s3_config"]["multipart_config"]["p_num_max"]
    if partNumber < p_num_min or partNumber > p_num_max then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " invalid prartNumber! partNumber=", partNumber)
        return "0010", "0000", false
    end

    local obj_uploadId = ubo.obj_key .. "_" .. uploadId
    local status,ok,rebody = metadb:get("temp_object", obj_uploadId, {"flag:flag", "mfest:loc"})
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to get temp_object from metadb. obj_uploadId=", obj_uploadId)
        return "4006", status, false
    end

    local object_flag = rebody["flag:flag"]
    if(nil == object_flag or '' == object_flag) then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " upload part objcet flag is nil or empty. obj_uploadId=", obj_uploadId)
        return "4005", status, false
    end

    --check if object upload is complete: 0 in progress;  1 completed; 2 aborted
    if("0" ~= object_flag) then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " this multi-upload has been completed or aborted. obj_uploadId=", obj_uploadId)
        return "4007", "0000", false
    end

    local ceph = objstore:get_store(rebody["mfest:loc"])
    assert(ceph)

    local uploadId_partNum = uploadId .. "_" .. partNumber
    local code,status,ok,etag = put_ceph_data(ceph, ubo.obj_name, uploadId_partNum, data_reader, cont_len, true)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " upload part to ceph failed")
        return "4009", status, false
    end

    if headers["content-md5"] then
        local user_md5 = string.lower(headers["content-md5"])
        local calc_md5 = string.lower(ngx.encode_base64(etag))
        if user_md5 ~= calc_md5 then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " The Content-MD5 '", user_md5, "' did not match the calculated md5 '", calc_md5, "'")
            return "4015", "0000", false
        end
    end

    local now_ts = string.format("%0.3f", ngx.now())
    local hbase_body = {
        "info", "size", cont_len,
        "info", "etag", etag,
        "info", "partnum", partNumber,
        "info", "chksize", sp_conf.config["chunk_config"]["size"],
        "info", "ctime", now_ts,
        "info", "mtime", now_ts,
    }

    local status,ok = metadb:put("temp_part", uploadId_partNum, hbase_body)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " upload part put metadb failed")
        return "4010", status, false
    end
    ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " finished the process upload part")

    return "0000", "0000", true, str.to_hex(etag)
end

local function process_upload_part(metadb, headers, ubo, uploadId, partNumber)
    local httpc =  http.new()
    local req_reader, err = httpc:get_client_body_reader()
    if err or not req_reader then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to get client body reader. err=", err)
        return "4009", "0000", false
    end

    local code,incode,ok,etaghex = _M.put_part_obj(metadb, headers, ubo, req_reader, uploadId, partNumber)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " put_part_obj faild. code=", code, " incode=", incode)
        return code, incode, ok
    end

    ngx.header["ETag"] = '"' .. etaghex .. '"'
    return "0000", "0000", true
end


-- op:  1, complete;  2, abort
local function mark_as_complete_or_aborted(metadb, object_rowkey, op)
    local innercode,ok,origin
    if 1 == op then  -- complete
        innercode,ok,origin = metadb:checkAndMutateAndGetRow("temp_object", object_rowkey, 
                                                             "flag", "flag", metadb.CompareOp.EQUAL, "0", 
                                                             {"flag", "flag", "1"}, nil, 
                                                             nil)
        if ok then
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " compelet multiupload successfully by setting flag:flag to 1. object_rowkey=", object_rowkey, 
                                                           " origin=", stringify(origin))
            return "0000", "0000", true
        end

        if "1121" == innercode then --failed, because of condition-check-failure
            if "1" == origin["flag:flag"] then   -- already completed, return success
                ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " multiupload was already completed. object_rowkey=", object_rowkey, 
                                                               " origin=", stringify(origin))
                return "0000", "0000", true
            else
                assert ("2" == origin["flag:flag"])  -- this upload was aborted, return failure
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " multiupload was already aborted, cannot complete. object_rowkey=", object_rowkey, 
                                                              " origin=", stringify(origin))
                return "4007", "0000", false
            end
        else --failed, because of other errors than condition-check-failure
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " multiupload complete failed. innercode=", innercode, " object_rowkey=", object_rowkey)
            return "0000",innercode,false
        end
    else
        assert(2 == op)  -- abort

        innercode,ok,origin = metadb:checkAndMutateAndGetRow("temp_object", object_rowkey, 
                                                             "flag", "flag", metadb.CompareOp.EQUAL, "0", 
                                                             {"flag", "flag", "2"}, nil,
                                                             nil)
        if ok then
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " abort multiupload successfully by setting flag:flag to 2. object_rowkey=", object_rowkey, 
                                                           " origin=", stringify(origin))
            return "0000", "0000", true
        end

        if "1121" == innercode then  --failed, because of condition-check-failure
            if "2" == origin["flag:flag"] then   -- already aborted, return success
                ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " multiupload was already aborted. object_rowkey=", object_rowkey, 
                                                               " origin=", stringify(origin))
                return "0000", "0000", true
            else
                assert ("1" == origin["flag:flag"])  -- already completed, return failure
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " multiupload was already completed. object_rowkey=", object_rowkey, 
                                                              " origin=", stringify(origin))
                return "4007", "0000", false
            end
        else --failed, because of other errors than condition-check-failure
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " multiupload abort failed. innercode=", innercode, " object_rowkey=", object_rowkey)
            return "0000",innercode,false
        end
    end

    --never reach here
    assert(false)
    return "0000","0000",true
end

local function parser_cmu_xmlbody()
    --todo et request body
    ngx.req.read_body()
    local xml_request_body = ngx.var.request_body
    if nil == xml_request_body or '' == xml_request_body then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "complete multipart upload body is nil")
        return "0010", "0000", false
    end

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "complete multipart upload xml request body is : ", xml_request_body)

    local p_num_min = sp_conf.config["s3_config"]["multipart_config"]["p_num_min"]
    local p_num_max = sp_conf.config["s3_config"]["multipart_config"]["p_num_max"]
    local part_num = 0
    local partarray = {}

    local xml = XmlParser:new()

    local parsedXml = xml:ParseXmlText(xml_request_body)
    local uploadxml = parsedXml.CompleteMultipartUpload
    if uploadxml  == nil or uploadxml == "" then
        ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ",  " request xml error")
        return "4019", "0000", false
    else
        repeat
            if uploadxml.Part == nil then
                break
            end
            local part
            local onepart
            if uploadxml.Part.PartNumber and uploadxml.Part.ETag then
                part = uploadxml.Part
                onepart = true
            else
                part = uploadxml.Part[part_num+1]
            end
            if part == nil or part == '' then
                break
            end
            local xml_part_num
            local xml_etag
            if part.PartNumber then
                xml_part_num = part.PartNumber:value()
            end
            if part.ETag then
                xml_etag = part.ETag:value()
            end

            local part_id = tonumber(xml_part_num)
            if not part_id or part_id < p_num_min or part_id > p_num_max or not xml_etag then
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "xml invalid: PartNumber="..(xml_part_num or "")..", ETag="..(xml_etag or "nil"))
                return "4019", "0000", false
            end

            partarray[part_id] = xml_etag

            part_num = part_num + 1
            if onepart then break end
        until part == nil
    end

    local part_max = table.maxn(partarray)
    if 0 == part_num then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "xml invalid: part_max="..part_max..", part_num="..part_num..", #partarray="..#partarray)
        return "4019", "0000", false
    end
    if 0 == #partarray or  #partarray ~= part_max or part_max ~= part_num then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "xml invalid: part_max="..part_max..", part_num="..part_num..", #partarray="..#partarray)
        return "4012", "0000", false
    end

    return "0000", "0000", true, partarray
end

function _M.list_finished_parts(metadb, headers, bucketname, objectname, req_info)
    local uploadid = req_info["args"]["uploadId"]

    if(nil == uploadid or '' == uploadid) then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "complete multipart upload param is nil or \'\'")
        return "0010", "0000", false
    end

    --get bucket tag
    local userinfo = req_info["user_info"]
    local useretag = userinfo["ver:tag"]
    local bucketrowkey = useretag.."_"..bucketname
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "bucketrowkey is ", bucketrowkey)
    local status,ok,rebody = metadb:get("bucket", bucketrowkey, {"ver:tag"})
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "metadb failed to get bucket etag :"..rebody)
        return "4001", status, false
    end
    if not rebody["ver:tag"]  then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "bucket doesn't have ver:tag")
        return "0012", "0000", false
    end
    local btag = rebody["ver:tag"]
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "#### buckettag is ", btag)

    --get temp_object flag
    local obj_uploadid = btag.."_"..objectname.."_"..uploadid
    local status,ok,rebody = metadb:get("temp_object", obj_uploadid, {"flag:flag","sattrs:json"})
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "metadb failed to get temp_object flag!")
        return "4006", status, false
    end
    local object_flag = rebody["flag:flag"]

    if(nil == object_flag or '' == object_flag) then
        ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ",  "metadb failed to get temp_object flag, flag is nil or empty")
        return "4005", status, false, {}
    end

    --check if object upload is complete: 0 in progress;  1 completed; 2 aborted
    if("0" ~= object_flag) then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " this multi-upload has been completed or aborted. uploadid=", uploadid)
        return "4007", "0000", false
    end

    --get all part info
    local status, ok, finished_parts = metadb:quickScanHash("temp_part", uploadid, uploadid..metadb.MAX_CHAR, nil, 10001)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "metadb failed to scan parts: status="..status)
        return "4011", status, false
    end

    return "0000", "0000", true, finished_parts
end

function _M.cmu_result(metadb, headers, clusterid, ubo, seq_no, complete_time, partarray, uploadId, is_sync_op)
    if(nil == uploadId or '' == uploadId) then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " uploadId must be present")
        return "0010", "0000", false
    end

    if nil == ubo.obj_key then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " object name cannot end with '/'. obj_name=", ubo.obj_name)
        return "0010", "0000", false
    end

    if is_sync_op then
        local innercode,ok,allow = check_placeholder(metadb, ubo.obj_key, seq_no)
        if not ok then
            ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " failed to check placeholder. we allow the op by default.")
        else
            if not allow then
                ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " current op is outdated. seq_no=", seq_no)
                return "0026", "0000", false
            end
        end
    end

    --get temp_object flag
    local obj_uploadId = ubo.obj_key .. "_" .. uploadId
    local status,ok,rebody = metadb:get("temp_object", obj_uploadId, {"flag:flag","sattrs:json","mfest:loc"})
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to get temp_object from metadb. obj_uploadId=", obj_uploadId)
        return "4006", status, false
    end

    local object_flag = rebody["flag:flag"]
    if(nil == object_flag or '' == object_flag) then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " upload part objcet flag is nil or empty. obj_uploadId=", obj_uploadId)
        return "4005", status, false
    end

    --check if object upload is complete: 0 in progress;  1 completed; 2 aborted
    if("1" == object_flag) then
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " this multi-upload has been already completed. obj_uploadId=", obj_uploadId)
        return "0000", "0000", true
    end

    if("2" == object_flag) then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " this multi-upload has been aborted, so you cannot complete it. obj_uploadId=", obj_uploadId)
        return "4007", "0000", false
    end

    --get all part info
    local status, ok, finished_parts = metadb:quickScanHash("temp_part", uploadId, uploadId..metadb.MAX_CHAR, nil, 10001)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to scan uploaded parts from metadb. status=", status)
        return "4011", status, false
    end

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " finished_parts=", inspect(finished_parts))

    local total_size = 0
    local md5etag = md5:new()
    local psize_min = sp_conf.config["s3_config"]["multipart_config"]["psize"]
    local psize = nil

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " partarray=", inspect(partarray))

    for part_id = 1, #partarray do
        local xml_etag = partarray[part_id]
        local key = uploadId.."_"..part_id
        local db_etag = finished_parts[key] and finished_parts[key]["info:etag"]
        if nil == db_etag or '' == db_etag then
            md5etag:reset()
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " etag of uploaded part is nil or empty. partId=", key)
            return "4012", "0000", false
        end

        local etag = '"'..str.to_hex(db_etag)..'"'

        if(xml_etag ~= etag) then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " the etag of uploaded part does not match the etag given in xml. etag=", etag, " xml_etag=", xml_etag)
            md5etag:reset()
            return "4012", "0000", false
        end

        md5etag:update(db_etag)

        local size = tonumber(finished_parts[key]["info:size"])
        assert(size)
        psize = psize or size
        if part_id < #partarray and (size < psize_min or psize ~= size) then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " size of non-last part must equal to psize, and psize cannot be less than psize_min. size=", size,
                              " psize=", psize, " psize_min=", psize_min)
            md5etag:reset()
            return "4016", "0000", false
        end
        total_size = total_size + size
    end
    local outetag = str.to_hex(md5etag:final()).."-"..(#partarray)
    md5etag:reset()

    local chunksize = sp_conf.config["chunk_config"]["size"]

    assert(rebody["mfest:loc"])

    local hbase_body = {
        "info",   "file",    "true",
        "info",   "oname",   ubo.obj_name,
        "info",   "bucket",  ubo.buck_key,
        "info",   "time",    complete_time,
        "info",   "size",    total_size,
        "info",   "hsize",   "0",     --no hdata for multi-uploaded objects
        "info",   "etag",    outetag, --all data md5 could not get
        "ver",    "tag",     uploadId,
        "ver",    "seq",     seq_no,
        "mfest",  "loc",     rebody["mfest:loc"],
        "mfest",  "psize",   psize,
        "mfest",  "chksize", chunksize,
    }

    if rebody["sattrs:json"] then
        local index = #hbase_body
        hbase_body[index+1] = "sattrs"
        hbase_body[index+2] = "json"
        hbase_body[index+3] = rebody["sattrs:json"]
    end


    local datalog_body =
    {
        "info",   "clusterid",   clusterid,
        "info",   "user",        ubo.user_key,
        "info",   "bucket",      ubo.buck_name,
        "info",   "obj",         ubo.obj_name,
        "info",   "type",        "multput",
        "info",   "timestamp",   complete_time,
        "info",   "vertag",      uploadId,
    }
    
    local del_metadata = {
        "info:link",
        "info:lock",
    }

    local code, incode, ok = _M.insert_object(metadb, ubo, seq_no, complete_time, total_size, hbase_body, datalog_body, del_metadata)
    if not ok then
        if "1121" == incode then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " insert_object failed because of a newer concurrent competitive write code=", code, " incode=", incode)
        else
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " insert_object failed, code=", code, " incode=", incode)
            return code, incode, ok
        end
    end

    local code,innercode,ok = mark_as_complete_or_aborted(metadb, obj_uploadId, 1)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " complete multiupload failed, obj_uploadId=", obj_uploadId)
        return code,innercode,false,nil
    end

    return "0000", "0000", true, outetag
end

local function complete_multipart_upload(metadb, headers, clusterid, ubo, uploadId)
    local code, incode, ok, partarray = parser_cmu_xmlbody()
    if not ok then
        return code, incode, ok
    end

    local complete_time = string.format("%0.3f", ngx.now())
    local seq_no = gen_seq_no(complete_time)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " seq_no generated for multi-uploaded object ", ubo.obj_name,
                      " complete_time=", complete_time, " seq_no=", seq_no)

    local code, incode, ok, outetag = _M.cmu_result(metadb, headers, clusterid, ubo, seq_no, complete_time, partarray, uploadId, false)
    if not ok then
        return code, incode, ok
    end

    local res_body = string.format([[<?xml version="1.0" encoding="UTF-8"?><CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Bucket>%s</Bucket><Key>%s</Key><ETag>"%s"</ETag></CompleteMultipartUploadResult>]], ubo.buck_name, utils.xmlencode(ubo.obj_name), outetag)

    ngx.header["Content-Type"] = "application/xml"
    ngx.header["Content_Length"] = #res_body

    ngx.print(res_body)
    return "0000", "0000", true
end

local function list_parts(metadb, ubo, uploadId, num_marker, max_parts)
    if nil == ubo.obj_key then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " object name cannot end with '/'. obj_name=", ubo.obj_name)
        return "0010", "0000", false
    end

    if(nil == uploadId or '' == uploadId) then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " uploadId must be present")
        return "0010", "0000", false
    end

    local obj_uploadId = ubo.obj_key .. "_" .. uploadId
    --get temp_object flag
    local status,ok,rebody = metadb:get("temp_object", obj_uploadId, {"flag:flag"})
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to get temp_object from metadb. obj_uploadId=", obj_uploadId)
        return "4006", status, false
    end

    local object_flag = rebody["flag:flag"]
    if(nil == object_flag or '' == object_flag) then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " upload part objcet flag is nil or empty. obj_uploadId=", obj_uploadId)
        return "4005", status, false
    end

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "upload part object flag is ", object_flag)
    --check if object upload is complete: 0 in progress;  1 completed; 2 aborted
    if("0" ~= object_flag) then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " this multi-upload has been completed or aborted. obj_uploadId=", obj_uploadId)
        return "4007", "0000", false
    end

    local start_partid = uploadId .. "_" .. (num_marker or "")
    local maxparts = sp_conf.config["s3_config"]["maxparts"]["default"]
    if nil ~= max_parts then
        maxparts = max_parts
        if maxparts > sp_conf.config["s3_config"]["maxparts"]["max"] then
            maxparts = sp_conf.config["s3_config"]["maxparts"]["max"]
        end
    end
    local scan_prefix = uploadId.."_"

    local out = {}
    local index = 1
    out[index] = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"..
                    "<ListPartsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">" ..
                    "<Bucket>" .. ubo.buck_name .. "</Bucket>"..
                    "<Key>" .. utils.xmlencode(ubo.obj_name) .. "</Key>"..
                    "<UploadId>" .. uploadId .. "</UploadId>"..
                    "<StorageClass>".."STANDARD".."</StorageClass>"..
                    "<PartNumberMarker>"..tostring(num_marker and tonumber(num_marker) or 0).."</PartNumberMarker>"..
                    "<MaxParts>" .. maxparts .. "</MaxParts>"

    local ok, result, common_prefixes, next_marker, truncated = _M.list_entries(metadb, "temp_part", {"info"}, start_partid, nil, scan_prefix, '', maxparts)
    if not ok then
        return "0012","0000",false
    end

    index = index + 1
    out[index] =    "<IsTruncated>" .. tostring(truncated) .. "</IsTruncated>"

    if truncated then
        local s,e = string.find(next_marker, "_")
        if not s then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " invalid next_marker:", next_marker)
            return "4012","0000",false
        end
        local partNum = string.sub(next_marker, e+1, -1)

        index = index + 1
        out[index] =    "<NextPartNumberMarker>" .. partNum  .. "</NextPartNumberMarker>"
    end

    index = index + 1
    out[index] =    "<Owner>"..
                        "<ID>"..ubo.user_uid.."</ID>"..
                        "<DisplayName>"..ubo.user_displayname.."</DisplayName>"..
                    "</Owner>"

    for i,v_tab in pairs(result) do
        local partnumber = v_tab.values["info:partnum"]
        local LastModfied = ngx.http_time(tonumber(v_tab.values["info:mtime"]))
        local ETag = str.to_hex(v_tab.values["info:etag"])
        local Size = v_tab.values["info:size"]
        local element = "<Part>"..
                            "<PartNumber>"..partnumber.."</PartNumber>"..
                            "<LastModfied>"..LastModfied.."</LastModfied>"..
                            "<ETag>".."&quot;"..ETag.."&quot;".."</ETag>"..
                            "<Size>"..Size.."</Size>"..
                        "</Part>"

        index = index + 1
        out[index] = element
    end

    local response_headers = sp_conf.config["s3_GET_Service"]["s3_response_headers"]
    for k,v in pairs(response_headers) do
        ngx.header[k] = v
    end
    ngx.header["Content-Type"] = "application/xml"

    index = index + 1
    out[index] = "</ListPartsResult>"
    ngx.print(table.concat(out))
    return "0000", "0000", true
end

local function abort_multipart_upload(metadb, headers, ubo, uploadId)
    if nil == ubo.obj_key then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " object name cannot end with '/'. obj_name=", ubo.obj_name)
        return "0010", "0000", false
    end

    if(nil == uploadId or '' == uploadId) then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " uploadId must be present")
        return "0010", "0000", false
    end

    local obj_uploadId = ubo.obj_key .. "_" .. uploadId

    local status,ok,rebody = metadb:get("temp_object", obj_uploadId, {"flag:flag","sattrs:json","mfest:loc"})
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to get temp_object from metadb. obj_uploadId=", obj_uploadId)
        return "4006", status, false
    end

    local object_flag = rebody["flag:flag"]
    if(nil == object_flag or '' == object_flag) then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " upload part objcet flag is nil or empty. obj_uploadId=", obj_uploadId)
        return "4005", status, false
    end

    --check if object upload is complete: 0 in progress;  1 completed; 2 aborted
    if("1" == object_flag) then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " this multi-upload has been completed, so you cannot abort it. obj_uploadId=", obj_uploadId)
        return "4007", "0000", false
    end
    if("2" == object_flag) then
        ngx.status = 204
        return "0000", "0000", true 
    end

    --get all part info
    local status,ok,part_info_rebody = metadb:quickScanHash("temp_part", uploadId, uploadId..metadb.MAX_CHAR, nil, 10001)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to scan uploaded parts from metadb. status=", status)
        return "4011", status, false
    end

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " part_info_rebody=", inspect(part_info_rebody))

    assert(rebody["mfest:loc"])

    for i, v_tab in pairs(part_info_rebody) do
        local  status, ok = delete_multipart(metadb, ubo, i, v_tab, rebody["mfest:loc"])
        if not ok then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to delete multipart. i=", i, " v_tab=", stringify(v_tab))
            return "4051", status, false
         end
    end

    local code,innercode,ok = mark_as_complete_or_aborted(metadb, obj_uploadId, 2)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to abort multiupload. obj_uploadId=", obj_uploadId)
        return code,innercode,false
    end

    ngx.status = 204
    return "0000", "0000", true
end

--运维需求：
--1. 对于伪源用户，如果是备集群，必须是配置了同步的用户或者bucket才可以响应服务
--2. 对于直播用户，无论主备集群，都可以响应服务
local function allow_object_ops(user_info, bucket_info)
    local role = sync_cfg["role"]
    local user_mode = user_info["info:mode"]
    local sync_flag = user_info["info:syncflag"] or bucket_info["info:syncflag"]
    if "master" == role or "dual" == user_mode or "true" == sync_flag then
        return true
    else
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " object ops is denied because of no syncflag configed in slave cluster")
        return false
    end
end

function _M.process_object(self, metadb, req_info)
    --根据content-length判断object大小决定存取object的方式
    local request_method = req_info["method"]
    local headers = req_info["headers"]
    local user_info = req_info["user_info"]
    local bucketname = req_info["op_info"]["bucketname"]
    local objectname = req_info["op_info"]["objectname"]
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "Enter process_object: method=" .. request_method .. ",bucket=" .. bucketname .. ",object=" .. objectname)

    local statcode = "0018" -- not supported
    local innercode = "0000"
    local ok = false
    local op_type = req_info["op_info"]["type"]
    local md5sum = false
    if nil ~= headers["content-md5"] then
        md5sum = true
    else
        md5sum = sp_conf.config["MD5SUM"]
    end

    -- we only support fs user now;
    assert(user_info["info:type"] == "fs")

    -- get bucket info from metadb;
    local buck_key  = user_info["ver:tag"].."_"..bucketname

    local columns = {
        "info:syncflag",
        "ver:tag",
        "quota:enabled",
        "quota:objects",
        "quota:size_kb",
        "stats:size_bytes",
        "stats:objects",
    }
    local errCode,succ,buck = metadb:get("bucket", buck_key, columns)
    if not succ then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to get bucket from metadb. buck_key=", buck_key, " errCode=", errCode)
        return "2004", errCode, false
    end

    if nil == buck["ver:tag"] then
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " bucket ", buck_key, " does not exist")
        return "0012","0000",false
    end

    local ok = allow_object_ops(user_info, buck)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ops is not allowed in this cluster, bucket: ", buck_key)
        return "1007", "0000", false
    end

    -- The object-row-key (obj_key) is in format:
    --        {BUCKET-TAG}_/dir1/dir2/#object         -- '#' stands for the DIR_MARKER
    --
    -- So we parse 'objectname' to build the obj_key. At the same time, we also build the row keys of the ancestor
    -- dirs (dirkeys), because we need to create these dirs if current OP is to create this object, such as put-obj,
    -- copy-obj and complete-multipart-upload.
    --
    -- If goofys (fs user) specifies an 'objectname' like "a/b/c/" (ending with '/'), the obj_key will be nil, because
    -- it stands for a dir instead of a file/object in Filesystem terminology; In this case, we just create the ancestor
    -- dirs.
    local client_type = get_client_type(headers["user-agent"])
    local errCode,succ,obj_key,dirkeys = oindex.parse_objname1(buck["ver:tag"], objectname, client_type)
    if not succ then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to parse objectname. objectname=", objectname, " errCode=", errCode)
        return "3017", errCode, false
    end

    -- ubo_info: user-bucket-object info
    local ubo_info = new_tab(0, 6)

    ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " req_info is ", inspect(req_info))

    ubo_info.user_key           = req_info["auth_info"]["accessid"]
    ubo_info.user_vtag          = user_info["ver:tag"]
    ubo_info.user_displayname   = user_info["info:displayname"]
    ubo_info.user_uid           = user_info["info:uid"]
    ubo_info.user_type          = user_info["info:type"]

    ubo_info.buck_key           = buck_key
    ubo_info.buck_name          = bucketname
    ubo_info.buck_vtag          = buck["ver:tag"]

    ubo_info.obj_key            = obj_key
    ubo_info.obj_name           = objectname
    ubo_info.obj_vtag           = nil
    ubo_info.obj_dirkeys        = nil
    ubo_info.syncflag           = user_info["info:syncflag"] or buck["info:syncflag"]

    if sp_conf.get_s3_op_rev("INITIATE_MULTIPART_UPLOAD") == op_type
        or sp_conf.get_s3_op_rev("PUT_OBJECT") == op_type
        or sp_conf.get_s3_op_rev("PUT_OBJECT_COPY") == op_type then
        local qok = quota_check(user_info)
        if qok == false then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " copy object exceed quota limit,quota and stats is: \n ", stringify(user_info))
            return "6100", "0000", false
        end
    end

    -- query args from URL
    local query_args = req_info["args"]

    -- clusterid
    local local_clusterid = sync_cfg["clusterid"]

    if sp_conf.get_s3_op_rev("INITIATE_MULTIPART_UPLOAD") == op_type then
        statcode, innercode, ok = process_init_multipart_upload(metadb, headers, ubo_info)
    elseif sp_conf.get_s3_op_rev("UPLOAD_PART") == op_type then
        local uploadId = query_args["uploadId"]
        local partNumber = query_args["partNumber"] and tonumber(query_args["partNumber"])
        statcode, innercode, ok = process_upload_part(metadb, headers, ubo_info, uploadId, partNumber)
    elseif sp_conf.get_s3_op_rev("LIST_PARTS") == op_type then
        local uploadId    = query_args["uploadId"]
        local num_marker  = query_args["part-number-marker"]
        local max_parts   = query_args["max-parts"] and tonumber(query_args["max-parts"])
        statcode, innercode, ok = list_parts(metadb, ubo_info, uploadId, num_marker, max_parts)
    elseif sp_conf.get_s3_op_rev("COMPLETE_MULTIPART_UPLOAD") == op_type then
        local uploadId = query_args["uploadId"]
        ubo_info.obj_dirkeys = dirkeys
        ubo_info.obj_vtag = uploadId
        statcode, innercode, ok = complete_multipart_upload(metadb, headers, local_clusterid, ubo_info, uploadId)
    elseif sp_conf.get_s3_op_rev("ABORT_MULTIPART_UPLOAD") == op_type then
        local uploadId = query_args["uploadId"]
        statcode, innercode, ok = abort_multipart_upload(metadb, headers, ubo_info, uploadId)
    elseif sp_conf.get_s3_op_rev("GET_OBJECT") == op_type then
        local req_seq_no = query_args["seqno"]
        local is_sync_op = (query_args["prepend-metadata"] ~= nil)
        statcode, innercode, ok = _M.get_obj(metadb, headers, ubo_info, false, req_seq_no, is_sync_op)
    elseif sp_conf.get_s3_op_rev("GET_OBJECT_ACL") == op_type then
        statcode, innercode, ok = get_obj_acl(metadb, headers, ubo_info)
    elseif sp_conf.get_s3_op_rev("PUT_OBJECT") == op_type  then
        ubo_info.obj_vtag = uuid()
        ubo_info.obj_dirkeys = dirkeys
        statcode, innercode, ok = s3_put_obj(metadb, headers, local_clusterid, ubo_info, md5sum)
    elseif sp_conf.get_s3_op_rev("DELETE_OBJECT") == op_type then
        local versionid = query_args["versionId"]
        ubo_info.obj_dirkeys = dirkeys
        statcode, innercode, ok = s3_delete_obj(metadb, local_clusterid, ubo_info, versionid)
    elseif sp_conf.get_s3_op_rev("HEAD_OBJECT") == op_type then
        local req_seq_no = query_args["seqno"]
        statcode, innercode, ok = _M.head_obj(metadb, headers, ubo_info, req_seq_no)
    elseif sp_conf.get_s3_op_rev("PUT_OBJECT_COPY") == op_type then
        ubo_info.obj_vtag = uuid()
        ubo_info.obj_dirkeys = dirkeys
        statcode, innercode, ok = put_obj_copy(metadb, headers, local_clusterid, ubo_info)
    else
        --method not supported
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### operation not supported:"..req_info["method"].." "..req_info["uri"].." "..op_type)
    end
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### operation resusts:",req_info["method"],statcode,innercode,ok)
    return statcode, innercode, ok
end

return _M
