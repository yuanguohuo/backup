-- By Yuanguo, 21/7/2016
-- For users who use out storage in a filesystem way, such as goofys;

local ok, new_tab = pcall(require, "table.new")
if not ok or type(new_tab) ~= "function" then
    new_tab = function (narr, nrec) return {} end
end

local ok,utils = pcall(require, "common.utils")
if not ok or not utils then
    error("failed to load common.utils:" .. (utils or "nil"))
end

local ok,sp_conf = pcall(require, "storageproxy_conf")
if not ok or not sp_conf then
    error("failed to load storageproxy_conf:" .. (sp_conf or "nil"))
end

local ASCII_SLASH = 47  --ascii code of '/' is 47;

local EMPTY_MD5 = sp_conf.config["constants"]["empty_md5"]

local _M = new_tab(0, 6)
_M._VERSION = '0.1'

--The following functions are used for goofys "root mount service", which means, once 
--mount we can see anything: all users, all buckets of all users, all objects of all 
--buckets of all users......  This is achived by mounting a dummy bucket called 
--{FS_ROOT_BUCK} of the root user.
--
--    /
--    ├── ACCESSKEY1_user1
--    │   ├── bucket1
--    │   │   ├── dir1
--    │   │   │   ├── file1
--    │   │   │   └── file2
--    │   │   └── dir2
--    │   └── bucket2
--    │       ├── dir3
--    │       └── dir4
--    ├── ACCESSKEY2_user2
--    └── ACCESSKEY3_user3
--        ├── bucket1
--        └── bucket2

local FS_ROOT_MOUNT_OP = "FS_ROOT_MOUNT"
local FS_ROOT_LS_OP = "FS_ROOT_LS"
local FS_USER_LS_OP = "FS_USER_LS"
local FS_LS_OBJECTS_OP = "FS_USER_LS_OBJECTS"

-- this bucket doesn't exist, it's a dummy bucket, but when the "root" user mounts 
-- goofys, it must use {FS_ROOT_BUCK} as the bucket param;
local FS_ROOT_BUCK="root"  
local FS_ROOT_ACCESS_ID="1WYCCJZ9JRLWZU8JTDQJ"
local FS_ROOT_SECRET_KEY = nil

local TABLE_USER="user"
local TABLE_BUCKET="bucket"

--return: is-valid, new-uri (nil means uri is not overwritten)
function _M.rewrite_request(method, uri, args, headers, metadb)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### Enter rewrite_request:", method, uri)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### args is ", utils.stringify(args))
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### headers is ", utils.stringify(headers))

    local auth = headers["authorization"]
    if nil == auth or '' == auth then
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " signature in url or anonymos user , skip overwrite")
        return true, nil
    end
    assert(auth) -- we are sure goofys has header "authorization"

    local rootAccId
    local key
    local cp, err = ngx.re.match(headers["Authorization"], "^AWS(\\s+)([0-9A-Za-z]+)\\s*:\\s*(\\S+)", "jo")
    if nil ~= cp and nil ~= next(cp) and nil ~= cp[2] and nil ~= cp[3] then  --AWS version 2
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### match---accessid is ", cp[2])
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "##### match---original_signature is ", cp[3])
        rootAccId = cp[2]
        key = cp[3]
    else
	return true,nil,2 --ref get_auth_info func in storageproxy_access.lua
    end


    if rootAccId ~= FS_ROOT_ACCESS_ID then
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " not root user, skip overwrite")
        return true, nil 
    end

    if not FS_ROOT_SECRET_KEY then
        local status, ok, user_info = metadb:get("user", rootAccId, {"info:secretkey"})
        if not ok then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " get root user_info failed(not client.get err and client.get error),status is:" .. status)
            return false, nil 
        elseif nil == user_info or nil == next(user_info) then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "get root user_info failed non-exist")
            return false, nil
        end
        ngx.log(ngx.ERR, "does this opertion execute every time?!!!!!!")
        FS_ROOT_SECRET_KEY = user_info["info:secretkey"]
    end
    headers["root_secretkey"] = FS_ROOT_SECRET_KEY
    headers["root_uri"] = uri
    --chop the leading slashes
    local i = 1
    while string.byte(uri,i,i) == ASCII_SLASH do
        i = i + 1
    end
    uri = string.sub(uri, i, -1)
    assert(uri)

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " uri=", uri)

    local orig_buck = "" 
    local uri_remain = nil 
    i = string.find(uri, "/")
    if not i then
        orig_buck = uri
        uri_remain = ""
    else
        orig_buck = string.sub(uri, 1,i-1)
        uri_remain = string.sub(uri, i+1, -1)
    end

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " uri_remain=", uri_remain)

    if orig_buck ~= FS_ROOT_BUCK then
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " not fs root bucket, skip overwrite")
        return true, nil 
    end

    --start to overwrite ... 
    if uri_remain == "" then   --original uri is "/root"
        if "GET" ~= method then
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " only GET is supported in dir /")
            return false, nil
        end

        if args["uploads"] then --when mount, goofys provides the "uploads" argument in its url
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " FS_ROOT_MOUNT_OP")
            ngx.ctx.fs_extent_op = FS_ROOT_MOUNT_OP
            return true, nil   -- uri is still "/root", will go to list_mult_uploads
        end

        local prefix = args["prefix"]
        if not prefix or "" == prefix or "/" == prefix then
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " FS_ROOT_LS_OP")
            ngx.ctx.fs_extent_op = FS_ROOT_LS_OP
            return true, ""    -- uri is empty, will go to process_service
        end

        local accId_uid = "" 
        local prefix_remain = nil

        i = string.find(prefix, "/")
        if not i then
            accId_uid = prefix
            prefix_remain = ""
        else
            accId_uid = string.sub(prefix, 1, i-1)
            prefix_remain = string.sub(prefix, i+1, -1)
        end

        i = string.find(accId_uid, "_")
        if not i then
            return true, nil
        end

        local real_accId = string.sub(accId_uid, 1, i-1)
        local real_auth = "AWS " .. real_accId .. ":" .. key

        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " overwrite authorization header with:", real_auth)
        headers["authorization"] = real_auth

        if prefix_remain == "" then
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " overwrite prefix with \"\"")
            args["prefix"] = ""

            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " FS_USER_LS_OP")
            ngx.ctx.fs_extent_op = FS_USER_LS_OP
            ngx.ctx.fs_extent_ls_prefix = accId_uid .. "/"

            return true, "" -- uri is empty, will go to process_service
        else
            local real_buck = ""
            i = string.find(prefix_remain, "/")
            if not i then
                real_buck = prefix_remain
                prefix_remain = ""
            else
                real_buck = string.sub(prefix_remain, 1, i-1)
                prefix_remain = string.sub(prefix_remain, i+1, -1)
            end

            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " overwrite prefix with: ", prefix_remain)
            args["prefix"] = prefix_remain

            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " FS_LS_OBJECTS_OP")
            ngx.ctx.fs_extent_op = FS_LS_OBJECTS_OP
            ngx.ctx.fs_extent_ls_prefix = accId_uid .. "/" .. real_buck .. "/"

            return true, "/" .. real_buck  -- uri is /bucket, will go to list_objects
        end
    else  -- uri is /root/7V8ISZ2Q5YB5PD60HYBM_yuanguo....
        i = string.find(uri_remain, "/")
        local accId_uid = nil
        if not i then
            accId_uid = uri_remain
            uri_remain = ""
        else
            accId_uid = string.sub(uri_remain, 1, i-1)
            uri_remain = string.sub(uri_remain, i, -1)  --keep the leading slash
        end

        if "GET" ~= method then
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " uri_remain=", uri_remain)

            if "" == uri_remain or "/" == uri_remain then
                ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " only GET is supported in /username/")
                return false, nil
            end

            local copy = uri_remain
            if string.byte(copy, 1, 1) == ASCII_SLASH then
                copy = string.sub(copy, 2, -1)
            end

            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " copy=", copy)

            i = string.find(copy, "/")
            if not i or #copy == i then  -- no / or / is the last char
                ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " only GET is supported in /username/bucket/")
                return false, nil
            end
        end

        i = string.find(accId_uid, "_")
        if not i then
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " unknown request")
            return true, nil
        end

        local real_accId = string.sub(accId_uid, 1, i-1)
        local real_auth = "AWS " .. real_accId .. ":" .. key

        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " overwrite authorization header with:", real_auth)
        headers["authorization"] = real_auth

        local copy_src = headers["x-amz-copy-source"]
        if copy_src then 
            local p,q = string.find(copy_src, FS_ROOT_BUCK .. "/" .. accId_uid .. "/")
            if p then
                local real_copy_src = string.sub(copy_src, q+1, -1)
                ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " overwrite x-amz-copy-source header with:", real_copy_src)
                headers["root-x-amz-copy-source"] = copy_src
                headers["x-amz-copy-source"] = real_copy_src
            end
        end

        return true, uri_remain
    end
end

function _M.is_root_mount()
    if ngx.ctx.fs_extent_op == FS_ROOT_MOUNT_OP then
        return true
    end
    return false 
end

function _M.root_mount()
    local out_xml = {}
    local index = 1

    out_xml[index] = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListMultipartUploadsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
    index = index + 1

    out_xml[index] = "<Bucket>"..FS_ROOT_BUCK.."</Bucket>"
    index = index + 1

    out_xml[index] = "<IsTruncated>false</IsTruncated>"
    index = index + 1

    out_xml[index] = "</ListMultipartUploadsResult>"

    --according to the demands of S3_bucket to process s3_response_header
    local response_headers = sp_conf.config["s3_GET_Service"]["s3_response_headers"]
    for k,v in pairs(response_headers) do
        ngx.header[k] = v
    end

    ngx.header["Content-Type"] = "application/xml"    
    ngx.print(table.concat(out_xml))
    return "0000", "0000", true
end

function _M.is_root_ls()
    if ngx.ctx.fs_extent_op == FS_ROOT_LS_OP then
        return true
    end
    return false 
end

function _M.root_ls(metadb, marker, delimiter, maxkeys)
    if delimiter ~= "" and delimiter ~= "/" then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " delimiter must be slash for filesystem users, but it's ", delimiter)
        return "0011","0000",false
    end

    local key_start = ""
    local key_end = metadb.MAX_CHAR
    if "" ~= marker then
        key_start = marker .. metadb.MIN_CHAR
    end

    local filter = "SingleColumnValueFilter('info','parent',=,'binary:root',false,true)"
    local code,ok,scanner = metadb:openScanner(TABLE_USER, key_start, key_end, {"info:uid", "info:parent"}, 0, filter)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " HBase openScanner failed, table=", TABLE_USER, " innercode=", code)
        return "2005", code, false
    end

    local max_once = sp_conf.config["hbase_config"]["max_scan_once"] or 1000

    local comPrefix_ret = {}
    local pindex = 1
    local truncated = false
    local last = nil

    local got = 0
    while got < maxkeys do
        local scan_num = math.min(max_once, maxkeys - got)
        local code, ok, ret, num = metadb:scan(TABLE_USER, scanner, scan_num)

        if not ok then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " HBase scan failed, table=", TABLE_USER, " innercode=", code)

            local code1,ok1 = metadb:closeScanner(scanner)
            if not ok1 then
                ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " HBase failed to close scanner, innercode=", code1)
            end

            return "2005", code, false
        end

        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ret=", utils.stringify(ret))

        for i, v in ipairs(ret) do
            last = v.key
            if last ~= FS_ROOT_ACCESS_ID then
                got = got + 1

                local uid = v.values["info:uid"] and v.values["info:uid"]
                assert(uid)

                comPrefix_ret[pindex] = "<CommonPrefixes><Prefix>"..
                                            utils.xmlencode(last.."_"..uid.."/")..
                                        "</Prefix></CommonPrefixes>"
                pindex = pindex + 1
            end
        end

        if num < scan_num then
            truncated = false 
            break
        else
            truncated = true 
        end
    end

    local code,ok = metadb:closeScanner(scanner)
    if not ok then
        ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " HBase failed to close scanner, innercode=", code)
    end

    local out_xml = {}
    local index = 1

    out_xml[index] = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
    index = index + 1

    out_xml[index] = "<Name>" .. FS_ROOT_BUCK .. "</Name>"
    index = index + 1

    if "" ~= marker then
        out_xml[index] = "<Marker>" .. marker .. "</Marker>"
        index = index + 1
    end

    out_xml[index] = "<MaxKeys>" .. maxkeys .. "</MaxKeys>"
    index = index + 1

    if truncated and last then
        out_xml[index] = "<NextMarker>"..last.."</NextMarker>"
        index = index + 1
    end

    out_xml[index] = "<Delimiter>"..delimiter.."</Delimiter>"
    index = index + 1

    out_xml[index] = "<Prefix>"..(ngx.ctx.fs_extent_ls_prefix or "").."</Prefix>"
    index = index + 1
    
    out_xml[index] = "<IsTruncated>"..tostring(truncated).."</IsTruncated>"
    index = index + 1

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " comPrefix_ret=", utils.stringify(comPrefix_ret))

    if nil ~= next(comPrefix_ret) then
        out_xml[index] = table.concat(comPrefix_ret)
        index = index + 1
    end

    out_xml[index] = "</ListBucketResult>"
    index = index + 1

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " out_xml=", utils.stringify(out_xml))

    local response_headers = sp_conf.config["s3_GET_Service"]["s3_response_headers"]
    for k,v in pairs(response_headers) do
        ngx.header[k] = v
    end

    ngx.header["Content-Type"] = "application/xml"    
    ngx.print(table.concat(out_xml))

    return "0000", "0000", true
end


function _M.is_user_ls()
    if ngx.ctx.fs_extent_op == FS_USER_LS_OP then
        return true
    end
    return false 
end

function _M.user_ls(metadb, userinfo, marker, delimiter, maxkeys)
    if not delimiter then
        delimiter = "/"
    end
    local usertag = userinfo["ver:tag"]
    local uid = userinfo["info:uid"]
    local displayname = userinfo["info:displayname"]
    local key_start = usertag .. "_"
    local key_end = key_start .. metadb.MAX_CHAR
    if "" ~= marker then
        key_start = key_start .. marker .. metadb.MIN_CHAR
    end

    local ls_prefix = ngx.ctx.fs_extent_ls_prefix or ""
    local user_root_dir = "<Contents>"..
                              "<Key>" .. utils.xmlencode(ls_prefix) .. "</Key>" ..
                              "<ETag>&quot;".. utils.md5hex(EMPTY_MD5) .."&quot;</ETag>" ..
                              "<Size>0</Size><StorageClass>STANDARD</StorageClass>"..
                              "<Owner><ID>"..uid.."</ID><DisplayName>"..displayname.."</DisplayName></Owner>"..
                          "</Contents>"

    local code,ok,scanner = metadb:openScanner(TABLE_BUCKET, key_start, key_end, nil, maxkeys)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " HBase openScanner failed, table=", TABLE_BUCKET, " innercode=", code)
        return "2005", code, false
    end

    local max_once = sp_conf.config["hbase_config"]["max_scan_once"] or 1000

    local comPrefix_ret = {}
    local pindex = 1
    local truncated = false
    local last = nil

    local got = 0
    while got < maxkeys do
        local scan_num = math.min(max_once, maxkeys - got)
        local code, ok, ret, num = metadb:scan(TABLE_BUCKET, scanner, scan_num)

        if not ok then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " HBase scan failed, table=", TABLE_BUCKET, " innercode=", code)

            local code1,ok1 = metadb:closeScanner(scanner)
            if not ok1 then
                ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " HBase failed to close scanner, innercode=", code1)
            end

            return "2005", code, false
        end

        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ret=", utils.stringify(ret))

        for i, v in ipairs(ret) do
            got = got + 1
            local upos = string.find(v.key, "_")
            last = string.sub(v.key, upos+1, -1) 

            comPrefix_ret[pindex] = "<CommonPrefixes><Prefix>"..
                                         utils.xmlencode(ls_prefix..last.."/")..
                                    "</Prefix></CommonPrefixes>"
            pindex = pindex + 1
        end

        if num < scan_num then
            truncated = false 
            break
        else
            truncated = true 
        end
    end

    local code,ok = metadb:closeScanner(scanner)
    if not ok then
        ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " HBase failed to close scanner, innercode=", code)
    end

    local out_xml = {}
    local index = 1

    out_xml[index] = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
    index = index + 1

    out_xml[index] = "<Name>" .. FS_ROOT_BUCK .. "</Name>"
    index = index + 1

    if "" ~= marker then
        out_xml[index] = "<Marker>" .. marker .. "</Marker>"
        index = index + 1
    end

    out_xml[index] = "<MaxKeys>" .. maxkeys .. "</MaxKeys>"
    index = index + 1

    if truncated and last then
        out_xml[index] = "<NextMarker>"..last.."</NextMarker>"
        index = index + 1
    end

    out_xml[index] = "<Prefix>"..ls_prefix.."</Prefix>"
    index = index + 1
    
    out_xml[index] = "<Delimiter>"..delimiter.."</Delimiter>"
    index = index + 1

    out_xml[index] = "<IsTruncated>"..tostring(truncated).."</IsTruncated>"
    index = index + 1

    out_xml[index] = user_root_dir 
    index = index + 1

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " comPrefix_ret=", utils.stringify(comPrefix_ret))

    if nil ~= next(comPrefix_ret) then
        out_xml[index] = table.concat(comPrefix_ret)
        index = index + 1
    end

    out_xml[index] = "</ListBucketResult>"
    index = index + 1

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " out_xml=", utils.stringify(out_xml))

    --according to the demands of S3_bucket to process s3_response_header
    local response_headers = sp_conf.config["s3_GET_Service"]["s3_response_headers"]
    for k,v in pairs(response_headers) do
        ngx.header[k] = v
    end
    ngx.header["Content-Type"] = "application/xml"    
    ngx.print(table.concat(out_xml))
    return "0000", "0000", true
end

return _M
