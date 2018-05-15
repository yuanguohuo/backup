-- By Yuanguo, 16/06/2017

--    /
--    ├── dir1
--    │   ├── dir11
--    │   └── file11
--    ├── dir2
--    │   ├── dir21
--    │   │   └── file211
--    │   ├── dir22
--    │   │   └── dir221
--    │   │       └── file2211
--    │   ├── dir23
--    │   ├── file21
--    │   └── file22
--    ├── dir3
--    ├── file1
--    ├── file2
--    └── file3

--  '#' stands for the DIR_MARKER

--        RowKey                              info:dir      info:file
--  -------------------------------------------------------------------
--    {BUCK-TAG}_/#dir1/                        true          false
--    {BUCK-TAG}_/#dir2/                        true          false
--    {BUCK-TAG}_/#dir3/                        true          false
--    {BUCK-TAG}_/#file1                        false         true
--    {BUCK-TAG}_/#file2                        false         true
--    {BUCK-TAG}_/#file3                        false         true
--
--    {BUCK-TAG}_/dir1/#dir11/                  true          false
--    {BUCK-TAG}_/dir1/#file11                  false         true
--
--    {BUCK-TAG}_/dir2/#dir21/                  true          false
--    {BUCK-TAG}_/dir2/#dir22/                  true          false
--    {BUCK-TAG}_/dir2/#dir23/                  true          false
--    {BUCK-TAG}_/dir2/#file21                  false         true
--    {BUCK-TAG}_/dir2/#file22                  false         true
--
--    {BUCK-TAG}_/dir2/dir21/#file211           false         true
--
--    {BUCK-TAG}_/dir2/dir22/#dir221/           true          false
--
--    {BUCK-TAG}_/dir2/dir22/dir221/#file2211   false         true

local ok, new_tab = pcall(require, "table.new")
if not ok or type(new_tab) ~= "function" then
    new_tab = function (narr, nrec) return {} end
end

local ok,ngx_re = pcall(require, "ngx.re")
if not ok or not ngx_re then
    error("failed to load ngx.re:" .. (ngx_re or "nil"))
end

local ok,sp_conf = pcall(require, "storageproxy_conf")
if not ok or not sp_conf then
    error("failed to load storageproxy_conf:" .. (sp_conf or "nil"))
end

local ok,utils = pcall(require, "common.utils")
if not ok or not utils then
    error("failed to load common.utils:" .. (utils or "nil"))
end

local stringify = utils.stringify
local xmlencode = utils.xmlencode
local toUTC     = utils.toUTC
local md5hex    = utils.md5hex

local ngx_re_gsub   = ngx.re.gsub
local ngx_re_gmatch = ngx.re.gmatch

local EMPTY_MD5 = sp_conf.config["constants"]["empty_md5"]

local ASCII_SLASH = 47  --ascii code of '/' is 47;
local OINDEX_TABLE = "object"
local ASCII_DIR_MARKER = 1
local DIR_MARKER = string.char(1) 
local DIR_COLS = {"info:dir", "ver:seq"}
local FILE_COLS = 
{
    "info:file",
    "info:oname",
    "info:bucket",
    "info:time",
    "info:rsyncmtime",
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

local FS_ROOT_ACCESS_ID="1WYCCJZ9JRLWZU8JTDQJ"
local FS_ROOT_SECRET_KEY = nil
local FS_ROOT_BUCK="root"  

local _M = new_tab(0, 7)

--return: innercode,ok,components
local function split_path(path)
    if #path == 0 then
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " path is empty")
        return "0000", true, {}
    end

    local iterator,err = ngx_re_gmatch(path, "(/+|[^/]+)", "o")
    if not iterator then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ngx_re_gmatch failed. path=", path, " err=", err)
        return "1003", false, nil
    end

    local components = new_tab(16, 0)

    while true do
        local m,err = iterator()
        if err then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ngx_re_gmatch iterator failed. err=", err)
            return "1003", false, nil
        end

        if not m then
            break -- done
        end

        table.insert(components, m[1])
    end

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " path=", path, " components=", stringify(components))
    return "0000", true, components
end

--return:innercode,ok,exists
local function dir_exists(metadb, dirkey)
    local innercode,ok,dentry = metadb:get(OINDEX_TABLE, dirkey, DIR_COLS)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " get dentry failed, dirkey=", dirkey)
        return innercode,false,nil 
    end

    if dentry and dentry["info:dir"] then
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " dir exists, dirkey=", dirkey)
        return "0000",true,true
    end

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " dir does not exist, dirkey=", dirkey)
    return "0000",true,false
end

--return: innercode,ok,dentry
function _M.stat(metadb, dentry_key)

    local obj_columns = 
    {
        "info:dir", 
        "info:file", 
        "info:oname",
        "info:bucket",
        "info:time",
        "info:rsyncmtime",
        "info:size",
        "info:hsize",
        "info:etag",
        "ver:tag",
        "ver:seq",
    }

    local code,ok,dentry = metadb:get(OINDEX_TABLE, dentry_key, obj_columns)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to get object from metadb. dentry_key=", dentry_key)
        return code, false, nil
    end

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " dentry_key= ", dentry_key, " dentry=", stringify(dentry))

    return "0000", true, dentry
end

--return:
--   a         => #a
--   ab        => #ab
--   a/        => #a/
--   ab/       => #ab/
--   ab/cd     => ab/#cd
--   ab/cd/    => ab/#cd/
--   ab/cd//   => ab/#cd//
--   ab//cd//  => ab//#cd//
local function add_dir_marker(path)
    local len = #path

    assert(len > 0)
    assert(string.byte(path,1,1) ~= ASCII_SLASH)

    local path_bytes = {string.byte(path, 1, -1)}

    local last_non_slash = nil
    for i=len, 1, -1 do
        if path_bytes[i] ~= ASCII_SLASH then
            last_non_slash = i
            break
        end
    end

    assert(nil ~= last_non_slash)

    local pre_slash=nil
    for i=last_non_slash-1, 1, -1 do
        if path_bytes[i] == ASCII_SLASH then
            pre_slash = i
            break
        end
    end

    local prefix,suffix = nil,nil
    if nil == pre_slash then
        prefix = ""
        suffix = path
    else
        prefix = string.sub(path, 1, pre_slash)
        suffix = string.sub(path, pre_slash+1, -1)
    end
    
    local dentry_key = prefix .. DIR_MARKER .. suffix

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " path=", path," dentry_key=", dentry_key)
    return dentry_key
end

--return:
--   #a         =>  a
--   #ab        =>  ab
--   #a/        =>  a/
--   #ab/       =>  ab/
--   ab/#cd     =>  ab/cd
--   ab/#cd/    =>  ab/cd/
--   ab/#cd//   =>  ab/cd//
--   ab//#cd//  =>  ab//cd//
function _M.del_dir_marker(path)
    local len = #path

    assert(len > 0)
    assert(string.byte(path,1,1) ~= ASCII_SLASH)

    local path_bytes = {string.byte(path, 1, -1)}

    local last_non_slash = nil
    for i=len, 1, -1 do
        if path_bytes[i] ~= ASCII_SLASH then
            last_non_slash = i
            break
        end
    end

    assert(nil ~= last_non_slash)

    local pre_slash=nil
    for i=last_non_slash-1, 1, -1 do
        if path_bytes[i] == ASCII_SLASH then
            pre_slash = i
            break
        end
    end

    local prefix,suffix = nil,nil
    if nil == pre_slash then
        prefix = ""
        suffix = path
    else
        prefix = string.sub(path, 1, pre_slash)
        suffix = string.sub(path, pre_slash+1, -1)
    end

    if string.byte(suffix,1,1) == ASCII_DIR_MARKER then
        suffix = string.sub(suffix, 2, -1) --remove the DIR_MARKER
    end

    local dentry_key = prefix .. suffix

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " path=", path," dentry_key=", dentry_key)
    return dentry_key
end


--function parse_objname() is deprecated. Because:
--   1. we don't allow s3api to put a non-empty file whose name ends with "/";
--   2. when s3api puts an empty file whose name ends with "/", we treat it as a dir. this
--      is the same as fs-user. Thus, I re-write a parse_objname1():
--             a/b/c/   --> 3 dirs: "a/", "a/b/", "a/b/c/"    -->  client_type = "s3" or "fs"
--      
--return:innercode,ok,objkey,dirkeys
--       client_type = "fs":
--             a/b/c/   --> 3 dirs: "a/", "a/b/", "a/b/c/"; no file
--       client_type = "s3":
--             a/b/c/   --> 2 dirs: "a/", "a/b/" and a file "a/b/c/"
function _M.parse_objname(bucktag, objname, client_type)
    assert(#objname>0)
    assert(string.byte(objname,1,1) ~= ASCII_SLASH)  -- objname doesn't start with '/'

    local path = objname
    if "fs" == client_type then
        local err = nil
        path,_,err = ngx_re_gsub(objname, "/+", "/", "jo")
        if not path then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ngx_re_gsub failed. err=", err)
            return "1003",false,nil,nil
        end
    end

    local innercode,ok,comps = split_path(path)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " split_path failed. path=", path, " innercode=", innercode)
        return innercode,false,nil,nil 
    end

    --  ends-with-slash          is-fs-user   last-component-is-file
    --  ----------------------------------------------------
    --    true  (opt=2)            true            false            --> case-1
    --    true  (opt=2)            false           true             --> case-2
    --    false (opt=1)            true            true             --> case-3
    --    false (opt=1)            false           true             --> case-4

    local filename = nil
    local end_with_slash = (string.byte(comps[#comps],-1,-1) == ASCII_SLASH)
    if end_with_slash then
        if "fs" == client_type then  -- fs user, the path='a/b/', so it's a dir, instead of file
            filename = nil
        else  -- not fs user, path="a/b/", the dir is 'a/', the object name is 'b/' 
            filename = comps[#comps-1] .. comps[#comps]
            table.remove(comps)
            table.remove(comps)
        end
    else
        filename = comps[#comps]
        table.remove(comps)
    end

    local dirkeys  = new_tab(16, 0)
    local objkey   = nil

    local pathname = bucktag .. "_/"
    for i=1, #comps-1, 2 do
        local cur_dirkey = pathname .. DIR_MARKER .. comps[i] .. comps[i+1]
        table.insert(dirkeys, cur_dirkey)
        pathname = pathname .. comps[i] .. comps[i+1]
    end

    if filename ~= nil then
        objkey = pathname .. DIR_MARKER .. filename
    end

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " bucktag=", bucktag, " objname=", objname, " client_type=", client_type, 
                      " objkey=", objkey, " dirkeys=", stringify(dirkeys))

    return "0000", true, objkey, dirkeys
end

--return:innercode,ok,objkey,dirkeys
--       client_type = "fs" or "s3"
--             a/b/c/   --> 3 dirs: "a/", "a/b/", "a/b/c/"; no file
function _M.parse_objname1(bucktag, objname, client_type)
    assert(#objname>0)
    assert(string.byte(objname,1,1) ~= ASCII_SLASH)  -- objname doesn't start with '/'

    local path = objname
    if "fs" == client_type then
        local err = nil
        path,_,err = ngx_re_gsub(objname, "/+", "/", "jo")
        if not path then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ngx_re_gsub failed. err=", err)
            return "1003",false,nil,nil
        end
    end

    local innercode,ok,comps = split_path(path)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " split_path failed. path=", path, " innercode=", innercode)
        return innercode,false,nil,nil 
    end

    local filename = nil
    local end_with_slash = (string.byte(comps[#comps],-1,-1) == ASCII_SLASH)
    if end_with_slash then
        filename = nil
    else
        filename = comps[#comps]
        table.remove(comps)
    end

    local dirkeys  = new_tab(16, 0)
    local objkey   = nil

    local pathname = bucktag .. "_/"
    for i=1, #comps-1, 2 do
        local cur_dirkey = pathname .. DIR_MARKER .. comps[i] .. comps[i+1]
        table.insert(dirkeys, cur_dirkey)
        pathname = pathname .. comps[i] .. comps[i+1]
    end

    if filename ~= nil then
        objkey = pathname .. DIR_MARKER .. filename
    end

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " bucktag=", bucktag, " objname=", objname, " client_type=", client_type, 
                      " objkey=", objkey, " dirkeys=", stringify(dirkeys))

    return "0000", true, objkey, dirkeys
end

--return innercode,ok
function _M.mkdir_p(metadb, dirkeys, seq_no, dir_type)
    if nil == dirkeys or nil == next(dirkeys) then
        return "0000", true
    end

    local DIR_BODY = {"info", "dir", "true", "ver", "seq", seq_no..dir_type}
    local last_dirkey = dirkeys[#dirkeys]
    local innercode, ok, origin = nil, nil, nil
    innercode, ok, origin = metadb:checkAndMutateAndGetRow(OINDEX_TABLE, last_dirkey, "ver", "seq", metadb.CompareOp.GREATER, seq_no, DIR_BODY, nil)
    if not ok and "1121" ~= innercode then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to mutate row, last_dirkey=", last_dirkey)
        return innercode, false
    end

    if not next(origin) then
        DIR_BODY = {"info", "dir", "true", "ver", "seq", seq_no.."sys"}
        for i=1, #dirkeys-1 do
            local innercode,ok = metadb:put(OINDEX_TABLE, dirkeys[i], DIR_BODY)
            if not ok then
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " put dentry failed, dirkey=", dirkeys[i], " DIR_BODY=", stringify(DIR_BODY))
                return innercode, false
            end
        end
    end

    return "0000", true
end

--return innercode,ok
function _M.rmdir(metadb, dirkey, versionid)
    local delete_seqno = versionid
    if not delete_seqno then
        local innercode,ok,dentry = _M.stat(metadb, dirkey)
        if not ok then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to stat dentry. dirkey=", dirkey, " innercode=", innercode)
            return innercode, false
        end

        if not dentry["info:dir"] then -- dir does not exist
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " dir does not exist. dirkey=", dirkey)
            return "0000", true
        end

        -- check if dir is empty. Is it necessary ?

        -- dirkey: {BUCK-TAG}_/a/b/#c/
        local dirname   = ngx.re.sub(dirkey, DIR_MARKER, "", "jo")  -- dirname   : {BUCK-TAG}_/a/b/c/ 
        local key_start = dirname .. DIR_MARKER                     -- key_start : {BUCK-TAG}_/a/b/c/#
        local key_end   = key_start .. metadb.MAX_CHAR              -- key_end   : {BUCK-TAG}_/a/b/c/#{MAX-CHAR}

        local innercode, ok, ret = metadb:quickScanHash(OINDEX_TABLE, key_start, key_end, {"info:file", "info:dir"}, 1)
        if not ok then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to check if dir is empty. dirkey=", dirkey, " innercode=", innercode)
            return innercode,false
        end

        if ret and next(ret) then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " dir is not empty")
            return "1001",false
        end
        delete_seqno = dentry["ver:seq"]
    end
    
    local innercode, ok = metadb:checkAndDelete(OINDEX_TABLE, dirkey, "ver", "seq", delete_seqno)
    if not ok then
        if "1121" == innercode then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " delete dir outdated. dirkey=", dirkey)
            return "0000", true
        end
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to delete dir from metadb. dirkey=", dirkey, " innercode=", innercode)
        return innercode, false
    end
    
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " rmdir succeeded. dirkey=", dirkey)
    return "0000", true
end

--return: innercode,ok,origin
function _M.link(metadb, objkey, seq_no, objmeta, delmeta)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " objkey=", objkey, " seq_no=", seq_no, " objmeta=", stringify(objmeta))

    local innercode,ok,origin = nil,nil,nil
    innercode,ok,origin = metadb:checkAndMutateAndGetRow(OINDEX_TABLE, objkey, "ver", "seq", metadb.CompareOp.GREATER, seq_no, objmeta, delmeta)
    if ok then
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " link succeeded. objkey=", objkey, " seq_no=", seq_no, " origin=", stringify(origin))
        return "0000", true, origin
    end

    if "1121" == innercode then --failed because of condition-check-failure
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " link failed. file is outdated. objkey=", objkey, " seq_no=", seq_no, 
                         " original seq_no=", origin["ver:seq"])
        return "1121",false,origin
    end

    --failed because of unknown reason (neither succeeded, nore failed because of condition-check-failure)
    ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " metadb checkAndMutateAndGetRow failed. innercode=", innercode, " objkey=", objkey, " seq_no=", seq_no)
    return innercode,false,nil
end

--return: innercode,ok,origin
function _M.unlink(metadb, objkey, seq_no, compareop)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " objkey=", objkey, " seq_no=", seq_no)

    local innercode,ok,origin = nil,nil,nil
    for r=1,3 do
        --why is EQUAL accepted? because expiration-timer will use the same seq_no to delete the object;
        innercode,ok,origin = metadb:checkAndMutateAndGetRow(OINDEX_TABLE, objkey, "ver", "seq", compareop, seq_no, nil, FILE_COLS)

        if ok then  -- checkAndMutateAndGetRow succeeded
            break
        end

        if innercode == "1121" then -- checkAndMutateAndGetRow failed because of condition-check-failure, no need to retry;
            break
        end
    end

    if ok then
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " unlink succeeded. objkey=", objkey, " seq_no=", seq_no, " origin=", stringify(origin))
        return "0000",true,origin
    end

    if "1121" == innercode then --failed because of condition-check-failure
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " unlink failed. delete-op is outdated. objkey=", objkey, " seq_no=", seq_no, 
                         " original seq_no=", origin["ver:seq"])
        return "1121",false,origin
    end

    --failed because of unknown reason (neither succeeded, nore failed because of condition-check-failure)
    ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " metadb checkAndMutateAndGetRow failed. innercode=", innercode, " objkey=", objkey, " seq_no=", seq_no)
    return innercode,false,nil
end

-- return: innercode,ok,obj_metadata
--      if error occurred:                     innercode=errCode ok=false, obj_metadata=nil
--      if no error, but object doesn't exist: innercode="0000"  ok=true,  obj_metadata={}
--      if no error, and object exist:         innercode="0000"  ok=true,  obj_metadata={ 
--                                                                              ["info:file"]="true", 
--                                                                              ["info:size"]=size,
--                                                                              ["info:etag"]=etag,
--                                                                              ......  }
function _M.head(metadb, dentry_key)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, "Enter. dentry_key=", dentry_key)

    local obj_columns = 
    {
        "info:file", 
        "info:size", 
        "info:etag", 
        "info:time", 
        "info:rsyncmtime", 
        "ver:tag", 
        "ver:seq", 
        "mfest:psize", 
        "sattrs:json",
    }

    local innercode,ok,dentry = metadb:get(OINDEX_TABLE, dentry_key, obj_columns)

    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to get a dentry from metadb, dentry_key=", dentry_key)
        return innercode,false,nil 
    end

    if not dentry["info:file"] or not dentry["ver:tag"] then  -- file doesn't exist
        return "0000", true, {}
    end

    return "0000", true, dentry
end

function _M.fs_ls(metadb, reqinfo, bucktag, path, marker, delimiter, req_dentries)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " Enter fs_ls, bucktag=", bucktag, 
                      " path=", path, " marker=", marker, " delimiter=", delimiter, " req_dentries=", req_dentries)

    if delimiter ~= "" and delimiter ~= "/" then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " delimiter should be slash for filesystem users, but it's ", delimiter)
        return "0011","0000",false
    end

    if #path > 0 then
        assert(string.byte(path,1,1) ~= ASCII_SLASH)
    end

    local orig_path = path
    local orig_marker = marker

    path,_,err = ngx_re_gsub(path, "/+", "/", "jo")
    if not path then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ngx_re_gsub failed. err=", err)
        return "0011","1003",false
    end

    if marker < path then
        marker = ""
    end

    local headers = reqinfo["headers"]
    local userinfo = reqinfo["user_info"]

    local uid = userinfo["info:uid"]
    local displayname = userinfo["info:displayname"]

    local root_dir = bucktag .. "_/"
    local root_dir_len = #root_dir

    local need_scan = nil 
    local ls_prefix = ngx.ctx.fs_extent_ls_prefix or ""

    local files_ret = {}
    local comPrefix_ret = {}
    local got = 0

    local truncated   = false
    local last_dentry = nil
    local versionid = reqinfo["args"]["versionId"]

    if path == "" then
        need_scan = true
        local file_dentry = "<Contents>"..
                                "<Key>" .. xmlencode(ls_prefix).."</Key>"..
                                "<ETag>&quot;".. md5hex(EMPTY_MD5) .."&quot;</ETag>"..
                                "<Size>0</Size><StorageClass>STANDARD</StorageClass>"..
                                "<Owner><ID>"..uid.."</ID><DisplayName>"..displayname.."</DisplayName></Owner>"..
                            "</Contents>"
        table.insert(files_ret, file_dentry)
    else
        local dentry_key = root_dir .. add_dir_marker(path)

        local innercode,ok,dentry = _M.stat(metadb, dentry_key)
        if not ok then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to stat ", dentry_key, " innercode=", innercode)
            return "2004", innercode, false
        end

        if string.byte(path,-1,-1) == ASCII_SLASH then   -- ls a/b/; it must be a dir;
            if not dentry["info:dir"]  then
                need_scan = false   -- the dir doesn't exist, no need to scan 
            else
                need_scan = true    -- the dir exists, we need to scan it
                local file_dentry = "<Contents>"..
                                        "<Key>" .. xmlencode(ls_prefix .. path).."</Key>"..
                                        "<ETag>&quot;".. md5hex(EMPTY_MD5) .."&quot;</ETag>"..
                                        "<Size>0</Size><StorageClass>STANDARD</StorageClass>"..
                                        "<Owner><ID>"..uid.."</ID><DisplayName>"..displayname.."</DisplayName></Owner>"..
                                    "</Contents>"
                table.insert(files_ret, file_dentry)
            end
        else  -- ls a/b; it must be a file
            need_scan = false  -- the dir doesn't exist, no need to scan

            if dentry["info:file"] and dentry["ver:tag"] then -- file exists
                local etag = md5hex(dentry["info:etag"] or "")

                local mtime = dentry["info:rsyncmtime"]
                if not mtime or mtime == "0" then
                    mtime = dentry["info:time"]
                end

                local file_dentry = "<Contents>"..
                                        "<Key>" .. xmlencode(ls_prefix .. path).."</Key>"..
                                        "<LastModified>"..toUTC(mtime).."</LastModified>"..
                                        "<ETag>&quot;"..etag.."&quot;</ETag>"..
                                        "<Size>"..dentry["info:size"].."</Size><StorageClass>STANDARD</StorageClass>"..
                                        "<Owner><ID>"..uid.."</ID><DisplayName>"..displayname.."</DisplayName></Owner>"..
                                    "</Contents>"
                got = got + 1
                table.insert(files_ret, file_dentry)
                last_dentry = path
            end
        end
    end

    if got >= req_dentries then  -- req_dentries=1 and we got one
        need_scan = false 
        truncated = true
    end


    if need_scan then
        local key_start = nil 
        local key_end = nil

        if path == "" then  -- root dir
            key_start = root_dir .. DIR_MARKER 
            key_end = key_start .. metadb.MAX_CHAR
            if marker ~= "" then
                key_start = key_start .. marker .. metadb.MIN_CHAR
            end
        else  -- path is something like "dir1/dir7/"
            key_start = root_dir .. path .. DIR_MARKER  -- key_start = {BUCKET-TAG}_dir1/dir7/#
            key_end = key_start .. metadb.MAX_CHAR
            if marker ~= "" then   -- marker = dir1/dir7/xyz
                local i,j,err = ngx.re.find(marker, path, "jo")
                if err then
                    ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ngx.re.find error")
                    return "0011","0000",false
                end
                if not i or i ~= 1 then
                    ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " marker should be prefixed with path")
                    return "0011","0000",false
                end
                local tail = string.sub(marker, j+1)  -- tail = xyz 
                key_start = key_start .. tail .. metadb.MIN_CHAR  -- key_start = {BUCKET-TAG}_dir1/dir7/#xyz{MIN_CHAR}
            end
        end

        local columns = {"info"}
        if versionid then
            columns = {"info", "ver"}
        end
        local code,ok,scanner = metadb:openScanner(OINDEX_TABLE, key_start, key_end, columns)
        local max_once = sp_conf.config["hbase_config"]["max_scan_once"] or 1000

        while got < req_dentries do
            local scan_num = math.min(max_once, req_dentries - got)

            local code, ok, ret, num = metadb:scan(OINDEX_TABLE, scanner, scan_num)
            if not ok then
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " metadb scan failed. innercode=", code)
                return "2005", code, false
            end

            for i, v in ipairs(ret) do
                last_dentry = string.sub(v.key, root_dir_len+1, -1)
                last_dentry = ngx.re.sub(last_dentry, DIR_MARKER, "", "jo")
                --last_dentry = string.gsub(last_dentry, DIR_MARKER, "")

                local ret_key = nil 
                local ret_etag = nil 
                local ret_size = nil

                if v.values["info:dir"] then -- dir
                    local ret_key  = ls_prefix .. last_dentry
                    local last_modified = v.values["info:time"] or ""
                    local commPref_dentry
                    if not versionid then
                        commPref_dentry = "<CommonPrefixes>" .. 
                        "<Prefix>".. utils.xmlencode(ret_key).. "</Prefix>" ..
                        "</CommonPrefixes>"
                    else
                        commPref_dentry = "<CommonPrefixes>" .. 
                        "<Prefix>".. utils.xmlencode(ret_key).. "</Prefix>" ..
                        "<VersionId>"..(v.values["ver:seq"] or "").."</VersionId>"..
                        "</CommonPrefixes>"
                    end

                    table.insert(comPrefix_ret, commPref_dentry)
                    got = got + 1
                elseif v.values["info:file"] == "true" then --file
                    local ret_key  = ls_prefix .. last_dentry
                    local ret_etag = md5hex(v.values["info:etag"] or "")
                    local ret_size = v.values["info:size"]
                    local file_dentry = "<Contents>"..
                                            "<Key>" .. utils.xmlencode(ret_key).."</Key>"..
                                            "<LastModified>"..utils.toUTC(v.values["info:time"]).."</LastModified>"..
                                            "<ETag>&quot;".. ret_etag .."&quot;</ETag>"..
                                            "<Size>" .. ret_size .. "</Size>"..
                                            "<StorageClass>STANDARD</StorageClass>"..
                                            "<Owner><ID>"..uid.."</ID><DisplayName>"..displayname.."</DisplayName></Owner>"..
                                        "</Contents>"
                    table.insert(files_ret, file_dentry)
                    got = got + 1
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
    end

    local out_xml = {}
    local index = 1

    out_xml[index] = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
    index = index + 1
    out_xml[index] = "<Name>" .. reqinfo["op_info"]["bucketname"] .. "</Name>"
    index = index + 1

    out_xml[index] = "<Prefix>" .. utils.xmlencode(ls_prefix..orig_path) .. "</Prefix>"
    index = index + 1

    if "" ~= orig_marker then
        out_xml[index] = "<Marker>" .. utils.xmlencode(orig_marker) .. "</Marker>"
        index = index + 1
    end

    out_xml[index] = "<MaxKeys>" .. req_dentries .. "</MaxKeys>"
    index = index + 1

    if truncated and last_dentry and "" ~= last_dentry then
        out_xml[index] = "<NextMarker>"..utils.xmlencode(last_dentry).."</NextMarker>"
        index = index + 1
    end

    if "" ~= delimiter then
        out_xml[index] = "<Delimiter>"..delimiter.."</Delimiter>"
        index = index + 1
    end

    out_xml[index] = "<IsTruncated>"..tostring(truncated).."</IsTruncated>"
    index = index + 1

    if nil ~= next(files_ret) then
        out_xml[index] = table.concat(files_ret)
        index = index + 1
    end

    if nil ~= next(comPrefix_ret) then
        out_xml[index] = table.concat(comPrefix_ret)
        index = index + 1
    end

    out_xml[index] = "</ListBucketResult>"
    index = index + 1

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " out_xml=", stringify(out_xml))

    local response_headers = sp_conf.config["s3_GET_Service"]["s3_response_headers"]
    for k,v in pairs(response_headers) do
        ngx.header[k] = v
    end

    ngx.header["Content-Type"] = "application/xml"    
    ngx.print(table.concat(out_xml))

    return "0000", "0000", true
end

return _M
