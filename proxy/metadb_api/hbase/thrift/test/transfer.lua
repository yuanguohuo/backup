local ok, new_tab = pcall(require, "table.new")
if not ok or type(new_tab) ~= "function" then
    new_tab = function (narr, nrec) return {} end
end

local ok, utils = pcall(require, "common.utils")
if not ok or not utils then
    error("failed to load common.utils:" .. (utils or "nil"))
end

local ok, hb = pcall(require, "hbase.hbase-instance")
if not ok or not hb then
    error("failed to load hbase.hbase-instance:" .. (hb or "nil"))
end

local ok,uuid = pcall(require, "resty.uuid")
if not ok or not uuid then
    error("failed to load resty.uuid:" .. (uuid or "nil"))
end
local cjson = require('cjson')

local ok, s3_op = pcall(require, "storageproxy_s3_op")
if not ok or not s3_op then
    error("failed to load s3_op:" .. (s3_op or "nil"))
end

local ok,rgwapi = pcall(require, "objstore.rgw.rgw")
if not ok or not rgwapi then
    error("failed to load objstore.rgw.rgw:"..(rgwapi or "nil"))
end

function ScanUser1()
    local code, ok, ret = hb:quickScanHash("user", nil,  nil, nil, 10000)
    for k,v in pairs(ret) do
        local key = k
        local value = v
        local part = {}
        part["key"] = key
        part["secretkey"] = value["info:secretkey"]
        part["displayname"] = value["info:displayname"]
        part["company"] = value["exattrs:company"]
        local jsonstr = cjson.encode(part)
        local file,err = io.open("/tmp/user1", "a+")
        
        if file == nil then
            ngx.say("open faile")
            ngx.flush()
        end
        io.output(file)
        io.write(jsonstr)
        io.close(file)
    end
    return true
end

function ScanBucket1()
    local code, ok, ret = hb:quickScanHash("bucket", nil,  nil, nil, 10000)

    for k,v in pairs(ret) do
        local key = k
        local value = v
        local part = {}
        part["accessid"] = value["info:accessid"]
        part["name"] = key
        local jsonstr = cjson.encode(part)
        local file,err = io.open("/tmp/bucket1", "a+")
        
        if file == nil then
            ngx.say("open faile")
            ngx.flush()
        end
        io.output(file)
        io.write(jsonstr)
        io.close(file)
    end
    return true
end


function ScanObject(btag, bucket_rowkey, accessId, bucketname)
   
    local start = btag.."_"..hb.MIN_CHAR
    local last_mark = btag.."_"..hb.MIN_CHAR
    while(1)
    do
        local num = 0
        local code, ok, ret = hb:quickScan("object", start,  btag.."_"..hb.MAX_CHAR, nil, 10000)

        for k,v in pairs(ret) do
            local key = v["key"]
            local value = v["values"]
            value["accessId"] = accessId
	        value["bucketKey"] = bucket_rowkey
	        value["buckname"] = bucketname
	        value["rowkey"] = key
            local jsonstr = cjson.encode(value)
            --local filename = "/home/hdata/object_"..accessId
            local filename = "/home/hdata/object_"
            local file,err = io.open(filename, "a+")
        
            if file == nil then
                ngx.say("open faile")
                ngx.flush()
		        return false
            end
            io.output(file)
            io.write(jsonstr)
            io.write("\n")
            io.close(file)
            num = num + 1
            last_mark = key
        end
        if num < 10000 then
            break;
        end
        start = last_mark..hb.MIN_CHAR
    end
    return true
end

function ScanBucket(utag)
    local code, ok, ret = hb:quickScan("bucket", utag.."_"..hb.MIN_CHAR,  utag.."_"..hb.MAX_CHAR, nil, 10000)

    for k,v in pairs(ret) do
        local key = v["key"]
        local value = v["values"]
	    local accessid = value["info:accessid"]
        value["rowkey"] = key
        local jsonstr = cjson.encode(value)
	    local btag = value["ver:tag"]
        --local filename = "/home/hdata/bucket_"..accessid
        local filename = "/home/hdata/bucket_"
        local file,err = io.open(filename, "a+")
        
        if file == nil then
            ngx.say("open faile")
            ngx.flush()
        end
        io.output(file)
        io.write(jsonstr)
        io.write("\n")
        io.close(file)
	    local s,e = ngx.re.find(key, "_", "jo")
        local buckname = string.sub(key, e+1, -1)
	    ScanObject(btag, key, accessid, buckname)
    end
    return true
end

local accessid = {}
accessid["MA6G7F4NG9L5FAF3BW"]   = "true"
accessid["NX0RBDWSO284JVUSS1KF"] = "true"
accessid["R6RO360T9KTRKCFVIW41"] = "true"
accessid["33FVJK1MIY2MZGMCTZPK"] = "true"
accessid["XIIGA08FL12W08FRTNM"]   = "true"


function ScanUser()
    local code, ok, ret = hb:quickScan("user", nil,  nil, nil, 10000)
    for k,v in pairs(ret) do
        local key = v["key"]
        local value = v["values"]
	    if nil ~= accessid[key] then
            value["rowkey"] = key
            local utag = value["ver:tag"]
            local jsonstr = cjson.encode(value)
            --local filename = "/home/hdata/user_"..key
            local filename = "/home/hdata/user_"
            local file,err = io.open(filename, "a+")
            
            if file == nil then
                ngx.say("open faile")
                ngx.flush()
            end
            io.output(file)
            io.write(jsonstr)
            io.write("\n")
            io.close(file)
        
            ScanBucket(utag)
        end
    end
    return true
end


function create_user()
    local file = io.input("/home/hdata/user_")
    repeat
        local line = io.read()
        if nil == line then
            break
        end
        
        local j_str = cjson.decode(line)

        local info_uid = j_str["info:uid"]
        local secretkey = j_str["info:secretkey"]
        local displayname = j_str["info:displayname"]
        local mtime = j_str["info:mtime"]
        local accessid = j_str["rowkey"]
        local company = j_str["exattrs:company"] 

        local hbase_body = {
            "info","uid", info_uid,
            "info","type","fs",
            "info","secretkey",secretkey,
            "info","displayname",displayname,
            "info","mtime",mtime,
            "info","maxbuckets",1024,
            "quota","enabled",-1,
            "quota","objects",-1,
            "quota","size_kb",-1,
            "bucket_quota","size_kb",-1,
            "bucket_quota","objects",-1,
            "bucket_quota","enabled",-1,
            "ver","tag",accessid,
            "ver","version","0",
            "exattrs","company",company,
        }
        local code, ok = hb:put("user", accessid, hbase_body)
        if not ok then
            ngx.say("putRow failed, hbase put user failed, innercode="..code, " accessid:", accessid)
            ngx.log(ngx.ERR, " putRow failed, hbase put user failed, innercode="..code, " accessid:", accessid)
            return false
        end

        local hbase_userid = {
            "info","accessid",accessid,
            "ver","tag",accessid,
        }

        local code, ok = hb:put("userid", info_uid, hbase_userid)
        if not ok then
            ngx.say("putRow failed, hbase put userid failed, innercode="..code, " accessid:", accessid)
            ngx.log(ngx.ERR, " putRow failed, hbase put userid failed, innercode="..code, " accessid:", accessid)
            return false
        end
    until(false)
    return true
end


function create_bucket()
    local file = io.input("/home/hdata/bucket_")
    repeat
        local line = io.read()
        if nil == line then
            break
        end
        
        local j_str = cjson.decode(line)
        
        local ctime = j_str["info:ctime"]
        local mtime = j_str["info:mtime"]
        local rowkey = j_str["rowkey"]
        local accessid = j_str["info:accessid"]
        local vertag = j_str["ver:tag"]

        local hbase_body = {"info", "ctime", ctime,
                          "info", "mtime", mtime,
                          "info", "accessid", accessid,
                          "quota", "enabled", -1,
                          "quota", "size_kb", -1,
                          "quota", "objects", -1,
                          "ver", "tag", vertag}
        local code, ok = hb:put("bucket", rowkey, hbase_body)
        if not ok then
            ngx.say("putRow failed, hbase put  bucket failed, innercode="..code, " accessid:", accessid, " rowkey:", rowkey)
            ngx.log(ngx.ERR, " putRow failed, hbase put bucket failed, innercode="..code, " accessid:", accessid, " rowkey:", rowkey)
            return false
        end

    until(false)
    return true
end

function create_object()
    local file = io.input("/home/hdata/object_")
    repeat
        local line = io.read()
        if nil == line then
            break
        end
        
        local j_str = cjson.decode(line)
        local code, incode, ok = s3_op.upgrade_obj(hb, j_str)
        if not ok then
            ngx.log(ngx.ERR,  "putRow failed, hbase put  object failed,  code="..code.. " innercode="..incode)
            ngx.say("putRow failed, hbase put  object failed,  code="..code.. " innercode="..incode)
            return false
        end

    until(false)
    return true
end

function delete_infolock_object(btag) 
    local start = btag.."_"..hb.MIN_CHAR
    local endkey = btag.."_"..hb.MAX_CHAR

    local code, ok, scanid = hb:openScanner("object", start, endkey, nil, nil)
    if not ok then
        ngx.log(ngx.ERR,  "hbase quickscan object failed,  code="..code.." start="..start)
        ngx.say(ngx.ERR,  "hbase quickscan object failed,  code="..code.." start="..start)
        return ok
    end
    
    local succ = true
    while(1)
    do
        local num = 0
        local code, ok, ret = hb:scan("object", scanid, 5000)
        if not ok then
            ngx.log(ngx.ERR,  "hbase scan object failed,  code="..code.." start="..start)
            ngx.say(ngx.ERR,  "hbase scan object failed,  code="..code.." start="..start)
            succ = false
            break
        end
        
        for k,v in pairs(ret) do
            local key = v["key"]
            local value = v["values"]
            local lock = value["info:lock"]
            local link = value["info:link"]
            if nil ~= lock or nil ~= link then
                local otag = value["ver:tag"]
                if nil == otag then
                    value["rowkey"] = key
                    local jsonstr = cjson.encode(value)
                    local filename = "/home/haojinglong/hdata/object_"
                    local file,err = io.open(filename, "a+")
                    
                    if file == nil then
                        ngx.say("open faile")
                        ngx.flush()
                    end
                    io.output(file)
                    io.write(jsonstr)
                    io.write("\n")
                    io.close(file)
                    -- garbage
                    -- file has been deleted
                    local code, ok = hb:delete("object", key, nil)
                    if not ok then
                        ngx.log(ngx.ERR,  "hbase delete object failed,  code="..code.." key="..key)
                        ngx.say(ngx.ERR,  "hbase delete object failed,  code="..code.." key="..key)
                        succ = false
                        break
                    end
                end
            end
            
            num = num + 1
        end
        if num < 5000 then
            break;
        end
        ngx.sleep(0.001)
    end

    local code,ok = hb:closeScanner(scanid)
    if not ok then
        ngx.log(ngx.ERR,  "hbase closeScanner failed,  code="..code.." scanid="..scanid)
        ngx.say(ngx.ERR,  "hbase closeScanner failed,  code="..code.." scanid="..scanid)
        succ = false 
    end

    return succ
end

function delete_infolock_bucket(utag)
    local code, ok, ret = hb:quickScan("bucket", utag.."_"..hb.MIN_CHAR,  utag.."_"..hb.MAX_CHAR, nil, 10000)
    
    if not ok then
        ngx.log(ngx.ERR,  "hbase quickscan bucket failed,  code="..code.." utag="..utag)
        ngx.say(ngx.ERR,  "hbase quickscan bucket failed,  code="..code.." utag="..utag)
        return ok
    end
    
    local succ = true
    for k,v in pairs(ret) do
        local key = v["key"]
        local value = v["values"]
	    local btag = value["ver:tag"]
        if nil == btag then
            succ = false
            ngx.log(ngx.ERR,  " bucket  ver:tag is nil, key:"..key)
            ngx.say(ngx.ERR,  " bucket  ver:tag is nil, key:"..key)
            break
        end
	    local ok = delete_infolock_object(btag)
        if not ok then
            succ = false
            ngx.log(ngx.ERR,  " delete infolock object failed, btag:"..btag)
            ngx.say(ngx.ERR,  " delete infolock object failed, btag:"..btag)
            break
        end
    end
    return succ
end

function delete_infolock()
    local code, ok, ret = hb:quickScan("user", nil,  nil, nil, 10000)
    if not ok then
        ngx.log(ngx.ERR,  "hbase quickscan user failed,  code="..code)
        ngx.say(ngx.ERR,  "hbase quickscan user failed,  code="..code)
        return ok
    end

    local succ = true
    for k,v in pairs(ret) do
        local key = v["key"]
        local value = v["values"]
        local utag = value["ver:tag"]
        if nil == utag then
            value["rowkey"] = key
            local jsonstr = cjson.encode(value)
            local filename = "/home/haojinglong/hdata/user_"
            local file,err = io.open(filename, "a+")
            
            if file == nil then
                ngx.say("open faile")
                ngx.flush()
            end
            io.output(file)
            io.write(jsonstr)
            io.write("\n")
            io.close(file)
            local code, ok = hb:delete("user", key, nil)
            if not ok then
                ngx.log(ngx.ERR,  "hbase delete user failed,  code="..code.." key="..key)
                ngx.say(ngx.ERR,  "hbase delete user failed,  code="..code.." key="..key)
                succ = false
                break
            end
            --succ = false
            --ngx.log(ngx.ERR,  " user  ver:tag is nil, key:"..key)
            --ngx.say(ngx.ERR,  " user  ver:tag is nil, key:"..key)
            --break
        else
            local ok = delete_infolock_bucket(utag)
            if not ok then
                ngx.log(ngx.ERR,  " delete infolock bucket failed, utag:"..utag)
                ngx.say(ngx.ERR,  " delete infolock bucket failed, utag:"..utag)
                succ = false
                break
            end
        end
    end

    return succ
end

function get_ceph_object(rgw, object, psize, ceph_objtag)
    local objs = {}
    if not psize or psize == 0 then
        objs[1] = {
            name = ceph_objtag.."_"..tostring(1),
            start_pos = 0,
            end_pos = 9,
        }
    else
        objs[1] = {
            name = ceph_objtag.."_"..tostring(1).."_"..tostring(1),
            start_pos = 0,
            end_pos = 9,
        }
    end

    local code,ok,res = rgw:get_obj(object, objs)
    if not ok or "table" ~= type(res) then
        return false
    end
    
    local len = string.len(res[1])
    if 10 ~= len then
        return false
    end
    return true
end

function check_infolink_object(btag) 
    local start = btag.."_"..hb.MIN_CHAR
    local endkey = btag.."_"..hb.MAX_CHAR
    
    local filter = "SingleColumnValueFilter('info','link',!=,'binary:xxx',true,true)"
    local code, ok, scanid = hb:openScanner("object", start, endkey, nil, 0, filter)
    if not ok then
        ngx.log(ngx.ERR,  "hbase quickscan object failed,  code="..code.." start="..start)
        ngx.say(ngx.ERR,  "hbase quickscan object failed,  code="..code.." start="..start)
        return ok
    end

    local rgw = rgwapi:new("127.0.0.1", "8000")
    local succ = true
    while(1)
    do
        local num = 0
        local code, ok, ret = hb:scan("object", scanid, 5000)
        if not ok then
            ngx.log(ngx.ERR,  "hbase scan object failed,  code="..code.." start="..start)
            ngx.say(ngx.ERR,  "hbase scan object failed,  code="..code.." start="..start)
            succ = false
            break
        end
        
        for k,v in pairs(ret) do
            local key = v["key"]
            local value = v["values"]
            local link = value["info:link"]
            assert(link)

            local s,e = string.find(link, "_")
            local ceph_objtag  = string.sub(link, 1, s-1) 
            local ceph_objname = string.sub(link, e+1, -1)
            local psize = tonumber(value["mfest:psize"])
            
            local ok = get_ceph_object(rgw, ceph_objname, psize, ceph_objtag)
            if not ok then
                -- link not empty,try read vtag
                ceph_objtag = value["ver:tag"]
                ceph_objname = value["info:oname"]
                local filename = "/home/haojinglong/hdata/object_"
                
                local ok = get_ceph_object(rgw, ceph_objname, psize, ceph_objtag)
                if ok then
                    filename = "/home/haojinglong/hdata/object_nolink_vtag"
                    local code, ok = hb:delete("object", key, {"info:link"})
                    if not ok then
                        ngx.log(ngx.ERR,  "hbase delete object failed,  code="..code.." key="..key)
                        ngx.say(ngx.ERR,  "hbase delete object failed,  code="..code.." key="..key)
                        succ = false
                        break
                    end
                else
                    filename = "/home/haojinglong/hdata/object_nolink_novtag"
                end
                
                value["rowkey"] = key
                local jsonstr = cjson.encode(value)
                local file,err = io.open(filename, "a+")
                if file == nil then
                    ngx.say("open faile")
                    ngx.flush()
                end
                io.output(file)
                io.write(jsonstr)
                io.write("\n")
                io.close(file)
            end
            num = num + 1
        end
        if num < 5000 then
            break;
        end
        ngx.sleep(0.001)
    end

    local code,ok = hb:closeScanner(scanid)
    if not ok then
        ngx.log(ngx.ERR,  "hbase closeScanner failed,  code="..code.." scanid="..scanid)
        ngx.say(ngx.ERR,  "hbase closeScanner failed,  code="..code.." scanid="..scanid)
        succ = false 
    end

    return succ
end

function check_infolink_bucket(utag)
    local code, ok, ret = hb:quickScan("bucket", utag.."_"..hb.MIN_CHAR,  utag.."_"..hb.MAX_CHAR, nil, 10000)
    
    if not ok then
        ngx.log(ngx.ERR,  "hbase quickscan bucket failed,  code="..code.." utag="..utag)
        ngx.say(ngx.ERR,  "hbase quickscan bucket failed,  code="..code.." utag="..utag)
        return ok
    end
    
    local succ = true
    for k,v in pairs(ret) do
        local key = v["key"]
        local value = v["values"]
	    local btag = value["ver:tag"]
        if nil == btag then
            succ = false
            ngx.log(ngx.ERR,  " bucket  ver:tag is nil, key:"..key)
            ngx.say(ngx.ERR,  " bucket  ver:tag is nil, key:"..key)
            break
        end
	    local ok = check_infolink_object(btag)
        if not ok then
            succ = false
            ngx.log(ngx.ERR,  " check infolink object failed, btag:"..btag)
            ngx.say(ngx.ERR,  " check infolink object failed, btag:"..btag)
            break
        end
    end
    return succ
end

function check_infolink_alluser()
    local code, ok, ret = hb:quickScan("user", nil,  nil, nil, 10000)
    if not ok then
        ngx.log(ngx.ERR,  "hbase quickscan user failed,  code="..code)
        ngx.say(ngx.ERR,  "hbase quickscan user failed,  code="..code)
        return ok
    end

    local succ = true
    for k,v in pairs(ret) do
        local key = v["key"]
        local value = v["values"]
        local utag = value["ver:tag"]
        if nil == utag then
            value["rowkey"] = key
            local jsonstr = cjson.encode(value)
            local filename = "/home/haojinglong/hdata/user_"
            local file,err = io.open(filename, "a+")
            
            if file == nil then
                ngx.say("open faile")
                ngx.flush()
            end
            io.output(file)
            io.write(jsonstr)
            io.write("\n")
            io.close(file)
        else
            local ok = check_infolink_bucket(utag)
            if not ok then
                ngx.log(ngx.ERR,  " delete infolock bucket failed, utag:"..utag)
                ngx.say(ngx.ERR,  " delete infolock bucket failed, utag:"..utag)
                succ = false
                break
            end
        end
    end

    return succ
end

function check_infolink_user(accessid)
    local code,ok,ret = hb:get("user", accessid, {"ver:tag"})
    if not ok then
        ngx.log(ngx.ERR,  "hbase get user vertag failed,  code="..code)
        ngx.say(ngx.ERR,  "hbase get user vertag failed,  code="..code)
        return ok
    end

    local succ = true
    local utag = ret["ver:tag"]
    if nil == utag then
        value["rowkey"] = key
        local jsonstr = cjson.encode(value)
        local filename = "/home/haojinglong/hdata/user_"
        local file,err = io.open(filename, "a+")
        
        if file == nil then
            ngx.say("open faile")
            ngx.flush()
        end
        io.output(file)
        io.write(jsonstr)
        io.write("\n")
        io.close(file)
    else
        local ok = check_infolink_bucket(utag)
        if not ok then
            ngx.log(ngx.ERR,  " check infolink bucket failed, utag:"..utag)
            ngx.say(ngx.ERR,  " check infolink bucket failed, utag:"..utag)
            succ = false
        end
    end

    return succ
end
function check_infolink()
    local args = ngx.req.get_uri_args()
    local accessid = args["accessid"]
    if nil ~= accessid then
        return check_infolink_user(accessid)
    else
        return check_infolink_alluser()
    end
end

local tests = 
{
--    ScanUser,
--    ScanBucket,
--    create_user,
--    create_bucket,
--    create_object,
--    delete_infolock
--    check_infolink
}

local total   = #tests 
local success = 0
local failure = 0
for i, func in ipairs(tests) do
    local ok = func()
    if ok then
        success = success + 1
    else
        failure = failure + 1
        break
    end
end
local notRun = total - success - failure
ngx.say("Total   : "..tostring(total))
ngx.say("Success : "..tostring(success))
ngx.say("Failure : "..tostring(failure))
ngx.say("Skipped : "..tostring(notRun))
