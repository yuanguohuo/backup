--By Yuanguo: 16/8/2016

local ok, new_tab = pcall(require, "table.new")
if not ok or type(new_tab) ~= "function" then
    new_tab = function (narr, nrec) return {} end
end

local ok,bit = pcall(require, "bit")
if not ok or not bit then
    error("failed to load bit:" .. (bit or "nil"))
end

local ok,sp_conf = pcall(require, "storageproxy_conf")
if not ok or not sp_conf then
    error("failed to load storageproxy_conf:" .. (sp_conf or "nil"))
end

local restystr = require "resty.string"

local ok,ffi = pcall(require, "ffi")
if not ok or not ffi then
    error("failed to load ffi:" .. (ffi or "nil"))
end

ffi.cdef[[
struct timeval {
    long int tv_sec;
    long int tv_usec;
};
int gettimeofday(struct timeval *tv, void *tz);
]];
local tm = ffi.new("struct timeval");

local serverId  = string.sub(ngx.md5(os.getenv("HOSTNAME")), 1, 10) .. string.format("%02d", ngx.worker.id())
local local_seq_no = 0

local ngx_md5_bin = ngx.md5_bin

local _M = new_tab(0,155)
_M._VERSION = '0.1'

local function timezone()
    local now = os.time()
    return os.difftime(now, os.time(os.date("!*t", now)))
end

-- Return a timezone string in ISO 8601:2000 standard form (+hhmm or -hhmm)
local function tz_offset(timezone)
    local h, m = math.modf(timezone / 3600)
    return string.format("%+.4d", 100 * h + 60 * m)
end

-- return a string describing current time in the same format with command "date -R"
function _M.date_r()
    local timezone = timezone()
    local tzoff = tz_offset(timezone)
    local dt = os.date("%a, %d %b %Y %H:%M:%S") 
    return dt.." "..tzoff
end

function _M.timestamp()
    ffi.C.gettimeofday(tm,nil);
    local sec =  tonumber(tm.tv_sec);        --seconds
    local msec =  math.floor(tonumber(tm.tv_usec)/1000); --milli seconds
    return sec*1000 + msec
end

-- time is 1478164339.264
function _M.toUTC(time)
    local msec = ''
    local pos = string.find(time, "%.")
    if nil ~= pos then
        msec = string.sub(time, pos, - 1)
    end
    return os.date("!%Y-%m-%dT%H:%M:%S", time)..msec.."Z"
end

-- time is 1478164339.264
-- return 20161103
function _M.toDATE(time)
    return os.date("%Y%m%d", time)
end

--date 20161103
function _M.date_to_sec(date)
    assert(8 == #date)
    local y = string.sub(date, 1, 4)
    local m = string.sub(date, 5, 6)
    local d = string.sub(date, 7, 8)
    return os.time{year=y, month=m, day=d, hour=0}
end

function _M.msleep(ms)
    ngx.sleep(ms/1000)
end

function _M.random()
    math.randomseed(tostring(_M.timestamp()):reverse():sub(1, 6))
    return math.random()
end

local function val2str(val, result, indent, skipLongStr, threshold, newline)
    local i = #result
    local t = type(val)

    if "nil" == t or "boolean" == t or "number" == t then
        result[i+1] = tostring(val)
    elseif "function" == t or "thread" == t or "userdata" == t then
        result[i+1] = tostring(val) 
    elseif "string" == t then
        local len = #val
        if skipLongStr and len > threshold then
            result[i+1] = "[a string of " .. tostring(len) .. " bytes, too long to print ...]"
        else
            result[i+1] = val
        end
    elseif "table" == t then
        result[i+1] = "{\n"
        i = #result
        local newindent = indent .. "    "

        --for beauty, we save the table members and print them 
        --at last
        local table_members = {}
        for k,v in pairs(val) do
            if "table" == type(v) then
                table_members[k] = v
            else
                result[i+1] = newindent .. tostring(k) .. "="
                val2str(v, result, newindent, skipLongStr, threshold, true) -- value
                i = #result
            end
        end

        --print the table members
        for k,v in pairs(table_members) do
            result[i+1] = newindent .. tostring(k) .. "="
            val2str(v, result, newindent, skipLongStr, threshold, true) -- value
            i = #result
        end

        result[i+1] = indent .. "}"
    else 
        assert(false)
    end

    if newline then  -- in a table, we need a '\n' for every key
        result[i+2] = "\n"
    end
end

function _M.stringify(value, skip_long_string, long_threshold)
    local skipLongStr = true -- skip long string by default
    local threshold = 300    -- a string is deemed as "long" if it's longer than 400 by default

    if skip_long_string == false then
        skipLongStr = false
    end
    if long_threshold then
        threshold = tonumber(long_threshold)
    end

    local result = new_tab(256,0)
    val2str(value, result, "", skipLongStr, threshold)
    return table.concat(result)
end


function _M.str2hex(str)
    local result = {}
    local index = 1
    for k,v in ipairs({str:byte(1,-1)}) do
        result[index] = string.format("%02X",v)
        index = index + 1
    end
    return string.lower(table.concat(result))
end

--in old proxy version, put_obj stored banary md5 result(16 byte) in hbase, in new proxy version, 
--proxy stored hex string(greater or equal than 32 byte) of md5 in hbase, we need to translate banary md5 value to hex,
function _M.md5hex(str)
    return ((#str > 16) and str or restystr.to_hex(str))
end

function _M.hexbin2num(bin)
    local s = bin:byte(1)
    if s >= 128 then  --negative
        local num = 0
        for k,v in ipairs({bin:byte(1,-1)}) do
            --Yuanguo: get "not" of "v". Note that "bit.not" treats "v" as a number (64 bit), so I 
            --lshift 56 bits, not, and then rshift 56
            local notbyte = bit.rshift((bit.bnot(bit.lshift(v,56))),56)
            num = num*256 + notbyte 
        end
        num = num + 1
        return 0-num
    else  --positive
        local num = 0
        for k,v in ipairs({bin:byte(1,-1)}) do
            num = num*256 + v
        end
        return num
    end
end

--Yuanguo: convert a number to a binary string whose length is 'len'; 
--   1. if the string is shorter than len, pad with '0x00'; if longer, throw error;
--   2. if the number is negative, throw error;
function _M.num2hexbin(num, len)
    if(num == nil) then
        return nil 
    end
    if num < 0 then
        error("common.utils.num2hexbin, negative number is not supported: num="..tostring(num))
    end
    local str = "" 
    while num > 0 and len > 0 do
        local mod = num % 256
        num = math.floor(num / 256)
        str = string.char(mod)..str
        len = len - 1
    end

    if num > 0 and len <= 0 then
        error("common.utils.num2hexbin, overflow: num="..tostring(num)..", len="..tostring(len))
    end

    while len > 0 do
        str = string.char(0)..str
        len = len - 1
    end

    return str
end

function _M.encodeuri(str)
    if(str) then
        --str=string.gsub(str, "\n", "\r\n")
	str = string.gsub(str, "([^%w%d%._%-%*])", function (c) return string.format("%%%02X", string.byte(c)) end )
	--str = string.gsub(str, "([^%w])", function (c) return string.format("%%%02X", string.byte(c)) end )
        str=string.gsub(str, " ", "+")
    end
    return str
end

local function make_event(evtName,stamp)
    local evt = new_tab(0,4)
    evt.name = evtName
    evt.time = stamp
    return evt
end

function _M.mark_event(evtName)
    local evts =ngx.ctx.trace_events  
    if evts and #evts then
        ngx.update_time()
        local now = ngx.now()
        local evt = make_event(evtName, now)
        table.insert(evts, evt)
    end
end

function _M.dump_events()
    local evts =ngx.ctx.trace_events  
    if evts and #evts >0 then
        local formated = new_tab(128,0)
        for i=1, #evts do
            formated[i] = "\t" .. evts[i].name .. "\t" .. tostring(evts[i].time) .. "\n"
        end
        local str = table.concat(formated)

        local total = evts[#evts].time - evts[1].time
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " TotalTimeConsumed=", total, "\n", str)
    end
end

function _M.xmlencode(tag)
    tag = string.gsub(tag, "&", "&amp;")
    tag = string.gsub(tag, "<", "&lt;")
    tag = string.gsub(tag, ">", "&gt;")
    tag = string.gsub(tag, "\'", "&apos;")
    tag = string.gsub(tag, "\"", "&quot;")
    return tag
end
function _M.xmldecode(tag)
    tag = string.gsub(tag, "&amp;", "&")
    tag = string.gsub(tag, "&lt;", "<")
    tag = string.gsub(tag, "&gt;", ">")
    tag = string.gsub(tag, "&apos;", "\'")
    tag = string.gsub(tag, "&quot;", "\"")
    
    tag = string.gsub(tag, "&#38;", "&")
    tag = string.gsub(tag, "&#60;", "<")
    tag = string.gsub(tag, "&#62;", ">")
    tag = string.gsub(tag, "&#39;;", "\'")
    tag = string.gsub(tag, "&#34;", "\"")
    return tag
end

--×Ö·û´®·Ö¸îº¯Êý
function _M.strsplit(str, pattern, num)
    print("begin to strsplist, str is ", str, " , and pattern is ", pattern, " , num is ", num)
    local sub_str_tab = {}

    if nil == str then
        return sub_str_tab
    end
    if nil == pattern then
        return {str} 
    end

    local i = 0
    local j = 0
    if num == nil then
        num = -1
    end

    while true do
        if num == 0 then
            table.insert(sub_str_tab, string.sub(str, i, -1))
            break
        end

        j = string.find(str, pattern, i+1)
        if j == nil then
            table.insert(sub_str_tab, string.sub(str, i, -1))
            break
        end
        table.insert(sub_str_tab, string.sub(str, i, j-1))
        i = j + 1
        num = num - 1
    end

    return sub_str_tab
end


--params:
--  timestamp: a string in format: 1495530144.020
--             1495530144 : the time in seconds;
--             020        : the milli-seconds;
--             You can get current time in this format by:
--                      string.format("%0.3f", ngx.now())
function _M.gen_seq_no(timestamp)
    local seq = string.format("%04d", local_seq_no)

    --increase local_seq_no
    local_seq_no = local_seq_no + 1

    -- 0x1FFF = 8191, a sinle nginx work can NEVER process more than this number of 
    -- requests within 1 milli-second
    local_seq_no = bit.band(local_seq_no, 0x1FFF)

    return timestamp .. "_" .. serverId .. "_" .. seq
end

local shard_arry = {}
function _M.get_shard(name, max, id)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", "get ", name, " max:", max," id:", id)
    
    if nil == shard_arry[name] then
        shard_arry[name] = {}
        for i=1, max do
            shard_arry[name][i] = string.sub(ngx.md5(name.."_"..i), 1, 10)
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", "get shard i:", i, " value:", shard_arry[name][i])
        end
    end
    
    if id >= 1 and id <= max then
        return shard_arry[name][id]   
    else
        assert(false)
    end

    return nil
   -- return string.sub(ngx.md5("datalog_"..id),1,10)
end

function _M.out_db_to_in(src)
    local dest = {}
    local index = 1
    for k, v in pairs(src) do
        local s,e = ngx.re.find(k, ":", "jo")
        local family = string.sub(k, 1, s-1)
        local column = string.sub(k, e+1, -1)
    
        dest[index] = family
        index = index + 1
        dest[index] = column
        index = index + 1
        dest[index] = v
        index = index + 1
    end
    return dest
end

--range is [0,65536)
function _M.hash(str)
    local md5bin = ngx_md5_bin(str)

    local ret = string.byte(md5bin,-2)
    ret = ret * 256 + string.byte(md5bin,-1)

    return ret
end

return _M
