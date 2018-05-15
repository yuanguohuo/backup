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

local args = ngx.req.get_uri_args()

local cmd = args["cmd"]
local tab = args["tab"]

if not cmd or type(cmd) ~= "string" or cmd == "" then
    ngx.say("Invalid command: "..utils.stringify(cmd))
    return 1
end

ngx.say("cmd='"..cmd.."'")
ngx.say("tab='"..tab.."'")

if string.lower(cmd) == "get" then
    -- curl "http://127.0.0.1:8191/hbase/thrift/test/cli?cmd=get&tab=user&row=JTLA6O1TF69Z0YB4I7O1&cols=ver:tag,info:mtime" 
    local row  = args["row"]
    local cols = args["cols"]

    ngx.say("row='"..row.."'")

    local fclist= {}
    for w in string.gmatch(cols, "[^,]+") do
        table.insert(fclist, w)
    end

    ngx.say("cols="..utils.stringify(fclist))

    local code,ok,ret = hb:get(tab, row, fclist)
    if not ok then
        ngx.say("Failure: code="..code..", ret="..utils.stringify(ret))
        return 1
    end
    ngx.say("Success: ret=\n"..utils.stringify(ret))
elseif string.lower(cmd) == "scan" then
    ngx.say("not supported")
elseif string.lower(cmd) == "set" then
    ngx.say("not supported")
end
