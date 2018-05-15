-- By Yuanguo, 31/08/2016

local CLIENT_POOL_SIZE = 32

local ok, sp_conf = pcall(require, "storageproxy_conf")
if not ok or not sp_conf then
    error("failed to load storageproxy_conf:" .. (sp_conf or "nil"))
end

local ok, new_tab = pcall(require, "table.new")
if not ok or type(new_tab) ~= "function" then
    new_tab = function (narr, nrec) return {} end
end

local ok, queue = pcall(require, "common.queue")
if not ok or not queue then
    error("failed to load common.queue:" .. (queue or "nil"))
end

local ok, utils = pcall(require, "common.utils")
if not ok or not utils then
    error("failed to load common.utils:" .. (utils or "nil"))
end

local inspect = require "inspect"
local TSocket = require('thrift.lualib.TCosocket')
local TBufferedTransport = require('thrift.lualib.TBufferedTransport')
local TFramedTransport = require('thrift.lualib.TFramedTransport')
local TBinaryProtocol = require('thrift.lualib.TBinaryProtocol')
local THBaseServiceClient = require('hbase.thrift.client.THBaseServiceClient')

local TAppend            = require "hbase.thrift.client.TAppend"
local TAuthorization     = require "hbase.thrift.client.TAuthorization"
local TCellVisibility    = require "hbase.thrift.client.TCellVisibility"
local TColumnIncrement   = require "hbase.thrift.client.TColumnIncrement"
local TColumn            = require "hbase.thrift.client.TColumn"
local TColumnValue       = require "hbase.thrift.client.TColumnValue"
local TDelete            = require "hbase.thrift.client.TDelete"
local TDeleteType        = require "hbase.thrift.client.TDeleteType"
local TDurability        = require "hbase.thrift.client.TDurability"
local TGet               = require "hbase.thrift.client.TGet"
local THRegionInfo       = require "hbase.thrift.client.THRegionInfo"
local THRegionLocation   = require "hbase.thrift.client.THRegionLocation"
local TIllegalArgument   = require "hbase.thrift.client.TIllegalArgument"
local TIncrement         = require "hbase.thrift.client.TIncrement"
local TIOError           = require "hbase.thrift.client.TIOError"
local TMutation          = require "hbase.thrift.client.TMutation"
local TPut               = require "hbase.thrift.client.TPut"
local TResult            = require "hbase.thrift.client.TResult"
local TRowMutations      = require "hbase.thrift.client.TRowMutations"
local TScan              = require "hbase.thrift.client.TScan"
local TServerName        = require "hbase.thrift.client.TServerName"
local TTimeRange         = require "hbase.thrift.client.TTimeRange"

local RETRY_INTERVAL = sp_conf.config["hbase_config"]["retry_interval"]
local RETRY_TIMES    = sp_conf.config["hbase_config"]["retry_times"]

local _M = new_tab(0, 11)
_M._VERSION = '0.1'
_M._THRIFT_VERSION = '0.9.2'

_M.MIN_CHAR = string.char(0)
_M.MAX_CHAR = string.char(255)

_M.CompareOp = 
{
    LESS             = 0,
    LESS_OR_EQUAL    = 1,
    EQUAL            = 2,
    NOT_EQUAL        = 3,
    GREATER_OR_EQUAL = 4,
    GREATER          = 5,
    NO_OP            = 6
}

--Yuanguo: for hbase COUNTER value, "get" op returns a string like this:
--             "0x00 0x00 0x00 0x00 0x00 0x00 0x03 0x1B"
--I need to convert it to a numberic value (see common.utils.hexbin2num() 
--function)
--But, the tricky point is that: I don't know which are COUNTER values, in
--the return value of "get", there is no field telling us it's a COUNTER.
--So, currently I only get this idea: 
--   1. put all COUNTER values of our projet into the "counter_set" below;
--   2. for "get", check if the return value is in "counter_set", and convert
--      it into num if it is;
local counter_set =
{
    ["user:stats:objects"] = true,
    ["user:stats:size_bytes"] = true,

    ["bucket:stats:size_bytes"] = true,
    ["bucket:stats:objects"] = true,
}

local client_pool = queue:new(CLIENT_POOL_SIZE)

local protocols = 
{
    binary = TBinaryProtocol,   
    compact = TCompactProtocol,   --Yuanguo: Not supported in Thrift 0.9.2
    json = TJSONProtocol,         --Yuanguo: Not supported in Thrift 0.9.2
}

local transports = 
{
    buffered = TBufferedTransport,
    framed = TFramedTransport,
    http = THttpTransport,        --Yuanguo: Not supported in Thrift 0.9.2
}

function _M.new(self, server, port, protocol, transport)
    local protoc = protocol or "binary"
    local transp = transport or "framed"
    if self._THRIFT_VERSION == "0.9.2" then
        if protoc ~= "binary" then
            if protoc == "compact" or protoc == "json" then
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " protocol ", protoc, " is not supported in Thrift 0.9.2")
            else
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " protocol ", protoc, " is not recognized")
            end
            return nil
        end
        if transp ~= "buffered" and transp ~= "framed" then
            if transp == "http" then
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " http transport is not supported in Thrift 0.9.2")
            else
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " transport ", transp, " is not recognized")
            end
            return nil
        end
    elseif self._THRIFT_VERSION == "0.9.3" then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " Thrift 0.9.3 is not compatible with openresty 1.9.15.1")
        return nil
    else
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " Thrift version ", self._THRIFT_VERSION, " is not recognized")
        return nil
    end

    return setmetatable(
               {server = server, port = port, protocol = protoc, transport = transp, storName = "HBase"},
               {__index = self}
           )
end

local function create_client(self)
    --1. create socket
    local sock = TSocket:new({host=self.server, port=self.port})
    if not sock then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to create Thrift TSocket, port=", utils.stringify(self.port))
        return nil, "failed to create Thrift TSocket"
    end

    --2. create transport
    local trans = transports[self.transport]:new({socket=sock, isServer=false})
    if not trans then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to create Thrift transport, transport=", utils.stringify(self.transport))
        return nil, "failed to create Thrift transport"
    end

    --3. create protocol
    local protoc = protocols[self.protocol]:new({transport=trans})
    if not protoc then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to create Thrift protocol, protocol=", utils.stringify(self.protocol))
        return nil, "failed to create Thrift protocol"
    end

    --4. create the client
    local client = THBaseServiceClient:new({protocol=protoc,counter=0})  --iprot and oprot will be the same: protoc
    if not client then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to create Thrift THBaseServiceClient")
        return nil, "failed to create Thrift THBaseServiceClient"
    end

    return client
end

local function close_client(client)
    if client then
        -- close the connection
        client:close()
    end
end

local function makeTGet(key, columns, op)
    if type(key) ~= "string" or key == "" then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " Invalid arguments for hbase operation, 'key' must be a non-empty string for ", op)
        return nil,"1100"
    end

    --although hbase support: if columns not specified, the entire row is returned
    --we don't support it for clearness;
    if nil == columns then
        return nil, "0000"
    end

    if type(columns) ~= "table" then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " Invalid arguments for hbase operation, 'columns' must be a table for ", op)
        return nil,"1100"
    end

    --although hbase support: if columns not specified, the entire row is returned
    --we don't support it for clearness;
    if nil == next(columns) then
        return nil, "0000"
    end

    local cols = {}
    local c = 1
    for i = 1, #columns, 1 do
        local family, column = nil, nil
        local k,j = string.find(columns[i],":")
        if k then
            family = string.sub(columns[i], 1, k-1)
            column = string.sub(columns[i], j+1, -1)
        else
            family = columns[i]
        end
        cols[c] = TColumn:new({family=family, qualifier = column})
        c = c + 1
    end
     
    local tget = TGet:new({
        row=key,
        columns=cols,
    })

    return tget
end

local function makeTIncrement(table_name, key, columns, op)
    if type(key) ~= "string" or key == "" then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " Invalid arguments for hbase operation, 'key' must be a non-empty string for ", op)
        return nil,"1100"
    end

    if nil == columns then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " Invalid arguments for hbase operation, 'columns' cannot be nil for ", op)
        return nil,"1100"
    end

    if type(columns) ~= "table" then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " Invalid arguments for hbase operation, 'columns' must be a table for ", op)
        return nil,"1100"
    end

    if #columns == 0 then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " Invalid arguments for hbase operation, family:column:amount must be present for ", op)
        return nil,"1100"
    end

    if #columns % 3 ~= 0 then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " Invalid arguments for hbase operation, family:column:amount must be in triples for ", op, ": ", unpack(columns))
        return nil,"1100"
    end

    local cols = {}
    local c = 1
    for i = 1, #columns-2, 3 do
        local fam, qua, val = columns[i], columns[i+1], columns[i+2]
        local tfq = table_name..":"..fam..":"..qua
        if not counter_set[tfq] then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " HBase op not allowed for non-COUNTER value, ", op, " not allowed for ", tfq, " because it is not a COUNTER value")
            return nil,"1109"
        end
        cols[c] = TColumnIncrement:new({family=fam, qualifier=qua, amount=val})
        c = c + 1
    end

    local tincr = TIncrement:new({
        row=key,
        columns=cols,
    })

    return tincr
end

local function makeTPut(table_name, key, columnVals, op, ttl)
    if type(key) ~= "string" or key == "" then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " Invalid arguments for hbase operation, 'key' must be a non-empty string for ", op)
        return nil,"1100"
    end

    if nil == columnVals then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " Invalid arguments for hbase operation, 'columnVals' cannot be nil for ", op)
        return nil,"1100"
    end

    if type(columnVals) ~= "table" then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " Invalid arguments for hbase operation, 'columnVals' must be a table for ", op)
        return nil,"1100"
    end

    if #columnVals == 0 then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " Invalid arguments for hbase operation, family:column:value must be present for ", op)
        return nil,"1100"
    end

    if #columnVals % 3 ~= 0 then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " Invalid arguments for hbase operation, family:column:value must be in triples for ", op, ": ", unpack(columnVals))
        return nil, "1100"
    end

    local colVals = {}
    local c = 1
    for i = 1, #columnVals-2, 3 do
        local fam, qua, val = columnVals[i], columnVals[i+1], columnVals[i+2]
        local tfq = table_name..":"..fam..":"..qua
        if nil == val then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " HBase op not allowed for nil value, ", op, " not allowed for ", tfq, " because its value is nil")
            return nil, "1110"
        end
        if counter_set[tfq] then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " HBase op not allowed for COUNTER value, ", op, " not allowed for ", tfq, " because it is a COUNTER value")
            return nil, "1110"
        end
        colVals[c] = TColumnValue:new({family=fam, qualifier=qua, value=val})
        c = c + 1
    end
    local attributes = {}
    if nil ~= ttl and '' ~= ttl then
        attributes["_ttl"] = utils.num2hexbin(ttl, 8)
    end
    local tput = TPut:new({
        row=key,
        columnValues = colVals,
        attributes = attributes,
    })

    return tput
end

local function makeTDelete(key, columns, op)
    if type(key) ~= "string" or key == "" then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " Invalid arguments for hbase operation, 'key' must be a non-empty string for ", op)
        return nil,"1100"
    end

    if type(columns) ~= "table" then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " Invalid arguments for hbase operation, 'columns' must be a table for ", op)
        return nil,"1100"
    end

    local cols = {}
    local c = 1
    for i = 1, #columns, 1 do
        local family, column = nil, nil
        local k,j = string.find(columns[i],":")
        if k then
            family = string.sub(columns[i], 1, k-1)
            column = string.sub(columns[i], j+1, -1)
        else
            family = columns[i]
        end
        cols[c] = TColumn:new({family=family, qualifier = column})
        c = c + 1
    end

    local tdel = TDelete:new({
        row=key,
        columns = cols,
    })

    return tdel
end

local function makeRowMutations(table_name, key, put, del, op, ttl)
    if type(key) ~= "string" or key == "" then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " Invalid arguments for hbase operation, 'key' must be a non-empty string for ", op)
        return nil,"1100"
    end

    --Yuanguo: although HBase allows you to put and delete the same cell, I don't think it 
    --         makes any sense, so disable it!
    if put and #put > 0 and del and #del > 0 then
        local put_set = {}
        for x=1, #put-2, 3 do
            local f = put[x]
            local c = put[x+1]
            if nil == put_set[f] then
                put_set[f] = {}
            end
            put_set[f][c] = true
        end

        for y=1, #del, 1 do
            local f, c = nil, nil
            local k,j = string.find(del[y],":")
            if k then
                f = string.sub(del[y], 1, k-1)
                c = string.sub(del[y], j+1, -1)
            else
                f = del[y]
            end

            if c == nil then  --delete the entire family
                if put_set[f] ~= nil then  --there is put in the family, conflict
                    ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " Invalid arguments for HBase ", op, 
                             ", delete and put must have NO intersection, but you're deleting family ", f, " and putting columns in it")
                    return nil, "1100"
                end
            else  --delete the specified column
                if put_set[f] and put_set[f][c] then
                    ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " Invalid arguments for HBase ", op,
                             ", delete and put must have NO intersection, but you're deleting and putting ", f, ":", c)
                    return nil, "1100"
                end
            end
        end
    end

    local tput = nil
    if put and #put > 0 then
        local thePut, code = makeTPut(table_name, key, put, op, ttl)
        if not thePut then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " Failed to make TPut instance for HBase ", op)
            return nil, code
        end
        tput = thePut
    end

    local tdel = nil
    if del and #del > 0 then
        local theDel, code = makeTDelete(key, del, op)
        if not theDel then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " Failed to make TDelete instance for HBase ", op)
            return nil, code
        end
        tdel = theDel
    end

    if not tput and not tdel then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " Put and delete CANNOT both be nil or empty table for HBase ", op)
        return nil, "1100"
    end

    local mutations={}
    local c = 1
    if tput then
        mutations[c] = TMutation:new({put=tput})
        c = c + 1
    end
    if tdel then
        mutations[c] = TMutation:new({deleteSingle = tdel})
        c = c + 1
    end

    local tmutate = TRowMutations:new({
        row=key,
        mutations = mutations,
    })

    return tmutate
end


local function makeTScan(start, stop, columns, op, cache, filte, reversed)
    if type(columns) ~= "table" then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " Invalid arguments for hbase operation, 'columns' must be a table for ", op)
        return nil,"1100"
    end

    ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " caching: ", cache, " filter: ", filte)
    local cols = {}
    local c = 1
    for i = 1, #columns, 1 do
        local family, column = nil, nil
        local k,j = string.find(columns[i],":")
        if k then
            family = string.sub(columns[i], 1, k-1)
            column = string.sub(columns[i], j+1, -1)
        else
            family = columns[i]
        end
        cols[c] = TColumn:new({family=family, qualifier = column})
        c = c + 1
    end
--struct TScan {
--  1: optional binary startRow,
--  2: optional binary stopRow,
--  3: optional list<TColumn> columns
--  4: optional i32 caching,
--  5: optional i32 maxVersions=1,
--  6: optional TTimeRange timeRange,
--  7: optional binary filterString,
--  8: optional i32 batchSize,
--  9: optional map<binary, binary> attributes
--  10: optional TAuthorization authorizations
--  11: optional bool reversed
--  12: optional bool cacheBlocks
--}


    local tscan = TScan:new({
        startRow = start,
        stopRow  = stop,
        columns  = cols,
        caching = cache,
        filterString = filte,
        reversed = reversed,
    })

    return tscan 
end

local function dequeue_client(self,op)
    local ok, client = client_pool:dequeue()
    if not ok or not client then
        ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " failed to get a client for hbase ", op, " from client pool, create a new one!")
        local clnt,err = create_client(self)
        if not clnt then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " Failed to create thrift hbase client for ", op, ", err=", utils.stringify(err))
            return nil, "1101"
        end
        client = clnt
    else
        client.counter = client.counter + 1
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " Thrift HBase Client reused times: ", client.counter)
    end

    --Yuanguo: when we create a new client, client.iprot.transport.sock (client.oprot.transport.sock) is nil; 
    --         when we enqueue_client, we have called setkeepalive, which will set client.iprot.transport.sock 
    --         (client.oprot.transport.sock) to nil (although the underlying connection is kept by cosocket)
    -- So, we open it;
    local status, err = pcall(client.open, client)
    if not status then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to open client, err=", utils.stringify(err))
--        return nil, "failed to open Thrit transport"
        return nil, "1116"
    end

    return client 
end

local function enqueue_client(client, op)
    --Yuanguo: set keepalive when we put the client into queue. the client.iprot.transport.sock 
    --(client.oprot.transport.sock) is set to nil.
    client:setkeepalive()

    local ok, err = client_pool:enqueue(client)
    if not ok then
        ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " failed to put client into client pool for hbase ", op, ", drop it!")
    end
end

--Params:
--    table_name: the name of the table
--    key       : the row key
--    columns   : an array containing an optional list of family[:column]; e.g.
--                         {"family1:column1", "family2", "family3:column3", ...}
--                if it's nil or an empty table, the call will succeed and an empty table will 
--                be returned.
--Return: innercode, ok, body; where body is a table as shown below in the case of
--        success or nil in the case of failure:
--                {
--                    [family:column] = value
--                    [family:column] = value
--                    ...
--                }
function _M.get(self, table_name, key, columns)
    local result = new_tab(0,6)

    local tget, code = makeTGet(key, columns, "get")
    if not tget then
        if code == "0000" then
            return "0000", true, result
        else
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " Failed to make TGet instance for HBase get")
            return code, false, nil
        end
    end

    local client,code = dequeue_client(self,"get")
    if not client then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " Failed to get a thrift hbase client for get")
        return code, false, nil
    end

    local ok, ret 
    for retry=1,RETRY_TIMES do
        ok, ret = pcall(client.get, client, table_name, tget)
        if ok then
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " HBase Thrift client get succeeded, retry=", retry)
            break
        end

        if type(ret) == "table" and type(ret.message) == "string" then
            local m = string.match(ret.message, "Table.*was not found")
            if m then
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " HBase Thrift client get failed, table was not found, no need to retry")
                break
            end
        end

        if retry<RETRY_TIMES then --don't sleep for the last round 
            ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " HBase Thrift client get failed, retry in ", retry*RETRY_INTERVAL, " seconds")
            ngx.sleep(retry * RETRY_INTERVAL)
        end
    end

    if not ok or not ret then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " HBase Thrift client get failed, err=", utils.stringify(ret))
        return "1102", false, nil
    else
        for i,v in pairs(ret.columnValues) do
            local vv = v.value
            if counter_set[table_name..":"..v.family..":"..v.qualifier] then
                vv = utils.hexbin2num(v.value)
            end
            result[v.family..":"..v.qualifier] = vv
        end
    end

    enqueue_client(client, "get")

    return "0000", true, result
end

--Params:
--    table_name: the name of the table
--    key       : the row key
--    colValues : an array containing family1, column1, amount, family2, column2, amount ... they 
--                must be in triples.
--Return: innercode, ok, current
--               current: the current values of the incremented columns; it's a table like this:
--               {
--                   [family:column] = value
--                   [family:column] = value
--                   ...
--               }
function _M.increment(self, table_name, key, colValues)
    local tincr, code = makeTIncrement(table_name, key, colValues, "increment")
    if not tincr then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " Failed to make TIncrement instance for HBase increment")
        return code, false,nil
    end

    local client,code = dequeue_client(self,"increment")
    if not client then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " Failed to get a thrift hbase client for increment")
        return code, false,nil
    end

    local innercode,stat
    local current={}
    local ok,ret 
    for retry=1,RETRY_TIMES do
        ok,ret = pcall(client.increment, client, table_name, tincr)
        if ok then
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " HBase Thrift client increment succeeded, retry=", retry)
            break
        end

        if type(ret) == "table" and type(ret.message) == "string" then
            local m = string.match(ret.message, "Table.*was not found")
            if m then
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " HBase Thrift client increment failed, table was not found, no need to retry")
                break
            end

            local s,_ = string.find(ret.message, "timeout")
            if s then
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " HBase Thrift client increment failed, timeout, no need retry")
                break
            end
        end

        if retry<RETRY_TIMES then --don't sleep for the last round 
            ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " HBase Thrift client increment failed, retry in ", retry*RETRY_INTERVAL, " seconds")
            ngx.sleep(retry * RETRY_INTERVAL)
        end
    end

    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " HBase Thrift client increment failed, err=", utils.stringify(ret))
        innercode = "1103" 
        stat = false
    else
        innercode = "0000" 
        stat = true 
        for i,v in ipairs(ret.columnValues) do
            local fc = v["family"]..":"..v["qualifier"]
            local value = utils.hexbin2num(v.value)
            current[fc] = value
        end
    end

    enqueue_client(client, "increment")

    return innercode, stat, current
end

--Params:
--    table_name: the name of the table
--    key       : the row key
--    colValues : an array containing family1, column1, value, family2, column2, value ... they 
--                must be in triples, e.g:
--                {[1]=family1, [2]=column1, [3]=value1, [4]=family2, [5]=column2, [6]=value2, ... }
--    ttl       : ttl in milli-seconds
--Return: innercode, ok
function _M.put(self, table_name, key, colValues, ttl)
    local tput, code = makeTPut(table_name, key, colValues, "put", ttl)
    if not tput then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " Failed to make TPut instance for HBase put")
        return code, false
    end

    local client,code = dequeue_client(self,"put")
    if not client then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " Failed to get a thrift hbase client for put")
        return code, false
    end

    local innercode,stat 
    local ok,ret 
    for retry=1,RETRY_TIMES do
        ok,ret= pcall(client.put, client, table_name, tput)
        if ok then
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " HBase Thrift client put succeeded, retry=", retry)
            break
        end

        if type(ret) == "table" and type(ret.message) == "string" then
            local m = string.match(ret.message, "Table.*was not found")
            if m then
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " HBase Thrift client put failed, table was not found, no need to retry")
                break
            end
        end

        if retry<RETRY_TIMES then --don't sleep for the last round 
            ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " HBase Thrift client put failed, retry in ", retry*RETRY_INTERVAL, " seconds")
            ngx.sleep(retry * RETRY_INTERVAL)
        end
    end

    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " HBase Thrift client put failed, err=", utils.stringify(ret))
        innercode = "1104" 
        stat = false
    else
        innercode = "0000" 
        stat = true 
    end

    enqueue_client(client, "put")

    return innercode, stat
end

--For the given row, do the put operation only if the value of 'checkFamily':'checkColumn' matches 'checkValue'
--Params:
--    table_name : the name of the table
--    key        : the key of the given row
--    checkFamily: the family to check
--    checkColumn: the column to check
--    checkValue : the value to check
--    colValues  : a table containing family1, column1, value, family2, column2, value ... they 
--                 must be in triples, e.g.
--                 {[1]=family1, [2]=column1, [3]=value1, [4]=family2, [5]=column2, [6]=value2, ... }
--    ttl        : ttl in milli-seconds
--Return: innercode, ok
--           innercode : "0000" if ok is true;
--                       "1121" if ok is false and the failure is caused by condition-check-failure;
--                       "xxxx" if ok is false and the failure is caused by other reasons;
--           ok        : true   if the 'Put' operation has been done; 
--                       false  if the 'Put' operation has NOT been done; if this is because of 
--                              condition-check-failure, innercode must be "1121"
function _M.checkAndPut(self, table_name, key, checkFamily, checkColumn, checkValue, colValues, ttl)
    local tput, code = makeTPut(table_name, key, colValues, "checkAndPut", ttl)
    if not tput then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " Failed to make TPut instance for HBase checkAndPut")
        return code, false
    end

    local tocheck = checkValue
    if counter_set[table_name..":"..checkFamily..":"..checkColumn] then
        tocheck = utils.num2hexbin(checkValue, 8)
    end

    local client,code = dequeue_client(self,"checkAndPut")
    if not client then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " Failed to get a thrift hbase client for checkAndPut")
        return code, false
    end

    local innercode,stat 
    local ok,ret 
    for retry=1,RETRY_TIMES do
        ok,ret= pcall(client.checkAndPut, client, table_name, key,
                        checkFamily, checkColumn, tocheck,
                        tput)
        if ok then -- succeeded, or failed with condition check failure, don't retry
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " HBase Thrift client checkAndPut succeeded or failed in condition check, retry=", retry)
            break
        end

        if type(ret) == "table" and type(ret.message) == "string" then
            local m = string.match(ret.message, "Table.*was not found")
            if m then
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " HBase Thrift client checkAndPut failed, table was not found, no need to retry")
                break
            end

            local s,_ = string.find(ret.message, "timeout")
            if s then
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " HBase Thrift client checkAndPut failed, timeout, no need to retry")
                break
            end
        end

        if retry<RETRY_TIMES then --don't sleep for the last round 
            ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " HBase Thrift client checkAndPut failed, retry in ", retry*RETRY_INTERVAL, " seconds")
            ngx.sleep(retry * RETRY_INTERVAL)
        end
    end

    if ok then  -- no error is thrown, ret is the return value
        if ret == true then
            innercode = "0000" 
            stat = true 
        else
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " HBase Thrift client checkAndPut failed, err=condition check failed")
            innercode = "1121" 
            stat = false
        end
    else  -- error is thrown, ret is the error;
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " HBase Thrift client checkAndPut failed, err=", utils.stringify(ret))
        innercode = "1105" 
        stat = false
    end

    enqueue_client(client, "checkAndPut")

    return innercode, stat
end

--Params:
--    table_name: the name of the table
--    key       : the row key
--    columns   : an array containing an optional list of family[:column]; e.g.
--                         {"family1:column1", "family2", "family3:column3", ...}
--                if columns is nil or an empty table, the entire row is deleted; else, 
--                the specified family and/or columns are deleted;
--Return: innercode, ok
function _M.delete(self, table_name, key, columns)
    local tdel, code = makeTDelete(key, columns or {}, "delete")
    if not tdel then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " Failed to make TDelete instance for HBase delete")
        return code, false
    end

    local client,code = dequeue_client(self,"delete")
    if not client then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " Failed to get a thrift hbase client for delete")
        return code, false
    end

    local innercode,stat 
    local ok,ret 
    for retry=1,RETRY_TIMES do
        ok,ret= pcall(client.deleteSingle, client, table_name, tdel)
        if ok then
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " HBase Thrift client delete succeeded, retry=", retry)
            break
        end

        if type(ret) == "table" and type(ret.message) == "string" then
            local m = string.match(ret.message, "Table.*was not found")
            if m then
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " HBase Thrift client delete failed, table was not found, no need to retry")
                break
            end
        end

        if retry<RETRY_TIMES then --don't sleep for the last round 
            ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " HBase Thrift client delete failed, retry in ", retry*RETRY_INTERVAL, " seconds")
            ngx.sleep(retry * RETRY_INTERVAL)
        end
    end

    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " HBase Thrift client delete failed, err=", utils.stringify(ret))
        innercode = "1106" 
        stat = false
    else
        innercode = "0000" 
        stat = true 
    end

    enqueue_client(client, "delete")

    return innercode, stat
end

--For the given row, do the delete operation only if the value of 'checkFamily':'checkColumn' matches 'checkValue'
--Params:
--    table_name : the name of the table
--    key        : the key of the given row
--    checkFamily: the family to check
--    checkColumn: the column to check
--    checkValue : the value to check
--    columns    : a table containing an optional list of family[:column]; if columns is nil 
--                 or an empty table, the entire row is deleted; else, the specified family 
--                 and/or columns are deleted;
--Return: innercode, ok
--           innercode : "0000" if ok is true;
--                       "1121" if ok is false and the failure is caused by condition-check-failure;
--                       "xxxx" if ok is false and the failure is caused by other reasons;
--           ok        : true   if the 'Delete' operation has been done; 
--                       false  if the 'Delete' operation has NOT been done; if this is because of 
--                              condition-check-failure, innercode must be "1121"
function _M.checkAndDelete(self, table_name, key, checkFamily, checkColumn, checkValue, columns)
    local tdel, code = makeTDelete(key, columns or {}, "checkAndDelete")
    if not tdel then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " Failed to make TDelete instance for HBase checkAndDelete")
        return code, false
    end

    local tocheck = checkValue
    if counter_set[table_name..":"..checkFamily..":"..checkColumn] then
        tocheck = utils.num2hexbin(checkValue, 8)
    end

    local client,code = dequeue_client(self,"checkAndDelete")
    if not client then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " Failed to get a thrift hbase client for checkAndDelete")
        return code, false
    end

    local innercode,stat 
    local ok,ret 
    for retry=1,RETRY_TIMES do
        ok,ret= pcall(client.checkAndDelete, client, table_name, key,
                        checkFamily, checkColumn, tocheck,
                        tdel)

        if ok then -- succeeded, or failed with condition check failure, don't retry
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " HBase Thrift client checkAndDelete succeeded or failed in condition check, retry=", retry)
            break
        end

        if type(ret) == "table" and type(ret.message) == "string" then
            local m = string.match(ret.message, "Table.*was not found")
            if m then
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " HBase Thrift client checkAndDelete failed, table was not found, no need to retry")
                break
            end

            local s,_ = string.find(ret.message, "timeout")
            if s then
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " HBase Thrift client checkAndDelete failed, timeout, no need to retry")
                break
            end
        end

        if retry<RETRY_TIMES then --don't sleep for the last round 
            ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " HBase Thrift client checkAndDelete failed, retry in ", retry*RETRY_INTERVAL, " seconds")
            ngx.sleep(retry * RETRY_INTERVAL)
        end
    end

    if ok then  -- no error is thrown, ret is the return value
        if ret == true then
            innercode = "0000" 
            stat = true 
        else
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " HBase Thrift client checkAndDelete failed, err=condition check failed")
            innercode = "1121" 
            stat = false
        end
    else  -- error is thrown, ret is the error;
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " HBase Thrift client checkAndDelete failed, err=", utils.stringify(ret))
        innercode = "1107" 
        stat = false
    end

    enqueue_client(client, "checkAndDelete")

    return innercode, stat
end

--For the given row, do the put and/or delete operation atomically 
--Params:
--    table_name : the name of the table
--    key        : the key of the given row
--    put        : an array with the family:column:value to put, it must be like this:
--                         {[1]=family1, [2]=column1, [3]=value1, [4]=family2, [5]=column2, [6]=value2, ... }
--    del        : an array containing an optional list of family[:column]; e.g.
--                         {"family1:column1", "family2", "family3:column3", ...}
--                 if del is nil or an empty table, no data is deleted; else, 
--                 the specified family and/or columns are deleted;
--    ttl        : ttl in milli-seconds.
--If you want to put only (no delete), you may pass nil to del or omit it.
--If you want to delete only (no put), you must pass nil or empty table to put.
--Return: innercode, ok
function _M.mutateRow(self, table_name, key, put, del, ttl)
    local tmutate, code= makeRowMutations(table_name, key, put, del, "mutateRow", ttl)
    if not tmutate then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " Failed to make TRowMutations instance for HBase mutateRow")
        return code, false
    end

    local client,code = dequeue_client(self,"mutateRow")
    if not client then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " Failed to get a thrift hbase client for mutateRow")
        return code, false
    end

    local innercode,stat 
    local ok,ret 
    for retry=1,RETRY_TIMES do
        ok,ret = pcall(client.mutateRow, client, table_name, tmutate)
        if ok then
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " HBase Thrift client mutateRow succeeded, retry=", retry)
            break
        end

        if type(ret) == "table" and type(ret.message) == "string" then
            local m = string.match(ret.message, "Table.*was not found")
            if m then
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " HBase Thrift client mutateRow failed, table was not found , no need to retry")
                break
            end
        end

        if retry<RETRY_TIMES then --don't sleep for the last round 
            ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " HBase Thrift client mutateRow failed, retry in ", retry*RETRY_INTERVAL, " seconds")
            ngx.sleep(retry * RETRY_INTERVAL)
        end
    end
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " HBase Thrift client mutateRow failed, err=", utils.stringify(ret))
        innercode = "1108" 
        stat = false
    else
        innercode = "0000" 
        stat = true 
    end

    enqueue_client(client, "mutateRow")

    return innercode,stat 
end

--For the given row, do the put and/or delete operations atomically only if  
--    compareOp(current-value-of-checkFamily:checkColumn, checkValue) = true
--Params:
--    table_name : the name of the table
--    key        : the key of the given row
--    checkFamily: the family to check
--    checkColumn: the column to check
--    compareOp  : the compare op. it should be one of
--                     LESS             = 0
--                     LESS_OR_EQUAL    = 1
--                     EQUAL            = 2
--                     NOT_EQUAL        = 3
--                     GREATER_OR_EQUAL = 4
--                     GREATER          = 5
--                     NO_OP            = 6
--    checkValue : the value to check
--    put        : an array with the family:column:value to put, it must be like this:
--                         {[1]=family1, [2]=column1, [3]=value1, [4]=family2, [5]=column2, [6]=value2, ... }
--    del        : an array containing an optional list of family[:column]; e.g.
--                         {"family1:column1", "family2", "family3:column3", ...}
--                 if del is nil or an empty table, no data is deleted; else, 
--                 the specified family and/or columns are deleted;
--    ttl        : ttl in milli-seconds
--Note:
--    if you want to put only (no delete), you may pass nil to del or omit it.
--    if you want to delete only (no put), you must pass nil or empty table to put.
--Return: innercode, ok
--           innercode : "0000" if ok is true;
--                       "1121" if ok is false and the failure is caused by condition-check-failure;
--                       "xxxx" if ok is false and the failure is caused by other reasons;
--           ok        : true   if the 'MutateRow' operation has been done; 
--                       false  if the 'MutateRow' operation has NOT been done; if this is because of 
--                              condition-check-failure, innercode must be "1121"
function _M.checkAndMutateRow(self, table_name, key, checkFamily, checkColumn, compareOp, checkValue, put, del, ttl)
    local tmutate, code= makeRowMutations(table_name, key, put, del, "checkAndMutateRow", ttl)
    if not tmutate then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " Failed to make TRowMutations instance for HBase checkAndMutateRow")
        return code, false
    end

    local tocheck = checkValue
    if counter_set[table_name..":"..checkFamily..":"..checkColumn] then
        tocheck = utils.num2hexbin(checkValue, 8)
    end

    local client,code = dequeue_client(self,"checkAndMutateRow")
    if not client then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " Failed to get a thrift hbase client for checkAndMutateRow")
        return code, false
    end

    local innercode,stat 
    local ok,ret 
    for retry=1,RETRY_TIMES do
        ok,ret = pcall(client.checkAndMutate, client, table_name, key, checkFamily, checkColumn, compareOp, tocheck, tmutate)
        if ok then -- succeeded, or failed with condition check failure, don't retry
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " HBase Thrift client checkAndMutateRow succeeded or failed in condition check, retry=", retry)
            break
        end

        if type(ret) == "table" and type(ret.message) == "string" then
            local m = string.match(ret.message, "Table.*was not found")
            if m then
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " HBase Thrift client checkAndMutateRow failed, table was not found, no need to retry")
                break
            end

            local s,_ = string.find(ret.message, "timeout")
            if s then
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " HBase Thrift client checkAndMutateRow failed, timeout, no need to retry")
                break
            end
        end

        if retry<RETRY_TIMES then --don't sleep for the last round 
            ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " HBase Thrift client checkAndMutateRow failed, retry in ", retry*RETRY_INTERVAL, " seconds")
            ngx.sleep(retry * RETRY_INTERVAL)
        end
    end

    if ok then  -- no error is thrown, ret is the return value
        if ret == true then
            innercode = "0000" 
            stat = true 
        else
            ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " HBase Thrift client checkAndMutateRow failed, err=condition check failed")
            innercode = "1121" 
            stat = false
        end
    else  -- error is thrown, ret is the error;
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " HBase Thrift client checkAndMutateRow failed, err=", utils.stringify(ret))
        innercode = "1119" 
        stat = false
    end

    enqueue_client(client, "checkAndMutateRow")

    return innercode,stat 
end


--The same as checkAndMutateRow, but return the original row at the same time.
--Params: The same as checkAndMutateRow.
--Note:
--    if you want to put only (no delete), you may pass nil to del or omit it.
--    if you want to delete only (no put), you must pass nil or empty table to put.
--Return: innercode, ok, result
--           innercode : "0000" if ok is true;
--                       "1121" if ok is false and the failure is caused by condition-check-failure;
--                       "xxxx" if ok is false and the failure is caused by other reasons;
--           ok        : true   if the 'MutateRow' operation has been done; 
--                       false  if the 'MutateRow' operation has NOT been done; if this is because of 
--                              condition-check-failure, innercode must be "1121";
--           result    : the original row. it is meaningful ONLY WHEN innercode is "0000" or "1121"; other failures
--                       may be caused by invalid argument, program crash, timeout and the like, thus 'result' is
--                       meanless; When meaningful, the result is a table like this:
--                              {
--                                  [family:column] = value
--                                  [family:column] = value
--                                  ...
--                              }
function _M.checkAndMutateAndGetRow(self, table_name, key, checkFamily, checkColumn, compareOp, checkValue, put, del, ttl)
    local tmutate, code= makeRowMutations(table_name, key, put, del, "checkAndMutateAndGetRow", ttl)
    if not tmutate then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " Failed to make TRowMutations instance for HBase checkAndMutateAndGetRow")
        return code, false
    end

    local tocheck = checkValue
    if counter_set[table_name..":"..checkFamily..":"..checkColumn] then
        tocheck = utils.num2hexbin(checkValue, 8)
    end

    local client,code = dequeue_client(self,"checkAndMutateAndGetRow")
    if not client then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " Failed to get a thrift hbase client for checkAndMutateAndGetRow")
        return code, false
    end

    local innercode,stat,result
    local ok,processedAndResult 
    
    for retry=1,RETRY_TIMES do
        ok,processedAndResult = pcall(client.checkAndMutateAndGetRow, client, table_name, key, checkFamily, checkColumn, compareOp, tocheck, tmutate)

        if ok then -- succeeded, or failed with condition check failure, don't retry
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " HBase Thrift client checkAndMutateAndGetRow succeeded or failed in condition check, retry=", retry)
            break
        end

        if type(ret) == "table" and type(ret.message) == "string" then
            local m = string.match(ret.message, "Table.*was not found")
            if m then
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " HBase Thrift client checkAndMutateAndGetRow failed, table was not found, no need to retry")
                break
            end

            local s,_ = string.find(ret.message, "timeout")
            if s then
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " HBase Thrift client checkAndMutateAndGetRow failed, timeout, no need to retry")
                break
            end
        end

        if retry<RETRY_TIMES then --don't sleep for the last round 
            ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " HBase Thrift client checkAndMutateAndGetRow failed, retry in ", retry*RETRY_INTERVAL, " seconds")
            ngx.sleep(retry * RETRY_INTERVAL)
        end
    end

    if ok then  -- no error is thrown, processedAndResult is the return value
        if processedAndResult.processed == true then
            innercode = "0000" 
            stat = true 
        else
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " HBase Thrift client checkAndMutateAndGetRow failed, err=condition check failed")
            innercode = "1121" 
            stat = false
        end

        result = new_tab(0, 7)
        for i,v in pairs(processedAndResult.result.columnValues) do
            local vv = v.value
            if counter_set[table_name..":"..v.family..":"..v.qualifier] then
                vv = utils.hexbin2num(v.value)
            end
            result[v.family..":"..v.qualifier] = vv
        end
    else  -- error is thrown, ret is the error;
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " HBase Thrift client checkAndMutateAndGetRow failed, err=", utils.stringify(processedAndResult))
        innercode = "1120" 
        stat = false
        result = nil
    end

    enqueue_client(client, "checkAndMutateAndGetRow")

    return innercode,stat,result
end

--Params:
--    table_name: the name of the table
--    startRow  : the start row for the scan, inclusive
--    stopRow   : the stop row for the scan, exclusive
--    columns   : a table containing an optional list of family[:column]; if columns 
--                is nil or an empty table, the entire rows between startRow and 
--                stopRow are returned; else, the specified family and/or columns 
--                of the rows are returned;
--    caching   : total number of rows to scan; for example, you specify 10000 here and 
--                later you call scan() function 10 times with param numRows=1000, thrift
--                server may do scan operation only once fetching 10000 rows from hbase
--                cluster and caching them. And then it services your request from the cache.
--    filter    : custom filters on the resulting rows; 
--    reversed  : scan in reverse order;
--Return: innercode, ok, scannerId 
function _M.openScanner(self, table_name, startRow, stopRow, columns, caching, filter, reversed)
    ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " caching:",caching or "")
    local tscan, code = makeTScan(startRow, stopRow, columns or {}, "openScanner", caching, filter, reversed)
    if not tscan then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " Failed to make TScan instance for HBase openScanner")
        return code, false, nil
    end

    local client,code = dequeue_client(self,"openScanner")
    if not client then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " Failed to get a thrift hbase client for openScanner")
        return code, false, nil
    end

    local innercode,stat,scannerId
    local ok,ret 
    for retry=1,RETRY_TIMES do
        ok,ret = pcall(client.openScanner, client, table_name, tscan)
        if ok then
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " HBase Thrift client openScanner succeeded, retry=", retry)
            break
        end

        if type(ret) == "table" and type(ret.message) == "string" then
            local m = string.match(ret.message, "Table.*was not found")
            if m then
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " HBase Thrift client openScanner failed, table was not found, no need to retry")
                break
            end
        end

        if retry<RETRY_TIMES then --don't sleep for the last round 
            ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " HBase Thrift client openScanner failed, retry in ", retry*RETRY_INTERVAL, " seconds")
            ngx.sleep(retry * RETRY_INTERVAL)
        end
    end
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " HBase Thrift client openScanner failed, err=", utils.stringify(ret))
        innercode = "1111" 
        stat = false
        scannerId = nil
    else
        innercode = "0000" 
        stat = true 
        scannerId = ret
    end

    enqueue_client(client, "openScanner")

    return innercode, stat, scannerId
end


--close the scanner that's opened by openScanner
--Params:
--    scannerId: then scanner ID returned by openScanner function
--Return: innercode, ok
function _M.closeScanner(self, scannerId)
    local client,code = dequeue_client(self,"closeScanner")
    if not client then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " Failed to get a thrift hbase client for closeScanner")
        return code, false
    end

    local innercode,stat
    local ok,ret 
    for retry=1,RETRY_TIMES do
        ok,ret = pcall(client.closeScanner, client, scannerId)
        if ok then
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " HBase Thrift client closeScanner succeeded, retry=", retry)
            break
        end

        if type(ret) == "table" and type(ret.message) == "string" then
            local m = string.match(ret.message, "Table.*was not found")
            if m then
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " HBase Thrift client closeScanner failed, table was not found, no need to retry")
                break
            end
        end

        if retry<RETRY_TIMES then --don't sleep for the last round 
            ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " HBase Thrift client closeScanner failed, retry in ", retry*RETRY_INTERVAL, " seconds")
            ngx.sleep(retry * RETRY_INTERVAL)
        end
    end
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " HBase Thrift client closeScanner failed, err=", utils.stringify(ret))
        innercode = "1112" 
        stat = false
    else
        innercode = "0000" 
        stat = true 
    end

    enqueue_client(client, "closeScanner")

    return innercode, stat
end


--Params:
--    table_name: the name of the table
--    scannerId : then scanner ID returned by openScanner function
--    numRows   : how many rows to return 
--Return: innercode, ok, result, count; where count is the number of rows returned; 
--        the caller can tell from if the scan has finished by count < numRows;
--        result is an empty table in the case of failure, and is an array as shown below in the 
--        case of success:
--        {
--            [1] = 
--            {
--                key = rowKey,
--                values = 
--                {
--                    [family:column] = value,
--                    [family:column] = value,
--                }
--            },
--            [2] = 
--            {
--                key = rowKey,
--                values = 
--                {
--                    [family:column] = value,
--                    [family:column] = value,
--                }
--            },
--            ...
--        }
--
function _M.scan(self, table_name, scannerId, numRows)
    local result = {}
    local count = 0

    local client,code = dequeue_client(self,"scan")
    if not client then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " Failed to get a thrift hbase client for scan")
        return code, false, result, count
    end

    local innercode,stat
    local ok,ret 
    for retry=1,RETRY_TIMES do
        ok,ret = pcall(client.getScannerRows, client, scannerId, numRows)
        if ok then
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " HBase Thrift client scan succeeded, retry=", retry)
            break
        end

        if type(ret) == "table" and type(ret.message) == "string" then
            local m = string.match(ret.message, "Table.*was not found")
            if m then
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " HBase Thrift client scan failed, table was not found, no need to retry")
                break
            end

            if ret.message == "Invalid scanner Id" then
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " HBase Thrift client scan failed, Invalid scanner Id, no need to retry")
                break
            end
        end

        if retry<RETRY_TIMES then --don't sleep for the last round 
            ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " HBase Thrift client scan failed, retry in ", retry*RETRY_INTERVAL, " seconds")
            ngx.sleep(retry * RETRY_INTERVAL)
        end
    end
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " HBase Thrift client scan failed, err=", utils.stringify(ret))
        if ret.message == "Invalid scanner Id" then
            innercode = "1114"
        else
            innercode = "1113" 
        end
        stat = false
    else
        innercode = "0000" 
        stat = true 
        
        count = #ret
        for i,v in ipairs(ret) do
            result[i] = {key = v.row, values = {}}
            for j,k in ipairs(v.columnValues) do
                local vv = k.value
                if counter_set[table_name..":"..k.family..":"..k.qualifier] then
                    vv = utils.hexbin2num(k.value)
                end
                result[i]["values"][k.family..":"..k.qualifier] = vv
            end
        end
    end

    enqueue_client(client, "scan")

    return innercode, stat, result, count
end

--Params:
--    table_name: the name of the table
--    scannerId : then scanner ID returned by openScanner function
--    numRows   : how many rows to return 
--Return: innercode, ok, result, count; where count is the number of rows returned; 
--        the caller can tell from if the scan has finished by count < numRows;
--        result is an empty table in the case of failure, and is an array as shown below in the 
--        case of success:
--        {
--            rowKey1 =
--            {
--                [family:column] = value
--                [family:column] = value
--                ...
--            }
--            rowKey2 =
--            {
--                [family:column] = value 
--                [family:column] = value
--                ...
--            }
--            ...
--        }
--
function _M.scanHash(self, table_name, scannerId, numRows)
    local result = {}
    local count = 0

    local client,code = dequeue_client(self,"scanHash")
    if not client then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " Failed to get a thrift hbase client for scanHash")
        return code, false, result, count
    end

    local innercode,stat
    local ok,ret 
    for retry=1,RETRY_TIMES do
        ok,ret = pcall(client.getScannerRows, client, scannerId, numRows)
        if ok then
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " HBase Thrift client scanHash succeeded, retry=", retry)
            break
        end

        if type(ret) == "table" and type(ret.message) == "string" then
            local m = string.match(ret.message, "Table.*was not found")
            if m then
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " HBase Thrift client scanHash failed, table was not found, no need to retry")
                break
            end
            if ret.message == "Invalid scanner Id" then
                ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " HBase Thrift client scanHash failed, Invalid scanner Id, no need to retry")
                break
            end
        end

        if retry<RETRY_TIMES then --don't sleep for the last round 
            ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " HBase Thrift client scanHash failed, retry in ", retry*RETRY_INTERVAL, " seconds")
            ngx.sleep(retry * RETRY_INTERVAL)
        end
    end
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " HBase Thrift client scanHash failed, err=", utils.stringify(ret))
        if ret.message == "Invalid scanner Id" then
            innercode = "1114"
        else
            innercode = "1113" 
        end
        stat = false
    else
        innercode = "0000" 
        stat = true 
        
        count = #ret
        for i,v in ipairs(ret) do
            result[v.row] = {}
            for j,k in ipairs(v.columnValues) do
                local vv = k.value
                if counter_set[table_name..":"..k.family..":"..k.qualifier] then
                    vv = utils.hexbin2num(k.value)
                end
                result[v.row][k.family..":"..k.qualifier] = vv
            end
        end
    end

    enqueue_client(client, "scanHash")

    return innercode, stat, result, count
end


--a short way of openScanner--scan--closeScanner, all is done in the single step;
--Params:
--    table_name: the name of the table
--    startRow  : the start row for the scan, inclusive
--    stopRow   : the stop row for the scan, exclusive
--    columns   : a table containing an optional list of family[:column]; if columns 
--                is nil or an empty table, the entire rows between startRow and 
--                stopRow are returned; else, the specified family and/or columns 
--                of the rows are returned;
--    numRows   : how many rows to return 
--    caching   : total number of rows to scan; for example, you specify 10000 here and 
--                later you call scan() function 10 times with param numRows=1000, thrift
--                server may do scan operation only once fetching 10000 rows from hbase
--                cluster and caching them. And then it services your request from the cache.
--    filter    : custom filters on the resulting rows; 
--    reversed  : scan in reverse order;
--Return: innercode, ok, result; where result is an empty table in the case of
--        failure, and is a table as shown below in the case of success:
--        {
--            1 = 
--            {
--                key = rowKey1
--                values =
--                {
--                    [family:column] = value
--                    [family:column] = value
--                    ...
--                }
--            }
--            2 = {
--                key = rowKey2
--                values = 
--                {
--                    [family:column] = value
--                    [family:column] = value
--                    ...
--                }
--            }
--            ...
--        }
-- Note: the only difference between this function and quickScanHash is: 
--         the row key order is kept in the return table of this function.
function _M.quickScan(self, table_name, startRow, stopRow, columns, numRows, caching, filter, reversed)
    local result = {}
   
    local tscan, code = makeTScan(startRow, stopRow, columns or {}, "quickScan", caching, filter, reversed)
    if not tscan then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " Failed to make TScan instance for HBase quickScan")
        return code, false, result
    end

    local client,code = dequeue_client(self,"quickScan")
    if not client then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " Failed to get a thrift hbase client for quickScan")
        return code, false, result 
    end

    local innercode,stat
    --Yuanguo: quickScan is actually named getScannerResults
    local ok,ret 
    for retry=1,RETRY_TIMES do
        ok,ret = pcall(client.getScannerResults, client, table_name, tscan, numRows) 
        if ok then
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " HBase Thrift client quickScan succeeded, retry=", retry)
            break
        end

        if type(ret) == "table" and type(ret.message) == "string" then
            local m = string.match(ret.message, "Table.*was not found")
            if m then
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " HBase Thrift client quickScan failed, table was not found, no need to retry")
                break
            end
        end

        if retry<RETRY_TIMES then --don't sleep for the last round 
            ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " HBase Thrift client quickScan failed, retry in ", retry*RETRY_INTERVAL, " seconds")
            ngx.sleep(retry * RETRY_INTERVAL)
        end
    end
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " HBase Thrift client quickScan failed, err=", utils.stringify(ret))
        innercode = "1115" 
        stat = false
    else
        innercode = "0000" 
        stat = true 
        for i,v in ipairs(ret) do
            --result[v.row] = {}
            result[i] = {key = v.row, values = {}}
            for j,k in ipairs(v.columnValues) do
                local vv = k.value
                if counter_set[table_name..":"..k.family..":"..k.qualifier] then
                    vv = utils.hexbin2num(k.value)
                end
                result[i]["values"][k.family..":"..k.qualifier] = vv
            end
        end
    end

    enqueue_client(client, "quickScan")

    return innercode, stat, result
end


--a short way of openScanner--scan--closeScanner, all is done in the single step;
--Params:
--    table_name: the name of the table
--    startRow  : the start row for the scan, inclusive
--    stopRow   : the stop row for the scan, exclusive
--    columns   : a table containing an optional list of family[:column]; if columns 
--                is nil or an empty table, the entire rows between startRow and 
--                stopRow are returned; else, the specified family and/or columns 
--                of the rows are returned;
--    numRows   : how many rows to return 
--    caching   : total number of rows to scan; for example, you specify 10000 here and 
--                later you call scan() function 10 times with param numRows=1000, thrift
--                server may do scan operation only once fetching 10000 rows from hbase
--                cluster and caching them. And then it services your request from the cache.
--    filter    : custom filters on the resulting rows; 
--    reversed  : scan in reverse order;
--Return: innercode, ok, result; where result is an empty table in the case of
--        failure, and is a table as shown below in the case of success:
--        {
--            rowKey1 =
--            {
--                [family:column] = value
--                [family:column] = value
--                ...
--            }
--            rowKey2 =
--            {
--                [family:column] = value 
--                [family:column] = value
--                ...
--            }
--            ...
--        }
--
-- Note: the only difference between this function and quickScan is: 
--         the row key order is kept in the return table of quickScan, but not kept
--         in the return table of this function.
function _M.quickScanHash(self, table_name, startRow, stopRow, columns, numRows, caching, filter, reversed)
    local result = {}
   
    local tscan, code = makeTScan(startRow, stopRow, columns or {}, "quickScanHash", caching, filter, reversed)
    if not tscan then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " Failed to make TScan instance for HBase quickScanHash")
        return code, false, result
    end

    local client,code = dequeue_client(self,"quickScanHash")
    if not client then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " Failed to get a thrift hbase client for quickScanHash")
        return code, false, result 
    end

    local innercode,stat
    --Yuanguo: quickScan is actually named getScannerResults
    local ok,ret 
    for retry=1,RETRY_TIMES do
        ok,ret = pcall(client.getScannerResults, client, table_name, tscan, numRows) 
        if ok then
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " HBase Thrift client quickScanHash succeeded, retry=", retry)
            break
        end

        if type(ret) == "table" and type(ret.message) == "string" then
            local m = string.match(ret.message, "Table.*was not found")
            if m then
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " HBase Thrift client quickScanHash failed, table was not found, no need to retry")
                break
            end
        end

        if retry<RETRY_TIMES then --don't sleep for the last round 
            ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " HBase Thrift client quickScanHash failed, retry in ", retry*RETRY_INTERVAL, " seconds")
            ngx.sleep(retry * RETRY_INTERVAL)
        end
    end
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " HBase Thrift client quickScanHash failed, err=", utils.stringify(ret))
        innercode = "1115" 
        stat = false
    else
        innercode = "0000" 
        stat = true 
        for i,v in ipairs(ret) do
            result[v.row] = {}
            for j,k in ipairs(v.columnValues) do
                local vv = k.value
                if counter_set[table_name..":"..k.family..":"..k.qualifier] then
                    vv = utils.hexbin2num(k.value)
                end
                result[v.row][k.family..":"..k.qualifier] = vv
            end
        end
    end

    enqueue_client(client, "quickScanHash")

    return innercode, stat, result
end

function _M.getLocalRegionLocations(self, table_name, server_name)
    local result = {}
    local count = 0

    local client,code = dequeue_client(self,"getAllRegionLocations")
    if not client then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " Failed to get a thrift hbase client for getAllRegionLocations ")
        return code, false, result, count
    end

    local ok, ret 
    for retry=1,RETRY_TIMES do
        ok, ret = pcall(client.getAllRegionLocations, client, table_name)
        if ok then
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " HBase Thrift client getAllRegionLocations succeeded, retry=", retry)
            break
        end
        if retry<RETRY_TIMES then --don't sleep for the last round 
            ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " HBase Thrift client getAllRegionLocations failed, retry in ", retry*RETRY_INTERVAL, " seconds")
            ngx.sleep(retry * RETRY_INTERVAL)
        end
    end
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " HBase Thrift client getAllRegionLocations failed ", utils.stringify(ret))
        return "1118", false, result, count
    else
        for i,v in ipairs(ret) do
            if v.serverName and server_name == v.serverName.hostName and v.regionInfo and not v.regionInfo.offline and not v.regionInfo.split then
                count = count + 1
                result[count] = {startkey=v.regionInfo.startKey, endkey=v.regionInfo.endKey}
            end
        end
        ngx.log(ngx.INFO, "result=", inspect(result))
    end

    enqueue_client(client, "getAllRegionLocations")

    return "0000", true, result, count
end

return _M
