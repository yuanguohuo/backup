-- By Yuanguo, 31/08/2016

local ok, new_tab = pcall(require, "table.new")
if not ok or type(new_tab) ~= "function" then
    new_tab = function (narr, nrec) return {} end
end

local utils = require "common.utils"

local _M = new_tab(0, 11)
_M._VERSION = '0.1'
_M._THRIFT_VERSION = '0.9.2'

_M.MIN_CHAR = string.char(0)
_M.MAX_CHAR = string.char(255)

local THE_LOCAL_VAR = "TheLocalVar"

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

function _M.new(self, server, port, protocol, transport)
    return setmetatable(
               {server = server, port = port, protocol = protocol, transport = transport, storeName = "HBase"},
               {__index = self}
           )
end

function _M.get(self, table_name, key, columns)
    local dumb_result = {}
    for _,col in pairs(columns) do
        local x,y = string.find(col, "stats:")
        if x then
            dumb_result[col] = 10000000
        else
            dumb_result[col] = "HBase Value " .. col
        end
    end
    return "0000", true, dumb_result
end

function _M.increment(self, table_name, key, colValues)
    print(self.storeName.." increment:", utils.stringify(colValues))

    local current = new_tab(0,6)
    for i=1,#colValues-2,3 do
        local fam,col,val = colValues[i],colValues[i+1],colValues[i+2]
        current[fam..":"..col] = val + 300000000
    end
    print("HBase current:", utils.stringify(current))
    return "0000", true, current 
end

function _M.put(self, table_name, key, colValues)
    print("Put into "..self.storeName..": ", utils.stringify(colValues))
    return "0000", true
end

function _M.checkAndPut(self, table_name, key, checkFamily, checkColumn, checkValue, colValues)
    print("checkAndPut into "..self.storeName..": ", utils.stringify(colValues))
    return "0000", true
end

function _M.delete(self, table_name, key, columns)
    if nil == columns or nil == next(columns) then
        print("Delete entire row from "..self.storeName.." row=" .. table_name.."_"..key)
    else
        print("Delete columns from "..self.storeName.." row=" .. table_name.."_"..key .. " columns=" .. utils.stringify(columns))
    end
    return "0000", true
end

function _M.checkAndDelete(self, table_name, key, checkFamily, checkColumn, checkValue, columns)
    print("checkAndDelete from "..self.storeName..": ", utils.stringify(columns))
    return "0000", true
end

function _M.mutateRow(self, table_name, key, put, del)
end


function _M.checkAndMutateAndGetRow(self, table_name, key, checkFamily, checkColumn, compareOp, checkValue, put, del, ttl)
    print("Enter HBase checkAndMutateAndGetRow")
    print("table_name=", table_name)
    print("key=", key)
    print("checkFamily=", checkFamily)
    print("checkColumn=", checkColumn)
    print("compareOp=", compareOp)
    print("checkValue=", checkValue)
    print("put=", utils.stringify(put) )
    print("del=", utils.stringify(del) )
    print("ttl=", ttl)

    local origin = 
    {
        ["info:info1"] = "aaaa-info:info1-origin",
        ["info:info2"] = "aaaa-info:info2-origin",
        ["zzzz:zzzz1"] = "aaaa-zzzz:zzzz1-origin",
        ["stats:stats1"] = "aaaa-stats:stats1-origin",
        ["stats:stats3"] = "aaaa-stats:stats3-origin",
    }


    if compareOp == _M.CompareOp.GREATER then
        local orig_value = origin[checkFamily..":"..checkColumn]
        if nil == orig_value  or checkValue > orig_value then
            print("condition met: ", checkValue, ">", orig_value)
            print("OriginRow: ", utils.stringify(origin))
            return "0000", true, origin
        else
            print("condition NOT met: ", checkValue, ">", orig_value)
            print("OriginRow: ", utils.stringify(origin))
            return "1121", false, origin
        end
    end

    print("HBase checkAndMutateAndGetRow failed")
    return "1120", false, nil
end


function _M.openScanner(self, table_name, startRow, stopRow, columns, caching , filter)
end


function _M.closeScanner(self, scannerId)
end

function _M.scan(self, table_name, scannerId, numRows)
end

function _M.quickScan(self, table_name, startRow, stopRow, columns, numRows, caching, filter)
    local params =
    {
        self = self,
        table_name = table_name,
        startRow = startRow,
        stopRow = stopRow,
        columns = columns,
        numRows = numRows,
        caching = caching,
        filter = filter,
    }
    print("params=", utils.stringify(params))

    print("quickScan", self.server, self.port, self.protocol)
    return table_name, startRow, stopRow
end

function _M.quickScanHash(self, table_name, startRow, stopRow, columns, numRows, caching, filter)
end

function _M.getLocalRegionLocations(self, table_name, server_name)
end

return _M
