--By Yuanguo
--07/09/2016
local ok, utils = pcall(require, "common.utils")
if not ok or not utils then
    error("failed to load common.utils:" .. (utils or "nil"))
end

local ok, TType = pcall(require, "thrift.lualib.TType")
if not ok or not TType then
    error("failed to load thrift.lualib.TType:" .. (TType or "nil"))
end

local ok, TMessageType = pcall(require, "thrift.lualib.TMessageType")
if not ok or not TMessageType then
    error("failed to load thrift.lualib.TMessageType:" .. (TMessageType or "nil"))
end

local ok, Client = pcall(require, "thrift.lualib.TClient")
if not ok or not Client then
    error("failed to load thrift.lualib.TClient:" .. (Client or "nil"))
end

local ok, TApplicationException = pcall(require, "thrift.lualib.TApplicationException")
if not ok or not TApplicationException then
    error("failed to load thrift.lualib.TApplicationException:" .. (TApplicationException or "nil"))
end

local ok, Tool = pcall(require, "thrift.lualib.TTool")
if not ok or not Tool then
    error("failed to load thrift.lualib.TTool:" .. (Tool or "nil"))
end

local ttype = Tool.ttype
local terror = Tool.terror
local thrift_print_r = Tool.thrift_print_r

local append_args                      = require "hbase.thrift.client.append_args"
local append_result                    = require "hbase.thrift.client.append_result"
local checkAndDelete_args              = require "hbase.thrift.client.checkAndDelete_args"
local checkAndDelete_result            = require "hbase.thrift.client.checkAndDelete_result"
local checkAndPut_args                 = require "hbase.thrift.client.checkAndPut_args"
local checkAndPut_result               = require "hbase.thrift.client.checkAndPut_result"
local closeScanner_args                = require "hbase.thrift.client.closeScanner_args"
local closeScanner_result              = require "hbase.thrift.client.closeScanner_result"
local deleteMultiple_args              = require "hbase.thrift.client.deleteMultiple_args"
local deleteMultiple_result            = require "hbase.thrift.client.deleteMultiple_result"
local deleteSingle_args                = require "hbase.thrift.client.deleteSingle_args"
local deleteSingle_result              = require "hbase.thrift.client.deleteSingle_result"
local exists_args                      = require "hbase.thrift.client.exists_args"
local exists_result                    = require "hbase.thrift.client.exists_result"
local getAllRegionLocations_args       = require "hbase.thrift.client.getAllRegionLocations_args"
local getAllRegionLocations_result     = require "hbase.thrift.client.getAllRegionLocations_result"
local get_args                         = require "hbase.thrift.client.get_args"
local getMultiple_args                 = require "hbase.thrift.client.getMultiple_args"
local getMultiple_result               = require "hbase.thrift.client.getMultiple_result"
local getRegionLocation_args           = require "hbase.thrift.client.getRegionLocation_args"
local getRegionLocation_result         = require "hbase.thrift.client.getRegionLocation_result"
local get_result                       = require "hbase.thrift.client.get_result"
local getScannerResults_args           = require "hbase.thrift.client.getScannerResults_args"
local getScannerResults_result         = require "hbase.thrift.client.getScannerResults_result"
local getScannerRows_args              = require "hbase.thrift.client.getScannerRows_args"
local getScannerRows_result            = require "hbase.thrift.client.getScannerRows_result"
local increment_args                   = require "hbase.thrift.client.increment_args"
local increment_result                 = require "hbase.thrift.client.increment_result"
local mutateRow_args                   = require "hbase.thrift.client.mutateRow_args"
local mutateRow_result                 = require "hbase.thrift.client.mutateRow_result"
local checkAndMutate_args              = require "hbase.thrift.client.checkAndMutate_args"
local checkAndMutate_result            = require "hbase.thrift.client.checkAndMutate_result"
local checkAndMutateAndGetRow_args     = require "hbase.thrift.client.checkAndMutateAndGetRow_args"
local checkAndMutateAndGetRow_result   = require "hbase.thrift.client.checkAndMutateAndGetRow_result"
local openScanner_args                 = require "hbase.thrift.client.openScanner_args"
local openScanner_result               = require "hbase.thrift.client.openScanner_result"
local put_args                         = require "hbase.thrift.client.put_args"
local putMultiple_args                 = require "hbase.thrift.client.putMultiple_args"
local putMultiple_result               = require "hbase.thrift.client.putMultiple_result"
local put_result                       = require "hbase.thrift.client.put_result"
local TAppend                          = require "hbase.thrift.client.TAppend"
local TAuthorization                   = require "hbase.thrift.client.TAuthorization"
local TCellVisibility                  = require "hbase.thrift.client.TCellVisibility"
local TColumnIncrement                 = require "hbase.thrift.client.TColumnIncrement"
local TColumn                          = require "hbase.thrift.client.TColumn"
local TColumnValue                     = require "hbase.thrift.client.TColumnValue"
local TDelete                          = require "hbase.thrift.client.TDelete"
local TDeleteType                      = require "hbase.thrift.client.TDeleteType"
local TDurability                      = require "hbase.thrift.client.TDurability"
local TGet                             = require "hbase.thrift.client.TGet"
local THRegionInfo                     = require "hbase.thrift.client.THRegionInfo"
local THRegionLocation                 = require "hbase.thrift.client.THRegionLocation"
local TIllegalArgument                 = require "hbase.thrift.client.TIllegalArgument"
local TIncrement                       = require "hbase.thrift.client.TIncrement"
local TIOError                         = require "hbase.thrift.client.TIOError"
local TMutation                        = require "hbase.thrift.client.TMutation"
local TPut                             = require "hbase.thrift.client.TPut"
local TResult                          = require "hbase.thrift.client.TResult"
local TProcessedAndResult              = require "hbase.thrift.client.TProcessedAndResult"
local TRowMutations                    = require "hbase.thrift.client.TRowMutations"
local TScan                            = require "hbase.thrift.client.TScan"
local TServerName                      = require "hbase.thrift.client.TServerName"
local TTimeRange                       = require "hbase.thrift.client.TTimeRange"


local _M = Client:new({
    __type = 'THBaseServiceClient'
}) 

--Params in init_obj:
--    protocol or iprot
--    _seqid
function _M.new(self, init_obj)
    if not init_obj.protocol and not init_obj.iprot then
        error("At least one of 'iprot' and 'protocol' must be present for new() of ".._M.__type)
    end
    return Client.new(self, init_obj)
end

function _M.exists(self, table, tget)
    self:send_exists(table, tget)
    return self:recv_exists(table, tget)
end

function _M.send_exists(self, table, tget)
    self.oprot:writeMessageBegin('exists', TMessageType.CALL, self._seqid)
    local args = exists_args:new{}
    args.table = table
    args.tget = tget
    args:write(self.oprot)
    self.oprot:writeMessageEnd()
    self.oprot.transport:flush()
end

function _M.recv_exists(self, table, tget)
    local fname, mtype, rseqid = self.iprot:readMessageBegin()
    if mtype == TMessageType.EXCEPTION then
        local x = TApplicationException:new{}
        x:read(self.iprot)
        self.iprot:readMessageEnd()
        error(x)
    end
    local result = exists_result:new{}
    result:read(self.iprot)
    self.iprot:readMessageEnd()
    if result.success then
        return result.success
    elseif result.io then
        error(result.io)
    end
    error(TApplicationException:new{errorCode = TApplicationException.MISSING_RESULT})
end

function _M.get(self, table, tget)
    self:send_get(table, tget)
    return self:recv_get(table, tget)
end

function _M.send_get(self, table, tget)
    self.oprot:writeMessageBegin('get', TMessageType.CALL, self._seqid)
    local args = get_args:new{}
    args.table = table
    args.tget = tget
    args:write(self.oprot)
    self.oprot:writeMessageEnd()
    self.oprot.transport:flush()
end

function _M.recv_get(self, table, tget)
    local fname, mtype, rseqid = self.iprot:readMessageBegin()
    if mtype == TMessageType.EXCEPTION then
        local x = TApplicationException:new{}
        x:read(self.iprot)
        self.iprot:readMessageEnd()
        error(x)
    end
    local result = get_result:new{}
    result:read(self.iprot)
    self.iprot:readMessageEnd()
    if result.success then
        return result.success
    elseif result.io then
        error(result.io)
    end
    error(TApplicationException:new{errorCode = TApplicationException.MISSING_RESULT})
end

function _M.getMultiple(self, table, tgets)
    self:send_getMultiple(table, tgets)
    return self:recv_getMultiple(table, tgets)
end

function _M.send_getMultiple(self, table, tgets)
    self.oprot:writeMessageBegin('getMultiple', TMessageType.CALL, self._seqid)
    local args = getMultiple_args:new{}
    args.table = table
    args.tgets = tgets
    args:write(self.oprot)
    self.oprot:writeMessageEnd()
    self.oprot.transport:flush()
end

function _M.recv_getMultiple(self, table, tgets)
    local fname, mtype, rseqid = self.iprot:readMessageBegin()
    if mtype == TMessageType.EXCEPTION then
        local x = TApplicationException:new{}
        x:read(self.iprot)
        self.iprot:readMessageEnd()
        error(x)
    end
    local result = getMultiple_result:new{}
    result:read(self.iprot)
    self.iprot:readMessageEnd()
    if result.success then
        return result.success
    elseif result.io then
        error(result.io)
    end
    error(TApplicationException:new{errorCode = TApplicationException.MISSING_RESULT})
end

function _M.put(self, table, tput)
    self:send_put(table, tput)
    self:recv_put(table, tput)
end

function _M.send_put(self, table, tput)
    self.oprot:writeMessageBegin('put', TMessageType.CALL, self._seqid)
    local args = put_args:new{}
    args.table = table
    args.tput = tput
    args:write(self.oprot)
    self.oprot:writeMessageEnd()
    self.oprot.transport:flush()
end

function _M.recv_put(self, table, tput)
    local fname, mtype, rseqid = self.iprot:readMessageBegin()
    if mtype == TMessageType.EXCEPTION then
        local x = TApplicationException:new{}
        x:read(self.iprot)
        self.iprot:readMessageEnd()
        error(x)
    end
    local result = put_result:new{}
    result:read(self.iprot)
    self.iprot:readMessageEnd()

    if result.io then
        error(result.io)
    end
end

function _M.checkAndPut(self, table, row, family, qualifier, value, tput)
    self:send_checkAndPut(table, row, family, qualifier, value, tput)
    return self:recv_checkAndPut(table, row, family, qualifier, value, tput)
end

function _M.send_checkAndPut(self, table, row, family, qualifier, value, tput)
    self.oprot:writeMessageBegin('checkAndPut', TMessageType.CALL, self._seqid)
    local args = checkAndPut_args:new{}
    args.table = table
    args.row = row
    args.family = family
    args.qualifier = qualifier
    args.value = value
    args.tput = tput
    args:write(self.oprot)
    self.oprot:writeMessageEnd()
    self.oprot.transport:flush()
end

function _M.recv_checkAndPut(self, table, row, family, qualifier, value, tput)
    local fname, mtype, rseqid = self.iprot:readMessageBegin()
    if mtype == TMessageType.EXCEPTION then
        local x = TApplicationException:new{}
        x:read(self.iprot)
        self.iprot:readMessageEnd()
        error(x)
    end
    local result = checkAndPut_result:new{}
    result:read(self.iprot)
    self.iprot:readMessageEnd()
    if true == result.success then
        return true 
    elseif false == result.success then
        return false 
    elseif result.io then
        error(result.io)
    end
    error(TApplicationException:new{errorCode = TApplicationException.MISSING_RESULT})
end

function _M.putMultiple(self, table, tputs)
    self:send_putMultiple(table, tputs)
    self:recv_putMultiple(table, tputs)
end

function _M.send_putMultiple(self, table, tputs)
    self.oprot:writeMessageBegin('putMultiple', TMessageType.CALL, self._seqid)
    local args = putMultiple_args:new{}
    args.table = table
    args.tputs = tputs
    args:write(self.oprot)
    self.oprot:writeMessageEnd()
    self.oprot.transport:flush()
end

function _M.recv_putMultiple(self, table, tputs)
    local fname, mtype, rseqid = self.iprot:readMessageBegin()
    if mtype == TMessageType.EXCEPTION then
        local x = TApplicationException:new{}
        x:read(self.iprot)
        self.iprot:readMessageEnd()
        error(x)
    end
    local result = putMultiple_result:new{}
    result:read(self.iprot)
    self.iprot:readMessageEnd()
end

function _M.deleteSingle(self, table, tdelete)
    self:send_deleteSingle(table, tdelete)
    self:recv_deleteSingle(table, tdelete)
end

function _M.send_deleteSingle(self, table, tdelete)
    self.oprot:writeMessageBegin('deleteSingle', TMessageType.CALL, self._seqid)
    local args = deleteSingle_args:new{}
    args.table = table
    args.tdelete = tdelete
    args:write(self.oprot)
    self.oprot:writeMessageEnd()
    self.oprot.transport:flush()
end

function _M.recv_deleteSingle(self, table, tdelete)
    local fname, mtype, rseqid = self.iprot:readMessageBegin()
    if mtype == TMessageType.EXCEPTION then
        local x = TApplicationException:new{}
        x:read(self.iprot)
        self.iprot:readMessageEnd()
        error(x)
    end
    local result = deleteSingle_result:new{}
    result:read(self.iprot)
    self.iprot:readMessageEnd()
end

function _M.deleteMultiple(self, table, tdeletes)
    self:send_deleteMultiple(table, tdeletes)
    return self:recv_deleteMultiple(table, tdeletes)
end

function _M.send_deleteMultiple(self, table, tdeletes)
    self.oprot:writeMessageBegin('deleteMultiple', TMessageType.CALL, self._seqid)
    local args = deleteMultiple_args:new{}
    args.table = table
    args.tdeletes = tdeletes
    args:write(self.oprot)
    self.oprot:writeMessageEnd()
    self.oprot.transport:flush()
end

function _M.recv_deleteMultiple(self, table, tdeletes)
    local fname, mtype, rseqid = self.iprot:readMessageBegin()
    if mtype == TMessageType.EXCEPTION then
        local x = TApplicationException:new{}
        x:read(self.iprot)
        self.iprot:readMessageEnd()
        error(x)
    end
    local result = deleteMultiple_result:new{}
    result:read(self.iprot)
    self.iprot:readMessageEnd()
    if result.success then
        return result.success
    elseif result.io then
        error(result.io)
    end
    error(TApplicationException:new{errorCode = TApplicationException.MISSING_RESULT})
end

function _M.checkAndDelete(self, table, row, family, qualifier, value, tdelete)
    self:send_checkAndDelete(table, row, family, qualifier, value, tdelete)
    return self:recv_checkAndDelete(table, row, family, qualifier, value, tdelete)
end

function _M.send_checkAndDelete(self, table, row, family, qualifier, value, tdelete)
    self.oprot:writeMessageBegin('checkAndDelete', TMessageType.CALL, self._seqid)
    local args = checkAndDelete_args:new{}
    args.table = table
    args.row = row
    args.family = family
    args.qualifier = qualifier
    args.value = value
    args.tdelete = tdelete
    args:write(self.oprot)
    self.oprot:writeMessageEnd()
    self.oprot.transport:flush()
end

function _M.recv_checkAndDelete(self, table, row, family, qualifier, value, tdelete)
    local fname, mtype, rseqid = self.iprot:readMessageBegin()
    if mtype == TMessageType.EXCEPTION then
        local x = TApplicationException:new{}
        x:read(self.iprot)
        self.iprot:readMessageEnd()
        error(x)
    end
    local result = checkAndDelete_result:new{}
    result:read(self.iprot)
    self.iprot:readMessageEnd()
    if true == result.success then
        return true 
    elseif false == result.success then
        return false 
    elseif result.io then
        error(result.io)
    end
    error(TApplicationException:new{errorCode = TApplicationException.MISSING_RESULT})
end

function _M.increment(self, table, tincrement)
    self:send_increment(table, tincrement)
    return self:recv_increment(table, tincrement)
end

function _M.send_increment(self, table, tincrement)
    self.oprot:writeMessageBegin('increment', TMessageType.CALL, self._seqid)
    local args = increment_args:new{}
    args.table = table
    args.tincrement = tincrement
    args:write(self.oprot)
    self.oprot:writeMessageEnd()
    self.oprot.transport:flush()
end

function _M.recv_increment(self, table, tincrement)
    local fname, mtype, rseqid = self.iprot:readMessageBegin()
    if mtype == TMessageType.EXCEPTION then
        local x = TApplicationException:new{}
        x:read(self.iprot)
        self.iprot:readMessageEnd()
        error(x)
    end
    local result = increment_result:new{}
    result:read(self.iprot)
    self.iprot:readMessageEnd()
    if result.success then
        return result.success
    elseif result.io then
        error(result.io)
    end
    error(TApplicationException:new{errorCode = TApplicationException.MISSING_RESULT})
end

function _M.append(self, table, tappend)
    self:send_append(table, tappend)
    return self:recv_append(table, tappend)
end

function _M.send_append(self, table, tappend)
    self.oprot:writeMessageBegin('append', TMessageType.CALL, self._seqid)
    local args = append_args:new{}
    args.table = table
    args.tappend = tappend
    args:write(self.oprot)
    self.oprot:writeMessageEnd()
    self.oprot.transport:flush()
end

function _M.recv_append(self, table, tappend)
    local fname, mtype, rseqid = self.iprot:readMessageBegin()
    if mtype == TMessageType.EXCEPTION then
        local x = TApplicationException:new{}
        x:read(self.iprot)
        self.iprot:readMessageEnd()
        error(x)
    end
    local result = append_result:new{}
    result:read(self.iprot)
    self.iprot:readMessageEnd()
    if result.success then
        return result.success
    elseif result.io then
        error(result.io)
    end
    error(TApplicationException:new{errorCode = TApplicationException.MISSING_RESULT})
end

function _M.openScanner(self, table, tscan)
    self:send_openScanner(table, tscan)
    return self:recv_openScanner(table, tscan)
end

function _M.send_openScanner(self, table, tscan)
    self.oprot:writeMessageBegin('openScanner', TMessageType.CALL, self._seqid)
    local args = openScanner_args:new{}
    args.table = table
    args.tscan = tscan
    args:write(self.oprot)
    self.oprot:writeMessageEnd()
    self.oprot.transport:flush()
end

function _M.recv_openScanner(self, table, tscan)
    local fname, mtype, rseqid = self.iprot:readMessageBegin()
    if mtype == TMessageType.EXCEPTION then
        local x = TApplicationException:new{}
        x:read(self.iprot)
        self.iprot:readMessageEnd()
        error(x)
    end
    local result = openScanner_result:new{}
    result:read(self.iprot)
    self.iprot:readMessageEnd()
    if result.success then
        return result.success
    elseif result.io then
        error(result.io)
    end
    error(TApplicationException:new{errorCode = TApplicationException.MISSING_RESULT})
end

function _M.getScannerRows(self, scannerId, numRows)
    self:send_getScannerRows(scannerId, numRows)
    return self:recv_getScannerRows(scannerId, numRows)
end

function _M.send_getScannerRows(self, scannerId, numRows)
    self.oprot:writeMessageBegin('getScannerRows', TMessageType.CALL, self._seqid)
    local args = getScannerRows_args:new{}
    args.scannerId = scannerId
    args.numRows = numRows
    args:write(self.oprot)
    self.oprot:writeMessageEnd()
    self.oprot.transport:flush()
end

function _M.recv_getScannerRows(self, scannerId, numRows)
    local fname, mtype, rseqid = self.iprot:readMessageBegin()
    if mtype == TMessageType.EXCEPTION then
        local x = TApplicationException:new{}
        x:read(self.iprot)
        self.iprot:readMessageEnd()
        error(x)
    end
    local result = getScannerRows_result:new{}
    result:read(self.iprot)
    self.iprot:readMessageEnd()
    if result.success then
        return result.success
    elseif result.io then
        error(result.io)
    elseif result.ia then
        error(result.ia)
    end
    error(TApplicationException:new{errorCode = TApplicationException.MISSING_RESULT})
end

function _M.closeScanner(self, scannerId)
    self:send_closeScanner(scannerId)
    self:recv_closeScanner(scannerId)
end

function _M.send_closeScanner(self, scannerId)
    self.oprot:writeMessageBegin('closeScanner', TMessageType.CALL, self._seqid)
    local args = closeScanner_args:new{}
    args.scannerId = scannerId
    args:write(self.oprot)
    self.oprot:writeMessageEnd()
    self.oprot.transport:flush()
end

function _M.recv_closeScanner(self, scannerId)
    local fname, mtype, rseqid = self.iprot:readMessageBegin()
    if mtype == TMessageType.EXCEPTION then
        local x = TApplicationException:new{}
        x:read(self.iprot)
        self.iprot:readMessageEnd()
        error(x)
    end
    local result = closeScanner_result:new{}
    result:read(self.iprot)
    self.iprot:readMessageEnd()
end

function _M.mutateRow(self, table, trowMutations)
    self:send_mutateRow(table, trowMutations)
    self:recv_mutateRow(table, trowMutations)
end

function _M.send_mutateRow(self, table, trowMutations)
    self.oprot:writeMessageBegin('mutateRow', TMessageType.CALL, self._seqid)
    local args = mutateRow_args:new{}
    args.table = table
    args.trowMutations = trowMutations
    args:write(self.oprot)
    self.oprot:writeMessageEnd()
    self.oprot.transport:flush()
end

function _M.recv_mutateRow(self, table, trowMutations)
    local fname, mtype, rseqid = self.iprot:readMessageBegin()
    if mtype == TMessageType.EXCEPTION then
        local x = TApplicationException:new{}
        x:read(self.iprot)
        self.iprot:readMessageEnd()
        error(x)
    end
    local result = mutateRow_result:new{}
    result:read(self.iprot)
    self.iprot:readMessageEnd()

    if result.io then
        error(result.io)
    end
end

function _M.checkAndMutate(self, table, row, family, qualifier, compareOp, value, rowMutations)
  self:send_checkAndMutate(table, row, family, qualifier, compareOp, value, rowMutations)
  return self:recv_checkAndMutate(table, row, family, qualifier, compareOp, value, rowMutations)
end

function _M.send_checkAndMutate(self, table, row, family, qualifier, compareOp, value, rowMutations)
  self.oprot:writeMessageBegin('checkAndMutate', TMessageType.CALL, self._seqid)
  local args = checkAndMutate_args:new{}
  args.table = table
  args.row = row
  args.family = family
  args.qualifier = qualifier
  args.compareOp = compareOp
  args.value = value
  args.rowMutations = rowMutations
  args:write(self.oprot)
  self.oprot:writeMessageEnd()
  self.oprot.transport:flush()
end

function _M.recv_checkAndMutate(self, table, row, family, qualifier, compareOp, value, rowMutations)
  local fname, mtype, rseqid = self.iprot:readMessageBegin()
  if mtype == TMessageType.EXCEPTION then
    local x = TApplicationException:new{}
    x:read(self.iprot)
    self.iprot:readMessageEnd()
    error(x)
  end
  local result = checkAndMutate_result:new{}
  result:read(self.iprot)
  self.iprot:readMessageEnd()
  if true == result.success then
    return true 
  elseif false == result.success then
    return false 
  elseif result.io then
    error(result.io)
  end
  error(TApplicationException:new{errorCode = TApplicationException.MISSING_RESULT})
end

function _M.checkAndMutateAndGetRow(self, table, row, family, qualifier, compareOp, value, rowMutations)
  self:send_checkAndMutateAndGetRow(table, row, family, qualifier, compareOp, value, rowMutations)
  return self:recv_checkAndMutateAndGetRow(table, row, family, qualifier, compareOp, value, rowMutations)
end

function _M.send_checkAndMutateAndGetRow(self, table, row, family, qualifier, compareOp, value, rowMutations)
  self.oprot:writeMessageBegin('checkAndMutateAndGetRow', TMessageType.CALL, self._seqid)
  local args = checkAndMutateAndGetRow_args:new{}
  args.table = table
  args.row = row
  args.family = family
  args.qualifier = qualifier
  args.compareOp = compareOp
  args.value = value
  args.rowMutations = rowMutations
  args:write(self.oprot)
  self.oprot:writeMessageEnd()
  self.oprot.transport:flush()
end

function _M.recv_checkAndMutateAndGetRow(self, table, row, family, qualifier, compareOp, value, rowMutations)
  local fname, mtype, rseqid = self.iprot:readMessageBegin()
  if mtype == TMessageType.EXCEPTION then
    local x = TApplicationException:new{}
    x:read(self.iprot)
    self.iprot:readMessageEnd()
    error(x)
  end
  local result = checkAndMutateAndGetRow_result:new{}
  result:read(self.iprot)
  self.iprot:readMessageEnd()

  if result.processedAndResult then
      return result.processedAndResult
  elseif result.io then
    error(result.io)
  end

  error(TApplicationException:new{errorCode = TApplicationException.MISSING_RESULT})
end


function _M.getScannerResults(self, table, tscan, numRows)
    self:send_getScannerResults(table, tscan, numRows)
    return self:recv_getScannerResults(table, tscan, numRows)
end

function _M.send_getScannerResults(self, table, tscan, numRows)
    self.oprot:writeMessageBegin('getScannerResults', TMessageType.CALL, self._seqid)
    local args = getScannerResults_args:new{}
    args.table = table
    args.tscan = tscan
    args.numRows = numRows
    args:write(self.oprot)
    self.oprot:writeMessageEnd()
    self.oprot.transport:flush()
end

function _M.recv_getScannerResults(self, table, tscan, numRows)
    local fname, mtype, rseqid = self.iprot:readMessageBegin()
    if mtype == TMessageType.EXCEPTION then
        local x = TApplicationException:new{}
        x:read(self.iprot)
        self.iprot:readMessageEnd()
        error(x)
    end
    local result = getScannerResults_result:new{}
    result:read(self.iprot)
    self.iprot:readMessageEnd()
    if result.success then
        return result.success
    elseif result.io then
        error(result.io)
    end
    error(TApplicationException:new{errorCode = TApplicationException.MISSING_RESULT})
end

function _M.getRegionLocation(self, table, row, reload)
    self:send_getRegionLocation(table, row, reload)
    return self:recv_getRegionLocation(table, row, reload)
end

function _M.send_getRegionLocation(self, table, row, reload)
    self.oprot:writeMessageBegin('getRegionLocation', TMessageType.CALL, self._seqid)
    local args = getRegionLocation_args:new{}
    args.table = table
    args.row = row
    args.reload = reload
    args:write(self.oprot)
    self.oprot:writeMessageEnd()
    self.oprot.transport:flush()
end

function _M.recv_getRegionLocation(self, table, row, reload)
    local fname, mtype, rseqid = self.iprot:readMessageBegin()
    if mtype == TMessageType.EXCEPTION then
        local x = TApplicationException:new{}
        x:read(self.iprot)
        self.iprot:readMessageEnd()
        error(x)
    end
    local result = getRegionLocation_result:new{}
    result:read(self.iprot)
    self.iprot:readMessageEnd()
    if result.success then
        return result.success
    elseif result.io then
        error(result.io)
    end
    error(TApplicationException:new{errorCode = TApplicationException.MISSING_RESULT})
end

function _M.getAllRegionLocations(self, table)
    self:send_getAllRegionLocations(table)
    return self:recv_getAllRegionLocations(table)
end

function _M.send_getAllRegionLocations(self, table)
    self.oprot:writeMessageBegin('getAllRegionLocations', TMessageType.CALL, self._seqid)
    local args = getAllRegionLocations_args:new{}
    args.table = table
    args:write(self.oprot)
    self.oprot:writeMessageEnd()
    self.oprot.transport:flush()
end

function _M.recv_getAllRegionLocations(self, table)
    local fname, mtype, rseqid = self.iprot:readMessageBegin()
    if mtype == TMessageType.EXCEPTION then
        local x = TApplicationException:new{}
        x:read(self.iprot)
        self.iprot:readMessageEnd()
        error(x)
    end
    local result = getAllRegionLocations_result:new{}
    result:read(self.iprot)
    self.iprot:readMessageEnd()
    if result.success then
        return result.success
    elseif result.io then
        error(result.io)
    end
    error(TApplicationException:new{errorCode = TApplicationException.MISSING_RESULT})
end

return _M
