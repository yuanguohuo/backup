--By Yuanguo
--07/09/2016

local ok, Exception = pcall(require, "thrift.lualib.TException")
if not ok or not Exception then
    error("failed to load thrift.lualib.TException:" .. (Exception or "nil"))
end

local ok, Tool = pcall(require, "thrift.lualib.TTool")
if not ok or not Tool then
    error("failed to load thrift.lualib.TTool:" .. (Tool or "nil"))
end

local ttype = Tool.ttype
local terror = Tool.terror
local thrift_print_r = Tool.thrift_print_r

local _M = Exception:new({
  UNKNOWN          = 0,
  INVALID_DATA     = 1,
  NEGATIVE_SIZE    = 2,
  SIZE_LIMIT       = 3,
  BAD_VERSION      = 4,
  INVALID_PROTOCOL = 5,
  DEPTH_LIMIT      = 6,
  __type = 'TProtocolException'
})

function _M.new(self, init_obj)
    if type(init_obj) ~= 'table' then
        error("Intial object must be a table for new() of ".._M.__type)
    end
    if nil ~= init_obj.errorCode and (init_obj.errorCode < 0 or init_obj.errorCode > 6) then
        error("Invalid range of 'errorCode' for new() of ".._M.__type)
    end
    return Exception.new(self, init_obj)
end

function _M.__errorCodeToString(self)
    if self.errorCode == self.INVALID_DATA then
        return 'Invalid data'
    elseif self.errorCode == self.NEGATIVE_SIZE then
        return 'Negative size'
    elseif self.errorCode == self.SIZE_LIMIT then
        return 'Size limit'
    elseif self.errorCode == self.BAD_VERSION then
        return 'Bad version'
    elseif self.errorCode == self.INVALID_PROTOCOL then
        return 'Invalid protocol'
    elseif self.errorCode == self.DEPTH_LIMIT then
        return 'Exceeded size limit'
    else
        return 'Default (unknown)'
    end
end

return _M
