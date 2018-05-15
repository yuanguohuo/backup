--By Yuanguo
--06/09/2016

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
    UNKNOWN                 = 0,
    UNKNOWN_METHOD          = 1,
    INVALID_MESSAGE_TYPE    = 2,
    WRONG_METHOD_NAME       = 3,
    BAD_SEQUENCE_ID         = 4,
    MISSING_RESULT          = 5,
    INTERNAL_ERROR          = 6,
    PROTOCOL_ERROR          = 7,
    INVALID_TRANSFORM       = 8,
    INVALID_PROTOCOL        = 9,
    UNSUPPORTED_CLIENT_TYPE = 10,
    __type = 'TApplicationException'
})

function _M.new(self, init_obj)
    if type(init_obj) ~= 'table' then
        error("Intial object must be a table for new() of ".._M.__type)
    end

    if nil ~= init_obj.errorCode and (init_obj.errorCode < 0 or init_obj.errorCode > 10) then
        error("Invalid range of 'errorCode' for new() of ".._M.__type)
    end

    return Exception.new(self, init_obj)
end

function _M.__errorCodeToString(self)
    if self.errorCode == self.UNKNOWN_METHOD then
        return 'Unknown method'
    elseif self.errorCode == self.INVALID_MESSAGE_TYPE then
        return 'Invalid message type'
    elseif self.errorCode == self.WRONG_METHOD_NAME then
        return 'Wrong method name'
    elseif self.errorCode == self.BAD_SEQUENCE_ID then
        return 'Bad sequence ID'
    elseif self.errorCode == self.MISSING_RESULT then
        return 'Missing result'
    elseif self.errorCode == self.INTERNAL_ERROR then
        return 'Internal error'
    elseif self.errorCode == self.PROTOCOL_ERROR then
        return 'Protocol error'
    elseif self.errorCode == self.INVALID_TRANSFORM then
        return 'Invalid transform'
    elseif self.errorCode == self.INVALID_PROTOCOL then
        return 'Invalid protocol'
    elseif self.errorCode == self.UNSUPPORTED_CLIENT_TYPE then
        return 'Unsupported client type'
    else
        return 'Default (unknown)'
    end
end

return _M
