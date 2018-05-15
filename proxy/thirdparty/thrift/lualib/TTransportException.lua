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
    UNKNOWN              = 0,
    NOT_OPEN             = 1,
    ALREADY_OPEN         = 2,
    TIMED_OUT            = 3,
    END_OF_FILE          = 4,
    INVALID_FRAME_SIZE   = 5,
    INVALID_TRANSFORM    = 6,
    INVALID_CLIENT_TYPE  = 7,
    SOCKET_RECV_FAIL     = 8,
    SOCKET_SEND_FAIL     = 9,
    __type = 'TTransportException'
})

function _M.new(self, init_obj)
    if type(init_obj) ~= 'table' then
        error("Intial object must be a table for new() of ".._M.__type)
    end
    if nil ~= init_obj.errorCode and (init_obj.errorCode < 0 or init_obj.errorCode > 9) then
        error("Invalid range of 'errorCode' for new() of ".._M.__type)
    end
    return Exception.new(self, init_obj)
end

function _M.__errorCodeToString(self)
    local str = nil
    if self.errorCode == self.NOT_OPEN then
        str = 'Transport not open'
    elseif self.errorCode == self.ALREADY_OPEN then
        str = 'Transport already open'
    elseif self.errorCode == self.TIMED_OUT then
        str = 'Transport timed out'
    elseif self.errorCode == self.END_OF_FILE then
        str = 'End of file'
    elseif self.errorCode == self.INVALID_FRAME_SIZE then
        str = 'Invalid frame size'
    elseif self.errorCode == self.INVALID_TRANSFORM then
        str = 'Invalid transform'
    elseif self.errorCode == self.INVALID_CLIENT_TYPE then
        str = 'Invalid client type'
    elseif self.errorCode == self.SOCKET_RECV_FAIL then
        str = 'Socket receive failure'
    elseif self.errorCode == self.SOCKET_SEND_FAIL then
        str = 'Socket send failure'
    else
        str = 'Unknown'
    end

    if(self.descrip) then
        str = str .. '('..self.descrip..')'
    end

    return str
end

return _M
