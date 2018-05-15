--By Yuanguo
--07/09/2016

local ok, TranBase = pcall(require, "thrift.lualib.TTransportBase")
if not ok or not TranBase then
    error("failed to load thrift.lualib.TTransportBase:" .. (TranBase or "nil"))
end

local ok, Tool = pcall(require, "thrift.lualib.TTool")
if not ok or not Tool then
    error("failed to load thrift.lualib.TTool:" .. (Tool or "nil"))
end

local ttype = Tool.ttype
local terror = Tool.terror
local thrift_print_r = Tool.thrift_print_r

local _M = TranBase:new({
    __type = 'TBufferedTransport',
    rBufSize = 2048,
    wBufSize = 2048
})

function _M.new(self, init_obj)
    if type(init_obj) ~= 'table' then
        error("Intial object must be a table for new() of ".._M.__type)
    end

    if init_obj.rBufSize then
        init_obj.rBufSize = tonumber(init_obj.rBufSize)
        if type(init_obj.rBufSize) ~= "number" then
            error("'rBufSize' must be a number for new() of ".._M.__type)
        end
    end
    if init_obj.wBufSize then
        init_obj.wBufSize = tonumber(init_obj.wBufSize)
        if type(init_obj.wBufSize) ~= "number" then
            error("'wBufSize' must be a number for new() of ".._M.__type)
        end
    end

    -- Ensure a socket is provided
    if nil == init_obj.socket then
        error("'socket' must be present for new() of ".._M.__type)
    end

    init_obj.wBuf = ''
    init_obj.rBuf = ''

    return TranBase.new(self, init_obj)
end

function _M.isOpen(self)
    return self.socket:isOpen()
end

function _M.open(self)
    return self.socket:open()
end

function _M.close(self)
  return self.socket:close()
end

function _M.setkeepalive(self, ...)
  return self.socket:setkeepalive(...)
end

function _M.read(self, len)
    return self.socket:read(len)
end

--Yuanguo: socket (TCosocket) doesn't have readAll() function, so it inherits readAll from TTransportBase, which
--         in turn calls read(self, len) of socket (note that the 'self' in TTransportBase:readAll() refers to the 
--         socket object);
function _M.readAll(self, len)
    return self.socket:readAll(len)
end

function _M.write(self, buf)
    self.wBuf = self.wBuf .. buf
    if #self.wBuf >= self.wBufSize then
        self.socket:write(self.wBuf)
        self.wBuf = ''
    end
end

function _M.flush(self)
    if #self.wBuf > 0 then
        self.socket:write(self.wBuf)
        self.wBuf = ''
    end
end

return _M
