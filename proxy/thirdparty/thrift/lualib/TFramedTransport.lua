--By Yuanguo
--10/11/2016

local ok, libluabpack = pcall(require, "libluabpack")
if not ok or not libluabpack then
    error("failed to load libluabpack:" .. (libluabpack or "nil"))
end

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
    __type = 'TFramedTransport',
})

function _M.new(self, init_obj)
    if type(init_obj) ~= 'table' then
        error("Intial object must be a table for new() of ".._M.__type)
    end

    -- Ensure a socket is provided
    if nil == init_obj.socket then
        error("'socket' must be present for new() of ".._M.__type)
    end

    init_obj.wBuf = ''

    init_obj.rBuf = ''
    init_obj.rBufPos = 1
    init_obj.rBufLen = 0

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

local function readFrame(self)
    local buf = self.socket:readAll(4)
    local frame_len = libluabpack.bunpack('i', buf)
    self.rBuf = self.socket:readAll(frame_len)
    self.rBufLen = frame_len
    self.rBufPos = 1 
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "TFramedTransport: a frame has been read, length=", frame_len)
end

function _M.read(self, len)
    if 0 == self.rBufLen then
        readFrame(self)
    end

    if len > self.rBufLen then
        local ret = string.sub(self.rBuf, self.rBufPos)
        self.rBufLen = 0
        return ret
    end

    local ret = string.sub(self.rBuf, self.rBufPos, self.rBufPos+len-1)
    self.rBufPos = self.rBufPos + len
    self.rBufLen = self.rBufLen - len
    return ret
end

--Yuanguo: unlike TBufferedTransport, TFramedTransport should not have the readAll() 
--         function. As a result, it heritates the readAll from TTransportBase, which 
--         will calls TFramedTransport:read(), which in turn reads data in frames;
--[[
function _M.readAll(self, len)
    return self.socket:readAll(len)
end
--]]

function _M.write(self, buf, len)
    if len and len < string.len(buf) then
        buf = string.sub(buf, 1, len)
    end
    self.wBuf = self.wBuf..buf
end

function _M.flush(self)
    local len = #self.wBuf
    if len > 0 then
        local buff = libluabpack.bpack('i', len)
        self.socket:write(buff)

        --TODO: Yuanguo: if write fails, self.wBuf will be dirty; so it is better
        --to make a copy and self.wBuf = '', and then write the copy; but it will
        --be CPU inefficient;
        self.socket:write(self.wBuf)
        self.wBuf = ''
    end
end

return _M
