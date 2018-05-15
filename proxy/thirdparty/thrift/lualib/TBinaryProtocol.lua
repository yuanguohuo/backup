--By Yuanguo
--07/09/2016

local ok, libluabpack = pcall(require, "libluabpack")
if not ok or not libluabpack then
    error("failed to load libluabpack:" .. (libluabpack or "nil"))
end

local ok, libluabitwise = pcall(require, "libluabitwise")
if not ok or not libluabitwise then
    error("failed to load libluabitwise:" .. (libluabitwise or "nil"))
end

local ok, TType = pcall(require, "thrift.lualib.TType")
if not ok or not TType then
    error("failed to load thrift.lualib.TType:" .. (TType or "nil"))
end

local ok, TMessageType = pcall(require, "thrift.lualib.TMessageType")
if not ok or not TMessageType then
    error("failed to load thrift.lualib.TMessageType:" .. (TMessageType or "nil"))
end

local ok, ProtocBase = pcall(require, "thrift.lualib.TProtocolBase")
if not ok or not ProtocBase then
    error("failed to load thrift.lualib.TProtocolBase:" .. (ProtocBase or "nil"))
end

local ok, ProtocExcep = pcall(require, "thrift.lualib.TProtocolException")
if not ok or not ProtocExcep then
    error("failed to load thrift.lualib.TProtocolException:" .. (ProtocExcep or "nil"))
end

local ok, Tool = pcall(require, "thrift.lualib.TTool")
if not ok or not Tool then
    error("failed to load thrift.lualib.TTool:" .. (Tool or "nil"))
end

local ttype = Tool.ttype
local terror = Tool.terror
local thrift_print_r = Tool.thrift_print_r
-- local TProtocolException = TProtocolException 
local TProtocolException = ProtocExcep
local _M = ProtocBase:new({
    __type = 'TBinaryProtocol',
    VERSION_MASK = -65536, -- 0xffff0000
    VERSION_1    = -2147418112, -- 0x80010000
    TYPE_MASK    = 0x000000ff,
    strictRead   = false,
    strictWrite  = true
})

function _M.new(self, init_obj)
    if type(init_obj) ~= 'table' then
        error("Intial object must be a table for new() of ".._M.__type)
    end

    -- Ensure a transport is provided
    if nil == init_obj.transport then
        error("'transport' must be present for new() of ".._M.__type)
    end

    return ProtocBase.new(self, init_obj)
end

function _M.writeMessageBegin(self, name, ttype, seqid)
    if self.strictWrite then
        self:writeI32(libluabitwise.bor(self.VERSION_1, ttype))
        self:writeString(name)
        self:writeI32(seqid)
    else
        self:writeString(name)
        self:writeByte(ttype)
        self:writeI32(seqid)
    end
end

function _M.writeMessageEnd(self) end
function _M.writeStructBegin(self, name) end
function _M.writeStructEnd(self) end

function _M.writeFieldBegin(self, name, ttype, id)
    self:writeByte(ttype)
    self:writeI16(id)
end

function _M.writeFieldEnd(self) end

function _M.writeFieldStop(self)
    self:writeByte(TType.STOP);
end

function _M.writeMapBegin(self, ktype, vtype, size)
    self:writeByte(ktype)
    self:writeByte(vtype)
    self:writeI32(size)
end

function _M.writeMapEnd(self) end

function _M.writeListBegin(self, etype, size)
    self:writeByte(etype)
    self:writeI32(size)
end

function _M.writeListEnd(self) end

function _M.writeSetBegin(self, etype, size)
    self:writeByte(etype)
    self:writeI32(size)
end

function _M.writeSetEnd(self) end

function _M.writeBool(self, bool)
    if bool then
        self:writeByte(1)
    else
        self:writeByte(0)
    end
end

function _M.writeByte(self, byte)
    local buff = libluabpack.bpack('c', byte)
    self.transport:write(buff)
end

function _M.writeI16(self, i16)
    local buff = libluabpack.bpack('s', i16)
    self.transport:write(buff)
end

function _M.writeI32(self, i32)
    local buff = libluabpack.bpack('i', i32)
    self.transport:write(buff)
end

function _M.writeI64(self, i64)
    local buff = libluabpack.bpack('l', i64)
    self.transport:write(buff)
end

function _M.writeDouble(self, dub)
    local buff = libluabpack.bpack('d', dub)
    self.transport:write(buff)
end

function _M.writeString(self, str)
    -- Should be utf-8
    self:writeI32(string.len(str))
    self.transport:write(str)
end

function _M.readMessageBegin(self)
    local sz, ttype, name, seqid = self:readI32()
    if sz < 0 then   --Yuanguo: sz = VERSION_1 | ttype; which means the server is in "strictWrite" mode;
        --Yuanguo:  in "strictWrite" mode, we can get the version and type from sz, and then read name and seqid;
        --          to read name, readString first read the length, then read the string itself. see writeMessageBegin;
        local version = libluabitwise.band(sz, self.VERSION_MASK)
        if version ~= self.VERSION_1 then
            terror(TProtocolException:new{
                message = 'Bad version in readMessageBegin: ' .. sz
            })
        end
        ttype = libluabitwise.band(sz, self.TYPE_MASK)
        name = self:readString()
        seqid = self:readI32()
    else      --Yuanguo: server is not in "strictWrite" mode;
        --Yuanguo: in "non-strictWrite" mode, sz is the length of name (because writeString() writes 
        --         the length first and then writes the string itself), see writeMessageBegin;
        if self.strictRead then
            terror(TProtocolException:new{message = 'No protocol version header'})
        end
        name = self.transport:readAll(sz)
        ttype = self:readByte()
        seqid = self:readI32()
    end
    return name, ttype, seqid
end

function _M.readMessageEnd(self) end

function _M.readStructBegin(self)
    return nil
end

function _M.readStructEnd(self) end

function _M.readFieldBegin(self)
    local ttype = self:readByte()
    if ttype == TType.STOP then
        return nil, ttype, 0
    end
    local id = self:readI16()
    return nil, ttype, id
end

function _M.readFieldEnd(self)
end

function _M.readMapBegin(self)
    local ktype = self:readByte()
    local vtype = self:readByte()
    local size = self:readI32()
    return ktype, vtype, size
end

function _M.readMapEnd(self)
end

function _M.readListBegin(self)
    local etype = self:readByte()
    local size = self:readI32()
    return etype, size
end

function _M.readListEnd(self)
end

function _M.readSetBegin(self)
    local etype = self:readByte()
    local size = self:readI32()
    return etype, size
end

function _M.readSetEnd(self)
end

function _M.readBool(self)
    local byte = self:readByte()
    if byte == 0 then
        return false
    end
    return true
end

function _M.readByte(self)
    local buff = self.transport:readAll(1)
    local val = libluabpack.bunpack('c', buff)
    return val
end

function _M.readI16(self)
    local buff = self.transport:readAll(2)
    local val = libluabpack.bunpack('s', buff)
    return val
end

function _M.readI32(self)
    local buff = self.transport:readAll(4)
    local val = libluabpack.bunpack('i', buff)
    return val
end

function _M.readI64(self)
    local buff = self.transport:readAll(8)
    local val = libluabpack.bunpack('l', buff)
    return val
end

function _M.readDouble(self)
    local buff = self.transport:readAll(8)
    local val = libluabpack.bunpack('d', buff)
    return val
end

function _M.readString(self)
    local len = self:readI32()
    local str = self.transport:readAll(len)
    return str
end

return _M
