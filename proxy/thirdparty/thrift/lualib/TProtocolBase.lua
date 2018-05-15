--By Yuanguo
--07/09/2016
local ok, TType = pcall(require, "thrift.lualib.TType")
if not ok or not TType then
    error("failed to load thrift.lualib.TType:" .. (TType or "nil"))
end

local ok, TMessageType = pcall(require, "thrift.lualib.TMessageType")
if not ok or not TMessageType then
    error("failed to load thrift.lualib.TMessageType:" .. (TMessageType or "nil"))
end

local ok, Object = pcall(require, "thrift.lualib.TObject")
if not ok or not Object then
    error("failed to load thrift.lualib.TObject:" .. (Object or "nil"))
end

local ok, Tool = pcall(require, "thrift.lualib.TTool")
if not ok or not Tool then
    error("failed to load thrift.lualib.TTool:" .. (Tool or "nil"))
end

local ttype = Tool.ttype
local terror = Tool.terror
local thrift_print_r = Tool.thrift_print_r

local _M = Object:new({
    __type = 'TProtocolBase',
})

function _M.new(self, init_obj)
    if type(init_obj) ~= 'table' then
        error("Intial object must be a table for new() of ".._M.__type)
    end

    return Object.new(self, init_obj)
end

function _M.writeMessageBegin(self, name, ttype, seqid) end
function _M.writeMessageEnd(self) end
function _M.writeStructBegin(self, name) end
function _M.writeStructEnd(self) end
function _M.writeFieldBegin(self, name, ttype, id) end
function _M.writeFieldEnd(self) end
function _M.writeFieldStop(self) end
function _M.writeMapBegin(self, ktype, vtype, size) end
function _M.writeMapEnd(self) end
function _M.writeListBegin(self, ttype, size) end
function _M.writeListEnd(self) end
function _M.writeSetBegin(self, ttype, size) end
function _M.writeSetEnd(self) end
function _M.writeBool(self, bool) end
function _M.writeByte(self, byte) end
function _M.writeI16(self, i16) end
function _M.writeI32(self, i32) end
function _M.writeI64(self, i64) end
function _M.writeDouble(self, dub) end
function _M.writeString(self, str) end
function _M.readMessageBegin(self) end
function _M.readMessageEnd(self) end
function _M.readStructBegin(self) end
function _M.readStructEnd(self) end
function _M.readFieldBegin(self) end
function _M.readFieldEnd(self) end
function _M.readMapBegin(self) end
function _M.readMapEnd(self) end
function _M.readListBegin(self) end
function _M.readListEnd(self) end
function _M.readSetBegin(self) end
function _M.readSetEnd(self) end
function _M.readBool(self) end
function _M.readByte(self) end
function _M.readI16(self) end
function _M.readI32(self) end
function _M.readI64(self) end
function _M.readDouble(self) end
function _M.readString(self) end

function _M.skip(self, ttype)
    if ttype == TType.STOP then
        return
    elseif ttype == TType.BOOL then
        self:readBool()
    elseif ttype == TType.BYTE then
        self:readByte()
    elseif ttype == TType.I16 then
        self:readI16()
    elseif ttype == TType.I32 then
        self:readI32()
    elseif ttype == TType.I64 then
        self:readI64()
    elseif ttype == TType.DOUBLE then
        self:readDouble()
    elseif ttype == TType.STRING then
        self:readString()
    elseif ttype == TType.STRUCT then
        local name = self:readStructBegin()
        while true do
            local name, ttype, id = self:readFieldBegin()
            if ttype == TType.STOP then
                break
            end
            self:skip(ttype)
            self:readFieldEnd()
        end
        self:readStructEnd()
    elseif ttype == TType.MAP then
        local kttype, vttype, size = self:readMapBegin()
        for i = 1, size, 1 do
            self:skip(kttype)
            self:skip(vttype)
        end
        self:readMapEnd()
    elseif ttype == TType.SET then
        local ettype, size = self:readSetBegin()
        for i = 1, size, 1 do
            self:skip(ettype)
        end
        self:readSetEnd()
    elseif ttype == TType.LIST then
        local ettype, size = self:readListBegin()
        for i = 1, size, 1 do
            self:skip(ettype)
        end
        self:readListEnd()
    end
end

return _M
