--By Yuanguo
--06/09/2016
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
    __type = 'TException'
})

function _M.new(self, init_obj)
    if type(init_obj) ~= 'table' then
        error("Intial object must be a table for new() of ".._M.__type)
    end
    return Object.new(self, init_obj)
end

--Yuanguo: used by leaf instance. leaf instance should have message field or 
--         errorCode field and __errorCodeToString function
function _M.__tostring(self)
    if self.message then
        return string.format('%s: %s', self.__type, self.message)
    else
        local message
        if self.errorCode and self.__errorCodeToString then
            message = string.format('%d: %s', self.errorCode, self:__errorCodeToString())
        else
            message = thrift_print_r(self)
        end
        return string.format('%s:%s', self.__type, message)
    end
end

--Yuanguo: used by leaf instance. read from network and set message and/or errorCode for leaf instance
function _M.read(self, iprot)
    iprot:readStructBegin()
    while true do
        local fname, ftype, fid = iprot:readFieldBegin()
        if ftype == TType.STOP then
            break
        elseif fid == 1 then
            if ftype == TType.STRING then
                self.message = iprot:readString()
            else
                iprot:skip(ftype)
            end
        elseif fid == 2 then
            if ftype == TType.I32 then
                self.errorCode = iprot:readI32()
            else
                iprot:skip(ftype)
            end
        else
            iprot:skip(ftype)
        end
        iprot:readFieldEnd()
    end
    iprot:readStructEnd()
end

--Yuanguo: used by leaf instance. leaf instance should have message and/or errorCode
function _M.write(self, oprot)
    oprot:writeStructBegin('TApplicationException')
    if self.message then
        oprot:writeFieldBegin('message', TType.STRING, 1)
        oprot:writeString(self.message)
        oprot:writeFieldEnd()
    end
    if self.errorCode then
        oprot:writeFieldBegin('type', TType.I32, 2)
        oprot:writeI32(self.errorCode)
        oprot:writeFieldEnd()
    end
    oprot:writeFieldStop()
    oprot:writeStructEnd()
end

return _M
