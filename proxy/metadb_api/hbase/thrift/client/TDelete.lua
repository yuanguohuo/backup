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
}) 

--Params in init_obj:
--    row
--    columns
--    timestamp
--    deleteType
--    attributes
--    durability
function _M.new(self, init_obj)
    return Object.new(self, init_obj)
end

function _M.read(self, iprot)
    iprot:readStructBegin()
    while true do
        local fname, ftype, fid = iprot:readFieldBegin()
        if ftype == TType.STOP then
            break
        elseif fid == 1 then
            if ftype == TType.STRING then
                self.row = iprot:readString()
            else
                iprot:skip(ftype)
            end
        elseif fid == 2 then
            if ftype == TType.LIST then
                self.columns = {}
                local _etype43, _size40 = iprot:readListBegin()
                for _i=1,_size40 do
                    _elem44 = TColumn:new{}
                    _elem44:read(iprot)
                    table.insert(self.columns, _elem44)
                end
                iprot:readListEnd()
            else
                iprot:skip(ftype)
            end
        elseif fid == 3 then
            if ftype == TType.I64 then
                self.timestamp = iprot:readI64()
            else
                iprot:skip(ftype)
            end
        elseif fid == 4 then
            if ftype == TType.I32 then
                self.deleteType = iprot:readI32()
            else
                iprot:skip(ftype)
            end
        elseif fid == 6 then
            if ftype == TType.MAP then
                self.attributes = {}
                local _ktype46, _vtype47, _size45 = iprot:readMapBegin() 
                for _i=1,_size45 do
                    _key49 = iprot:readString()
                    _val50 = iprot:readString()
                    self.attributes[_key49] = _val50
                end
                iprot:readMapEnd()
            else
                iprot:skip(ftype)
            end
        elseif fid == 7 then
            if ftype == TType.I32 then
                self.durability = iprot:readI32()
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

function _M.write(self, oprot)
    oprot:writeStructBegin('TDelete')
    if self.row then
        oprot:writeFieldBegin('row', TType.STRING, 1)
        oprot:writeString(self.row)
        oprot:writeFieldEnd()
    end
    if self.columns then
        oprot:writeFieldBegin('columns', TType.LIST, 2)
        --Yuanguo: Thrift generated incorrect code, fix it
        --oprot:writeListBegin(TType.STRUCT, string.len(self.columns))
        oprot:writeListBegin(TType.STRUCT, #self.columns)
        for _,iter51 in ipairs(self.columns) do
            iter51:write(oprot)
        end
        oprot:writeListEnd()
        oprot:writeFieldEnd()
    end
    if self.timestamp then
        oprot:writeFieldBegin('timestamp', TType.I64, 3)
        oprot:writeI64(self.timestamp)
        oprot:writeFieldEnd()
    end
    if self.deleteType then
        oprot:writeFieldBegin('deleteType', TType.I32, 4)
        oprot:writeI32(self.deleteType)
        oprot:writeFieldEnd()
    end
    if self.attributes then
        oprot:writeFieldBegin('attributes', TType.MAP, 6)
        --Yuanguo: Thrift generated incorrect code, fix it
        --oprot:writeListBegin(TType.STRUCT, string.len(self.columns))
        oprot:writeMapBegin(TType.STRING, TType.STRING, #self.attributes)
        for kiter52,viter53 in pairs(self.attributes) do
            oprot:writeString(kiter52)
            oprot:writeString(viter53)
        end
        oprot:writeMapEnd()
        oprot:writeFieldEnd()
    end
    if self.durability then
        oprot:writeFieldBegin('durability', TType.I32, 7)
        oprot:writeI32(self.durability)
        oprot:writeFieldEnd()
    end
    oprot:writeFieldStop()
    oprot:writeStructEnd()
end

return _M
