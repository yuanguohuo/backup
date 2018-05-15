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
--    attributes
--    durability
--    cellVisibility
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
                local _etype57, _size54 = iprot:readListBegin()
                for _i=1,_size54 do
                    _elem58 = TColumnIncrement:new{}
                    _elem58:read(iprot)
                    table.insert(self.columns, _elem58)
                end
                iprot:readListEnd()
            else
                iprot:skip(ftype)
            end
        elseif fid == 4 then
            if ftype == TType.MAP then
                self.attributes = {}
                local _ktype60, _vtype61, _size59 = iprot:readMapBegin() 
                for _i=1,_size59 do
                    _key63 = iprot:readString()
                    _val64 = iprot:readString()
                    self.attributes[_key63] = _val64
                end
                iprot:readMapEnd()
            else
                iprot:skip(ftype)
            end
        elseif fid == 5 then
            if ftype == TType.I32 then
                self.durability = iprot:readI32()
            else
                iprot:skip(ftype)
            end
        elseif fid == 6 then
            if ftype == TType.STRUCT then
                self.cellVisibility = TCellVisibility:new{}
                self.cellVisibility:read(iprot)
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
    oprot:writeStructBegin('TIncrement')
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
        for _,iter65 in ipairs(self.columns) do
            iter65:write(oprot)
        end
        oprot:writeListEnd()
        oprot:writeFieldEnd()
    end
    if self.attributes then
        oprot:writeFieldBegin('attributes', TType.MAP, 4)
        oprot:writeMapBegin(TType.STRING, TType.STRING, string.len(self.attributes))
        for kiter66,viter67 in pairs(self.attributes) do
            oprot:writeString(kiter66)
            oprot:writeString(viter67)
        end
        oprot:writeMapEnd()
        oprot:writeFieldEnd()
    end
    if self.durability then
        oprot:writeFieldBegin('durability', TType.I32, 5)
        oprot:writeI32(self.durability)
        oprot:writeFieldEnd()
    end
    if self.cellVisibility then
        oprot:writeFieldBegin('cellVisibility', TType.STRUCT, 6)
        self.cellVisibility:write(oprot)
        oprot:writeFieldEnd()
    end
    oprot:writeFieldStop()
    oprot:writeStructEnd()
end

return _M
