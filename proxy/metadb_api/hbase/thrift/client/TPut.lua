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
--    columnValues
--    timestamp
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
                self.columnValues = {}
                local _etype29, _size26 = iprot:readListBegin()
                for _i=1,_size26 do
                    _elem30 = TColumnValue:new{}
                    _elem30:read(iprot)
                    table.insert(self.columnValues, _elem30)
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
        elseif fid == 5 then
            if ftype == TType.MAP then
                self.attributes = {}
                local _ktype32, _vtype33, _size31 = iprot:readMapBegin() 
                for _i=1,_size31 do
                    _key35 = iprot:readString()
                    _val36 = iprot:readString()
                    self.attributes[_key35] = _val36
                end
                iprot:readMapEnd()
            else
                iprot:skip(ftype)
            end
        elseif fid == 6 then
            if ftype == TType.I32 then
                self.durability = iprot:readI32()
            else
                iprot:skip(ftype)
            end
        elseif fid == 7 then
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
    oprot:writeStructBegin('TPut')
    if self.row then
        oprot:writeFieldBegin('row', TType.STRING, 1)
        oprot:writeString(self.row)
        oprot:writeFieldEnd()
    end
    if self.columnValues then
        oprot:writeFieldBegin('columnValues', TType.LIST, 2)
        --Yuanguo: Thrift generated incorrect code, fix it
        --oprot:writeListBegin(TType.STRUCT, string.len(self.columnValues))
        oprot:writeListBegin(TType.STRUCT, #self.columnValues)
        for _,iter37 in ipairs(self.columnValues) do
            iter37:write(oprot)
        end
        oprot:writeListEnd()
        oprot:writeFieldEnd()
    end
    if self.timestamp then
        oprot:writeFieldBegin('timestamp', TType.I64, 3)
        oprot:writeI64(self.timestamp)
        oprot:writeFieldEnd()
    end
    if self.attributes then
        oprot:writeFieldBegin('attributes', TType.MAP, 5)
        --by hjl
        --oprot:writeMapBegin(TType.STRING, TType.STRING, string.len(self.attributes))
        local total = 0
        for kiter38,viter39 in pairs(self.attributes) do
            total = total + 1
        end
        oprot:writeMapBegin(TType.STRING, TType.STRING, total)
        for kiter38,viter39 in pairs(self.attributes) do
            oprot:writeString(kiter38)
            oprot:writeString(viter39)
        end
        oprot:writeMapEnd()
        oprot:writeFieldEnd()
    end
    if self.durability then
        oprot:writeFieldBegin('durability', TType.I32, 6)
        oprot:writeI32(self.durability)
        oprot:writeFieldEnd()
    end
    if self.cellVisibility then
        oprot:writeFieldBegin('cellVisibility', TType.STRUCT, 7)
        self.cellVisibility:write(oprot)
        oprot:writeFieldEnd()
    end
    oprot:writeFieldStop()
    oprot:writeStructEnd()
end

return _M
