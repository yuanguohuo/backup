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

local ok, TColumn = pcall(require, "thrift.client.TColumn")
if not ok or not TColumn then
    error("failed to load thrift.client.TColumn:" .. (TColumn or "nil"))
end


local ok, TTimeRange = pcall(require, "thrift.client.TTimeRange")
if not ok or not TTimeRange then
    error("failed to load thrift.client.TTimeRange:" .. (TTimeRange or "nil"))
end

local ok, TAuthorization = pcall(require, "thrift.client.TAuthorization")
if not ok or not TAuthorization then
    error("failed to load thrift.client.TAuthorization:" .. (TAuthorization or "nil"))
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
--    timeRange
--    maxVersions
--    filterString
--    attributes
--    authorizations
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
                local _etype15, _size12 = iprot:readListBegin()
                for _i=1,_size12 do
                 local  _elem16 = TColumn:new{}
                    _elem16:read(iprot)
                    table.insert(self.columns, _elem16)
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
            if ftype == TType.STRUCT then
                self.timeRange = TTimeRange:new{}
                self.timeRange:read(iprot)
            else
                iprot:skip(ftype)
            end
        elseif fid == 5 then
            if ftype == TType.I32 then
                self.maxVersions = iprot:readI32()
            else
                iprot:skip(ftype)
            end
        elseif fid == 6 then
            if ftype == TType.STRING then
                self.filterString = iprot:readString()
            else
                iprot:skip(ftype)
            end
        elseif fid == 7 then
            if ftype == TType.MAP then
                self.attributes = {}
                local _ktype18, _vtype19, _size17 = iprot:readMapBegin() 
                for _i=1,_size17 do
                 local   _key21 = iprot:readString()
                 local   _val22 = iprot:readString()
                    self.attributes[_key21] = _val22
                end
                iprot:readMapEnd()
            else
                iprot:skip(ftype)
            end
        elseif fid == 8 then
            if ftype == TType.STRUCT then
                self.authorizations = TAuthorization:new{}
                self.authorizations:read(iprot)
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
    oprot:writeStructBegin('TGet')
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
        for _,iter23 in ipairs(self.columns) do
            iter23:write(oprot)
        end
        oprot:writeListEnd()
        oprot:writeFieldEnd()
    end
    if self.timestamp then
        oprot:writeFieldBegin('timestamp', TType.I64, 3)
        oprot:writeI64(self.timestamp)
        oprot:writeFieldEnd()
    end
    if self.timeRange then
        oprot:writeFieldBegin('timeRange', TType.STRUCT, 4)
        self.timeRange:write(oprot)
        oprot:writeFieldEnd()
    end
    if self.maxVersions then
        oprot:writeFieldBegin('maxVersions', TType.I32, 5)
        oprot:writeI32(self.maxVersions)
        oprot:writeFieldEnd()
    end
    if self.filterString then
        oprot:writeFieldBegin('filterString', TType.STRING, 6)
        oprot:writeString(self.filterString)
        oprot:writeFieldEnd()
    end
    if self.attributes then
        oprot:writeFieldBegin('attributes', TType.MAP, 7)
        oprot:writeMapBegin(TType.STRING, TType.STRING, string.len(self.attributes))
        for kiter24,viter25 in pairs(self.attributes) do
            oprot:writeString(kiter24)
            oprot:writeString(viter25)
        end
        oprot:writeMapEnd()
        oprot:writeFieldEnd()
    end
    if self.authorizations then
        oprot:writeFieldBegin('authorizations', TType.STRUCT, 8)
        self.authorizations:write(oprot)
        oprot:writeFieldEnd()
    end
    oprot:writeFieldStop()
    oprot:writeStructEnd()
end

return _M
