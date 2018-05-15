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

local ok, TAuthorization= pcall(require, "thrift.client.TAuthorization")
if not ok or not TAuthorization then
    error("failed to load thrift.client.TAuthorization:" .. (TAuthorization or "nil"))
end

local ttype = Tool.ttype
local terror = Tool.terror
local thrift_print_r = Tool.thrift_print_r

local _M = Object:new({
}) 

--Params in init_obj:
--    startRow
--    stopRow
--    columns
--    caching
--    maxVersions
--    timeRange
--    filterString
--    batchSize
--    attributes
--    authorizations
--    reversed
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
                self.startRow = iprot:readString()
            else
                iprot:skip(ftype)
            end
        elseif fid == 2 then
            if ftype == TType.STRING then
                self.stopRow = iprot:readString()
            else
                iprot:skip(ftype)
            end
        elseif fid == 3 then
            if ftype == TType.LIST then
                self.columns = {}
                local _etype85, _size82 = iprot:readListBegin()
                for _i=1,_size82 do
                    local _elem86 = TColumn:new{}
                    _elem86:read(iprot)
                    table.insert(self.columns, _elem86)
                end
                iprot:readListEnd()
            else
                iprot:skip(ftype)
            end
        elseif fid == 4 then
            if ftype == TType.I32 then
                self.caching = iprot:readI32()
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
            if ftype == TType.STRUCT then
                self.timeRange = TTimeRange:new{}
                self.timeRange:read(iprot)
            else
                iprot:skip(ftype)
            end
        elseif fid == 7 then
            if ftype == TType.STRING then
                self.filterString = iprot:readString()
            else
                iprot:skip(ftype)
            end
        elseif fid == 8 then
            if ftype == TType.I32 then
                self.batchSize = iprot:readI32()
            else
                iprot:skip(ftype)
            end
        elseif fid == 9 then
            if ftype == TType.MAP then
                self.attributes = {}
                local _ktype88, _vtype89, _size87 = iprot:readMapBegin() 
                for _i=1,_size87 do
                    local _key91 = iprot:readString()
                    local _val92 = iprot:readString()
                    self.attributes[_key91] = _val92
                end
                iprot:readMapEnd()
            else
                iprot:skip(ftype)
            end
        elseif fid == 10 then
            if ftype == TType.STRUCT then
                self.authorizations = TAuthorization:new{}
                self.authorizations:read(iprot)
            else
                iprot:skip(ftype)
            end
        elseif fid == 11 then
            if ftype == TType.BOOL then
                self.reversed = iprot:readBool()
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
    oprot:writeStructBegin('TScan')
    if self.startRow then
        oprot:writeFieldBegin('startRow', TType.STRING, 1)
        oprot:writeString(self.startRow)
        oprot:writeFieldEnd()
    end
    if self.stopRow then
        oprot:writeFieldBegin('stopRow', TType.STRING, 2)
        oprot:writeString(self.stopRow)
        oprot:writeFieldEnd()
    end
    if self.columns then
        oprot:writeFieldBegin('columns', TType.LIST, 3)
        oprot:writeListBegin(TType.STRUCT, #self.columns)
        for _,iter93 in ipairs(self.columns) do
            iter93:write(oprot)
        end
        oprot:writeListEnd()
        oprot:writeFieldEnd()
    end
    if self.caching then
        oprot:writeFieldBegin('caching', TType.I32, 4)
        oprot:writeI32(self.caching)
        oprot:writeFieldEnd()
    end
    if self.maxVersions then
        oprot:writeFieldBegin('maxVersions', TType.I32, 5)
        oprot:writeI32(self.maxVersions)
        oprot:writeFieldEnd()
    end
    if self.timeRange then
        oprot:writeFieldBegin('timeRange', TType.STRUCT, 6)
        self.timeRange:write(oprot)
        oprot:writeFieldEnd()
    end
    if self.filterString then
        oprot:writeFieldBegin('filterString', TType.STRING, 7)
        oprot:writeString(self.filterString)
        oprot:writeFieldEnd()
    end
    if self.batchSize then
        oprot:writeFieldBegin('batchSize', TType.I32, 8)
        oprot:writeI32(self.batchSize)
        oprot:writeFieldEnd()
    end
    if self.attributes then
        oprot:writeFieldBegin('attributes', TType.MAP, 9)
        oprot:writeMapBegin(TType.STRING, TType.STRING, #self.attributes)
        for kiter94,viter95 in pairs(self.attributes) do
            oprot:writeString(kiter94)
            oprot:writeString(viter95)
        end
        oprot:writeMapEnd()
        oprot:writeFieldEnd()
    end
    if self.authorizations then
        oprot:writeFieldBegin('authorizations', TType.STRUCT, 10)
        self.authorizations:write(oprot)
        oprot:writeFieldEnd()
    end
    if self.reversed then
        oprot:writeFieldBegin('reversed', TType.BOOL, 11)
        oprot:writeBool(self.reversed)
        oprot:writeFieldEnd()
    end
    oprot:writeFieldStop()
    oprot:writeStructEnd()
end

return _M
