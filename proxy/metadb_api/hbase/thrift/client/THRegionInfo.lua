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
--    regionId
--    tableName
--    startKey
--    endKey
--    offline
--    split
--    replicaId
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
            if ftype == TType.I64 then
                self.regionId = iprot:readI64()
            else
                iprot:skip(ftype)
            end
        elseif fid == 2 then
            if ftype == TType.STRING then
                self.tableName = iprot:readString()
            else
                iprot:skip(ftype)
            end
        elseif fid == 3 then
            if ftype == TType.STRING then
                self.startKey = iprot:readString()
            else
                iprot:skip(ftype)
            end
        elseif fid == 4 then
            if ftype == TType.STRING then
                self.endKey = iprot:readString()
            else
                iprot:skip(ftype)
            end
        elseif fid == 5 then
            if ftype == TType.BOOL then
                self.offline = iprot:readBool()
            else
                iprot:skip(ftype)
            end
        elseif fid == 6 then
            if ftype == TType.BOOL then
                self.split = iprot:readBool()
            else
                iprot:skip(ftype)
            end
        elseif fid == 7 then
            if ftype == TType.I32 then
                self.replicaId = iprot:readI32()
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
    oprot:writeStructBegin('THRegionInfo')
    if self.regionId then
        oprot:writeFieldBegin('regionId', TType.I64, 1)
        oprot:writeI64(self.regionId)
        oprot:writeFieldEnd()
    end
    if self.tableName then
        oprot:writeFieldBegin('tableName', TType.STRING, 2)
        oprot:writeString(self.tableName)
        oprot:writeFieldEnd()
    end
    if self.startKey then
        oprot:writeFieldBegin('startKey', TType.STRING, 3)
        oprot:writeString(self.startKey)
        oprot:writeFieldEnd()
    end
    if self.endKey then
        oprot:writeFieldBegin('endKey', TType.STRING, 4)
        oprot:writeString(self.endKey)
        oprot:writeFieldEnd()
    end
    if self.offline then
        oprot:writeFieldBegin('offline', TType.BOOL, 5)
        oprot:writeBool(self.offline)
        oprot:writeFieldEnd()
    end
    if self.split then
        oprot:writeFieldBegin('split', TType.BOOL, 6)
        oprot:writeBool(self.split)
        oprot:writeFieldEnd()
    end
    if self.replicaId then
        oprot:writeFieldBegin('replicaId', TType.I32, 7)
        oprot:writeI32(self.replicaId)
        oprot:writeFieldEnd()
    end
    oprot:writeFieldStop()
    oprot:writeStructEnd()
end

return _M
