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

local ok, TServerName = pcall(require, "hbase.thrift.client.TServerName")
if not ok or not TServerName then
    error("failed to load thrift.lualib.TTool:" .. (TServerName or "nil"))
end

local ok, THRegionInfo = pcall(require, "hbase.thrift.client.THRegionInfo")
if not ok or not THRegionInfo then
    error("failed to load thrift.lualib.TTool:" .. (THRegionInfo or "nil"))
end

local ttype = Tool.ttype
local terror = Tool.terror
local thrift_print_r = Tool.thrift_print_r

local _M = Object:new({
}) 

--Params in init_obj:
--   serverName
--   regionInfo
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
            if ftype == TType.STRUCT then
                self.serverName = TServerName:new{}
                self.serverName:read(iprot)
            else
                iprot:skip(ftype)
            end
        elseif fid == 2 then
            if ftype == TType.STRUCT then
                self.regionInfo = THRegionInfo:new{}
                self.regionInfo:read(iprot)
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
    oprot:writeStructBegin('THRegionLocation')
    if self.serverName then
        oprot:writeFieldBegin('serverName', TType.STRUCT, 1)
        self.serverName:write(oprot)
        oprot:writeFieldEnd()
    end
    if self.regionInfo then
        oprot:writeFieldBegin('regionInfo', TType.STRUCT, 2)
        self.regionInfo:write(oprot)
        oprot:writeFieldEnd()
    end
    oprot:writeFieldStop()
    oprot:writeStructEnd()
end

return _M
