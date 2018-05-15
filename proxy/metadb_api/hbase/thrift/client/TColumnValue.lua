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
--  family
--  qualifier
--  value
--  timestamp
--  tags
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
        self.family = iprot:readString()
      else
        iprot:skip(ftype)
      end
    elseif fid == 2 then
      if ftype == TType.STRING then
        self.qualifier = iprot:readString()
      else
        iprot:skip(ftype)
      end
    elseif fid == 3 then
      if ftype == TType.STRING then
        self.value = iprot:readString()
      else
        iprot:skip(ftype)
      end
    elseif fid == 4 then
      if ftype == TType.I64 then
        self.timestamp = iprot:readI64()
      else
        iprot:skip(ftype)
      end
    elseif fid == 5 then
      if ftype == TType.STRING then
        self.tags = iprot:readString()
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
    oprot:writeStructBegin('TColumnValue')
    if self.family then
        oprot:writeFieldBegin('family', TType.STRING, 1)
        oprot:writeString(self.family)
        oprot:writeFieldEnd()
    end
    if self.qualifier then
        oprot:writeFieldBegin('qualifier', TType.STRING, 2)
        oprot:writeString(self.qualifier)
        oprot:writeFieldEnd()
    end
    if self.value then
        oprot:writeFieldBegin('value', TType.STRING, 3)
        oprot:writeString(self.value)
        oprot:writeFieldEnd()
    end
    if self.timestamp then
        oprot:writeFieldBegin('timestamp', TType.I64, 4)
        oprot:writeI64(self.timestamp)
        oprot:writeFieldEnd()
    end
    if self.tags then
        oprot:writeFieldBegin('tags', TType.STRING, 5)
        oprot:writeString(self.tags)
        oprot:writeFieldEnd()
    end
    oprot:writeFieldStop()
    oprot:writeStructEnd()
end

return _M
