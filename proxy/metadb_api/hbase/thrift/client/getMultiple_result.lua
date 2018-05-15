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
--  success
--  io
function _M.new(self, init_obj)
    return Object.new(self, init_obj)
end

function _M.read(self, iprot)
    iprot:readStructBegin()
    while true do
        local fname, ftype, fid = iprot:readFieldBegin()
        if ftype == TType.STOP then
            break
        elseif fid == 0 then
            if ftype == TType.LIST then
                self.success = {}
                local _etype111, _size108 = iprot:readListBegin()
                for _i=1,_size108 do
                    _elem112 = TResult:new{}
                    _elem112:read(iprot)
                    table.insert(self.success, _elem112)
                end
                iprot:readListEnd()
            else
                iprot:skip(ftype)
            end
        elseif fid == 1 then
            if ftype == TType.STRUCT then
                self.io = TIOError:new{}
                self.io:read(iprot)
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
    oprot:writeStructBegin('getMultiple_result')
    if self.success then
        oprot:writeFieldBegin('success', TType.LIST, 0)
        oprot:writeListBegin(TType.STRUCT, string.len(self.success))
        for _,iter113 in ipairs(self.success) do
            iter113:write(oprot)
        end
        oprot:writeListEnd()
        oprot:writeFieldEnd()
    end
    if self.io then
        oprot:writeFieldBegin('io', TType.STRUCT, 1)
        self.io:write(oprot)
        oprot:writeFieldEnd()
    end
    oprot:writeFieldStop()
    oprot:writeStructEnd()
end

return _M
