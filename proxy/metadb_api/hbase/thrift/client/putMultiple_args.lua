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
--  table
--  tputs
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
                self.table = iprot:readString()
            else
                iprot:skip(ftype)
            end
        elseif fid == 2 then
            if ftype == TType.LIST then
                self.tputs = {}
                local _etype117, _size114 = iprot:readListBegin()
                for _i=1,_size114 do
                    _elem118 = TPut:new{}
                    _elem118:read(iprot)
                    table.insert(self.tputs, _elem118)
                end
                iprot:readListEnd()
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
    oprot:writeStructBegin('putMultiple_args')
    if self.table then
        oprot:writeFieldBegin('table', TType.STRING, 1)
        oprot:writeString(self.table)
        oprot:writeFieldEnd()
    end
    if self.tputs then
        oprot:writeFieldBegin('tputs', TType.LIST, 2)
        oprot:writeListBegin(TType.STRUCT, string.len(self.tputs))
        for _,iter119 in ipairs(self.tputs) do
            iter119:write(oprot)
        end
        oprot:writeListEnd()
        oprot:writeFieldEnd()
    end
    oprot:writeFieldStop()
    oprot:writeStructEnd()
end

return _M