--By Yuanguo
--06/09/2016

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
    __type = 'TClient',
    _seqid = 0
})

function _M.new(self, init_obj)
    if type(init_obj) ~= 'table' then
        error("Intial object must be a table for new() of ".._M.__type)
    end

    if init_obj.protocol then
        init_obj.iprot = init_obj.protocol
        init_obj.oprot = init_obj.protocol
        init_obj.protocol = nil
    end

    if not init_obj.oprot then
        init_obj.oprot = init_obj.iprot
    end

    return Object.new(self, init_obj)
end

function _M.open(self) 
    self.iprot.transport:open()
    if self.oprot ~= self.iprot then
        self.oprot.transport:open()
    end
end

function _M.close(self) 
    self.iprot.transport:close()
    if self.oprot ~= self.iprot then
        self.oprot.transport:close()
    end
end

function _M.setkeepalive(self, ...)
    self.oprot.transport:setkeepalive(...)
    if self.oprot ~= self.iprot then
        self.iprot.transport:setkeepalive(...)
    end
end

return _M
