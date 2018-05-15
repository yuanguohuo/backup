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
    __type = 'TTransportBase'
})

function _M.new(self, init_obj)
    if type(init_obj) ~= 'table' then
        error("Intial object must be a table for new() of ".._M.__type)
    end
    return Object.new(self, init_obj)
end

function _M.isOpen(self) end
function _M.open(self) end
function _M.read(self, len) end

function _M.readAll(self, len)
  local buf, have, chunk = '', 0
  while have < len do
    chunk = self:read(len - have)
    local chunkLen = #chunk

    if chunkLen == 0 then
      terror(TTransportException:new{
        errorCode = TTransportException.END_OF_FILE
      })
    end

    have = have + chunkLen
    buf = buf .. chunk
  end
  return buf
end

function _M.write(self, buf) end
function _M.flush(self) end

function _M.close(self) end
function _M.setkeepalive(self, ...) end

return _M
