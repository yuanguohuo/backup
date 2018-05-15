--By Yuanguo
--06/09/2016

local ok, TranBase = pcall(require, "thrift.lualib.TTransportBase")
if not ok or not TranBase then
    error("failed to load thrift.lualib.TTransportBase:" .. (TranBase or "nil"))
end

local ok, TTransportException = pcall(require, "thrift.lualib.TTransportException")
if not ok or not TTransportException then
    error("failed to load thrift.lualib.TTransportException:" .. (TTransportException or "nil"))
end

local ok, Tool = pcall(require, "thrift.lualib.TTool")
if not ok or not Tool then
    error("failed to load thrift.lualib.TTool:" .. (Tool or "nil"))
end

local ttype = Tool.ttype
local terror = Tool.terror
local thrift_print_r = Tool.thrift_print_r

local func_create_sock = ngx.socket.tcp

local _M = TranBase:new({
    __type = 'TCosocket',
    timeout = 5000000,
    host = 'localhost',
    port = 9090,
})

function _M.new(self, init_obj)
    if type(init_obj) ~= 'table' then
        error("Intial object must be a table for new() of ".._M.__type)
    end
    if init_obj.timeout then
        init_obj.timeout = tonumber(init_obj.timeout)
        if type(init_obj.timeout) ~= "number" then
            error("'timeout' must be a number for new() of ".._M.__type)
        end
    end
    if init_obj.port then
        init_obj.port = tonumber(init_obj.port)
        if type(init_obj.port) ~= "number" then
            error("'port' must be a number for new() of ".._M.__type)
        end
    end

    init_obj.sock = nil

    return TranBase.new(self, init_obj)
end

function _M.getSocketInfo(self)
    if self.sock then
        return {host=self.host, port=self.port}
    end
    terror(TTransportException:new{errorCode = TTransportException.NOT_OPEN})
end

function _M.setTimeout(self, timeout)
    if timeout and ttype(timeout) == 'number' then
        if self.sock then
            self.sock:settimeout(timeout)
        end
        self.timeout = timeout
    end
end

function _M.isOpen(self)
    if self.sock then
        return true
    else
        return false
    end
end

function _M.open(self)
    if self.sock then
        return  --already open
    end

    self.sock = func_create_sock()
    if not self.sock then
        terror(TTransportException:new{
            message = 'Could not create ngx.socket.tcp instance'
        })
    end

    local ok, err = self.sock:connect(self.host, self.port)
    if not ok then
        self.sock:close()
        self.sock = nil    
        -- only print log?
        terror(TTransportException:new{
            message = 'Could not connect to ' .. self.host .. ':' .. self.port
            .. ' (' .. (err or "nil") .. ')'
        })
    end
end

function _M.read(self, len)
    local buf, err = self.sock:receive(len)
    if not buf then
        terror(TTransportException:new{errorCode = TTransportException.SOCKET_RECV_FAIL, descrip=err})
    end
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "TCosocket: a chunk has been read, length=", len)
    return buf
end

function _M.write(self, buf)
    local bytes, err = self.sock:send(buf)
    if not bytes then
        terror(TTransportException:new{errorCode = TTransportException.SOCKET_SEND_FAIL, descrip=err})
    end
end

function _M.flush(self)
end

function _M.close(self)
    if self.sock then
        self.sock:close()
        self.sock = nil
    end
end

function _M.setkeepalive(self, ...)
    if not self.sock then
        return
    end

    local reuse, err = self.sock:getreusedtimes()   --check reuse times before close
    if reuse ~= nil then
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "tcosocket http connection reused count: "..(reuse or "nil"))
        -- defalust 60s keepalvie timeout,30 connections
        local ok,err = self.sock:setkeepalive(...)
        if not ok then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", "tcosocket setkeepalive err is :" .. err)
            self.sock:close()
        end
    else
        self.sock:close()
        ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ",  "tcosocket http connection reused err: ".. err)
    end
    self.sock = nil
end

return _M
