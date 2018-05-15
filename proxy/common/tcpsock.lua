-- By Yuanguo, 22/7/2016

local ok, new_tab = pcall(require, "table.new")
if not ok or type(new_tab) ~= "function" then
    new_tab = function (narr, nrec) return {} end
end

local func_create_sock = ngx.socket.tcp

local _M = new_tab(0, 16)
_M._VERSION = '0.1'

local SOCK_TYPE_INET = 1
local SOCK_TYPE_UNIX = 2

--params:
--  addr: "ip:port" for INET; unix sock path for UNIX SOCKET;
function _M.new(self, addr)
    local host = nil
    local port = nil
    local unisockpath = nil
    local sock_type = nil

    local i,j = string.find(addr, ":")
    if not i then  --unix sock
        unisockpath = addr
        sock_type = SOCK_TYPE_UNIX
    else
        host = string.sub(addr,1,i-1)
        port = string.sub(addr,i+1,-1)
        if not tonumber(port) then
            return nil, "invalid address " .. addr .. ": port "..(port or "nil")
        end
        sock_type = SOCK_TYPE_INET
    end

    local sock = func_create_sock()

    return setmetatable(
               {sock = sock, sock_type = sock_type, host = host, port = port, unisockpath = unisockpath, sock_key = addr, connected = false},
               {__index = self}
           )
end

function _M.connect(self)
    if nil == self.sock then
        self.sock = func_create_sock()
        self.connected = false
        ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " socket recreated: ", self.sock_key)
    end

    if self.connected then
        return true, "already connected"
    end

    local ok, err = nil, nil

    if SOCK_TYPE_INET == self.sock_type then
        ok,err = self.sock:connect(self.host, self.port)
    else
        ok,err = self.sock:connect(self.unisockpath)
    end
    
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " socket failed to connect ", self.sock_key, ": ", err)
        --fatal error (read timeout error is the only error that is not fatal), so connection is closed automatically. 
        --we force recreate the socket
        self.sock = nil
        self.connected = false
        return false, err
    end

    self.connected = true
    return true, "SUCCESS"
end

function _M.is_connected(self)
    if not self.sock then
        return false
    end
    return self.connected
end

function _M.get_reused_times(self)
    if not self.sock or not self.connected then
        return nil, "socket is not created or not connected"
    end
    return self.sock:getreusedtimes()
end

--params:
--  arg: if number-like, then it is interpreted as a size, will not return until it reads exactly this size of data or an error occurs;
--       if non-number-like, then it is interpreted as a "pattern":
--           '*a': reads from the socket until the connection is closed. No end-of-line translation is performed;
--           '*l': reads a line of text from the socket. The line is terminated by a Line Feed (LF) character (ASCII 10), 
--                 optionally preceded by a Carriage Return (CR) character (ASCII 13). The CR and LF characters are not 
--                 included in the returned line. In fact, all CR characters are ignored by the pattern. 
--       if no argument is specified, then it is assumed to be the pattern '*l', that is, the line reading pattern.
function _M.receive(self, ...)
    if not self.sock or not self.connected then
        return nil, "not connected", nil
    end

    --we don't call settimeout. the timeout is controlled by lua_socket_read_timeout config directive;
    local data,err,partial = self.sock:receive(...)
    if err then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " socket receive failed: ", self.sock_key, " err: ", err)
        if err ~= "timeout" then
            --fatal error (read timeout error is the only error that is not fatal), so connection is closed automatically. 
            --we force recreate the socket
            self.sock = nil
            self.connected = false
            return nil, err, partial
        end
        -- timeout, non-fatal error, connection is not closed; we just return the error and the parital data;
        return nil, err, partial
    end

    return data, nil, nil
end

function _M.send(self, data)
    if not self.sock or not self.connected then
        return nil, "not connected"
    end

    --we don't call settimeout. the timeout is controlled by lua_socket_read_timeout config directive;
    local bytes,err = self.sock:send(data)
    if err then
        --fatal error (read timeout error is the only error that is not fatal), so connection is closed automatically. 
        --we force recreate the socket
        self.sock = nil
        self.connected = false
        return nil, err
    end

    return bytes, nil
end

function _M.setkeepalive(self)
    if not self.sock then
        return true, "socket is not created"
    end

    if not self.connected then
        return true, "socket is not connected"
    end

    --timeout: the maximal idle timeout (in milliseconds) for the current connection.
    --         if omitted, the default setting in the lua_socket_keepalive_timeout config directive will be used.
    --         if the 0 value is given, then the timeout interval is unlimited.
    --size   : the maximal number of connections allowed in the connection pool for the current server (i.e., the
    --         current host-port pair or the unix domain socket file path).
    --         note that the size of the connection pool cannot be changed once the pool is created.
    --         when this argument is omitted, the default setting in the lua_socket_pool_size config directive will
    --         be used.
    local ok, err = self.sock:setkeepalive()
    if not ok then
        ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " socket setkeepalive failed: ", self.sock_key, " err=", err)
        --fatal error (read timeout error is the only error that is not fatal), so connection is closed automatically. 
        --we force recreate the socket
        self.sock = nil
        self.connected = false
        return true, "socket destroyed due to error"
    end

    self.connected = false
    return true, "SUCCESS"
end

function _M.close(self)
    if not self.sock then
        return true, "socket is not created"
    end 
    if not self.connected then
        return true, "socket is not connected"
    end

    local ok, err = self.sock:close()
    if not ok then
        ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " socket close failed: ", self.sock_key, " err=", err)
        --fatal error (read timeout error is the only error that is not fatal), so connection is closed automatically. 
        --we force recreate the socket
        self.sock = nil
        self.connected = false
        return true, "socket destroyed due to error: " .. err
    end

    self.connected = false
    return true, "SUCCESS"
end

return _M
