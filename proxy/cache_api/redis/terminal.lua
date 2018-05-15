-- By Yuanguo, 27/9/2016
--
-- Terminal is an abstract of the session which is started when you enter "redis-cli" and 
-- terminated when you enter "exit". And essentially, the session is "a connection on a node".
-- You can create more than one terminals (connections) on the single node. Why do we need that?
-- Answer: it's needed by transaction-operations: two concurrent transactions cannot share a single connection. 

local ok, new_tab = pcall(require, "table.new")
if not ok or type(new_tab) ~= "function" then
    new_tab = function (narr, nrec) return {} end
end

local TcpSock = require("common.tcpsock")
if not ok or not TcpSock then
    error("failed to load common.tcpsock:" .. (TcpSock or "nil"))
end

local utils = require("common.utils")
if not ok or not utils then
    error("failed to load common.utils:" .. (utils or "nil"))
end

local _M = new_tab(0, 7)
_M._VERSION = '0.1'

function _M.new(self, node)
    local tcpSock,err = TcpSock:new(node:get_addr())
    if not tcpSock then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to create tcpSock: ", err)
        return nil, err
    end

    return setmetatable(
               {node = node, tcpSock = tcpSock},
               { __index = self }
           )
end

function _M.connect(self)
    local ok,err = self.tcpSock:connect()
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " tcpSock connect failed to connect ", self.node:get_addr(), ": ", err)
        return false, err
    end
    return true,"SUCCESS"
end

function _M.get_addr(self)
    return self.node:get_addr()
end

function _M.do_cmd(self, ...)
    if not self.tcpSock:is_connected() then
        return nil, "Terminal not connected"
    end
    return self.node:do_cmd(self.tcpSock, ...)
end

function _M.setkeepalive(self)
    self.tcpSock:setkeepalive()
    return true,"SUCCESS"
end

function _M.close(self)
    self.tcpSock:close()
    return true,"SUCCESS"
end

return _M
