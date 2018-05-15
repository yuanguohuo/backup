-- By Yuanguo, 22/7/2016

local ok, new_tab = pcall(require, "table.new")
if not ok or type(new_tab) ~= "function" then
    new_tab = function (narr, nrec) return {} end
end

local utils = require("common.utils")
if not ok or not utils then
    error("failed to load common.utils:" .. (utils or "nil"))
end

local TcpSock = require("common.tcpsock")
if not ok or not TcpSock then
    error("failed to load common.tcpsock:" .. (TcpSock or "nil"))
end

local _M = new_tab(0, 9)
_M._VERSION = '0.1'

local function _gen_req(args)
    local nargs = #args

    local req = new_tab(nargs * 5 + 1, 0)
    req[1] = "*" .. nargs .. "\r\n"
    local nbits = 2

    for i = 1, nargs do
        local arg = args[i]
        if type(arg) ~= "string" then
            arg = tostring(arg)
        end

        req[nbits] = "$"
        req[nbits + 1] = #arg
        req[nbits + 2] = "\r\n"
        req[nbits + 3] = arg
        req[nbits + 4] = "\r\n"

        nbits = nbits + 5
    end

    -- it is much faster to do string concatenation on the C land
    -- in real world (large number of strings in the Lua VM)
    return req
end

local function _read_reply(self, tcpSock)
    local line, err = tcpSock:receive()
    if not line then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " tcpSock receive failed: ", err)
        return nil, err
    end
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "line="..line)

    local prefix = string.byte(line)

    if prefix == 36 then    -- char '$'
        -- print("bulk reply")
        local size = tonumber(string.sub(line, 2))
        if size < 0 then
            return ngx.null
        end

        local data, err = tcpSock:receive(size)
        if not data then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " tcpSock receive failed: ", err, " size=", size)
            return nil, err
        end

        local dummy, err = tcpSock:receive(2) -- ignore CRLF
        if not dummy then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " tcpSock receive failed: ", err, ", failed to ignore CRLF")
            return nil, err
        end
        return data
    elseif prefix == 43 then    -- char '+'
        -- print("status reply")

        return string.sub(line, 2)

    elseif prefix == 42 then -- char '*'
        local n = tonumber(string.sub(line, 2))

        -- print("multi-bulk reply: ", n)
        if n < 0 then
            return ngx.null
        end

        local vals = new_tab(n, 0)
        local nvals = 0
        for i = 1, n do
            local res, err = _read_reply(self, tcpSock)
            if res then
                nvals = nvals + 1
                vals[nvals] = res

            elseif res == nil then
                return nil, err

            else
                -- be a valid redis error value
                nvals = nvals + 1
                vals[nvals] = {false, err}
            end
        end

        return vals

    elseif prefix == 58 then    -- char ':'
        -- print("integer reply")
        return tonumber(string.sub(line, 2))

    elseif prefix == 45 then    -- char '-'
        -- print("error reply: ", n)

        return false, string.sub(line, 2)

    else
        -- when `line` is an empty string, `prefix` will be equal to nil.
        return nil, "unkown prefix: \"" .. tostring(prefix) .. "\""
    end
end

function _M.new(self, addr)
    return setmetatable(
               {addr = addr},
               { __index = self }
           )
end

function _M.get_addr(self)
    return self.addr
end

--When a cmd is done, there are 3 cases about the tcpSock
--   1. succeess: the connection is alive;
--   2. non fatal error (read timeout): the connection is alive;
--   3. fatal error: the socket object is destroyed;
--in either case, we can call setkeepalive/close safely.
function _M.do_cmd(self, tcpSock, ...)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "Enter node:do_cmd()")
    local args = {...}
    local req = _gen_req(args)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "req="..utils.stringify(req))

    local bytes, err = tcpSock:send(req)
    if not bytes then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "failed to send request")
        return nil, err
    end

    local res, err = _read_reply(self, tcpSock)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "res="..utils.stringify(res))

    if not res then
        if err and string.find(err, "NOSCRIPT No matching script") then
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  err)
        else
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "failed to receive reply. err="..utils.stringify(err))
        end
        return nil, err
    end

    return res, err
end

return _M
