--[[
  Copyright (c) 2016. Adobe Systems Incorporated. All rights reserved.

    This file is licensed to you under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software distributed under the License is
    distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR RESPRESENTATIONS OF ANY KIND,
    either express or implied.  See the License for the specific language governing permissions and
    limitations under the License.

  ]]

--
-- This modules is a wrapper for the lua-resty-http (https://github.com/pintsized/lua-resty-http) library
-- exposing the "request" method to be compatible with the embedded http client (api-gateway.aws.httpclient.http)
-- User: ddascal
-- Date: 08/03/16
--

local _M = {}
local http = require "resty.http"

function _M:new(o)
    local o = o or {}
    setmetatable(o, self)
    self.__index = self
    return o
end

function _M:request( req )
    local ok, code
    local httpc = http.new()
    httpc:set_timeout(req.timeout or 60000)

    local c, err = httpc:connect(req.host, req.port)
    if not c then
        --return nil, err
        return false, err, nil, err, nil
    end

    if req.scheme == "https" then
        local verify = true
        if req.ssl_verify == false then
            verify = false
        end
        local ok, err = self:ssl_handshake(nil, req.host, verify)
        if not ok then
        --    return nil, err
        return false, err, nil, err, nil
        end
    end

    local res, err = httpc:request({
        path = req.url,
        method = req.method,
        body = req.body,
        headers = req.headers,
        ssl_verify = false
     })
    if not res then
        --return nil, err
        return false, err, nil, err, nil
    end

    local body, err = res:read_body()
    if not body then
        --return nil, err
        return false, err, nil, err, nil
    end

    res.body = body

--    local ok, err = httpc:set_keepalive()
    local ok, err =httpc:set_keepalive(req.keepalive or 3000 , req.poolsize or 100)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  err)
    end

--    return res, nil

--
---- request_uri have set keepalive,so no need to set below
--    local res, err = httpc:request_uri(req.scheme .. "://" .. req.host .. ":" .. req.port, {
--        path = req.url,
--        method = req.method,
--        body = req.body,
--        headers = req.headers,
--        ssl_verify = false
--    })
--
--    --ok, err = httpc:set_keepalive(max_idle_timeout, pool_size)
--    if not res then
--        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "failed to make request: ", err)
--        return false, err, nil, err, nil
--    end
--
----    httpc:set_keepalive(req.keepalive or 3000 , req.poolsize or 100)
    return ok, res.status, res.headers, res.status, res.body
end

return _M

