-- By Yuanguo, 21/7/2016

local NUM_SLOT = 16384

-- We need to refresh the slotmap in the following 2 cases:
--   1. When we get a "MOVED" error;
--   2. When the total error reaches a threshold;
-- Case 1 is straightforward; while the logic for case 2 is:
--   if a master node is down and fails over to a slave, error (not MOVED) will
--   occurr because the slotmap is outdated. Thus, if the total error number 
--   reaches a given threshold, a refresh should be done:
--         threshold = errnum2refresh * {number of master nodes}
--   ERRNUM2REFRESH is the default value for errnum2refresh; caller may
--   overwrite the value by function:
--         set_errnum2refresh()
--
local ERRNUM2REFRESH = 600

local ok, new_tab = pcall(require, "table.new")
if not ok or type(new_tab) ~= "function" then
    new_tab = function (narr, nrec) return {} end
end

local ok, node = pcall(require, "redis.node")
if not ok or not node then
    error("failed to load redis.node:" .. (node or "nil"))
end

local ok, terminal = pcall(require, "redis.terminal")
if not ok or not terminal then
    error("failed to load redis.terminal:" .. (terminal or "nil"))
end

local ok, crc16 = pcall(require, "redis.crc16")
if not ok or not crc16 then
    error("failed to load redis.crc16:" .. (crc16 or "nil"))
end

local utils = require("common.utils")
if not ok or not utils then
    error("failed to load common.utils:" .. (utils or "nil"))
end

local ok,uuid = pcall(require, "resty.uuid")
if not ok or not uuid then
    error("failed to load resty.uuid:" .. (uuid or "nil"))
end


local _M = new_tab(0, 9)
_M._VERSION = '0.1'


function _M.new(self, ...)
    local nodemap = new_tab(0, 11)
    local args = {...}
    for i = 1, #args do
        local node_key = args[i]  -- node_key is "unix socket" or "host:port"

        ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " ",  "node_key: "..(node_key or "nil"))

        nodemap[node_key] = node:new(node_key)
        if not nodemap[node_key] then
            return nil,"failed to create node " .. (node_key or "nil")
        end
    end

    local slotmap = new_tab(NUM_SLOT, 0)

    local cluster_id = "redis-cluster-"..uuid()

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " create ", cluster_id)

    return setmetatable(
               {cluster_id = cluster_id, nodemap = nodemap, nodenum = 0, slotmap = slotmap, needrefresh = true, errnum2refresh = ERRNUM2REFRESH, errnum = 0},
               {__index = self}
           )
end

function _M.refresh(self)
    local got = false
    self.nodenum = 0
    ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " Enter refresh ", self.cluster_id, ". Initial nodemap: ", utils.stringify(self.nodemap))

    --when one coroutine is refreshing, other coroutines should not do that again.
    --so we set the flag to false. 
    --And if refresh fails, we set it to true, so that other coroutines have chances to retry refreshing.
    self.needrefresh = false
    self.errnum = 0

    for nodekey,nd in pairs(self.nodemap) do
        ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " ",  "connect "..(nodekey or "nil").. " to get node map")

        local res = nil

        local term = terminal:new(nd)
        local ok,err = term:connect()

        if ok then
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  "succeeded to connect "..nodekey)
            res,err = term:do_cmd("cluster", "nodes")
        else
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "failed to connect "..nodekey)
            self.nodemap[nodekey] = nil
        end
        
        term:setkeepalive()

        if not res then   -- failed to get node map 
            ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ",  "failed to get node map from " .. (nodekey or "nil"))
        else              -- succeeded to get node map
            ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " ",  "succeeded to get node map from " .. (nodekey or "nil"))
            --[[
            an example of node map:
            62f05cdf4bcaad8fcfa0cc2c32ac8e19e1c538b9 127.0.0.1:7001 master - 0 1469374853889 2 connected 6462-10922
            82ef688fd2bb3de2ae2e503925c5cf78ae8ce320 127.0.0.1:7000 myself,master - 0 0 1 connected 999-5460
            5870f4c91d1cab3cdd213e81f99f0a1d8c783a35 127.0.0.1:7004 slave 62f05cdf4bcaad8fcfa0cc2c32ac8e19e1c538b9 0 1469374855404 5 connected
            ad1538a53ff6cf1db8a0f59bcb9c88f307cbc07d 127.0.0.1:7005 slave 83ab234d0dd5a0d4423bfb2033cedab33bf18e5e 0 1469374854395 7 connected
            83ab234d0dd5a0d4423bfb2033cedab33bf18e5e 127.0.0.1:7002 master - 0 1469374855909 7 connected 0-998 5461-6461 10923-16383
            4834b7f8a6f418e3d1006513561149e580768d5b 127.0.0.1:7003 slave 82ef688fd2bb3de2ae2e503925c5cf78ae8ce320 0 1469374854898 4 connected

            fields of each line:
            1. Node ID
            2. ip:port
            3. flags: master, slave, myself, fail, ...
            4. if it is a slave, the Node ID of the master
            5. Time of the last pending PING still waiting for a reply.
            6. Time of the last PONG received.
            7. Configuration epoch for this node (see the Cluster specification).
            8. Status of the link to this node.
            9. Slots served...
            --]]
            for line in string.gmatch(res, "[^\r\n]+") do
                ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " ",  "line: ".. (line or "nil"))
                local nid,nkey,nflags,master,tlast_ping,tlast_pong,conf_epoch,status = 
                      string.match(line,"([^%s]+)[%s]+([^%s]+)[%s]+([^%s]+)[%s]+([^%s]+)[%s]+([^%s]+)[%s]+([^%s]+)[%s]+([^%s]+)[%s]+([^%s]+)")

                ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " ",  "    nodeId     :  " .. (nid or "nil"))
                ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " ",  "    nodeKey    :  " .. (nkey or "nil"))
                ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " ",  "    flags      :  " .. (nflags or "nil"))
                ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " ",  "    masterID   :  " .. (master or "nil"))
                ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " ",  "    tlastPing  :  " .. (tlast_ping or "nil"))
                ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " ",  "    tlastPong  :  " .. (tlast_pong or "nil"))
                ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " ",  "    conf_epoch :  " .. (conf_epoch or "nil"))
                ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " ",  "    status     :  " .. (status or "nil"))

                if nid then   -- current line is a valid line
                    local i,j = string.find(nflags, "master") 
                    if i then -- current line is master
			    -- nkey /ipaddr use 127.0.0.1,should use origin node augremnt or redis cluster should reply non-local ipaddr.
                        self.nodemap[nkey] = node:new(nkey)
                        if not self.nodemap[nkey] then
                            return nil,"failed to create node " .. (nkey or "nil")
                        end
                        self.nodenum = self.nodenum + 1

                        local s,p = string.find(line,status)
                        local slotRanges = string.sub(line, p+1)   -- get everything after status field, that's slot ranges
                        ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " ",  "    slotRanges :  " .. (slotRanges or "nil"))

                        for range in string.gmatch(slotRanges, "[^ ]+") do  -- slotRanges is a string like "0-998 5461-6461 10923-16383, we get each range". so 10923-16383 [3372-<-29bcb8f75d8c3230d0090a0106828249f4d58e8c]
                            local s,e = string.match(range, "(%d+)-(%d+)")
                            ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " ",  "        range      :  " .. (s or "nil") .. "-" .. (e or "nil") .. "==> node:" .. (nkey or "nil"))
                            if nil ~= s and nil ~= e then
                                for k = s, e do
                                    self.slotmap[k] = self.nodemap[nkey]
                                end
                            end
                        end
                    else  -- current line is slave
                        ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " ",  "    skip slave " .. (nkey or "nil"))
                    end
                end
            end
            got = true
            break
        end
    end
    if not got then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " Exit refresh ", self.cluster_id, ". FAILED. Finial nodemap: ", utils.stringify(self.nodemap))
        self.needrefresh = true    -- refresh failed. let other coroutines retry ...
        self.errnum = 0
        return nil, "command 'cluster nodes' failed on all nodes"
    end

    ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " Exit refresh ", self.cluster_id, ". SUCCEEDED. Finial nodemap: ", utils.stringify(self.nodemap))
    return true, "SUCCESS"
end


-- If the total error number reaches a given threshold, a refresh should be done.
--         threshold = errnum2refresh * {number of master nodes}
-- this function is to set errnum2refresh, whose default value is ERRNUM2REFRESH
function _M.set_errnum2refresh(self, num)
    self.errnum2refresh = (num or ERRNUM2REFRESH) 
end

-- Yuanguo: according to the Redis Cluster specification: when calculating the
-- slot of a given key, if there is a hash tag (substring inside {}) in the key,
-- only the hash tag is hashed;
-- Yuanguo: if there is nothing inside {}, I don't think it's a valid hash tag,
-- and thus, I still hash the whole key.
local function _get_slot(key)
    local hashtag = string.match(key,"{(.+)}")
    if not hashtag then
        hashtag = key
    end
    return crc16:crc({hashtag:byte(1,-1)}) % NUM_SLOT
end

function _M.create_terminal(self,key)
    local slot = _get_slot(key)

    local nd = self.slotmap[slot]
    if not nd then
        ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ",  "there is no node for slot "..tostring(slot)..", refresh and retry")
        local ok,err = _M.refresh(self)
        if not ok then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "refresh failed: "..(err or "nil"))
            return nil, "no node for slot "..tostring(slot).." and refresh failed"
        end
        self.needrefresh = false
        self.errnum = 0

        nd = self.slotmap[slot]
        if not nd then
            return nil, "still no node for slot "..tostring(slot).." after a refresh"
        end
    end

    ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " slot=", slot, " node=", nd:get_addr())

    return terminal:new(nd)
end

local function check_cmd(cmd)
    --the tested commands. other command may also be added into this 
    --list once tested;
    if  cmd ~= "set"     and
        cmd ~= "hset"    and
        cmd ~= "get"     and
        cmd ~= "hget"    and
        cmd ~= "mget"    and
        cmd ~= "hmget"   and
        cmd ~= "mset"    and
        cmd ~= "hmset"   and
        cmd ~= "del"     and
        cmd ~= "hdel"    and
        cmd ~= "exists"  and
        cmd ~= "hexists" and
        cmd ~= "scan"    and
        cmd ~= "hscan"   and
        cmd ~= "hgetall" and
        cmd ~= "incr"    and
        cmd ~= "incrby"  and
        cmd ~= "expire"  and
        cmd ~= "multi"   and
        cmd ~= "exec"    and
        cmd ~= "watch"   and
        cmd ~= "unwatch" and
        cmd ~= "getset"  and
        cmd ~= "discard" and
        cmd ~= "eval"    and
        cmd ~= "evalsha" then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "command " .. (cmd or "nil") .. " not supported")
            return nil, "Command Not Supported"
    end
    return true,"SUCCESS"
end

--This function is for transaction operations (operations are related to one another 
--forming a transaction, e.g. watch, get, multi, set, exec); for single commands, please
--use do_cmd_quick() instead.
--To use this function (do_cmd function) you need:
--    1. term = cluster:create_terminal(key)
--    2. term.connect() 
--    3. cluster:do_cmd(term, watch, ...)    --> this function;
--       cluster:do_cmd(term, multi)         --> this function;
--       cluster:do_cmd(term, set, ...)      --> this function;
--       ...                                     --> more operations by this function;
--       cluster:do_cmd(term, exec)          --> this function;
--    4. term:setkeepalive() or term:close()
--return: res,err.
--    Note: if err == "KEY_MOVED", you should:
--       a. term:do_cmd("discard") if you have started a transaction;
--       b. term:do_cmd("unwatch") if you have watched some keys
--       c. term:setkeepalive() or term:close()
--       d. create a new teminal and retry your operations;
function _M.do_cmd(self, term, ...)
    if self.needrefresh then
        ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " need to refresh slotmap ...")
        local ok,err = _M.refresh(self)
        if ok then
            ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ",  "refresh success")
            self.needrefresh = false
            self.errnum = 0
        else
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "refresh failed: "..(err or "nil"))
        end
    end

    local args = {...}
    local cmd = string.lower(args[1])
    local ok,err = check_cmd(cmd)
    if not ok then
        return nil,err
    end

    local res,err = term:do_cmd(...)

    if not res then
        if string.match(err,"MOVED%s+%d+%s+") then 
            ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ",  "key moved, need refresh")
            self.needrefresh = true
            self.errnum = 0
            err = "KEY_MOVED"
        else -- not the "MOVED" error
            self.errnum = self.errnum + 1
            local total_errnum2refresh = self.errnum2refresh * self.nodenum 
            if self.errnum >= total_errnum2refresh then
                ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ",  "too many errors ("..self.errnum.."), need refresh")
                self.needrefresh = true
                self.errnum = 0
            end
        end
    end

    --[[
    if res == ngx.null then
        res = nil
    end
    --]]

    return res, err
end

local function internal_do_cmd(self, key, ...)
    local term,err= _M.create_terminal(self, key)
    if not term then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "failed to create terminal for key ", key, ": " , err)
        return nil, "failed to create terminal", false  --false, no need to retry
    end

    local ok,err = term:connect()
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "terminal failed to connect ", term:get_addr(), ": ", err)
        local ok,err = _M.refresh(self)
        if ok then
            ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ",  "refresh success")
        else
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "refresh failed: "..(err or "nil"))
            return nil, "terminal connect failed", false    --false, no need to retry
        end
        return nil, "terminal connect failed", true    --false, need to retry
    end

    local res,err = _M.do_cmd(self, term, ...)

    term:setkeepalive()

    local retry = false
    if not res then
        if err == "KEY_MOVED" then  --failed, and err is "KEY_MOVED", need retry
            ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ",  "KEY_MOVED error found, need to retry")
            retry = true
        else
            if err and string.find(err, "NOSCRIPT No matching script") then
                ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  err)
            else
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "do_cmd failed, and no need to retry: "..(err or "nil"))
            end
        end
    end

    return res,err,retry
end

function _M.do_cmd_quick(self, ...)
    local args = {...}
     
    ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " ", "enter do_cmd_quick, args: ", utils.stringify(args))

    if nil == next(args) then
        return nil, "Invalid Command: Command missing"
    end

    local cmd = string.lower(args[1])

    if cmd == "multi" or cmd == "exec" or cmd == "watch" or cmd == "unwatch" or cmd == "discard" then
        return nil, "Command Not Supported: use do_cmd() instead"
    end

    if cmd == "eval" then
        return nil, "Command Not Supported: use eval() or do_cmd() instead"
    end

    local key = args[2]
    if not key then
        return nil, "Invalid Command: Key missing"
    end

    local res,err,retry = internal_do_cmd(self, key, ...)
    if not retry then
        return res,err
    end

    --needrefresh must have been set to true above, so we just need to call internal_do_cmd again,
    --in do_cmd(), refresh will be called.
    ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " ",  "retry internal_do_cmd")
    local res,err,retry = internal_do_cmd(self, key, ...)

    return res,err
end

--Params:
--    script : a string containing the content of the script
--    num    : number of keys;
--    ...    : the keys and arguments that are passed to the script;
--Yuanguo: Note that if you pass multiple keys to the script, make sure they are in the
--         same slot. You can achieve this by "{}".
function _M.eval(self, script, num, ...)
    local vars = {...}
    local key = vars[1]   -- the first key

    local res,err,retry = internal_do_cmd(self, key, "eval", script, num, ...)

    if not retry then
        return res,err
    end

    --needrefresh must have been set to true above, so we just need to call internal_do_cmd again,
    --in do_cmd(), refresh will be called.
    ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " ",  "retry internal_do_cmd")
    local res,err,retry = internal_do_cmd(self, key, "eval", script, num, ...)

    return res,err
end

--Params:
--    script : the script
--    sha    : sha of a script;
--    num    : number of keys;
--    ...    : the keys and arguments that are passed to the script;
--Yuanguo: 1. Note that if you pass multiple keys to the script, make sure they are in the
--            same slot. You can achieve this by "{}".
--         2. In this function, 'evalsha' is tried first, if error "NOSCRIPT No matching 
--            script. Please use EVAL" is encountered, 'eval' will be used instead. That's
--            why we need both the script and its sha1 as the parameters.
--Return:
--  res : ret value;
--  err : description of error; 
--  eval_used: if true, 'evalsha' failed and 'eval' is used instead.
function _M.evalsha(self, script, sha, num, ...)
    local vars = {...}
    local key = vars[1]   -- the first key

    local res,err,retry = nil,nil,nil
    
    res,err,retry = internal_do_cmd(self, key, "evalsha", sha, num, ...)

    if retry then
        res,err = internal_do_cmd(self, key, "evalsha", sha, num, ...)
    end

    local eval_used = false

    if err == "NOSCRIPT No matching script. Please use EVAL." then
        ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " evalsha failed with 'NOSCRIPT No matching script', use 'eval' command. script:", script)
        res,err = internal_do_cmd(self, key, "eval", script, num, ...)
        eval_used = true
    end

    return res,err,eval_used
end

return _M
