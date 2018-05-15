-- By Yuanguo, 28/11/2017

local ok, new_tab = pcall(require, "table.new")
if not ok or type(new_tab) ~= "function" then
    new_tab = function (narr, nrec) return {} end
end

local ok, metadb = pcall(require, "metadb-instance")
if not ok or not metadb or type(metadb) ~= "table" then
    error("failed to load metadb-instance:" .. (metadb or "nil"))
end

local ok,rgwapi = pcall(require, "objstore.rgw.rgw")
if not ok or not rgwapi then
    error("failed to load objstore.rgw.rgw:" .. (rgwapi or "nil"))
end

local ok,spconf = pcall(require, "storageproxy_conf")
if not ok or not spconf then
    error("failed to load storageproxy_conf:" .. (spconf or "nil"))
end

local ok, cephconf = pcall(require, "ceph_conf")
if not ok or not cephconf then
    error("failed to load ceph_conf:" .. (cephconf or "nil"))
end

local ok,utils = pcall(require, "common.utils")
if not ok or not utils then
    error("failed to load common.utils:" .. (utils or "nil"))
end

local ok, math = pcall(require, "math")
if not ok or not math then
    error("failed to load math:" .. (math or "nil"))
end

local floor = math.floor
local random = utils.random
local stringify = utils.stringify

local retries = spconf.config["rgw_config"]["retry_times"] or 3
local interval = spconf.config["rgw_config"]["retry_interval"] or 6
local local_ceph = cephconf.local_ceph

local setmetatable = setmetatable
local ngx = ngx
local _M = new_tab(0, 7)
_M._VERSION = '0.1'

function _M.new(self, id, state, weight)
    assert(id and "" ~= id)
    local loopback_rgw = nil
    if local_ceph and "" ~= local_ceph then
        loopback_rgw = rgwapi:new("127.0.0.1", "8000")
    end
    local num_weight = tonumber(weight)
    return setmetatable(
               {id = id, state = state, weight = num_weight, loopback_rgw = loopback_rgw, rgwlist = nil},
               {__index = self}
           )
end

function _M.load_from_hbase(self)
    local code,ok,rgws = metadb:quickScan("rgw", self.id.."-"..metadb.MIN_CHAR, self.id.."-"..metadb.MAX_CHAR, nil, 1000)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to scan rgw list from hbase for ", self.id, " code=", code)
        return false
    end

    if nil == rgws or nil == next(rgws) then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " no rgw configured for ", self.id)
        return false
    end

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " rgw configured for ", self.id, ": ", stringify(rgws))

    local rlist = new_tab(128,0)
    for _,r in ipairs(rgws) do
        local rkey = r.key
        local rval = r.values
        local rstate = rval["info:state"]
        if rstate and "OK" ~= rstate then
            ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ", self.id, " skip rgw ", rkey, " because its state is not OK, rstate=", rstate)
        else
            local server_port = string.sub(rkey, #self.id+2, -1)
            local s,e = string.find(server_port, ":")
            assert(s and s==e)

            local server = string.sub(server_port, 1, s-1)
            local port = string.sub(server_port, e+1, -1)
            assert(#server>0 and #port>0)
            
            local gateway = rgwapi:new(server, port)
            table.insert(rlist, gateway)
        end
    end

    if nil == next(rlist) then
        ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " no rgw is in OK state, keep previous rgw list for ", self.id)
        return true
    end

    self.rgwlist = rlist
    return true
end

local function pick_rgw(self, excludes)
    local num = #self.rgwlist

    if num == 1 then -- we have only one rgw, just pick it!
        return self.rgwlist[1]
    end

    local num_excl = 0
    if excludes then
        num_excl = #excludes
    end

    --pick one!
    local i = ((floor(random()*1000)) % num) + 1
    local picked = self.rgwlist[i]

    --if there's no excludes, or we have less rgws than excludes, just use it!
    if num_excl == 0 or num <= num_excl then 
        return picked
    end

    -- there are execludes, and we have more than those excluded, so make sure we pick one that's not excluded;
    while true do
        local excluded = false
        for _,e in pairs(excludes) do
            if e.server == picked.server and e.port == picked.port then
                excluded = true
                break
            end
        end

        if not excluded then
            return picked
        end

        i = i+1
        if i > num then
            i = 1
        end
        picked = self.rgwlist[i]
    end

    --should never reach here!
    assert(false)
end

--Yuanguo: put a list of objects in pipeline.
--params:
--    object : the destination object in rgw.
--    instances : objectname1, body1, objectname2, body2, ... they must be in pairs.
function _M.put_obj(self, object, instances)
    if #instances % 2 ~= 0 then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", self.id, " invalid arguments for put_obj, 'objectname'==>'body' should be in pairs")
        return "2002", false, "Internal Server Error"
    end

    local code,ok,ret = nil,nil,nil

    local excludes = {}
    for retry=1,retries,1 do
        local rgw = nil
        --if the ceph-cluster is local-ceph, then use the loopback_rgw for the 1st retry.
        if retry == 1 and local_ceph == self.id and self.loopback_rgw then
            rgw = self.loopback_rgw
            table.insert(excludes, rgw)
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", self.id, " loopback_rgw is picked for retry=1")
        else
            rgw = pick_rgw(self, excludes)
            table.insert(excludes, rgw)
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", self.id, " rgw ", rgw.server, ":", rgw.port, " is picked for retry=", retry)
        end

        code,ok,ret = rgw:put_obj(object, instances)
        if ok then
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", self.id, " succeeded to put object. retry=", retry, " rgw=", rgw.server, ":", rgw.port)
            return "0000", true, nil
        else
            -- code==2008 is timeout error
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", self.id, " failed to put object. retry=", retry, " rgw=", rgw.server, ":", rgw.port, " code=", code, " ret=", stringify(ret))
            ngx.sleep(interval)
        end
    end

    ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", self.id, " failed to put object after all retries")
    return code,ok,nil
end

--Yuanguo: get a list of objects in pipeline.
--params:
--    object : the destination object in rgw.
--    ...    : the obejct name list 
--return:
--    1. http code
--    2. inner code 
--    3. an array containing the body for each object being read. the order of these body 
--       readers are the same as the object names.
function _M.get_obj(self, object, instances)
    local code,ok,ret = nil,nil,nil

    local excludes = {}
    for retry=1,retries,1 do
        local rgw = nil
        --if the ceph-cluster is local-ceph, then use the loopback_rgw for the 1st retry.
        if retry == 1 and local_ceph == self.id and self.loopback_rgw then
            rgw = self.loopback_rgw
            table.insert(excludes, rgw)
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", self.id, " loopback_rgw is picked for retry=1")
        else
            rgw = pick_rgw(self, excludes)
            table.insert(excludes, rgw)
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", self.id, " rgw ", rgw.server, ":", rgw.port, " is picked for retry=", retry)
        end

        code,ok,ret = rgw:get_obj(object, instances)
        if ok then
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", self.id, " succeeded to get object. retry=", retry, " rgw=", rgw.server, ":", rgw.port)
            return "0000", true, ret
        else
            -- code==2008 is timeout error
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", self.id, " failed to get object. retry=", retry, " rgw=", rgw.server, ":", rgw.port, " code=", code, " ret=", stringify(ret))
            ngx.sleep(interval)
        end
    end

    ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", self.id, " failed to get object after all retries")
    return code,ok,nil
end


--Yuanguo: delete a list of objects in pipeline.
--params:
--    object : the destination object in rgw.
--    ...    : the obj instances list 
--return:
--    1. http code
--    2. inner code 
function _M.delete_obj(self, object, instances)
    local code,ok,ret = nil,nil,nil

    local excludes = {}
    for retry=1,retries,1 do
        local rgw = nil
        --if the ceph-cluster is local-ceph, then use the loopback_rgw for the 1st retry.
        if retry == 1 and local_ceph == self.id and self.loopback_rgw then
            rgw = self.loopback_rgw
            table.insert(excludes, rgw)
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", self.id, " loopback_rgw is picked for retry=1")
        else
            rgw = pick_rgw(self, excludes)
            table.insert(excludes, rgw)
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", self.id, " rgw ", rgw.server, ":", rgw.port, " is picked for retry=", retry)
        end

        code,ok,ret = rgw:delete_obj(object, instances, false)
        if ok then
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", self.id, " succeeded to delete object. retry=", retry, " rgw=", rgw.server, ":", rgw.port)
            return "0000", true
        else
            --code==2008 is timeout error
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", self.id, " failed to delete object. retry=", retry, " rgw=", rgw.server, ":", rgw.port, " code=", code, " ret=", stringify(ret))
            ngx.sleep(interval)
        end
    end

    ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", self.id, " failed to delete object after all retries")
    return code,ok
end

return _M
