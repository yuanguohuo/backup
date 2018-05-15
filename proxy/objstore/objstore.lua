-- By Yuanguo, 29/11/2017

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

local ok, cephconf = pcall(require, "ceph_conf")
if not ok or not cephconf then
    error("failed to load ceph_conf:" .. (cephconf or "nil"))
end

local ok, Ceph = pcall(require, "objstore.ceph")
if not ok or not Ceph then
    error("failed to load objstore.ceph: " .. (Ceph or "nil"))
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
local hash =  utils.hash 
local timestamp = utils.timestamp

local setmetatable = setmetatable
local ngx = ngx

local _M = new_tab(0, 7)
_M._VERSION = '0.1'

local function weight_distribute(self)
    -- save the writeable ceph-cluster in table 'writeable';
    local writeable = new_tab(32,0)
    local total_weight = 0
    for _,ceph in pairs(self.cephmap) do
        if "OK" == ceph.state then
            if not ceph.weight then
                ceph.weight = 10
            end
            if ceph.weight > 0 then
                total_weight = total_weight + ceph.weight
                table.insert(writeable, ceph)
            end
        end
    end

    assert(total_weight > 0)

    -- for example, there are 3 ceph-clusters in table 'writeable', whose weight are: 10,60,30
    --     then total_weight is 100, and each of them will get a range:
    --          [0,0.1)
    --          [0.1, 0.7)
    --          [0.7, 1)
    local m = 0
    for _,ceph in ipairs(writeable) do
        ceph["range_beg"] = m
        m = m + ceph.weight/total_weight
        ceph["range_end"] = m
    end

    self.writeable = writeable
end

local function load_from_hbase(self)
    local now = timestamp()
    if self.hbase_loading then
        local elapsed = now - self.hbase_load_time
        if elapsed <= 180000 then  -- 3 minutes
            ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " previous load_from_hbase is in progress")
            return 1
        else
            ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " previous load_from_hbase has taken more than 3 minutes, let me retry ...")
        end
    else
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " load_from_hbase is NOT in progress, let me load it ...")
        self.hbase_loading = true
    end

    self.hbase_load_time = now
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " load_from_hbase start, now=", now)

    -- load centralized configuration from hbase to overwrite the default configuration; if the 
    -- load fails, default configuration will be used.
    local code,ok,cephs = metadb:quickScan("ceph", metadb.MIN_CHAR, metadb.MAX_CHAR, nil, 100)
    if not ok then
        self.hbase_loaded = false
        self.hbase_loading = false
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " load_from_hbase failed, failed to scan ceph list, keep original cephmap: ", stringify(self))
        return 2
    end

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ceph stores configured in hbase: ", stringify(cephs))

    if nil == cephs or nil == next(cephs) then  -- hbase not configured; return success.
        self.hbase_loaded = true
        self.hbase_loading = false
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " load_from_hbase succeeded, but no ceph configured in hbase, keep original cephmap: ", stringify(self))
        return 0
    end

    local cephmap = new_tab(0,7)
    local succ = true

    for _,c in ipairs(cephs) do
        local cid = c.key
        local ceph = Ceph:new(cid, c.values["info:state"], c.values["info:weight"])

        local ok = ceph:load_from_hbase()
        if not ok then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to load ", cid, " from hbase")
            succ = false
            break
        else
            cephmap[cid] = ceph
        end
    end

    if not succ then
        self.hbase_loaded = false
        self.hbase_loading = false
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " load_from_hbase failed, failed to load a ceph cluster, keep original cephmap: ", stringify(self))
        return 3
    else
        self.cephmap = cephmap
        weight_distribute(self)
        self.hbase_loaded = true
        self.hbase_loading = false
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " load_from_hbase succeeded, new cephmap: ", stringify(self))
        return 0
    end
end

local function check_loaded(self)
    if not self.hbase_loaded then
        load_from_hbase(self)
    end
end

function _M.new(self)
    -- load the default configuration from ceph_conf;
    local cmap = new_tab(0,7)

    for cid,cconf in pairs(cephconf.cephlist) do
        local c = Ceph:new(cid, cconf.state, cconf.weight)
        local rlist = new_tab(128,0)
        for _,r in ipairs(cconf.rgwlist) do
            local gateway = rgwapi:new(r[1], r[2])
            table.insert(rlist, gateway)
        end
        c.rgwlist = rlist
        cmap[cid] = c
    end

    local obj = 
    {
        cephmap = cmap,
        writeable = nil,
        hbase_loaded = false,
        hbase_loading = false,
        hbase_load_time = 0,
    }

    weight_distribute(obj)

    setmetatable(obj, {__index = self})

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " objstore:new(), default: ", stringify(obj))
    return obj
end

--pick a ceph for write;
function _M.pick_store(self, obj)
    check_loaded(self)

    local num_writeable = #self.writeable
    assert(num_writeable > 0)

    local key_hash = hash(obj)   -- [0,65536)
    local seed = key_hash/65536  -- [0,1)

    --we don't have many ceph-clusters (at most 8 or 10 for example), so the loop should not be a problem;
    for _,ceph in ipairs(self.writeable) do
        if ceph["range_beg"] <= seed and seed < ceph["range_end"] then
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " obj=", obj, " key_hash=", key_hash, " seed=", seed, " objstore picked: ", ceph.id)
            return ceph
        end
    end

    --didn't get one, maybe becuase of possible gap in ranges
    local i = (key_hash % num_writeable) + 1
    local ceph = self.writeable[i]
    assert(ceph)

    ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " objstore picked: ", ceph.id)
    return ceph
end

--get a ceph based on the given id;
function _M.get_store(self, id)
    check_loaded(self)

    local ceph = self.cephmap[id]
    assert(ceph)
    return ceph
end

return _M
