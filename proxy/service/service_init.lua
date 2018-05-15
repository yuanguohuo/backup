local uuid = require 'resty.jit-uuid'
uuid.seed() -- very important!

local sp_conf = require("storageproxy_conf")
local rcache_enabled = sp_conf["config"]["cache_config"]["rcache"]["enabled"]

local ok,inspect = pcall(require, "inspect")
if not ok or not inspect then
    error("failed to load inspect:" .. (inspect or "nil"))
end

local ok, cache_flusher = pcall(require, "cache.flusher")
if not ok or not cache_flusher or type(cache_flusher) ~= "table" then
    error("failed to load cache.flusher:" .. (cache_flusher or "nil"))
end

local ok, recover_worker = pcall(require, "recover.worker")
if not ok or not recover_worker or type(recover_worker) ~= "table" then
    error("failed to load recover.worker:" .. (recover_worker or "nil"))
end


local reqmonit = require("reqmonit")
local service_op = require("service_op")

local new_timer = ngx.timer.at
local log = ngx.log
local ERR = ngx.ERR
local INFO = ngx.INFO
local WARN = ngx.WARN

local stats_delay = 5 -- in seconds
local stats_service
stats_service = function(premature)
    ngx.ctx.reqid = "timer_stats_service"

    if premature then
        log(WARN, "RequestID=", ngx.ctx.reqid, " timer expired prematurely")
        reqmonit.reset_cycle_workload()
        return
    end

    log(INFO, "RequestID=", ngx.ctx.reqid, " start")
    reqmonit.update_cycle_workload()

    local ok, err = new_timer(stats_delay, stats_service)
    if not ok then
        log(ERR, "RequestID=", ngx.ctx.reqid, " failed to create timer: ", err)
        return
    end
end

local expire_delay = sp_conf.config["expire"]["timer_cycle"] -- in seconds
local expire_service
expire_service = function(premature)
    ngx.ctx.reqid = "timer_expire_service_"..ngx.worker.id()

    if premature then
        log(WARN, "RequestID=", ngx.ctx.reqid, " timer expired prematurely")
        return
    end

    log(INFO, "RequestID=", ngx.ctx.reqid, " start")
    local busy = reqmonit.check_system_busy()
    if not busy then
        service_op.expire_objects()
    end
    
    local ok, err = new_timer(expire_delay, expire_service)
    if not ok then
        log(ERR, "RequestID=", ngx.ctx.reqid, " failed to create timer: ", err)
        return
    end
end

--[[
local expire_placeholder_delay = sp_conf.config["expire_placeholder"]["timer_cycle"] -- in seconds
local expire_placeholder_service
expire_placeholder_service = function(premature)
    ngx.ctx.reqid = "timer_expire_placeholder_service_"..ngx.worker.id()

    if premature then
        log(WARN, "RequestID=", ngx.ctx.reqid, " timer expired placeholder  prematurely")
        return
    end

    log(INFO, "RequestID=", ngx.ctx.reqid, " start")
    local busy = reqmonit.check_system_busy()
    if not busy then
        service_op.expire_placeholder_objects()
    end
    
    local ok, err = new_timer(expire_placeholder_delay, expire_placeholder_service)
    if not ok then
        log(ERR, "RequestID=", ngx.ctx.reqid, " failed to create timer: ", err)
        return
    end
end
--]]

local gc_delay = sp_conf.config["deleteobj"]["timer_cycle"]
local gc_service
gc_service = function(premature)
    ngx.ctx.reqid = "timer_gc_service_"..ngx.worker.id()

    if premature then
        log(WARN, "RequestID=", ngx.ctx.reqid, " timer expired prematurely")
        return
    end
    
    log(INFO, "RequestID=", ngx.ctx.reqid, " start")
    local busy = reqmonit.check_system_busy()
    if not busy then
        service_op.gc_objects()
    end
    
    local ok, err = new_timer(gc_delay, gc_service)
    if not ok then
        log(ERR, "RequestID=", ngx.ctx.reqid, " failed to create timer: ", err)
        return
    end
end

local flush_cache_delay = 120 -- in seconds
local flush_cache_service
flush_cache_service = function(premature)
    ngx.ctx.reqid = "timer_flush_cache_service"

    if premature then
        log(WARN, "RequestID=", ngx.ctx.reqid, " timer expired prematurely")
        return
    end

    log(INFO, "RequestID=", ngx.ctx.reqid, " start")
    local ok = cache_flusher.flush()
    if not ok then
        log(ERR, "RequestID=", ngx.ctx.reqid, " failed")
    else
        log(INFO, "RequestID=", ngx.ctx.reqid, " succeeded")
    end
    
    local ok, err = new_timer(flush_cache_delay, flush_cache_service)
    if not ok then
        log(ERR, "RequestID=", ngx.ctx.reqid, " failed to create timer: ", err)
        return
    end
end

--init workload stats
reqmonit.init_dicts()

if 0 == ngx.worker.id() then
    local ok, err = new_timer(stats_delay, stats_service)
    if not ok then
        log(ERR, "RequestID=init-worker-", ngx.worker.id(), " failed to create stats_service timer: ", err)
        return
    end

    if rcache_enabled then
        local ok, err = new_timer(flush_cache_delay, flush_cache_service)
        if not ok then
            log(ERR, "RequestID=init-worker-", ngx.worker.id(), " failed to create flush_cache_service timer: ", err)
            return
        end
    end
end

--math.randomseed(os.time())
local meta_sync_delay = 3
local meta_sync_service
meta_sync_service = function(premature)
    ngx.ctx.reqid = "timer_meta_sync_service_"..ngx.worker.id()
    
    if premature then
        ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ", "meta sync timer expired prematurely")
        return
    end

    service_op.sync_meta()
    
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", "start meta_sync_service") 
   
    local ok, err = new_timer(meta_sync_delay, meta_sync_service)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", "failed to create meta sync timer: ", err)
        return
    end
end

local sync_delay = math.random(1, 15)
local sync_service
sync_service = function(premature)
    ngx.ctx.reqid = "timer_sync_service_"..ngx.worker.id()
    
    if premature then
        ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ", "sync timer expired prematurely")
        return
    end
    
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", "start sync_service") 
    local busy = reqmonit.check_system_busy()
    if not busy then
        service_op.sync_objects()
    end
   
    sync_delay = math.random(1, 15)
    local ok, err = new_timer(sync_delay, sync_service)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", "failed to create sync timer: ", err)
        return
    end
end

if sp_conf.config["sync_config"]["peers"] and next(sp_conf.config["sync_config"]["peers"]) then
    local sync_timers = sp_conf.config["sync_timers"] or 1
    for i=1, sync_timers do
        local ok, err = new_timer(sync_delay, sync_service)
	if not ok then
	    log(ERR, "failed to create timer: ", err)
	    return
        end
    end
end

local ok, err = new_timer(expire_delay, expire_service)
if not ok then
    log(ERR, "RequestID=init-worker-", ngx.worker.id(), " failed to create expire_service timer: ", err)
    return
end

--[[
local ok, err = new_timer(expire_placeholder_delay, expire_placeholder_service)
if not ok then
    log(ERR, "RequestID=init-worker-", ngx.worker.id(), " failed to create expire_placeholder_service timer: ", err)
    return
end
--]]

-- gc  delete objcet
local ok, err = new_timer(gc_delay, gc_service)
    if not ok then
        log(ERR, "RequestID=init-worker-", ngx.worker.id(), " failed to create gc_service timer: ", err)
    return
end

-- full remote data 
local full_delay = sp_conf.config["sync_config"]["full"]["timer_cycle"]
local full_service
full_service = function(premature)
    ngx.ctx.reqid = "timer_full_service_"..ngx.worker.id()

    if premature then
        log(WARN, "RequestID=", ngx.ctx.reqid, " ", " timer expired prematurely")
        return
    end
    
    log(INFO, "RequestID=", ngx.ctx.reqid, " ", " start")
    local busy = reqmonit.check_system_busy()
    if not busy then
        service_op.full_objects()
    end
    
    local ok, err = new_timer(full_delay, full_service)
    if not ok then
        log(ERR, "RequestID=", ngx.ctx.reqid, " ", " failed to create timer: ", err)
        return
    end
end

local ok, err = new_timer(full_delay, full_service)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=init-worker-", ngx.worker.id(), " failed to create full_server timer: ", err)
    return
end

-- delete bucket all objects 
local delete_bucket_delay = sp_conf.config["delete_bucket"]["timer_cycle"]
local delete_bucket_service
delete_bucket_service = function(premature)
    ngx.ctx.reqid = "timer_delete_bucket_service_"..ngx.worker.id()

    if premature then
        ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ", " timer expired prematurely")
        return
    end
    
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", " start")
    local busy = reqmonit.check_system_busy()
    if not busy then
        service_op.gc_bucket()
    end
    
    local ok, err = new_timer(delete_bucket_delay, delete_bucket_service)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", " failed to create timer: ", err)
        return
    end
end

local ok, err = new_timer(delete_bucket_delay, delete_bucket_service)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=init-worker-", ngx.worker.id(), " failed to create delete bucket service timer: ", err)
    return
end

-- delete completed/aborted multiuploads
local delete_mu_delay = sp_conf.config["deletemu"]["timer_cycle"]
local delete_mu_service
delete_mu_service = function(premature)
    ngx.ctx.reqid = "timer_" .. sp_conf.config["deletemu"]["name"] .. "_service_"..ngx.worker.id()

    if premature then
        ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " timer expired prematurely")
        return
    end
    
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " start ")
    local busy = reqmonit.check_system_busy()
    if not busy then
        service_op.delete_mu()
    end
    
    local ok, err = new_timer(delete_mu_delay, delete_mu_service)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to create timer: ", err)
        return
    end
end

local ok, err = new_timer(delete_mu_delay, delete_mu_service)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=init-worker-", ngx.worker.id(), " failed to create timer: ", sp_conf.config["deletemu"]["name"], " err: ", err)
    return
end
