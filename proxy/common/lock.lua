--By Yuanguo, 29/9/2016

local ok, new_tab = pcall(require, "table.new")
if not ok or type(new_tab) ~= "function" then
    new_tab = function (narr, nrec) return {} end
end

local ok,uuid = pcall(require, "resty.uuid")
if not ok or not uuid then
    error("failed to load resty.uuid:" .. (uuid or "nil"))
end

local ok,sha1 = pcall(require, "sha1")
if not ok or not sha1 then
    error("failed to load sha1:" .. (sha1 or "nil"))
end

local ok, utils = pcall(require, "common.utils")
if not ok or not utils then
    error("failed to load common.utils:" .. (utils or "nil"))
end

local ok, redis = pcall(require, "redis.cluster-instance")
if not ok or not redis then
    error("failed to load redis.cluster-instance:" .. (redis or "nil"))
end

local NULL = ngx.null

local _M = new_tab(0,7)
_M._VERSION = '0.1'

--KEYS[1] : the lock key;
--ARGV[1] : the signature;
--ARGV[2] : the time to live, in seconds
--return: 
--   nil(userdata: NULL): if the key already exists
--   OK : succeeded
local script_lock = "return redis.call('set', KEYS[1], ARGV[1], 'EX', ARGV[2], 'NX')"
local script_lock_sha1 = sha1(script_lock)

--params:
--   key: the lock key;
--   ttl: time to live, in seconds;
--return:
--   nil,err: failed to call redis; err will describe the reason;
--   false,err: succeeded to call redis, but lock failed; err will describe the reason; 
--   true,signature: the signature is used to extend lease of the lock or to release the lock; only 
--                   the one who's holding this signature has the right to do such operations;
function _M.lock(key, ttl)
    local signature = "LockSig_"..uuid()

        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " key type=", type(key), " signature type=", type(signature))

    local res,err = redis:evalsha(script_lock, script_lock_sha1, 1, key, signature, ttl)

    if nil == res then -- failed to call redis;
        return nil,err
    end
    if NULL == res then --succeeded to call redis, but the script returned nil(userdata: NULL);
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " lock - Conflict: key=", key, " signature=", signature)
        return false,"Lock Conflict"
    end

    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " lock - Success: key=", key, " signature=", signature, " ttl=", ttl)
    return true,signature
end

--KEYS[1] : the lock key;
--ARGV[1] : the signature; only the one who's holding the signature has the right to extend the lease;
--ARGV[2] : new time to live, in seconds;
--return:
--   nil(userdata: NULL): redis.call('get', ...) returns nil; in this case, there is no such lock or it has expired;
--   current signature: redis.call('get', ...) returns current signature; in this case, the lock has 
--           expired and has been acquired by other one with the current signature;
--   0: the lock has not expired when redis.call('get', ...), but has expired when redis.call('expire', ...);
--   1: succeeded to extend the lease;
local script_extend_lock = "local sig = redis.call('get', KEYS[1]) "  ..
                           "if sig ~= ARGV[1] then return sig end "   ..
                           "return redis.call('expire', KEYS[1], ARGV[2])"
local script_extend_lock_sha1 = sha1(script_extend_lock)

--params:
--   key: the lock key;
--   signature: the signature returned by lock() function; only the one who's holding correct signature has right
--              to extend the lock;
--   ttl: new time to live, in seconds;
--return:
--   nil,err: failed to call redis; err will describe the reason;
--   false,err: succeeded to call redis, but failed to extend the lock; err will describe the reason;
--   true: succeeded to extend the lock;
function _M.extend_lock(key, signature, ttl)
    local res,err = redis:evalsha(script_extend_lock, script_extend_lock_sha1, 1, key, signature, ttl)
    if nil == res then -- failed to call redis;
        return nil,err
    end
    if NULL == res then --succeeded to call redis, but the script returned nil(userdata: NULL);
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " extend_lock - No Such Lock or Lock Expired: key=", key)
        return false, "No Such Lock or Lock Expired"
    end
    local num_res = tonumber(res)
    if 0 == num_res then
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " extend_lock - Lock Expired: key=", key)
        return false, "Lock Expired"
    end
    if 1 == num_res then
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " extend_lock - Success: key=", key, " signature=", signature, " new-ttl=", ttl)
        return true 
    end

    --tonumber failed, it should be current signature; that is, the lock has expired and has been acquired by other one with
    --this signature
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " extend_lock - Locked By Other With Sig ", res ," key=", key, " signature=", signature)
    return false, "Locked By Other With Sig " .. res
end


--KEYS[1] : the lock key;
--ARGV[1] : the signature; only the one who's holding the signature has the right to unlock;
--return: 
--   nil(userdata: NULL): redis.call('get', ...) returns nil; in this case, there is no such lock or it has expired;
--   current signature: redis.call('get', ...) returns current signature; in this case, the lock has
--                      expired and has been acquired by other one with the current signature;
--   0: the lock has not expired when redis.call('get', ...), but has expired when redis.call('del', ...)
--   1: succeeded to delete the key;
local script_unlock = "local sig = redis.call('get', KEYS[1]) "    ..
                      "if sig ~= ARGV[1] then return sig end "     ..
                      "return redis.call('del', KEYS[1])"
local script_unlock_sha1 = sha1(script_unlock)


function _M.unlock(key, signature)
    local res,err = redis:evalsha(script_unlock, script_unlock_sha1, 1, key, signature)
    if nil == res then -- failed to call redis;
        return nil,err
    end
    if NULL == res then --succeeded to call redis, but the script returned nil(userdata: NULL);
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " unlock - No Such Lock or Lock Expired: key=", key)
        return false, "No Such Lock or Lock Expired"
    end
    local num_res = tonumber(res)
    if 0 == num_res then
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " unlock - Lock Expired: key=", key)
        return false, "Lock Expired"
    end
    if 1 == num_res then
        ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " unlock - Success: key=", key, " signature=", signature)
        return true 
    end
    --tonumber failed, it should be current signature; that is, the lock has expired and has been acquired by other one with
    --this signature
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " unlock - Locked By Other With Sig ", res ," key=", key, " signature=", signature)
    return false, "Locked By Other With Sig " .. res
end

return _M
