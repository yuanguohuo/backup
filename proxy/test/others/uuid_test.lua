local uuid = require("resty.uuid")
local uuid_old = require 'resty.jit-uuid'

local dict = ngx.shared.statics_dict

local function Test_uuid()
    local time1 = os.time()
    for i= 1, 5000000 do
        local uuid = uuid()
    end
    local time2 = os.time()
    ngx.say(time2 - time1)
end

local function Test_uuid2()
    local uuid = uuid()
    local newval, err = dict:incr(uuid, 1)
    if err then
        assert(err == "not found")
        dict:set(key, 1)
        ngx.log(ngx.ERR, " uuid=", uuid, ", workerid=", ngx.worker.id())
        ngx.status = 200
    else
        ngx.log(ngx.ERR, "error, conflicted uuid"..uuid)
        ngx.status = 500
        ngx.exit(0)
    end
end

--Test_uuid()
Test_uuid2()
