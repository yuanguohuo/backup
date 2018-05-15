local _M = {
    _VERSION = '1.00',
}
_M.config = {}

_M.config["cache_config"] = {
    rcache = {
        enabled = true,
        expire = 600, -- in seconds
    },
    lcache = {
        enabled = true,
        items = 2000,
        expire = 300,  -- in seconds
    }
}
return _M
