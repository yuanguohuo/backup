local _M = {}
_M._VERSION = '0.01'

local ffi = require "ffi"
local ffi_new = ffi.new
local ffi_str = ffi.string

ffi.cdef[[
unsigned int ngx_hash_key(const unsigned char *data, size_t len, size_t n);
]]

local libhash = ffi.load("hash")

function _M.hash(data, key)
    local ret = libhash.ngx_hash_key(data, #data, key)
    return ret+1
end

return _M
