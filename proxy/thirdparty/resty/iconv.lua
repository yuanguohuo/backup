local ffi = require 'ffi'
local type = type
local tonumber = tonumber
local ffi_c = ffi.C
local ffi_new = ffi.new
local ffi_cast = ffi.cast
local ffi_gc = ffi.gc
local ffi_string = ffi.string
local ffi_typeof = ffi.typeof
local ffi_errno = ffi.errno
ffi.cdef[[
    typedef void *iconv_t;
    iconv_t iconv_open (const char *__tocode, const char *__fromcode);
    size_t iconv (
        iconv_t __cd,
        char ** __inbuf, size_t * __inbytesleft,
        char ** __outbuf, size_t * __outbytesleft
    );
    int iconv_close (iconv_t __cd);
]]

local maxsize = 4096
local char_ptr = ffi_typeof('char *')
local char_ptr_ptr = ffi_typeof('char *[1]')
local sizet_ptr = ffi_typeof('size_t[1]')
local iconv_open_err = ffi_cast('iconv_t', ffi_new('int', -1))

local ok, new_tab = pcall(require, "table.new")
if not ok then
    new_tab = function (narr, nrec) return {} end
end

local _M = new_tab(0, 8)
_M._VERSION = '0.2.0'

local mt = { __index = _M }

function _M.new(self, to, from, _maxsize)
    if not to or 'string' ~= type(to) or 1 > #to then
        return nil, 'dst charset required'
    end
    if not from or 'string' ~= type(from) or 1 > #from then
        return nil, 'src charset required'
    end
    _maxsize = tonumber(_maxsize) or maxsize
    local ctx = ffi_c.iconv_open(to, from)
    if ctx == iconv_open_err then
        ffi_c.iconv_close(ctx)
        return nil, ('conversion from %s to %s is not supported'):format(from, to)
    else
        ctx = ffi_gc(ctx, ffi_c.iconv_close)
        local buffer = ffi_new('char[' .. _maxsize .. ']')
        return setmetatable({
            ctx = ctx,
            buffer = buffer,
            maxsize = _maxsize,
        }, mt)
    end
end


function _M.convert(self, text)
    local ctx = self.ctx
    if not ctx then
        return nil, 'not initialized'
    end
    if not text or 'string' ~= type(text) or 1 > #text then
        return nil, 'text required'
    end
    local maxsize = self.maxsize
    local buffer = self.buffer
    
    local dst_len = ffi_new(sizet_ptr, maxsize)
    local dst_buff = ffi_new(char_ptr_ptr, ffi_cast(char_ptr, buffer))
        
    local src_len = ffi_new(sizet_ptr, #text)
    local src_buff = ffi_new(char_ptr_ptr)
    src_buff[0] = ffi_new('char['.. #text .. ']', text)

    local ok = ffi_c.iconv(ctx, src_buff, src_len, dst_buff, dst_len)
    if 0 <= ok then
        local len = maxsize - dst_len[0]
        local dst = ffi_string(buffer, len)
        return dst, tonumber(ok)
    else
        local err = ffi_errno()
        return nil, 'failed to convert, errno ' .. err
    end
end

function _M.finish(self)
    local ctx = self.ctx
    if not ctx then
        return nil, 'not initialized'
    end
    return ffi_c.iconv_close(ffi_gc(ctx, nil))
end

local function is_utf8(str)
    local nBytes = 0 --UFT8 can use 1-6 bytes for encoding, ASCII use 1 byte
    for i = 1, #str do
        local chr = string.byte(str, i)
        if 0 == nBytes then
            --ASCII: the highest bit mark as 0, low 7 bits for encoding, 0xxxxxxx 
            --if not ASCII, it should be multi byte encoding, calc the num of encoding bytes
            if chr >= 0x80 then
                if chr >= 0xFC and chr <= 0xFD then
                    nBytes = 6
                elseif chr >= 0xF8 then
                    nBytes = 5
                elseif chr >= 0xF0 then
                    nBytes = 4
                elseif chr >= 0xE0 then
                    nBytes = 3
                elseif chr >= 0xC0 then
                    nBytes = 2
                else
                    return false
                end

                nBytes= nBytes - 1
            end
        else
            --for Multi-byte,  non-first byte style should be 10xxxxxx 
            if chr >= 0xC0 then
                return false
            end
            --dec to zero
            nBytes = nBytes - 1
        end
    end

    --invalid UTF8 encoding rule
    if nBytes ~= 0 then
        return false
    end

    return true
end

function _M.gbk_to_utf8(self, src)
    if not src or 'string' ~= type(src) or 1 > #src then
        return nil, "invalid source" .. (src or "nil")
    end

    if is_utf8(src) then
        return nil, "source is already utf8"
    end

    local toutf8, err = self:new("utf-8", "gb18030")
    if not toutf8 then
        return nil, "iconv new failed!"
    end

    local utf8_dst, count = toutf8:convert(src)
    toutf8:finish()
    if not utf8_dst then
        return nil, "iconv convert failed!"
    end
    return utf8_dst
end

return _M
