--By Yuanguo, 22/12/2016
--Description: rewrite phase;
--    1. generate request ID;

--local ok,resty_uuid = pcall(require, "resty.uuid")
--if not ok or not resty_uuid then
--    error("failed to load resty.uuid:" .. (resty_uuid or "nil"))
--end
local ok, iconv = pcall(require, "resty.iconv")
if not ok or not iconv then
    error("failed to load resty.iconv:" .. (iconv or "nil"))
end

local ngx = ngx
local ok,uuid = pcall(require, "resty.uuid")
if not ok or not uuid then
    error("failed to load resty.uuid:" .. (uuid or "nil"))
end
ngx.ctx.reqid = uuid()

local uri = ngx.var.uri
local new_uri, err = iconv:gbk_to_utf8(uri)
if new_uri then
    ngx.req.set_uri(new_uri)
end

local args = ngx.req.get_uri_args()
local dst, err = iconv:gbk_to_utf8(args["prefix"])
if dst then
    args["prefix"] = dst
    ngx.req.set_uri_args(args)
end
