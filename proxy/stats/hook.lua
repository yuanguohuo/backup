-- chen tao
-- 
local reqmonit = require("reqmonit")
if "6080" ~= ngx.var.server_port then
    local request_time = ngx.now()*1000 - ngx.req.start_time()*1000
    reqmonit.update_ops(ngx.shared.statics_dict, request_time)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", "request_time="..request_time)
    local code = tonumber(ngx.var.status)
    reqmonit.update_code(ngx.shared.statics_dict, code)

    reqmonit.update_hitrate(ngx.shared.statics_dict)
end
