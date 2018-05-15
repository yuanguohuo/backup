local function mytestfunc()
     ngx.say("HAHAHAHAHAHA")
     ngx.flush()
     return "xxx"
end



ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  mytestfunc())

exit
