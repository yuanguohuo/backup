-- chentao
--

local reqmonit = require("reqmonit")

local uri_args = ngx.req.get_uri_args()
local key = uri_args["key"]

ngx.say("requests stats start.")
reqmonit:analyse(ngx.shared.statics_dict, key)
ngx.say("requests stats end.")
ngx.exit(ngx.HTTP_OK)

