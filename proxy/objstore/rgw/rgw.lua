-- By Yuanguo, 08/08/2016

local sp_conf = require("storageproxy_conf")

local ok,utils = pcall(require, "common.utils")
if not ok or not utils then
    error("failed to load common.utils:" .. (utils or "nil"))
end

local ok, new_tab = pcall(require, "table.new")
if not ok or type(new_tab) ~= "function" then
    new_tab = function (narr, nrec) return {} end
end

local ok, http = pcall(require, "resty.http")
if not ok or not http then
    error("failed to load resty.http:" .. (http or "nil"))
end

local ok, inspect = pcall(require, "inspect")
if not ok or not inspect then
    error("failed to load inspect:" .. (inspect or "nil"))
end

local stringify = utils.stringify
local setmetatable = setmetatable
local ngx = ngx

local _M = new_tab(0, 155)
_M._VERSION = '0.1'

function _M.new(self, server, port, timeout)
    return setmetatable(
               {server = server, port = port},
               {__index = self}
           )
end

local function create_httpc(self,op)
    local httpc = http.new()
    if not httpc then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to create resty.http for rgw "..op)
        return nil, "2011"
    end

    local ok, err = httpc:connect(self.server, self.port)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " resty.http failed to connect to "..self.server..":"..self.port..". err="..(err or "nil"))
        return nil, "2001"
    end

    return httpc, "0000"
end

local function request_pipeline(self, op, params_table, op_success)
    local ret = {}

    local httpc,errcode = create_httpc(self, op)
    if not httpc then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to get resty.http for rgw get_obj")
        return errcode, false, "Internal Server Error"
    end

    local resps, err = httpc:request_pipeline(params_table)
    if err then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " request_pipeline failed. err="..err)
        httpc:set_keepalive()
        return "2004", false, "httpc request_pipeline failed"
    end

    if not resps then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " request_pipeline failed. responses are empty")
        httpc:set_keepalive()
        return "2005", false, "httpc request_pipeline failed"
    end

    for i, r in ipairs(resps) do
        if not r then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " request_pipeline failed. response of request "..i.." is nil")
            httpc:set_keepalive()
            return "2006", false, "httpc request_pipeline failed"
        end

        local rstat = r.status
        if not rstat then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " request_pipeline failed, "..i.."response status is nil")
            httpc:set_keepalive()
            return "2007", false, "httpc request_pipeline failed"
        end

        local rbody = r:read_body()
        if op_success(rstat) then
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ",  op, " request_pipeline succeeded. body length=" , (string.len(rbody) or 0))
            ret[i] = rbody
        elseif 408 == rstat then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " request_pipeline return 408")
            httpc:set_keepalive()
            return "2008", false, "rgw request_pipeline timed out"
        else
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  op, " request_pipeline failed. response of "..i..": status=".. rstat ..", body="..(rbody or "nil"))
            httpc:set_keepalive()
            return "2007", false, "rgw request_pipeline failed"
        end
    end

    local reuse = httpc:get_reused_times()   --check reuse times before close
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " http connection reused: ", reuse)

    httpc:set_keepalive()
    return "0000", true, ret
end

local function put_success(status)
    return (200 == status)
end

local function get_success(status)
    return (200 == status or 206 == status)
end

local function delete_success(status)
    return (204 == status or 404 == status)
end

--Yuanguo: put a list of objects in pipeline.
--params:
--    object : the destination object in rgw.
--    instances : objectname1, body1, objectname2, body2, ... they must be in pairs.
function _M.put_obj(self, object, instances)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " Enter rgw put_obj. object="..(object or "nil"))

    if #instances % 2 ~= 0 then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " invalid arguments for rgw put_obj, 'objectname'==>'body' should be in pairs")
        return "2002", false, "Internal Server Error"
    end

    --to be implemented
    --check encode_object length, xfs needs filename length < 255
    local encode_object = utils.encodeuri(object) 
    if sp_conf.config["use_ngx_capture"] then
        for i = 1, #instances-1, 2 do
            local path = "/rados/"..encode_object.."/"..instances[i]
            local thebody = instances[i+1]

            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " put ", path)

            local res = ngx.location.capture(
                path,
                {   
                    method = ngx.HTTP_PUT,
                    body = thebody,
                }
            )

            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " put ", path, ", capture return: ", stringify(res))

            if not res then
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " capture returned nil when put data into rgw")
                return "2012", false, nil
            end

            if res.status ~= 200 then
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " capture didn't return 200 when put data into rgw, status=", res.status)
                return "2014", false, nil
            end
        end 

        return "0000", true, nil 
    else
        local params_table = {}
        local index = 1
        for i = 1, #instances-1, 2 do
            local path = "/rados/"..encode_object.."/"..instances[i]
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " put ", path)
            local body = instances[i+1]

            params_table[index] = {
                method = "PUT",
                path = path, 
                body = body,
            }
            index = index + 1
        end

        local code, ok, ret = request_pipeline(self, "put_obj", params_table, put_success)
        if not ok then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " request_pipeline failed, code=" .. code)
            return code, ok, nil
        end

        return "0000", true, nil 
    end
end

--Yuanguo: get a list of objects in pipeline.
--params:
--    object : the destination object in rgw.
--    ...    : the obejct name list 
--return:
--    1. http code
--    2. inner code 
--    3. an array containing the body for each object being read. the order of these body 
--       readers are the same as the object names.
function _M.get_obj(self, object, instances)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " Enter rgw get_obj. object="..(object or "nil"))
    local encode_object = utils.encodeuri(object)
    if sp_conf.config["use_ngx_capture"] then

        local ret = {} 

        for i = 1, #instances do
            local path = "/rados/"..encode_object.."/"..instances[i].name

            local range = "bytes=" .. instances[i].start_pos .. "-" .. instances[i].end_pos

            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " get ", path, ", Range:", range)
    
            local res = ngx.location.capture(
                path,
                {   
                    method = ngx.HTTP_GET,
                    vars = { rangestr = range } 
                }
            )

            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " get ", path, ", Range:", range, ", capture return:", stringify(res))

            if not res then
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " capture returned nil when get data from rgw")
                return "2013", false, nil
            end

            if res.status ~= 200 and res.status ~= 206 then
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " capture didn't return 200 or 206 when get data from rgw, status=", res.status)
                return "2015", false, nil
            end

            if res.truncated then
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " capture returned truncated data")
                return "2016", false, nil
            end

            ret[i] = res.body 
        end

        return "0000", true, ret 
    else
        local params_table = {}
        for i = 1, #instances do
            local path = "/rados/"..encode_object.."/"..instances[i].name
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " get ", path)
    
            local tmpheader = {}
            tmpheader["Range"] = "bytes=" .. instances[i].start_pos .. "-" .. instances[i].end_pos
            params_table[i] = {
                method = "GET",
                path = path, 
                headers = tmpheader,
            }
        end
    
        local code, ok, ret = request_pipeline(self, "get_obj", params_table, get_success)
        if not ok then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " request_pipeline failed, code=", code, " ret=", stringify(ret))
            return code, ok, nil
        end

        return "0000", true, ret 
    end
end

--chentao: delete a list of objects in pipeline.
--params:
--    object : the destination object in rgw.
--    ...    : the obj instances list 
--return:
--    1. http code
--    2. inner code 
function _M.delete_obj(self, object, instances, is_subrequest)
    ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " Enter rgw delete_obj. object="..(object or "nil"))

    local encode_object = utils.encodeuri(object)
    if is_subrequest then
        for i = 1, #instances do
            local path = "/rados/"..encode_object.."/"..instances[i].name

            local res = ngx.location.capture(
                path,
                {   
                    method = ngx.HTTP_DELETE,
                }
            )

            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " delete ", path, ", capture return:", stringify(res))

            if not res then
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " capture returned nil when delete data from rgw")
                return "2013", false
            end

            if res.status ~= 204 and res.status ~= 404 then
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " capture didn't return 204 or 404 when delete data from rgw, status=", res.status)
                return "2015", false, nil
            end
        end

        return "0000", true 
    else
        local params_table = {}
    
        for i = 1, #instances do
            local path = "/rados/"..encode_object.."/"..instances[i].name
            ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " delete ", path)
    
            params_table[i] = {
                method = "DELETE",
                path = path, 
            }
        end
    
        local code, ok, ret = request_pipeline(self, "delete_obj", params_table, delete_success)
        if not ok then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " request_pipeline failed, code=".. code)
            return code, ok
        end
        return "0000", true
    end
end

function _M.get_obj_size(self, obj_key)
    local httpc,errcode = create_httpc(self, "get_obj_size")
    if not httpc then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " failed to get resty.http for rgw get_obj_size")
        return false, nil 
    end 

    local res, err = httpc:request({
          path = "/rados/" .. obj_key,
          headers = { 
              ["Host"] = "recoverhost",
          },  
      })  

      if not res then
          ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " res is nil in get_obj_size")
          httpc:set_keepalive()
          return false, nil 
      end 

      --ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " get_obj_size, res=", stringify(res))

      local status = res["status"]
      if status ~= 200 then
          ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " res.status=", status)
          httpc:set_keepalive()
          return false, nil 
      end 

      local headers = res.headers
      if not headers then
          ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " res.headers is nil in get_obj_size")
          httpc:set_keepalive()
          return false, nil 
      end

      local len = headers["Content-Length"]

      if not len or len == "" then
          ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " no Content-Length in headers in get_obj_size")
          httpc:set_keepalive()
          return false, nil
      end

      httpc:set_keepalive()
      return true, tonumber(len)
end

return _M
