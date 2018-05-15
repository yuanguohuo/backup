local ok, inspect = pcall(require, "inspect")
if not ok or not inspect then
    error("failed to load inspect:" .. (inspect or "nil"))
end

local _M = {
    _VERSION = '1.00',
}

_M.Statement = {
    ["Statement"] = true,
}

_M.RULE = {
    ["Effect"] = true,
    ["Action"] = true,
    ["Resource"] = true,
}

_M.Effect = {
    ["Allow"] = true,
    ["Deny"] = true,
}

_M.Action = {
    ["*"] = true,
    ["ListBuckets"] = true,
    ["PutBucket"] = true,
    ["ListObjects"] = true,
    ["DeleteBucket"] = true,
    ["HeadBucket"] = true,
    ["GetObject"] = true,
    ["DeleteObject"] = true,
    ["PutObject"] = true,
}

_M.OP_MAP = {
    ["LIST_BUCKETS"]               = "ListBuckets",

    ["CREATE_BUCKET"]              = "PutBucket",
    ["LIST_OBJECTS"]               = "ListObjects",
    ["DELETE_BUCKET"]              = "DeleteBucket",
    ["HEAD_BUCKET"]                = "HeadBucket",
    ["GET_BUCKET_ACL"]             = "HeadBucket",

    ["GET_OBJECT"]                 = "GetObject",
    ["GET_OBJECT_ACL"]             = "GetObject",
    ["HEAD_OBJECT"]                = "GetObject",
    ["DELETE_OBJECT"]              = "DeleteObject",
    ["PUT_OBJECT"]                 = "PutObject",
    ["INITIATE_MULTIPART_UPLOAD"]  = "PutObject",
    ["UPLOAD_PART"]                = "PutObject",
    ["COMPLETE_MULTIPART_UPLOAD"]  = "PutObject",
    ["ABORT_MULTIPART_UPLOAD"]     = "PutObject",
    ["LIST_PARTS"]                 = "PutObject",
}

_M.Resource = {
    "*",
}

function _M.check_permission(self, ptable)
    if not ptable or "table" ~= type(ptable) then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", "ptable is nil or not talbe")
        return false
    end

    local statement = ptable["Statement"]
    if not ptable["Statement"] or not next(statement) then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", "Statement is nil or empty")
        return false
    end

    local permissions = {
        Deny = {},
        Allow = {},
        Prefix = {},
    }

    local count = 1
    for i, rule in ipairs(statement) do
        local effect, actions, resources, prefixes = rule["Effect"], rule["Action"], rule["Resource"], rule["Prefix"]

        if not effect or not self.Effect[effect] then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", "effect invalid: " .. (effect or "nil"))
            return false
        end

        if not actions or not next(actions) or not resources or not next(resources) then
            ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", "actions or resources is nil or empty")
            return false
        end

        local ops = {}
        for i, op in ipairs(actions) do
            if not self.Action[op] then
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", "actions op is invalid: " .. op)
                return false
            end
            ops[op] = true
        end 

        for i, res in ipairs(resources) do
            if not permissions[effect][res] then
                permissions[effect][res] = ops
            else
                for k, v in pairs(ops) do
                    permissions[effect][res][k] = v
                end
            end
        end

        if "Allow" == effect and prefixes and next(prefixes) then
            if ops["*"] or ops["ListBuckets"] then
                for _, prefix in ipairs(prefixes) do
                    permissions["Prefix"][count] = prefix
                    count = count + 1
                end
            end
        end
    end

    return true, permissions
end

local function normal_pattern(str)
    local pattern = string.gsub(str, "*", ".*")
    return pattern
end
-- return
-- false: op denied
-- true: op allowed
-- prefix_table: if op is list ops, return prefix of permissions
function _M.verify_permission(self, uri, op_name, ptable)
    local ok, permissions = self:check_permission(ptable)
    if not ok then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", "check_permission failed.")
        return false
    end

    for k, v in pairs(permissions["Deny"]) do
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", "k, uri:" .. k .. ", " .. uri)
        local cp, err = ngx.re.match(uri, normal_pattern(k))
        if cp then
            if v["*"] then
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", "all op is denied for resource: "..k)
                return false
            end

            local action = self.OP_MAP[op_name]
            if action and v[action] then
                ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", "op is denied: " .. action)
                return false
            end
        end
    end

    --for list buckets op, the uri can't be matched with the resource, so we use the
    --resource as prefix for list
    if "LIST_BUCKETS" == op_name and next(permissions["Prefix"]) then
        ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", "permission prefixes :" .. inspect(permissions["Prefix"]))
        return true, permissions["Prefix"]
    end

    for k, v in pairs(permissions["Allow"]) do
        ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " ", "k, uri:" .. k .. ", " .. uri)
        local cp, err = ngx.re.match(uri, normal_pattern(k))
        if cp then
            if v["*"] then
                return true
            end
            
            local action = self.OP_MAP[op_name]
            if action and v[action] then
                return true
            end
        end
    end

    ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", "no permission for op: " .. op_name)
    return false
end

return _M
