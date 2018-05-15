--
-- Created by IntelliJ IDEA.
-- User: ddascal
-- Date: 15/05/14
-- Time: 15:09
--
-- Implements the new Version 4 HMAC authorization.
--
local resty_sha256 = require "resty.sha256"
local str = require "resty.string"
local resty_hmac = require "api-gateway.resty.hmac"

local HmacAuthV4Handler = {}

function HmacAuthV4Handler:new(o)
    o = o or {}
    setmetatable(o, self)
    self.__index = self
    if ( o ~= nil) then
        self.aws_service = o.aws_service
        self.aws_region = o.aws_region
        self.aws_secret_key = o.aws_secret_key
        self.aws_access_key = o.aws_access_key
        ---
        -- Whether to double url-encode the resource path when constructing the
        -- canonical request. By default, double url-encoding is true.
        -- Different sigv4 services seem to be inconsistent on this. So for
        -- services that want to suppress this, they should set it to false.
        self.doubleUrlEncode = o.doubleUrlEncode or true
    end
    -- set amazon formatted dates
   -- local utc = ngx.utctime()
    local etime = ngx.time()
    local utc = os.date("!%Y-%m-%d %H:%M:%S",etime)
    ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " ",  "utc time first is:\n" .. utc)
    self.aws_date_short = string.gsub(string.sub(utc, 1, 10),"-","")
    self.aws_date = self.aws_date_short .. 'T' .. string.gsub(string.sub(utc, 12),":","") .. 'Z'
--    self.aws_dateh = os.date("%a, %d %b %Y %H:%M:%S +0000",etime)
    self.aws_dateh = ngx.http_time(etime)
    ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " ",  "aws_dateh  is:\n" .. self.aws_dateh)
    return o
end

local function _sign_sha256_FFI(key, msg, raw)
    local hmac_sha256 = resty_hmac:new()
    local digest = hmac_sha256:digest("sha256",key, msg, raw)
    return digest
end


local function _sha256_hex(msg)
    local sha256 = resty_sha256:new()
    sha256:update(msg)
    return str.to_hex(sha256:final())
end

local _sign = _sign_sha256_FFI
local _hash = _sha256_hex

local function trim (s)
	    return (string.gsub(s, "^%s*(.-)%s*$", "%1"))
--	    canonical_header_values = function (self, values)
--		    		return values:gsub('%s+', ' '):gsub('^%s+', ''):gsub('%s+$', '')
--					end,
end
local function get_hashed_canonical_request(method, uri, querystring, headers, request_payload)
    local hash = method .. '\n' ..
                 uri .. '\n' ..
                (querystring or "") .. '\n'
    -- add canonicalHeaders
    -- todo canonicalheaders should using host,contentype if not nil,Any x-amz-* headers
    --                              and x-amz-content-sha256.min host and x-amz-content-s    --                              ha256
    --      we can use all headers as siged to protect                         
--    local canonicalHeaders = ""
--    local signedHeaders = ""
--    for h_n,h_v in pairs(headers) do
--        -- todo: trim and lowercase
--        canonicalHeaders = canonicalHeaders .. h_n .. ":" .. h_v .. "\n"
--        signedHeaders = signedHeaders .. h_n .. ";"
--    end
--    --remove the last ";" from the signedHeaders
--    signedHeaders = string.sub(signedHeaders, 1, -2)
    
    local canonicalHeaders = ""
    local canonicalHeadersT = {}
    local signedHeaders = ""
    local signedHeadersT = {}

    -- frome get_req_headers,defaltu lowcase and - change to _-,should set the authv4 header as nil
    -- so leave remaing headers.
    -- now all headers signed
    for k,v in pairs(headers) do 
        k = string.lower(k)
        if type(v) == 'table' then
            table.sort(v)
            v = table.concat(v, ",")
        end
            table.insert(signedHeadersT, k)
            canonicalHeadersT[k] = v
    end

    table.sort(signedHeadersT)
    for _, k in ipairs(signedHeadersT) do 
        table.insert(canonicalHeadersT, k .. ":" .. trim(canonicalHeadersT[k]).. '\n')
    end
    canonicalHeaders = table.concat(canonicalHeadersT)
    signedHeaders = table.concat(signedHeadersT, ";")
    hash = hash .. canonicalHeaders .. "\n"
            .. signedHeaders .. "\n"
    local bodyhash
--    -- hash = hash .. _hash(requestPayload or "")
--    headers["X-AMZ-Content-Sha256"] = bodyhash
    if headers["X-AMZ-Content-Sha256"] == "UNSIGNED-PAYLOAD"  then
--      if headers["x-amz-content-sha256"] == "UNSIGNED-PAYLOAD"  then
        
      else
    -- hash = hash .. _hash(requestPayload or "")
         bodyhash = _hash(request_payload or "") 
         headers["X-AMZ-Content-Sha256"] = bodyhash
         hash = hash .. bodyhash
	    
    end	    

    ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " ",  "client Canonical String to Sign is:\n" .. hash)

    local final_hash = _hash(hash)
    ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " ",  "client Canonical String HASHED is:\n" .. final_hash .. "\n")
    -- return signedHeaders, final_hash
    return bodyhash, signedHeaders, final_hash

end

local function get_string_to_sign(algorithm, request_date, credential_scope, hashed_canonical_request)
    local s = algorithm .. "\n" .. request_date .. "\n" .. credential_scope .. "\n" .. hashed_canonical_request
    ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " ",  "String-to-Sign is:\n" .. s)
    return s
end

local function get_derived_signing_key(aws_secret_key, date, region, service )
    local kDate = _sign("AWS4" .. aws_secret_key, date, true )
    local kRegion = _sign(kDate, region, true)
    local kService = _sign(kRegion, service, true)
    local kSigning = _sign(kService, "aws4_request", true)

    return kSigning
end

local function urlEncode(inputString)
        if (inputString) then
            inputString = string.gsub (inputString, "\n", "\r\n")
            inputString = string.gsub (inputString, "([^%w %-%_%.%~])",
                function (c) return string.format ("%%%02X", string.byte(c)) end)
            inputString = ngx.re.gsub (inputString, " ", "+", "ijo")
            -- AWS workarounds following Java SDK
            -- see https://github.com/aws/aws-sdk-java/blob/master/aws-java-sdk-core/src/main/java/com/amazonaws/util/SdkHttpUtils.java#L80-87
            -- replace '+' ( %2B ) with ( %20 )
            inputString = ngx.re.gsub(inputString, "%2B", "%20", "ijo")
            -- replace %2F with "/"
            inputString = ngx.re.gsub(inputString, "%2F", "/", "ijo")
            -- replace %7E with "~"
            inputString = ngx.re.gsub(inputString, "%7E", "~", "ijo")
        end
        return inputString
        --[[local s = ngx.escape_uri(inputString)
        -- replace '+' ( %2B ) with ( %20 )
        s = ngx.re.gsub(s, "%2B", "%20", "ijo")
        -- replace "," with %2C
        s = ngx.re.gsub(s, ",", "%2C", "ijo")
        return s]]
end

local function getTableIterator(uri_args, urlParameterKeys)
    -- use the keys to get the values out of the uri_args table in alphabetical order
    local i = 0
    local keyValueIterator = function ()
        i = i + 1
        if urlParameterKeys[i] == nil then
            return nil
        end
        return urlParameterKeys[i], uri_args[urlParameterKeys[i]]
    end
    return keyValueIterator
end

function HmacAuthV4Handler:formatQueryString(uri_args)
    local uri = ""

    local urlParameterKeys = {}
    -- insert all the url parameter keys into array
    for n in pairs(uri_args) do
        table.insert(urlParameterKeys, n)
    end
    -- sort the keys
    table.sort(urlParameterKeys)

    local iterator = getTableIterator(uri_args, urlParameterKeys)

    for param_key, param_value in iterator do
        uri = uri .. urlEncode(param_key) .. "=" .. urlEncode(param_value) .. "&"
    end
    --remove the last "&" from the signedHeaders
    uri = string.sub(uri, 1, -2)
    return uri
end

-- function HmacAuthV4Handler:getSignature(http_method, request_uri, uri_arg_table, request_payload )
function HmacAuthV4Handler:getSignature(http_method, request_uri, uri_arg_table, request_payload, headers)
    
    local uri_args 
    if uri_arg_table then
	    uri_args = self:formatQueryString(uri_arg_table)
    end
    local utc = ngx.utctime()
    --use epoch time,can use os.date to conversion
    ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " ",  "utc time  is:\n" .. utc)
    local date1 = self.aws_date_short --tod update?
    local date2 = self.aws_date
    local dateh = self.aws_dateh
--    local gmtdate = os.date(a,utc)
    ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " ",  "date1 time  is:\n" .. date1)
    ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " ",  "date2 time  is:\n" .. date2)
--    local headers = {}
--    todo how process the date ,first or here?
    --todo domain name process
    --headers.host = self.aws_service .. "." .. self.aws_region .. ".amazonaws.com"
    headers.host = "127.0.0.1:1984"
    --todo client using the current UTC time in ISO 8601 format ,
    --now we using the gmt? 
    --headers["x-amz-date"] = date2
    headers["x-amz-date"] = dateh

    local encoded_request_uri = request_uri
    if (self.doubleUrlEncode == true) then
        encoded_request_uri = urlEncode(request_uri)
    end

    ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " ",  "request_payload in getSign funct is:\n" .. request_payload)
    local bodyhash, signedheaders, hash_canonical_req =  get_hashed_canonical_request(
                http_method, encoded_request_uri,
                uri_args,
                headers, request_payload) 
    -- ensure parameters in query string are in order
--    local sign = _sign( get_derived_signing_key( self.aws_secret_key,
--        date1,
--        self.aws_region,
--        self.aws_service),
--        get_string_to_sign("AWS4-HMAC-SHA256",
--            date2,
--            date1 .. "/" .. self.aws_region .. "/" .. self.aws_service .. "/aws4_request",
--            get_hashed_canonical_request(
--                http_method, encoded_request_uri,
--                uri_args,
--                headers, request_payload) ) )
    local sign = _sign( get_derived_signing_key( self.aws_secret_key,
        date1,
        self.aws_region,
        self.aws_service),
        get_string_to_sign("AWS4-HMAC-SHA256",
            date2,
            date1 .. "/" .. self.aws_region .. "/" .. self.aws_service .. "/aws4_request",
            hash_canonical_req) )
--    return sign
    return bodyhash, signedheaders ,sign
end

function HmacAuthV4Handler:getAuthorizationHeader(http_method, request_uri, uri_arg_table, request_payload , headers)
    local bodyhash, signedheaders, auth_signature = self:getSignature(http_method, request_uri, uri_arg_table, request_payload, headers)
    --todo constructor the header,when intruding the all signedheaders.
    --local authHeader = "AWS4-HMAC-SHA256 Credential=" .. self.aws_access_key.."/" .. self.aws_date_short .. "/" .. self.aws_region
    --       .."/" .. self.aws_service.."/aws4_request,SignedHeaders=host;x-amz-date,Signature="..auth_signature
    local authHeader = "AWS4-HMAC-SHA256 Credential=" .. self.aws_access_key.."/" .. self.aws_date_short .. "/" .. self.aws_region
           .."/" .. self.aws_service.."/aws4_request,SignedHeaders=" .. signedheaders .. ",Signature="..auth_signature
    ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " ", "client authheader is ",authHeader)	   
    return bodyhash, authHeader
end

return HmacAuthV4Handler









