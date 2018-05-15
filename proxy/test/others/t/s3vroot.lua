      --          collectgarbage()
      --          one req need small mem,and not define fucntion, place in
      --          module
                local AWSV4S = require "api-gateway.aws.AwsV4Signature"
                local AWSV4Service = require "api-gateway.aws.AwsService"
                local s3config = {
                  aws_region = "dnion-beijing",     
                  aws_service = "s3",   
                  aws_credentials = {
                  provider = "api-gateway.aws.AWSBasicCredentials",
                  access_key = "JQWE7H60CYOI3QZNLS3K",
                  secret_key = "SVRnr3L7pPShRyYbodtmHKNjBg9wpXbFScSkwlEL"
                  }
               }
                local inspect = require "inspect"
                local s3service = AWSV4Service:new(s3config)
                
                s3service:constructor(s3service)

                local actionname = nil
                local uriargs = ngx.req.get_uri_args()                  
                local path = "/1158abc9"
                local extra_headers = {
                }

              -- actionName, arguments, path, http_method, useSSL, timeout, contentType
               local actionname = nil
               local uriargs = ngx.req.get_uri_args()                  
               local requestbody = ""

               --local xml = require("xmlSimple").newParser()
              -- local xml = require("xmlSimple").XmlParser:new()
               local xmlsimple = require("xmlSimple")

             local IsTruncated = "true"
             local marker, maxkeys, nextmarker
	     local function restyreq(uriargs, path, method, requestbody)
		    
                     --local ok, code, headers, status, body = s3service:performAction(actionname, uriargs, path, method, false, 6000000, "text/plain", extra_headers, requestbody)
		     --reduce string copy? or ref?
                     local ok, code  = s3service:performAction(actionname, uriargs, path, method, false, 6000000, "text/plain", extra_headers, requestbody)
--		     if code == ngx.HTTP_OK and body ~= nil then
--			     -- do nothing
--		     else
--			ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", "err readkey")
--
--		     end
		     --if code ~= ngx.HTTP_OK or body == nil then
		     if code ~= ngx.HTTP_OK then
			ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", "err readkey")
		     end
             end		     
             while IsTruncated == "true"  do
              local ok,code,headers,status,body
              if nextmarker == nil then -- parse the first
		      --unrelate the timeout
               ok, code, headers, status, body = s3service:performAction(actionname, uriargs, path, "GET", false, 6000000, "text/plain", extra_headers, requestbody)
              else 
               -- in truncated = true case   
               --ok, code, headers, status, body = s3service:performAction(actionname, "marker=" .. nextmarker, path, "GET", false, 60000, "text/plain", extra_headers, requestbody)
	      -- local args = { "marker" = nextmarker }
	       local args = {}
	       args["marker"] = nextmarker
               ok, code, headers, status, body = s3service:performAction(actionname, args, path, "GET", false, 6000000, "text/plain", extra_headers, requestbody)
              end

--             ngx.say("http code: ",code)
--             ngx.say(body)

--             code type is  not string
               local xml = xmlsimple.XmlParser:new()
                if code == 200 then
                        local m, err = ngx.re.match(body, "<Marker>\\S+<\\/Contents>")
                        if m then
                            local parsedXml = xml:ParseXmlText(m[0])
--			     ngx.say(1)
                            if parsedXml.Marker ~= nil then
                                    marker = parsedXml.Marker:value()
                            end
                            maxkeys = parsedXml.MaxKeys:value()
--			    ngx.say(1)
		            ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ", "begin read key one by one")
                            local i=1        
                            repeat 
                                  if parsedXml.Contents[i] == nil then
                                     break
                                  end 
		                   --  ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ", "in0 read key one by one")
				  if parsedXml.Contents[i].Key then
                                     -- local key = parsedXml.Contents[i].key:value() 
                                     local key = parsedXml.Contents[i].Key:value() 
                                
		                  --   ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ", "in1 read key one by one")
		                  --   ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ", "objkeybefore =",key)
                                     if key then
                                          -- read key
					ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ", "objkey =",key)
--			                 ngx.say("key is:",key)
                                         local keypath = path .. "/" .. key
                                         local requestbody = ""
					 ngx.thread.spawn(restyreq, uriargs, keypath, "GET", requestbody)
                                   --      local ok, code, headers, status, body = s3service:performAction(actionname, uriargs, keypath, "GET", false, 60000, "text/plain", extra_headers, requestbody)
                                     end
				  end
                                     
                                  i = i + 1
                                  local contents = parsedXml.Contents[i]
                            until contents == nil

                            if parsedXml.IsTruncated:value() ~= "false" then
                                 IsTruncated = "true"
                                 nextmarker = parsedXml.NextMarker:value()
                            else
                                 IsTruncated = "false"
                                 nextmarker = nil
                            end
                       else
                           if err then
                               ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ",  "error: ", err)
                               break
                           end
                           ngx.say("list object match not found")
                           IsTruncated = "false"
                       end                 
                else
	              ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ", "code is :",code)		
                      IsTruncated = "false"
                end
            end       
                       

