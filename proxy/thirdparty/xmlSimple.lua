--local string.gsub = string.gsub
--local string.sub = string.sub
--local string.find = string.find
--local string.char = string.char
--local string.byte = string.byte
--local string.format = string.format
--local error = error
--local io.open = io.open
--local io.close = io.close
--local print = print
--local setmetatable = setmetatable
--local type = type
--local table.insert = table.insert
--local table.remove = table.remove
local system = system
local node = {}
node.___value = nil
node.___name = nil
node.___children = {}
node.___props = {}
function node:new(o) 
    o = o or {}     -- create table if user does not provide one
    setmetatable(o, self)
    self.__index = self
    return o
end  
function node:value() return self.___value end
function node:setValue(val) self.___value = val end
function node:name() return self.___name end
function node:setName(name) self.___name = name end
function node:children() return self.___children end
function node:numChildren() return #self.___children end
function node:addChild(child)
    if self[child:name()] ~= nil then
        if type(self[child:name()].name) == "function" then
            local tempTable = {}
            table.insert(tempTable, self[child:name()])
            self[child:name()] = tempTable
        end
        table.insert(self[child:name()], child)
    else
        self[child:name()] = child
    end
    table.insert(self.___children, child)
end

function node:properties() return self.___props end
function node:numProperties() return #self.___props end
function node:addProperty(name, value)
    local lName = "@" .. name
    if self[lName] ~= nil then
        if type(self[lName]) == "string" then
            local tempTable = {}
            table.insert(tempTable, self[lName])
            self[lName] = tempTable
        end
        table.insert(self[lName], value)
    else
        self[lName] = value
    end
    table.insert(self.___props, { name = name, value = self[name] })
end

-- depthYuanguo = 0
--
-- traversed={}
---- numchildren have some awrekd?
--function node:tostr()
--
--	ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " ",  "depth=", depthYuanguo)
--	depthYuanguo = depthYuanguo + 1
--
--	traversed[tostring(self)] = 1
--
--        local name = self:name()
--        local value = self:value() or ""
--	local str = "<" .. name
--	local prostr = ""
---- 	ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " ", "pronum is: ", self:numProperties() )
--	if self:numProperties() ~= 0 then
--		for i,pro in ipairs( self.___pros) do
--			prostr = prostr .. pro.name .. "=" .. pro.value
--	        end
--        end
--
--	if prostr == "" then
--            str = str .. ">"
--        else    
--	    str = str .. " " .. prostr .. ">"
--	end
--
--	local childstr
--        local inspect = require "inspect"
--        local tb = {a=1,b,c,d}
--	local aKey,aValue
--	for aKey, aValue in pairs( self ) do
----             if type(rawget(self, aKey )) == "function" then
--	                        ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " ", "akey is: ", aKey )
--	                        ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " ", "rawakey is: ", tostring(rawget(self,aKey)) )
----	      end
--	end
--
-- 	ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " ", "children print is: ", inspect(self.___children) )
-- 	ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " ", "self print : ", inspect(self))
--	ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " ",  self:numChildren())
--	ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " ",  #self.___children)
--	if self:numChildren() ~=  0 then --why always 4?
----	local num = rawget(self,"numChildren")
----	if num()  ~=  0 then
--               for i,childnode in pairs(self.___children) do
----	                        ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " ", "childnode is: ",  i .. " " .. childnode:name() )
----				ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " ",  "self=", tostring(self), "child=", tostring(childnode))
-- 	                        ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " ", "childnode is:", i ..  inspect(childnode))
--
----				--[[
----				if tostring(self) ~= tostring(childnode) then
----		                              childstr = childnode:tostr()
----		                end
----				--]]
----				if not traversed[tostring(childnode)] then
----		                              childstr = childnode:tostr()
----			        end
--                                local childname =childnode:name()			        
--	                --        ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " ", "childnode is: ",  i .. " " .. childnode:name() )
--	                  --      ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " ", "self[childnode] is: ",  i .. " " .. inspect(self[childnode:name()]))
--        		   --   	childstr = self.(childnode:name()):tostr()       
--        		   --   	childstr = (self.childname):tostr()  
--        		     -- 	childstr = childname:tostr()  
--				if not traversed[tostring(childnode)] then
--					if self[childnode:name()] then
--						--childstr =  self[childnode:name()]:tostr()
--				--		childstr =  self[tostring(rawget(childnode,___name))]:tostr()
--	                        childstr =  childnode:tostr()
--				        end
--			        end
--               end
--        end
--	if childstr then
--
--           str = str .. childstr or "" .. value .. "</" .. name .. ">"
--        else	   
--           str = str .. value .. "</" .. name .. ">"
--	end
--
--        return str
--end       
--

function node:tostr()
        local value = self:value() or ""
	local name = self:name()
	local str = "<" .. name 
	local prostr = ""
--	ngx.log(ngx.DEBUG, "RequestID=", ngx.ctx.reqid, " ", "pronum is: ", self:numProperties() )
	if self:numProperties() ~= 0 then
		for i,pro in ipairs( self.___props) do
			prostr = prostr .. pro.name .. "=" .. pro.value
	        end
	end
	if prostr == "" then
            str = str .. ">"
        else    
	    str = str .. " " .. prostr .. ">"
	end
	str = str .. value .. "</" .. name .. ">" 
        return str
end
-- example
-- in module 
-- local nnode = node:new()
-- ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ", nnode:value())
-- ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ", node.value(nnode,nil))
-- 
-- ngx.log(ngx.WARN, "RequestID=", ngx.ctx.reqid, " ", nnode.__value)
-- 

-- external module use
-- need return this funciton node:new or all fucntion




-- obj XmlParser define
-- need to export node and  xmlparser, node use to create xml,maybe not export
local XmlParser = {};
--local mt = { __index = XmlParser } 
--function XmlParser:new(o)
--    local self = {
--        keys = {},
--        hasht = {},
--        free_queue = queue_init(size),
--        cache_queue = queue_init(),
--        key2node = {},
--        node2key = {},
--    }
--    return setmetatable(self, mt)
--end
function XmlParser:new(o)
        o = o or {}     -- create table if user does not provide one
        setmetatable(o, self)
        self.__index = self
        return o
end  
function XmlParser:ToXmlString(value)
    value = string.gsub(value, "&", "&amp;"); -- '&' -> "&amp;"
    value = string.gsub(value, "<", "&lt;"); -- '<' -> "&lt;"
    value = string.gsub(value, ">", "&gt;"); -- '>' -> "&gt;"
    value = string.gsub(value, "\"", "&quot;"); -- '"' -> "&quot;"
    value = string.gsub(value, "([^%w%&%;%p%\t% ])",
        function(c)
            return string.format("&#x%X;", string.byte(c))
        end);
    return value;
end

function XmlParser:FromXmlString(value)
    value = string.gsub(value, "&#x([%x]+)%;",
        function(h)
            return string.char(tonumber(h, 16))
        end);
    value = string.gsub(value, "&#([0-9]+)%;",
        function(h)
            return string.char(tonumber(h, 10))
        end);
    value = string.gsub(value, "&quot;", "\"");
    value = string.gsub(value, "&apos;", "'");
    value = string.gsub(value, "&gt;", ">");
    value = string.gsub(value, "&lt;", "<");
    value = string.gsub(value, "&amp;", "&");
    return value;
end

function XmlParser:ParseArgs(node, s)
    string.gsub(s, "(%w+)=([\"'])(.-)%2", function(w, _, a)
        node:addProperty(w, self:FromXmlString(a)) 
        --self is obj target of the XmlParser
    end)
end

function XmlParser:ParseXmlText(xmlText)
    local stack = {}
    --create empty Node obj instance.
    -- local top = newNode() --use the module newnode
    local top = node:new()
    --insert top node obj to stack table
    table.insert(stack, top)
    local ni, c, label, xarg, empty
    local i, j = 1, 1
    while true do
        ni, j, c, label, xarg, empty = string.find(xmlText, "<(%/?)([%w_:]+)(.-)(%/?)>", i)
        if not ni then break end
        local text = string.sub(xmlText, i, ni - 1);
        if not string.find(text, "^%s*$") then
            local lVal = (top:value() or "") .. self:FromXmlString(text)
            stack[#stack]:setValue(lVal)
        end
        if empty == "/" then -- empty element tag
            --local lNode = newNode(label)
            local lNode = node:new({___name = label})
            self:ParseArgs(lNode, xarg)
            top:addChild(lNode)
        elseif c == "" then -- start tag
            --local lNode = newNode(label)
            local lNode = node:new({___name = label})
            self:ParseArgs(lNode, xarg)
            table.insert(stack, lNode)
    	top = lNode
        else -- end tag
            local toclose = table.remove(stack) -- remove top

            top = stack[#stack]
            if #stack < 1 then
                return nil
                --error("XmlParser: nothing to close with " .. label)
            end
            if toclose:name() ~= label then
                --error("XmlParser: trying to close " .. toclose.name .. " with " .. label)
                return nil
            end
            top:addChild(toclose)
        end
        i = j + 1
    end
    local text = string.sub(xmlText, i);
    if #stack > 1 then
        -- error("XmlParser: unclosed " .. stack[#stack]:name())
        return nil
    end
    return top
end

function XmlParser:loadFile(xmlFilename, base)
    if not base then
        base = system.ResourceDirectory
    end

    local path = system.pathForFile(xmlFilename, base)
    local hFile, err = io.open(path, "r");

    if hFile and not err then
        local xmlText = hFile:read("*a"); -- read file content
        io.close(hFile);
        return self:ParseXmlText(xmlText), nil;
    else
        print(err)
        return nil
    end
end
--return XmlParser



-- local _M = { _VERSION = '0.09' }
-- _M = { XmlParser, node }
-- return _M
return { _VERSION = '0.1',XmlParser = XmlParser, node = node }
-- return obj:new, shoudl use the fucntion,or return _M
----
--_M = { XmlParser, node, newXP, newnode }
--or fuuncton _M.newXP()
--function _M.newnode()
--	return { _M,xmlparser,node}
