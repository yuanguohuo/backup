-- By Yuanguo, 29/11/2017

local ok, objstore = pcall(require, "objstore.objstore")
if not ok or not objstore then
    error("failed to load objstore.objstore: " .. (objstore or "nil"))
end

local instance = objstore:new()

return instance
