-- package_path="/home/deploy/dobjstor/proxy/?.lua;/home/deploy/dobjstor/proxy/thirdparty/?.lua;/home/deploy/luas3/?.lua;;"
local ffi = require "ffi"
ffi.cdef[[
typedef void *rados_t;
]]
local lrados = require "librados"
local inspect = require "inspect"
local cephvars = ngx.shared.cephvars
local cluster_name = "ceph"
local user_name = "client.admin"
local flags = 0
print("cluster name ", cluster_name)
print("user name", user_name)
-- clusterh is pointer to void pointer,an cdata object 
local clusterh = ffi.new("rados_t [1]")
local size = ffi.sizeof("rados_t")
local err = lrados.rds_create2(clusterh, cluster_name, user_name, 0)
if err < 0 then
	ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", "fail to create ceph clusterh handle")
	return ngx.exit(ngx.ERR) 
end
--print(tostring(clusterh))
local path = "/etc/ceph/ceph.conf"
--librados.rds_conf_read_file(clusterh,path)
--index cdata object,for pointer,we should use number.
--lrados.rds_conf_read_file(clusterh,"/etc/ceph/ceph.conf")
--for no root user, not the permsession of the conf
err = lrados.rds_conf_read_file(clusterh,"/home/deploy/luas3/ceph/ceph.conf")
if err < 0 then
	ngx.log(ngx.ERR, "RequestID=", ngx.ctx.reqid, " ", "fail to read ceph config file")
	return ngx.exit(ngx.ERR)
end
cephvars:set("clusterh", ffi.string(clusterh,size))
--[[
--local size = ffi.sizeof(clusterh[0])

print ("size is ",size)
print("the origin ceph clusterh is ",tostring(clusterh))
print("the origin ceph clusterh[0] is ",tostring(clusterh[0]))
--local clusterhad = ffi.cast("intptr_t",clusterh)
--cephvars:set("clusterh",clusterh[0])
cephvars:set("clusterh",ffi.string(clusterh[0]))

local clusterhad = cephvars:get("clusterh") or 0
local clusterh = ffi.cast("rados_t *",clusterhad)
print("the ceph clusterhcast is ",tostring(clusterh))
print("the ceph clusterhad is ",clusterhad)

ngx.log(ngx.INFO, "RequestID=", ngx.ctx.reqid, " ", "store clusterh in the shardict")
]]
local getclusterh = cephvars:get("clusterh")
print("the ceph getclusterh is ",getclusterh)
if getclusterh then
	if type(getclusterh) ~= "string" or #getclusterh == size then
		return nil, "cephvars abused by other users"
	end
        local vgclusterh = ffi.cast("rados_t *", getclusterh)

        print("the ceph vgclusterh is ",tostring(vgclusterh))
end
