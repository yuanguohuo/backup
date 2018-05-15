package_path="/home/deploy/dobjstor/proxy/?.lua;/home/deploy/dobjstor/proxy/thirdparty/?.lua;/home/deploy/luas3/?.lua;;"

local lrados = require "librados"
local inspect = require "inspect"
local cluster_name = "ceph"
local user_name = "client.admin"
local flags = 0
print("cluster name ", cluster_name)
print("user name", user_name)
-- clusterh is pointer to void pointer,an cdata object 
local clusterh
clusterh = lrados.rds_create2(clusterh,cluster_name,user_name,0)
print(tostring(clusterh))
local path = "/etc/ceph/ceph.conf"
--librados.rds_conf_read_file(clusterh,path)
--index cdata object,for pointer,we should use number.
--lrados.rds_conf_read_file(clusterh,"/etc/ceph/ceph.conf")
--for no root user, not the permsession of the conf 
lrados.rds_conf_read_file(clusterh,"/home/deploy/luas3/ceph/ceph.conf")
lrados.rados_connect(clusterh)
local clusterstat
local err
err,clusterstat = lrados.rados_cluster_stat(clusterh,clusterstat)
print(inspect(clusterstat)) --only pointer,need index,how return lua string
err,ioctx = lrados.rados_ioctx_create(clusterh,"testpool")
--lrados.rados_write(ioctx,"first","first writed object",18,0)
--[[
local pools = lrados.rados_pool_list(clusterh)
print(pools)
err = lrados.rados_pool_create(clusterh,"testpool")
pools = lrados.rados_pool_list(clusterh)
print(pools)
]]





local len,buf
len,buf = lrados.rados_read(ioctx,"first",16,0)
print(buf)
if tonumber(len) < 0 then
	print("read error")
else 
   print("readed len bytes on oject ,content is ", buf,"len is",len)
end
--[[
/**
 * @typedef rados_callback_t
 * Callbacks for asynchrous operations take two parameters:
 * - cb the completion that has finished
 * - arg application defined data made available to the callback function
 */
typedef void (*rados_callback_t)(rados_completion_t cb, void *arg);
]]
--
--local function callback(complection,arg) end
--lrados.rados_aio_write(ioctx,oid,completion,buf,len,off)

--rados_wait_for_complete(comp); --in memory


lrados.rados_shutdown(clusterh)
