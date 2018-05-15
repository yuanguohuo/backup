-- package_path="/home/deploy/dobjstor/proxy/?.lua;/home/deploy/dobjstor/proxy/thirdparty/?.lua;/home/deploy/luas3/?.lua;;"

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




--[[
local len,buf
len,buf = lrados.rados_read(ioctx,"first",16,0)
print(buf)
if tonumber(len) < 0 then
	print("read error")
else 
   print("readed len bytes on oject ,content is ", buf,"len is",len)
end
]]

--[[
/**
 * @typedef rados_callback_t
 * Callbacks for asynchrous operations take two parameters:
 * - cb the completion that has finished
 * - arg application defined data made available to the callback function
 */
typedef void (*rados_callback_t)(rados_completion_t cb, void *arg);
]]
--local function callback(complection,arg) end
--lrados.rados_aio_write(ioctx,oid,completion,buf,len,off)

--rados_wait_for_complete(comp); --in memory

local pc
local oid = "objname3"
pc = lrados.rados_aio_create_completion(nil,nil,nil)
local objbuf = "how to do aio in multithreadlllll"
local err = lrados.rados_aio_write(ioctx,oid,pc,objbuf,#objbuf,0)
if err == 0 then
end
local function waitandrelease(pc)
            lrados.rados_wait_for_complete(pc)
            lrados.rados_wait_for_safe(pc)
            lrados.rados_aio_release(pc)
end
waitandrelease(pc)
--local spawn = ngx.thread.spawn
--local wait = ngx.thread.wait
local len,buf
len,buf = lrados.rados_read(ioctx,oid,#objbuf,0)
print(buf)
if tonumber(len) < 0 then
	print("read error")
else 
   print("readed ", len," bytes on oject ,content is ", buf,"oid is",oid)
end


--[[
do
--todo :how pass cb functon?
--local function ack_cb(pcomp,

local arg = 1
local function cb_completion(arg) --maybe the callback func can place in the ffi function to make for                                  -- a closure.
	arg = arg +1
	print("cb_comple arg is ",arg)
end
local function cb_safe(arg)
	arg = arg +1
	print("cb_safe arg is", arg)
end

local pc
pc = lrados.rados_aio_create_completion(arg, cb_completion,cb_safe)
local objbuf = "using callback how to do aio in multithreadlllll"
local err = lrados.rados_aio_write(ioctx,"callbackaiobj",pc,objbuf,#objbuf,0)
if err == 0 then
end
lrados.rados_wait_for_complete(pc)
lrados.rados_wait_for_safe(pc)
lrados.rados_aio_release(pc)

local len,buf
len,buf = lrados.rados_read(ioctx,"aiobj",#objbuf,0)
print(buf)
if tonumber(len) < 0 then
	print("read error")
else 
   print("readed ", len," bytes on oject ,content is ", buf)
end

end
]]
--[[
local arg = 1
local function cb_completion(arg) --maybe the callback func in closure
--	arg = arg + 1
	print("cb_comple arg is ",arg)
end
local function cb_safe(arg)
--	arg = arg + 1
	print("cb_safe arg is", arg)
end
--local function multioutput(ioctx,data,len,numw)
--end 
local pc
pc = lrados.rados_aio_create_completion(arg, cb_completion,cb_safe)
local objbuf = "1using callback how to do aio in multithreadlllll"
local err = lrados.rados_aio_write(ioctx,"17callbackaiobj",pc,objbuf,#objbuf,0)
if err == 0 then
   print "aio write success!"
end

print "entering into waitforcomp"
lrados.rados_wait_for_complete(pc)
print "exiting waitfor comp"

print "entering into waitforsafe"
lrados.rados_wait_for_safe(pc)
print "exiting into waitforsafe"

lrados.rados_aio_release(pc)


local len,buf
len,buf = lrados.rados_read(ioctx,"17callbackaiobj",#objbuf,0)
print(buf)
if tonumber(len) < 0 then
	print("read error")
else 
   print("readed ", len," bytes on oject ,content is ", buf)
end

]]
lrados.rados_shutdown(clusterh)
