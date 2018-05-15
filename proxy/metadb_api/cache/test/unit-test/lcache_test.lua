#!/usr/local/stor-openresty/luajit/bin/luajit

local utils = require "common.utils"

local ok, new_tab = pcall(require, "table.new")
if not ok or type(new_tab) ~= "function" then
    new_tab = function (narr, nrec) return {} end
end

package.path="./?.lua;../?.lua;../../?.lua;../../../?.lua;;"
local lcache = require "lcache"

local cache = lcache:new()

local a,b,c,d = cache:quickScan("user", "1234", "5678", {"info"}, 100)
print(a,b,c,d)


print("===============Test get=================")
local errcode,ok,ret = cache:get("user", "123", 
           {"info:info1", "info:info2", "yyy:yyy1", "stats:objects", "stats:size_bytes", "quota:quota1", "stats:statsDEF", "xxx:xxx1", "stats:statsABC"})
print("errcode=", utils.stringify(errcode))
print("ok=", ok)
print("ret=", utils.stringify(ret))


print("===============Test increment===========")
local errcode,ok,ret = cache:increment("user", "123", {"stats", "objects", 1, "stats", "size_bytes", 10240, "xxx", "xxx1", 3, "xxx", "xxx2", 4})
print("errcode=", utils.stringify(errcode))
print("ok=", ok)
print("ret=", utils.stringify(ret))


print("===============Test put=================")
local errcode,ok,ret = cache:put("user", "123", {"info", "info1", "11111111", "info", "info2", "22222222", "quota", "quota1", "33333333", "yyy", "yyy1", "xxxxxxxx"}, 8000)
print("errcode=", utils.stringify(errcode))
print("ok=", ok)

print("===============Test checkAndPut=================")
local errcode,ok,ret = cache:checkAndPut("user", "123", "info", "info1", "xyz",  
        {"info", "info1", "AAAAAAAA", "info", "info2", "BBBBBBBB", "quota", "quota1", "CCCCCCCC", "yyy", "yyy1", "xxxxxxxx"}, 8000)
print("errcode=", utils.stringify(errcode))
print("ok=", ok)


print("===============Test delete entire row==============")
local errcode,ok,ret = cache:delete("user", "123")
print("errcode=", utils.stringify(errcode))
print("ok=", ok)


print("===============Test delete==============")
local errcode,ok,ret = cache:delete("user", "123", 
           {"info:info1", "info:info2", "yyy:yyy1", "stats:stats1", "xxx:xxx2", "stats:stats2", "quota:quota1", "xxx:xxx1"})
print("errcode=", utils.stringify(errcode))
print("ok=", ok)


print("===============Test checkAndDelete==============")
local errcode,ok,ret = cache:checkAndDelete("user", "123", "info", "info1", "xyz",
           {"info:info1", "info:info2", "yyy:yyy1", "stats:stats1", "xxx:xxx2", "stats:stats2", "quota:quota1", "xxx:xxx1"})
print("errcode=", utils.stringify(errcode))
print("ok=", ok)


print("===============Test checkAndMutateAndGetRow: Success==============")
local errcode,ok,origin = cache:checkAndMutateAndGetRow(
           "user", "ABCD", "info", "info1", cache.CompareOp.GREATER, "bbbb",
           {
               "info",      "info1",     "info1-value", 
               "ver",       "ver1",      "ver1-value", 
               "xxx",       "xxx1",      "xxx1-value", 
           },
           {
               "stats:stats1", 
           },
           8000
           )
print("errcode=", utils.stringify(errcode))
print("ok=", ok)
print("origin=", utils.stringify(origin))


print("===============Test checkAndMutateAndGetRow: Condition-check-failure==============")
local errcode,ok,origin = cache:checkAndMutateAndGetRow(
           "user", "EFGH", "info", "info1", cache.CompareOp.GREATER, "000aaaa",
           {
               "info",      "info1",     "info1-value-value", 
               "ver",       "ver1",      "ver1-value-value", 
               "xxx",       "xxx1",      "xxx1-value-value", 
           },
           {
               "stats:stats2", 
           },
           16000
           )
print("errcode=", utils.stringify(errcode))
print("ok=", ok)
print("origin=", utils.stringify(origin))


print("===============Test checkAndMutateAndGetRow: unknown-failure==============")
local errcode,ok,origin = cache:checkAndMutateAndGetRow(
           "user", "EFGH", "info", "info1", cache.CompareOp.LESS, "XXXXXX",
           {
               "info",      "info1",     "info1-value-value", 
               "ver",       "ver1",      "ver1-value-value", 
               "xxx",       "xxx1",      "xxx1-value-value", 
           },
           {
               "stats:stats2", 
           },
           16000
           )
print("errcode=", utils.stringify(errcode))
print("ok=", ok)
print("origin=", utils.stringify(origin))
