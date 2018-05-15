-- By Yuanguo, 22/7/2016

local DEF_CAPACITY = 32 

local ok, new_tab = pcall(require, "table.new")
if not ok or type(new_tab) ~= "function" then
    new_tab = function (narr, nrec) return {} end
end

local ok, lock = pcall(require, "resty.lock")
if not ok or not lock then
    error("failed to load resty.lock. err="..(lock or "nil"))
end

local _M = new_tab(0, 16)
_M._VERSION = '0.1'

function _M.new(self,c,safe,lock_key)
    local cap = c or DEF_CAPACITY
    local tab = new_tab(cap, 0)
    local qlock = nil
    if safe then
        if not lock_key then
            return nil, "lock_key is mandatory if safe is set. please use host:port or unix socket as the lock_key."
        end
        if type(lock_key) ~= "string" then
            return nil, "lock_key must be a string. please use host:port or unix socket as the lock_key."
        end
        local lock_opts = {exptime = 30, timeout = 0, step = 0.001, ratio = 2, max_step = 0.5}
        qlock = lock:new("queue_dict", lock_opts)
    end
    return setmetatable(
               {data = tab, capacity = cap, size = 0, head = 1, tail = 1, qlock = qlock, qkey = lock_key},
               {__index = self}
           )
end

function _M.enqueue(self,element)
    if self.qlock then
        local elapsed,err = self.qlock:lock(self.qkey)
        if not elapsed then
            return nil, "failed to lock the queue"
        end
    end

    if self.size == self.capacity then
        return nil, "queue is full, size=".. (self.size or "nil")..", capacity="..(self.capacity or "nil")
    end
    self.data[self.tail] = element
    self.tail = self.tail % self.capacity + 1
    self.size = self.size + 1

    if self.qlock then
        self.qlock:unlock()
    end

    return true, "SUCCESS"
end

function _M.dequeue(self)
    if self.qlock then
        local elapsed,err = self.qlock:lock(self.qkey)
        if not elapsed then
            return nil, "failed to lock the queue"
        end
    end

    if self.size == 0 then
        return nil, "queue is empty"
    end
    local element = self.data[self.head]
    self.data[self.head] = nil
    self.head = self.head % self.capacity + 1
    self.size = self.size - 1

    if self.qlock then
        self.qlock:unlock()
    end

    return true, element 
end

function _M.get_size(self)
    return self.size
end

return _M
