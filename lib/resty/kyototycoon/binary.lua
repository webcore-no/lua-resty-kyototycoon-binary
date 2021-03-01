-- Copyright (C) Soojin Nam

-- lots of code is borrowed from lua-resty-mysql


local bit = require "bit"

local tcp = ngx.socket.tcp
local strbyte = string.byte
local strchar = string.char
local band = bit.band
local bor = bit.bor
local lshift = bit.lshift
local rshift = bit.rshift
local concat = table.concat
local insert = table.insert
local tostring = tostring
local setmetatable = setmetatable
local ipairs = ipairs


local _M = { _VERSION = '0.2.1' }


-- constants

local INTERNAL_SERVER_ERROR = 0xBF
local OP_PLAY_SCRIPT = 0xB4
local OP_SET_BULK = 0xB8
local OP_REMOVE_BULK = 0xB9
local OP_GET_BULK = 0xBA

-- Every numeric value are expressed in big-endian order.

local function _get_byte (data, i)
   local a = strbyte(data, i)
   return a, i + 1
end


local function _get_byte2 (data, i)
   local b, a = strbyte(data, i, i + 1)
   return bor(a, lshift(b, 8)), i + 2
end


local function _get_byte4 (data, i)
   local d, c, b, a = strbyte(data, i, i + 3)
   return bor(a, lshift(b, 8), lshift(c, 16), lshift(d, 24)), i + 4
end


local function _get_byte8 (data, i)
    local h, g, f, e, d, c, b, a = strbyte(data, i, i + 7)
    local lo = bor(a, lshift(b, 8), lshift(c, 16), lshift(d, 24))
    local hi = bor(e, lshift(f, 8), lshift(g, 16), lshift(h, 24))
    return lo + hi * 4294967296, i + 8
end


local function _set_byte (n)
   return strchar(band(n, 0xff))
end


local function _set_byte2 (n)
   return strchar(band(rshift(n, 8), 0xff), band(n, 0xff))
end


local function _set_byte4 (n)
   return strchar(band(rshift(n, 24), 0xff),
                  band(rshift(n, 16), 0xff),
                  band(rshift(n, 8), 0xff),
                  band(n, 0xff))
end


local function _set_byte8 (n)
   local hn = n * 4294967296

   return strchar(band(rshift(hn, 24), 0xff),
                  band(rshift(hn, 16), 0xff),
                  band(rshift(hn, 8), 0xff),
                  band(hn, 0xff),
                  band(rshift(n, 24), 0xff),
                  band(rshift(n, 16), 0xff),
                  band(rshift(n, 8), 0xff),
                  band(n, 0xff))
end


local function _send_request (self, magic, flags, req)
   local sock = self.sock
   local request = _set_byte(magic) .. _set_byte4(flags) .. req
   return sock:send(request)
end


local mt = { __index = _M }


function _M.new()
   local sock, err = tcp()
   if not sock then
      return nil, err
   end
   return setmetatable({ sock = sock }, mt)
end


function _M:play_script(name, tab)
   local flags = 0
   local sock = self.sock

   if not name or not tab or #tab == 0 then
      return nil, "invalid arguments"
   end

   local t = { _set_byte4(#name) }  -- nsiz

   insert(t, _set_byte4(#tab))      -- rnum
   insert(t, name)                  -- procedure name

   for _, v in ipairs(tab) do
      local key = v.key or v.KEY
      local value = v.value or v.VALUE
      insert(t, _set_byte4(#key))   -- ksiz
      insert(t, _set_byte4(#value)) -- vsiz
      insert(t, key)                -- key
      insert(t, value)              -- value
   end

   local bytes, err = _send_request(self, OP_PLAY_SCRIPT, flags, concat(t))

   if not bytes then
      return nil, "fail to send packet: " .. err
   end

   local data, err = sock:receive(5)
   if not data then
      return nil, "failed to receive packet: " .. err
   end

   local rv, pos = _get_byte(data, 1)

   if rv == INTERNAL_SERVER_ERROR then
      return nil, "interner server error"
   end

   local num = _get_byte4(data, pos)

   --print("hits= ", num)

   -- data
   local results, ksiz, vsiz = {}
   for _=1, num do
      local res = {}
      data = sock:receive(8)

      ksiz, pos = _get_byte4(data, 1)
      --print("ksiz= ", ksiz)

      vsiz = _get_byte4(data, pos)
      --print("vsiz= ", vsiz)

      data = sock:receive(ksiz)
      --print("key= ", data)
      res["key"] = data

      data = sock:receive(vsiz)
      --print("val= ", data)
      res["value"] = data

      results[#results+1] = res
   end

   if #results == 0 then
      return nil, "no record was found"
   else
      return results, nil
   end
end


local function _set_bulk (self, tab)
   local flags = 0
   local sock = self.sock

   if not tab or #tab == 0 then
      return nil, "invalid arguments"
   end

   local t = { _set_byte4(#tab) }    -- rnum

   for _, v in ipairs(tab) do
      local key = v[1]
      local value = v[2]
      if type(value) ~= "string" then
         value = tostring(value)
      end
      local xt = v[3] or 0xffffffff   -- max int ???
      local dbidx = v[4] or 0
      insert(t, _set_byte2(dbidx))    -- dbidx
      insert(t, _set_byte4(#key))     -- ksiz
      insert(t, _set_byte4(#value))   -- vsiz
      insert(t, _set_byte8(xt))       -- xt
      insert(t, key)                  -- key
      insert(t, value)                -- value
   end

   local bytes, err = _send_request(self, OP_SET_BULK, flags, concat(t))

   if not bytes then
      return nil, "fail to send packet: " .. err
   end

   local data, err = sock:receive(5)
   if not data then
      return nil, "failed to receive packet: " .. err
   end

   local rv, pos = _get_byte(data, 1)

   if rv == INTERNAL_SERVER_ERROR then
      return nil, "interner server error"
   end

   rv = _get_byte4(data, pos)

   --print("# of stored= ", rv)

   return rv, nil
end


_M.set_bulk = _set_bulk


function _M:set(...)
   return _set_bulk(self, {{...}})
end


local function _remove_bulk(self, tab)
   local flags = 0
   local sock = self.sock

   if not tab or #tab == 0 then
      return nil, "invalid arguemtns"
   end

   local t = { _set_byte4(#tab) }    -- rnum

   for _, v in ipairs(tab) do
      local key = v
      insert(t, _set_byte2(0))        -- dbidx
      insert(t, _set_byte4(#key))     -- ksiz
      insert(t, key)                  -- key
   end

   local bytes, err = _send_request(self, OP_REMOVE_BULK, flags, concat(t))

   if not bytes then
      return nil, "fail to send packet: " .. err
   end

   local data, err = sock:receive(5)
   if not data then
      return nil, "failed to receive packet: " .. err
   end

   local rv, pos = _get_byte(data, 1)

   if rv == INTERNAL_SERVER_ERROR then
      return nil, "interner server error"
   end

   rv = _get_byte4(data, pos)

   --print("# of removed= ", rv)

   return rv, nil
end


_M.remove_bulk = _remove_bulk


function _M:remove(key)
   return _remove_bulk(self, {key})
end

do
	local request = {
		_set_byte(OP_GET_BULK),
		_set_byte4(0), -- flags
		_set_byte4(1), -- 1 key
		_set_byte2(0), -- database index 0
	}

	function _M:get(key)
		if not key then
			return nil, "missing argument"
		end

		local sock, klen = self.sock, #key

		request[5], request[6] = _set_byte4(klen), key

		local data, err = sock:send(request)
		if not data then
			return nil, "fail to send packet: " .. (err or 'unknown')
		end

		data, err = sock:receive(5)
		if not data then
			return nil, "failed to receive packet: " .. (err or 'unknown')
		end

		if (_get_byte(data, 1)) == INTERNAL_SERVER_ERROR then
			return nil, "internal server error"
		end

		if (_get_byte4(data, 2)) == 0 then
			return nil -- not found
		end

		data, err = sock:receive(18+klen)

		if not data then
			return nil, "failed to receive packet: " .. (err or 'unknown')
		end

		return sock:receive((_get_byte4(data, 7)))
	end
end

local function _get_bulk (self, tab)
   local sock, flags = self.sock, 0

   if not tab or #tab == 0 then
      return nil, "missing argument"
   end

   local t = { _set_byte4(#tab) }    -- rnum

   for _, v in ipairs(tab) do
      local key = v
      insert(t, _set_byte2(0))        -- dbidx
      insert(t, _set_byte4(#key))     -- ksiz
      insert(t, key)                  -- key
   end

   local bytes, err = _send_request(self, OP_GET_BULK, flags, concat(t))

   if not bytes then
      return nil, "fail to send packet: " .. err
   end

   local data, err = sock:receive(5)
   if not data then
      return nil, "failed to receive packet: " .. err
   end

   local rv, pos = _get_byte(data, 1)

   if rv == INTERNAL_SERVER_ERROR then
      return nil, "interner server error"
   end

   local num = _get_byte4(data, pos)

   --print("hits= ", num)

   -- data
   local results = {}

   for _=1, num do
      local res, ksiz, vsiz = {}
      data = sock:receive(18)

      rv, pos = _get_byte2(data, 1) -- 1,2
      --print("dbidx= ", rv)
      t["dbidx"] = rv

      ksiz, pos = _get_byte4(data, pos) -- 3,4,5,6
      --print("ksiz= ", ksiz)

      vsiz, pos = _get_byte4(data, pos) -- 7,8,9.10
      --print("vsiz= ", vsiz)

      local xt = _get_byte8(data, pos)
      --print("xt= ", xt)
      res["xt"] = xt

      data = sock:receive(ksiz)
      --print("key= ", data)
      res["key"] = data

      data = sock:receive(vsiz)
      --print("val= ", data)
      res["value"] = data

      results[#results+1] = res
   end

   if #results == 0 then
      return nil, "no record was found"
   else
      return results, nil
   end
end

_M.get_bulk = _get_bulk

function _M:set_timeout(timeout)
   return self.sock:settimeout(timeout)
end


function _M:connect(...)
   return self.sock:connect(...)
end


function _M:set_keepalive(...)
   return self.sock:setkeepalive(...)
end


function _M:get_reused_times()
	return self.sock:getreusedtimes()
end


function _M:close()
	return self.sock:close()
end

return _M
