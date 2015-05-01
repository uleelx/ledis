local loop = require("socketloop")
local glue = require("glue")
local flatdb = require("flatdb")
local sha1 = require("sha1")

local string_sub, string_format = string.sub, string.format
local string_upper, string_lower = string.upper, string.lower
local string_find, string_gsub = string.find, string.gsub
local table_remove, table_concat, table_insert = table.remove, table.concat, table.insert
local unpack = unpack or table.unpack

-- initialize everything
--======================
local db = flatdb("./db")

if not db[0] then
	db[0] = {}
end

if not db.expire then
	db.expire = {[0] = {}}
end

local loaded_scripts = {}

local pubsub = glue.autotable()

local shutdown = false

--~~~~~~~~~~~~~~~~~~~~~~

-- common functions
--======================

local function start_stop(start, stop, size)
	start, stop = tonumber(start), tonumber(stop)
	if start < 0 then start = start + size end
	if start < 0 then start = 0 end
	start = start + 1

	if stop < 0 then stop = stop + size end
	if stop < 0 then stop = 0 end
	if stop > size - 1 then stop = size - 1 end
	stop = stop + 1

	return start, stop
end

local function get_sorted_keys_by_values(t, rev)
	return glue.keys(t, function(k1, k2)
		return (t[k1] == t[k2] and k1 <= k2 or t[k1] < t[k2]) == not rev
	end)
end

--~~~~~~~~~~~~~~~~~~~~~~


-- bulletin board stuff for blocking commands
--======================
local board = {}

local function reg(page, ...)
	local thread = loop.current()
	if not board[page] then
		board[page] = {}
	end
	for _, key in ipairs{...} do
		if not board[page][key] then
			board[page][key] = {}
		end
		table_insert(board[page][key], thread)
	end
	return loop.suspend()
end

local function unreg(page, ...)
	local me = loop.current()
	for _, key in ipairs{...} do
		for index, thread in ipairs(board[page][key]) do
			if thread == me then
				table_remove(board[page][key], index)
				break
			end
		end
	end
end

local function wake(page, key, n)
	if board[page] and type(board[page][key]) == "table" then
		for i = 1, n do
			if not next(board[page][key]) then break end
			loop.resume(table_remove(board[page][key], 1), key)
		end
	end
end
--~~~~~~~~~~~~~~~~~~~~~~


-- define keys expiring stuff
--======================
local expire = db.expire
local last_expire_time = os.time()

local function check_expired(page, key)
	if not key then return false end
	if expire[page][key] and expire[page][key] < os.time() then
		db[page][key] = nil
		expire[page][key] = nil
		return true
	end
	return false
end

local function launch_expire()
	for page, keyspace in pairs(db) do
		if tonumber(page) then
			local key
			repeat
				local expired = 0
				local tmp = {}
				for i = 1, 20 do
					key = next(keyspace, key)
					if key == nil then break end
					tmp[#tmp + 1] = key
				end
				for _, key in ipairs(tmp) do
					expired = expired + (check_expired(page, key) and 1 or 0)
				end
			until expired < 5
		end
	end
end
--~~~~~~~~~~~~~~~~~~~~~~


-- define redis stuff
--======================

local RESP
RESP = {
	simple_string = function (s)
		return string_format("+%s\r\n", s)
	end,
	error = function (s)
		return string_format("-%s\r\n", s)
	end,
	integer = function (n)
		return string_format(":%d\r\n", n)
	end,
	bulk_string = function (s)
		if not s then return "$-1\r\n" end
		return string_format("$%d\r\n%s\r\n", #tostring(s), s)
	end,
	array = function (t, length)
		length = length or (t and #t or 0)
		local ret = {"*"..length.."\r\n"}
		for i = 1, length do
			local v = t[i]
			ret[i + 1] = RESP.auto(v)
		end
		return table_concat(ret)
	end,
	auto = function(o)
		local tp = type(o)
		if tp == "number" then
			if math.floor(o) == o then
				return RESP.integer(o)
			else
				return RESP.bulk_string(o)
			end
		elseif tp == "table" then
			if o.ok then return RESP.simple_string(o.ok) end
			if o.err then return RESP.error(o.err) end
			if o.len then return RESP.array(o[1], o.len) end
			return RESP.array(o)
		elseif o == true then
			return RESP.integer(1)
		else
			return RESP.bulk_string(o)
		end
	end
}

local COMMANDS
local COMMAND_ARGC

local function do_commands(skt, page, cmd, args)
	local ret, r, tag
	if COMMANDS[cmd] then
		if #args >= COMMAND_ARGC[cmd] then
			if cmd == "SUBSCRIBE" or cmd == "UNSUBSCRIBE" then
				ret, r = COMMANDS[cmd](skt, unpack(args))
			else
				check_expired(page, args[1])
				ret, r, tag = COMMANDS[cmd](page, unpack(args))
			end
		else
			ret, r = "ERROR: not enough arguments", RESP.error
		end
	else
		ret, r = "ERROR: command not found", RESP.error
	end
	return ret, r, tag
end

local cmd_server = {
	SAVE = function()
		if db:save() then
			return "OK", RESP.simple_string
		else
			return "ERROR", RESP.error
		end
	end,
	SHUTDOWN = function(page, key)
		loop.stop()
		if not key or string_upper(key) == "SAVE" then
			if not db:save() then return "ERROR", RESP.error end
		end
		shutdown = true
	end,
	DBSIZE = function(page)
		return glue.count(db[page]), RESP.integer
	end,
	FLUSHDB = function(page)
		db[page] = {}
		expire[page] = {}
		return "OK", RESP.simple_string
	end,
	FLUSHALL = function()
		for page in pairs(db) do
			db[page] = {}
		end
		expire = db.expire
		expire[0] = {}
		return "OK", RESP.simple_string
	end
}

local cmd_connection = {
	ECHO = function(page, message)
		return message, RESP.bulk_string
	end,
	PING = function()
		return "PONG", RESP.simple_string
	end,
	QUIT = function()
		return true, nil, true
	end,
	SELECT = function (page, index)
		index = tonumber(index)
		if index and index < 16 then
			if not db[index] then
				db[index] = {}
			end
			if not expire[index] then
				expire[index] = {}
			end
			return "OK", RESP.simple_string, index
		else
			return "ERROR: database not found", RESP.error
		end
	end
}

local cmd_keys = {
	DEL = function(page, ...)
		local c = 0
		local args = {...}
		for i, v in ipairs(args) do
			if db[page][v] then
				db[page][v] = nil
				c = c + 1
			end
			if expire[page][v] then
				expire[page][v] = nil
			end
		end
		if #args == 1 then
			return "OK", RESP.simple_string
		else
			return c, RESP.integer
		end
	end,
	EXISTS = function(page, key)
		return (db[page][key] and 1 or 0), RESP.integer
	end,
	RANDOMKEY = function(page)
		for key in pairs(db[page]) do
			if check_expired(page, key) == false then
				return key, RESP.bulk_string
			end
		end
		return false, RESP.bulk_string
	end,
	EXPIRE = function(page, key, seconds)
		if db[page][key] then
			expire[page][key] = os.time() + tonumber(seconds)
			return 1, RESP.integer
		else
			return 0, RESP.integer
		end
	end,
	TTL = function(page, key)
		if db[page][key] then
			if expire[page][key] then
				return (expire[page][key] - os.time()), RESP.integer
			else
				return -1, RESP.integer
			end
		else
			return -2, RESP.integer
		end
	end,
	PERSIST = function(page, key)
		if db[page][key] and expire[page][key] then
			expire[page][key] = nil
			return 1, RESP.integer
		else
			return 0, RESP.integer
		end
	end,
	KEYS = function(page, pattern)
		pattern = string_gsub(pattern, "\\?[*?]", {
			["\\?"] = "%?",
			["\\*"] = "%*",
			["*"] = ".*",
			["?"] = ".?"
		})
		local ret = {}
		for k in pairs(db[page]) do
			if string_find(k, pattern) and check_expired(page, k) == false then
				ret[#ret + 1] = k
			end
		end
		return ret, RESP.array
	end
}

local cmd_strings
cmd_strings = {
	GET = function(page, key)
		return db[page][key], RESP.bulk_string
	end,
	SET = function(page, key, value, EX, seconds)
		db[page][key] = value
		if EX == nil then
			expire[page][key] = nil
		else
			expire[page][key] = os.time() + tonumber(seconds)
		end
		return "OK", RESP.simple_string
	end,
	GETSET = function(page, key, value)
		local ret, r = cmd_strings.GET(page, key)
		cmd_strings.SET(page, key, value)
		return ret, r
	end,
	MGET = function(page, ...)
		local length = select("#", ...)
		local ret = {}
		for i, key in ipairs{...} do
			ret[i] = db[page][key]
		end
		return {ret, len = length}, RESP.auto
	end,
	MSET = function(page, ...)
		for i = 1, select("#", ...), 2 do
			local key, value = select(i, ...)
			cmd_strings.SET(page, key, value)
		end
		return "OK", RESP.simple_string
	end,
	SETEX = function(page, key, seconds, value)
		db[page][key] = value
		expire[page][key] = os.time() + tonumber(seconds)
		return "OK", RESP.simple_string
	end,
	INCR = function(page, key)
		db[page][key] = (tonumber(db[page][key]) or 0) + 1
		return db[page][key], RESP.integer
	end,
	DECR = function(page, key)
		db[page][key] = (tonumber(db[page][key]) or 0) - 1
		return db[page][key], RESP.integer
	end,
	INCRBY = function(page, key, increment)
		db[page][key] = (tonumber(db[page][key]) or 0) + increment
		return db[page][key], RESP.integer
	end,
	DECRBY = function(page, key, decrement)
		db[page][key] = (tonumber(db[page][key]) or 0) - decrement
		return db[page][key], RESP.integer
	end
}

local cmd_lists
cmd_lists = {
	RPUSH = function(page, key, ...)
		if not db[page][key] then
			db[page][key] = {}
		end
		if type(db[page][key]) ~= "table" then
			return "ERROR: list required", RESP.error
		end
		for _, value in ipairs{...} do
			table_insert(db[page][key], value)
		end
		wake(page, key, select("#", ...))
		return #db[page][key], RESP.integer
	end,
	LPUSH = function(page, key, ...)
		if not db[page][key] then
			db[page][key] = {}
		end
		if type(db[page][key]) ~= "table" then
			return "ERROR: list required", RESP.error
		end
		for _, value in ipairs{...} do
			table_insert(db[page][key], 1, value)
		end
		wake(page, key, select("#", ...))
		return #db[page][key], RESP.integer
	end,
	RPOP = function(page, key)
		if db[page][key] then
			if type(db[page][key]) ~= "table" then
				return "ERROR: list required", RESP.error
			end
			return table_remove(db[page][key]), RESP.bulk_string
		else
			return false, RESP.bulk_string
		end
	end,
	BRPOP = function(page, ...)
		local keys = {...}
		table_remove(keys)
		for i = 1, #keys do
			local key = keys[i]
			if db[page][key] then
				if type(db[page][key]) ~= "table" then
					return "ERROR: list required", RESP.error
				end
				if next(db[page][key]) then
					return {key, table_remove(db[page][key])}, RESP.array
				end
			end
		end
		local key = reg(page, unpack(keys))
		unreg(page, unpack(keys))
		return {key, table_remove(db[page][key])}, RESP.array
	end,
	LPOP = function(page, key)
		if db[page][key] then
			if type(db[page][key]) ~= "table" then
				return "ERROR: list required", RESP.error
			end
			return table_remove(db[page][key], 1), RESP.bulk_string
		else
			return false, RESP.bulk_string
		end
	end,
	BLPOP = function(page, ...)
		local keys = {...}
		table_remove(keys)
		for i = 1, #keys do
			local key = keys[i]
			if db[page][key] then
				if type(db[page][key]) ~= "table" then
					return "ERROR: list required", RESP.error
				end
				if next(db[page][key]) then
					return {key, table_remove(db[page][key], 1)}, RESP.array
				end
			end
		end
		local key = reg(page, unpack(keys))
		unreg(page, unpack(keys))
		return {key, table_remove(db[page][key], 1)}, RESP.array
	end,
	LLEN = function(page, key)
		if type(db[page][key]) ~= "table" then
			return "ERROR: list required", RESP.error
		end
		return (#db[page][key] or 0), RESP.integer
	end,
	LINDEX = function(page, key, index)
		if type(db[page][key]) == "table" then
			local i = tonumber(index)
			if i < 0 then i = i + #db[page][key] end
			i = i + 1
			return db[page][key][i], RESP.bulk_string
		else
			return false, RESP.bulk_string
		end
	end,
	LRANGE = function(page, key, start, stop, trim)
		if type(db[page][key]) ~= "table" then
			return "ERROR: list required", RESP.error
		end
		local t = db[page][key]
		start, stop = start_stop(start, stop, #t)
		local ret = {}
		for i = start, stop do
			ret[#ret + 1] = t[i]
		end
		if trim then return ret end
		return ret, RESP.array
	end,
	LTRIM = function(page, key, start, stop)
		local t = cmd_lists.LRANGE(page, key, start, stop, true)
		if type(t) ~= "table" then return t end
		db[page][key] = t
		return "OK", RESP.simple_string
	end,
	LSET = function(page, key, index, value)
		if type(db[page][key]) ~= "table" then
			return "ERROR: list required", RESP.error
		end
		local i = tonumber(index)
		if i < 0 then i = i + #db[page][key] end
		i = i + 1
		if i < 0 or i > #db[page][key] then
			return "ERROR: out of range indexes", RESP.error
		end
		db[page][key][i] = value
		return "OK", RESP.simple_string
	end
}

local cmd_hashes
cmd_hashes = {
	HGET = function(page, key, field)
		if db[page][key] then
			if type(db[page][key]) ~= "table" then
				return "ERROR: hash required", RESP.error
			end
			if db[page][key][field] then
				return db[page][key][field], RESP.bulk_string
			end
		end
		return false, RESP.bulk_string
	end,
	HSET = function(page, key, field, value, NX)
		if not db[page][key] then
			db[page][key] = {}
		end
		if type(db[page][key]) ~= "table" then
			return "ERROR: hash required", RESP.error
		end
		local ret = db[page][key][field] and 0 or 1
		if ret == 1 or not NX then db[page][key][field] = value end
		return ret, RESP.integer
	end,
	HSETNX = function(page, key, field, value)
		return cmd_hashes.HSET(page, key, field, value, true)
	end,
	HMGET = function(page, key, ...)
		local length = select("#", ...)
		local ret = {}
		if db[page][key] then
			if type(db[page][key]) ~= "table" then
				return "ERROR: hash required", RESP.error
			end
			for i, fd in ipairs{...} do
				ret[i] = db[page][key][fd]
			end
		end
		return {ret, len = length}, RESP.auto
	end,
	HMSET = function(page, key, ...)
		if not db[page][key] then
			db[page][key] = {}
		end
		if type(db[page][key]) ~= "table" then
			return "ERROR: hash required", RESP.error
		end
		for i = 1, select("#", ...), 2 do
			local field, value = select(i, ...)
			db[page][key][field] = value
		end
		return "OK", RESP.simple_string
	end,
	HEXISTS = function(page, key, field)
		if db[page][key] then
			if type(db[page][key]) ~= "table" then
				return "ERROR: hash required", RESP.error
			end
			if db[page][key][field] then
				return 1, RESP.integer
			end
		end
		return 0, RESP.integer
	end,
	HDEL = function(page, key, ...)
		local c = 0
		if db[page][key] then
			for _, fd in ipairs{...} do
				if db[page][key][fd] then
					db[page][key][fd] = nil
					c = c + 1
				end
			end
		end
		return c, RESP.integer
	end,
	HLEN = function(page, key)
		if db[page][key] then
			if type(db[page][key]) ~= "table" then
				return "ERROR: hash required", RESP.error
			end
			return glue.count(db[page][key]), RESP.integer
		end
		return 0, RESP.integer
	end,
	HGETALL = function(page, key)
		local ret = {}
		if db[page][key] then
			if type(db[page][key]) ~= "table" then
				return "ERROR: hash required", RESP.error
			end
			for field, value in pairs(db[page][key]) do
				table_insert(ret, field)
				table_insert(ret, value)
			end
		end
		return ret, RESP.array
	end,
	HKEYS = function(page, key)
		if db[page][key] then
			if type(db[page][key]) ~= "table" then
				return "ERROR: hash required", RESP.error
			end
			return glue.keys(db[page][key]), RESP.array
		end
		return false, RESP.array
	end,
	HVALS = function(page, key)
		local ret = {}
		if db[page][key] then
			if type(db[page][key]) ~= "table" then
				return "ERROR: hash required", RESP.error
			end
			for _, val in pairs(db[page][key]) do
				table_insert(ret, val)
			end
		end
		return ret, RESP.array
	end,
	HINCRBY = function(page, key, field, increment)
		if not db[page][key] then
			db[page][key] = {}
		end
		if type(db[page][key]) ~= "table" then
			return "ERROR: hash required", RESP.error
		end
		db[page][key][field] = (tonumber(db[page][key][field]) or 0) + increment
		return db[page][key][field], RESP.integer
	end
}

local cmd_sets
cmd_sets = {
	SADD = function(page, key, ...)
		if not db[page][key] then
			db[page][key] = {}
		end
		if type(db[page][key]) ~= "table" then
			return "ERROR: set required", RESP.error
		end
		local c = 0
		for _, member in ipairs{...} do
			if not db[page][key][member] then
				db[page][key][member] = true
				c = c + 1
			end
		end
		return c, RESP.integer
	end,
	SREM = function(page, key, ...)
		local c = 0
		if db[page][key] then
			if type(db[page][key]) ~= "table" then
				return "ERROR: set required", RESP.error
			end
			for _, member in ipairs{...} do
				if db[page][key][member] then
					db[page][key][member] = nil
					c = c + 1
				end
			end
		end
		return c, RESP.integer
	end,
	SISMEMBER = function(page, key, member)
		return ((db[page][key] and db[page][key][member]) and 1 or 0), RESP.integer
	end,
	SCARD = function(page, key)
		return cmd_hashes.HLEN(page, key)
	end,
	SINTER = function(page, key, ...)
		local ret = glue.merge({}, db[page][key])
		if type(ret) == "table" then
			for i, key in ipairs{...} do
				if type(db[page][key]) ~= "table" then
					return false, RESP.array
				end
				local tmp = {}
				for member in pairs(ret) do
					tmp[member] = db[page][key][member]
				end
				ret = tmp
			end
		end
		return (ret and glue.keys(ret)), RESP.array
	end,
	SMEMBERS = function(page, key)
		return cmd_sets.SINTER(page, key)
	end,
	SUNION = function(page, ...)
		local ret = {}
		for _, key in ipairs{...} do
			if type(db[page][key]) == "table" then
				for member in pairs(db[page][key]) do
					ret[member] = true
				end
			end
		end
		return glue.keys(ret), RESP.array
	end,
	SDIFF = function(page, key, ...)
		local ret = glue.merge({}, db[page][key])
		if type(ret) == "table" then
			for _, key in ipairs{...} do
				if type(db[page][key]) == "table" then
					for member in pairs(db[page][key]) do
						ret[member] = nil
					end
				end
			end
		end
		return (ret and glue.keys(ret)), RESP.array
	end,
	SRANDMEMBER = function(page, key, count)
		if count then
			if type(db[page][key]) == "table" then
				count = tonumber(count)
				local ret = {}
				if count > 0 then
					for member in pairs(db[page][key]) do
						ret[#ret + 1] = member
						count = count - 1
						if count == 0 then break end
					end
				elseif count < 0 then
					count = math.abs(count)
					local all = glue.keys(db[page][key])
					for i = 1, count do
						ret[#ret + 1] = all[math.random(#all)]
					end
				end
				return ret, RESP.array
			end
			return false, RESP.array
		end
		return (db[page][key] and (next(db[page][key]))), RESP.bulk_string
	end
}

local cmd_sorted_sets
cmd_sorted_sets = {
	ZADD = function(page, key, ...)
		if not db[page][key] then
			db[page][key] = {}
		end
		if type(db[page][key]) ~= "table" then
			return "ERROR: sorted set required", RESP.error
		end
		local c = 0
		for i = 1, select("#", ...), 2 do
			local score, member = select(i, ...)
			if not db[page][key][member] then
				c = c + 1
			end
			db[page][key][member] = tonumber(score)
		end
		return c, RESP.integer
	end,
	ZREM = cmd_sets.SREM,
	ZSCORE = function(page, key, member)
		return (db[page][key] and db[page][key][member]), RESP.bulk_string
	end,
	ZCARD = cmd_sets.SCARD,
	ZRANGE = function(page, key, start, stop, WITHSCORES, rev)
		if type(db[page][key]) == "table" then
			local members = get_sorted_keys_by_values(db[page][key], rev)
			local ret = {}
			start, stop = start_stop(start, stop, #members)
			for i = start, stop do
				table_insert(ret, members[i])
				if WITHSCORES == "WITHSCORES" then
					table_insert(ret, db[page][key][members[i]])
				end
			end
			return ret, RESP.array
		end
		return false, RESP.array
	end,
	ZREVRANGE = function(page, key, start, stop, WITHSCORES)
		return cmd_sorted_sets.ZRANGE(page, key, start, stop, WITHSCORES, true)
	end,
	ZRANK = function(page, key, member, rev)
		if type(db[page][key]) == "table" and db[page][key][member] then
			local members = get_sorted_keys_by_values(db[page][key], rev)
			return (glue.index(members)[member] - 1), RESP.integer
		end
		return false, RESP.bulk_string
	end,
	ZREVRANK = function(page, key, member)
		return cmd_sorted_sets.ZRANK(page, key, member, true)
	end,
	ZINCRBY = function(page, key, increment, member)
		if not db[page][key] then
			db[page][key] = {}
		end
		if type(db[page][key]) ~= "table" then
			return "ERROR: sorted set required", RESP.error
		end
		db[page][key][member] = (tonumber(db[page][key][member]) or 0) + increment
		return db[page][key][member], RESP.bulk_string
	end
}

local cmd_pubsub = {
	SUBSCRIBE = function(skt, ...)
		local ret = {}
		for _, channel in ipairs{...} do
			pubsub[skt][channel] = true
			pubsub[channel][skt] = true
			table_insert(ret, RESP.array{"subscribe", channel, glue.count(pubsub[skt])})
		end
		return table_concat(ret)
	end,
	UNSUBSCRIBE = function(skt, ...)
		local ret = {}
		local channels = (...) and {...} or glue.keys(pubsub[skt])
		for _, channel in ipairs(channels) do
			pubsub[skt][channel] = nil
			pubsub[channel][skt] = nil
			table_insert(ret, RESP.array{"unsubscribe", channel, glue.count(pubsub[skt])})
		end
		return table_concat(ret)
	end,
	PUBLISH = function(_, channel, message)
		local c = 0
		for skt in pairs(pubsub[channel]) do
			if skt:send(RESP.array{"message", channel, message}) then
				c = c + 1
			end
		end
		return c, RESP.integer
	end
}

local cmd_scripting
cmd_scripting = {
	EVAL = function(page, script, numkeys, ...)
		local sha1hex = sha1.sha1(script)
		if not loaded_scripts[sha1hex] then
			loaded_scripts[sha1hex] = script
		end
		local KEYS = {}
		local ARGV = {select(numkeys + 1, ...)}
		for i = 1, numkeys do
			local key = select(i, ...)
			check_expired(page, key)
			table_insert(KEYS, key)
		end
		local redis
		redis = {
			call = function(cmd, ...)
				cmd = string_upper(cmd)
				local ret, r, tag = do_commands(nil, page, cmd, {...})
				if tag ~= nil then
					if cmd == "SELECT" then
						page = tonumber(tag)
					end
				end
				if r == RESP.simple_string then
					ret = {ok = ret}
				elseif r == RESP.error then
					error(ret)
				end
				return ret
			end,
			pcall = function(cmd, ...)
				local ok, ret = pcall(redis.call, cmd, ...)
				if ok then return ret end
				return {err = ret}
			end,
			status_reply = function(status_string)
				return {ok = status_string}
			end,
			error_reply = function(error_string)
				return {err = error_string}
			end
		}
		local chunk, err = load(script, "redis_script.lua", "t", glue.merge({
			KEYS = KEYS,
			ARGV = ARGV,
			redis = redis
		}, _G))
		if chunk then
			local ret = {pcall(chunk)}
			if ret[1] then
				return ret[2], RESP.auto
			else
				return "Run script error: "..ret[2], RESP.error
			end
		else
			return "Load script error: "..err, RESP.error
		end
	end,
	EVALSHA = function(page, sha1, numkeys, ...)
		sha1 = string_lower(sha1)
		if loaded_scripts[sha1] then
			return cmd_scripting.EVAL(page, loaded_scripts[sha1], numkeys, ...)
		else
			return "ERROR: Script("..sha1..") not exists.", RESP.error
		end
	end,
	SCRIPT = function(page, cmd, script, ...)
		cmd = string_upper(cmd)
		if cmd == "LOAD" then
			local sha1hex = sha1.sha1(script)
			loaded_scripts[sha1hex] = script
			return sha1hex, RESP.bulk_string
		elseif cmd == "FLUSH" then
			loaded_scripts = {}
			return "OK", RESP.simple_string
		elseif cmd == "EXISTS" then
			local ret = {}
			for _, sha1hex in ipairs{script, ...} do
				table_insert(ret, loaded_scripts[sha1hex] and 1 or 0)
			end
			return ret, RESP.array
		end
	end
}


COMMANDS = glue.merge({},
	cmd_server,
	cmd_connection,
	cmd_keys,
	cmd_strings,
	cmd_lists,
	cmd_hashes,
	cmd_sets,
	cmd_sorted_sets,
	cmd_pubsub,
	cmd_scripting
)

COMMAND_ARGC = {
	SAVE = 0,
	SHUTDOWN = 0,
	DBSIZE = 0,
	FLUSHDB = 0,
	FLUSHALL = 0,
	--
	ECHO = 1,
	PING = 0,
	QUIT = 0,
	SELECT = 1,
	--
	DEL = 1,
	EXISTS = 1,
	RANDOMKEY = 0,
	EXPIRE = 2,
	TTL = 1,
	PERSIST = 1,
	KEYS = 1,
	--
	GET = 1,
	SET = 2,
	GETSET = 2,
	MGET = 1,
	MSET = 2,
	SETEX = 3,
	INCR = 1,
	DECR = 1,
	INCRBY = 2,
	DECRBY = 2,
	--
	RPUSH = 2,
	LPUSH = 2,
	RPOP = 1,
	BRPOP = 2,
	LPOP = 1,
	BLPOP = 2,
	LLEN = 1,
	LINDEX = 2,
	LRANGE = 3,
	LTRIM = 3,
	LSET = 3,
	--
	HGET = 2,
	HSET = 3,
	HSETNX = 3,
	HMGET = 2,
	HMSET = 3,
	HEXISTS = 2,
	HDEL = 1,
	HLEN = 1,
	HGETALL = 1,
	HKEYS = 1,
	HVALS = 1,
	HINCRBY = 3,
	--
	SADD = 2,
	SREM = 2,
	SISMEMBER = 2,
	SCARD = 1,
	SINTER = 1,
	SMEMBERS = 1,
	SUNION = 1,
	SDIFF = 1,
	SRANDMEMBER = 1,
	--
	ZADD = 3,
	ZREM = 2,
	ZSCORE = 2,
	ZCARD = 1,
	ZRANGE = 3,
	ZREVRANGE = 3,
	ZRANK = 2,
	ZREVRANK = 2,
	ZINCRBY = 3,
	--
	SUBSCRIBE = 1,
	UNSUBSCRIBE = 0,
	PUBLISH = 2,
	--
	EVAL = 2,
	EVALSHA = 2,
	SCRIPT = 1
}

--~~~~~~~~~~~~~~~~~~~~~~


-- ledis server
--======================
local function get_args(skt)
	local args = {}
	local tmp = skt:receive()
	if tmp == "" then tmp = skt:receive() end
	if not tmp or string_sub(tmp, 1, 1) ~= "*" then return nil end
	tmp = string_sub(tmp, 2)
	local argc = tonumber(tmp)
	for i = 1, argc do
		local tmp = skt:receive()
		if tmp == "" then tmp = skt:receive() end
		if not tmp or string_sub(tmp, 1, 1) ~= "$" then return nil end
		tmp = string_sub(tmp, 2)
		tmp = skt:receive(tonumber(tmp))
		args[#args + 1] = tmp
	end
	return args
end

local function handler(skt)
	local page = 0
	while true do
		if last_expire_time - os.time() > 1 then
			last_expire_time = os.time()
			launch_expire()
		end
		local args = get_args(skt)
		if not args then break end
		local cmd = string_upper(table_remove(args, 1))
		local ret, r, tag = do_commands(skt, page, cmd, args)
		if type(r) == "function" then ret = r(ret) end
		if tag ~= nil then
			if cmd == "SELECT" then
				page = tag
			elseif cmd == "QUIT" then
				skt:close(); break
			end
		end
		if shutdown then skt:close(); break end
		skt:send(ret)
	end
end

loop.newserver("*", 6379, handler)

loop.start()
