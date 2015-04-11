local loop = require("socketloop")
local glue = require("glue")

local flatdb = require("flatdb")

local db = assert(flatdb("./db"))

if not db[0] then
	db[0] = {}
end

if not db.expire then
	db.expire = {[0] = {}, size = 0}
end

local shutdown = false

local RESP
RESP = {
	simple_string = function (s)
		return string.format("+%s\r\n", s)
	end,
	error = function (s)
		return string.format("-%s\r\n", s)
	end,
	integer = function (n)
		return string.format(":%d\r\n", n)
	end,
	bulk_string = function (s)
		if s == nil then return "$-1\r\n" end
		return string.format("$%d\r\n%s\r\n", #tostring(s), s)
	end,
	array = function (t)
		local ret = {"*"..#t.."\r\n"}
		for i, v in ipairs(t) do
			ret[i + 1] = type(v) == "string" and RESP.bulk_string(v) or RESP.integer(v)
		end
		return table.concat(ret)
	end
}

local cmd_server = {
	SAVE = function()
		if db:save() then
			return RESP.simple_string("OK")
		else
			return RESP.error("ERROR")
		end
	end,
	SHUTDOWN = function(page, key)
		loop.stop()
		if not key or string.upper(key) == "SAVE" then
			if not db:save() then return RESP.error("ERROR") end
		end
		shutdown = true
	end
}

local cmd_connection = {
	ECHO = function(page, message)
		return RESP.bulk_string(message)
	end,
	PING = function()
		return RESP.simple_string("PONG")
	end,
	SELECT = function (page, index)
		index = tonumber(index)
		if index and index < 16 then
			if not db[index] then
				db[index] = {}
			end
			if not db.expire[index] then
				db.expire[index] = {}
			end
			return RESP.simple_string("OK"), index
		else
			return RESP.error("ERROR: database not found")
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
			if db.expire[page][v] then
				db.expire[page][v] = nil
				db.expire.size = db.expire.size - 1
			end
		end
		return #args == 1 and RESP.simple_string("OK") or RESP.integer(c)
	end,
	EXPIRE = function(page, key, seconds)
		if db[page][key] then
			db.expire[page][key] = os.time() + tonumber(seconds)
			db.expire.size = db.expire.size + 1
			return RESP.integer(1)
		else
			return RESP.integer(0)
		end
	end,
	KEYS = function(page, pattern)
		pattern = string.gsub(pattern, "\\?[*?]", {
			["\\?"] = "%?",
			["\\*"] = "%*",
			["*"] = ".*",
			["?"] = ".?"
		})
		local ret = {}
		for k in pairs(db[page]) do
			if string.find(k, pattern) then
				ret[#ret + 1] = k
			end
		end
		return RESP.array(ret)
	end
}

local cmd_strings = {
	GET = function(page, key)
		return RESP.bulk_string(db[page][key])
	end,
	SET = function(page, key, value, seconds)
		db[page][key] = value
		if seconds == nil then
			db.expire[page][key] = nil
			if db.expire[page][key] then db.expire.size = db.expire.size - 1 end
		else
			if not db.expire[page][key] then db.expire.size = db.expire.size + 1 end
			db.expire[page][key] = os.time() + tonumber(seconds)
		end
		return RESP.simple_string("OK")
	end,
	SETEX = function(page, key, seconds, value)
		db[page][key] = value
		if not db.expire[page][key] then db.expire.size = db.expire.size + 1 end
		db.expire[page][key] = os.time() + tonumber(seconds)
		return RESP.simple_string("OK")
	end,
	INCR = function(page, key)
		db[page][key] = (tonumber(db[page][key]) or 0) + 1
		return RESP.integer(db[page][key])
	end,
	DECR = function(page, key)
		db[page][key] = (tonumber(db[page][key]) or 0) - 1
		return RESP.integer(db[page][key])
	end
}

local cmd_lists = {
	RPUSH = function(page, key, value)
		if not db[page][key] then
			db[page][key] = {}
		end
		if type(db[page][key]) ~= "table" then
			return RESP.error("ERROR: list required")
		end
		table.insert(db[page][key], value)
		return RESP.integer(#db[page][key])
	end,
	LPUSH = function(page, key, value)
		if not db[page][key] then
			db[page][key] = {}
		end
		if type(db[page][key]) ~= "table" then
			return RESP.error("ERROR: list required")
		end
		table.insert(db[page][key], 1, value)
		return RESP.integer(#db[page][key])
	end,
	RPOP = function(page, key)
		if db[page][key] then
			if type(db[page][key]) ~= "table" then
				return RESP.error("ERROR: list required")
			end
			return RESP.bulk_string(table.remove(db[page][key]))
		else
			return RESP.bulk_string(nil)
		end
	end,
	LPOP = function(page, key)
		if db[page][key] then
			if type(db[page][key]) ~= "table" then
				return RESP.error("ERROR: list required")
			end
			return RESP.bulk_string(table.remove(db[page][key], 1))
		else
			return RESP.bulk_string(nil)
		end
	end,
	LLEN = function(page, key)
		if type(db[page][key]) ~= "table" then
			return RESP.error("ERROR: list required")
		end
		return RESP.integer(#db[page][key] or 0)
	end,
	LINDEX = function(page, key, value)
		if type(db[page][key]) == "table" then
			local i = tonumber(value)
			if i < 0 then i = i + #db[page][key] end
			i = i + 1
			return RESP.bulk_string(db[page][key][i])
		else
			return RESP.bulk_string(nil)
		end
	end,
	LRANGE = function(page, key, start, stop)
		if type(db[page][key]) ~= "table" then
			return RESP.error("ERROR: list required")
		end
		local t = db[page][key]
		start, stop = tonumber(start), tonumber(stop)
		if start >= #t then return "*0\r\n" end
		if start < 0 then start = #t + start end
		if start < 0 then start = 0 end
		start = start + 1
		if stop > #t - 1 then stop = #t - 1 end
		if stop < 0 then stop = #t + stop end
		if stop < 0 then stop = 0 end
		stop = stop + 1
		if start > stop then start, stop = stop, start end
		local ret = {}
		for i = start, stop do
			ret[#ret + 1] = t[i]
		end
		return RESP.array(ret)
	end
}

local COMMANDS = glue.merge({},
	cmd_server,
	cmd_connection,
	cmd_keys,
	cmd_strings,
	cmd_lists
)

local COMMAND_ARGC = {
	SAVE = 0,
	SHUTDOWN = 0,
	--
	ECHO = 1,
	PING = 0,
	SELECT = 1,
	--
	DEL = 1,
	EXPIRE = 2,
	KEYS = 1,
	--
	GET = 1,
	SET = 2,
	SETEX = 3,
	INCR = 1,
	DECR = 1,
	--
	RPUSH = 2,
	LPUSH = 2,
	RPOP = 1,
	LPOP = 1,
	LLEN = 1,
	LINDEX = 2,
	LRANGE = 3
}

local function wait_char(skt, char, res)
	local input = res or ""
	while true do
		if #input > 0 and string.sub(input, 1, 1) == char then
			return string.sub(input, 2)
		end
		local tmp = skt:receive()
		if tmp then
			input = input..tmp
		else
			skt:close()
			return nil
		end
	end
end

local function wait_number(skt, res)
	local input = res
	while true do
		local i, j = string.find(input, "%d+")
		if i then
			return tonumber(string.sub(input, i, j)), string.sub(input, j + 1)
		end
		local tmp = skt:receive()
		if tmp then
			input = input..tmp
		end
	end
end

local function wait_argv(skt, res)
	local res = wait_char(skt, "$", res)
	local n, res = wait_number(skt, res)
	local input = res
	while true do
		if #input>=n then
			return string.sub(input, 1, n), string.sub(input, n + 1)
		end
		local tmp = skt:receive(n)
		if tmp then
			input = input..tmp
		end
	end
end

local function get_args(skt)
	local argc, argv
	local args = {}
	local res = wait_char(skt, "*")
	if res == nil then return nil end
	argc, res = wait_number(skt, res)
	for i = 1, argc do
		argv, res = wait_argv(skt, res)
		args[#args + 1] = argv
	end
	return args
end

local function check_expired(page, key)
	if db.expire[page][key] and db.expire[page][key] < os.time() then
		db[page][key] = nil
		db.expire[page][key] = nil
		db.expire.size = db.expire.size - 1
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

local function handler(skt)
	local page = 0
	while true do
		if db.expire.size > 320 then launch_expire() end
		local ret, tag
		local args = get_args(skt)
		if not args then break end
		local cmd = string.upper(table.remove(args, 1))
		if COMMANDS[cmd] then
			if #args >= COMMAND_ARGC[cmd] then
				check_expired(page, args[1])
				ret, tag = COMMANDS[cmd](page, unpack(args))
			else
				ret = RESP.error("ERROR: wrong argument numbers")
			end
		else
			ret = RESP.error("ERROR: command not found")
		end
		if tag ~= nil and cmd == "SELECT" then
			page = tag
		end
		if shutdown then skt:close(); break end
		skt:send(ret)
	end
end

loop.newserver("*", 6379, handler)

loop.start()
