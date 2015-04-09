local pp = require("pp")
local lfs = require("lfs")
local loop = require("socketloop")

------------------------------------------------------
----------------------- FlatDB -----------------------
------------------------------------------------------

local flatdb
do
	local function isFile(path)
		return lfs.attributes(path, "mode") == "file"
	end

	local function isDir(path)
		return lfs.attributes(path, "mode") == "directory"
	end

	local function load_page(path)
		return dofile(path)
	end

	local function store_page(path, page)
		if type(page) == "table" then
			local f = io.open(path, "wb")
			if f then
				f:write("return ")
				f:write(pp.format(page))
				f:close()
				return true
			end
		end
		return false
	end

	local pool = {}

	local db_funcs = {
		save = function(db, p)
			if p then
				if type(p) == "string" and type(db[p]) == "table" then
					return store_page(pool[db].."/"..p, db[p])
				else
					return false
				end
			end
			for p, page in pairs(db) do
				store_page(pool[db].."/"..p, page)
			end
			return true
		end
	}

	local mt = {
		__index = function(db, k)
			if db_funcs[k] then return db_funcs[k] end
			if isFile(pool[db].."/"..k) then
				db[k] = load_page(pool[db].."/"..k)
			end
			return rawget(db, k)
		end
	}

	pool.hack = db_funcs

	flatdb = setmetatable(pool, {
		__mode = "kv",
		__call = function(pool, path)
			if pool[path] then return pool[path] end
			if not isDir(path) then return end
			local db = {}
			setmetatable(db, mt)
			pool[path] = db
			pool[db] = path
			return db
		end
	})
end

-----------------------------------------------------
----------------------- Ledis -----------------------
-----------------------------------------------------

local db = assert(flatdb("./db"))

if not db["0"] then
	db["0"] = {}
end

local expire = {["0"] = {}}

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

local COMMANDS = {
	PING = function()
		return RESP.simple_string("PONG")
	end,
	SAVE = function(page)
		if db:save(page) then
			return RESP.simple_string("OK")
		else
			return RESP.error("ERROR")
		end
	end,
	SELECT = function (page, key)
		if tonumber(key) and tonumber(key) < 16 then
			if not db[key] then
				db[key] = {}
			end
			if not expire[key] then
				expire[key] = {}
			end
			return RESP.simple_string("OK"), key
		else
			return RESP.error("ERROR: database not found")
		end
	end,
	EXPIRE = function(page, key, seconds)
		if db[page][key] then
			expire[page][key] = os.time() + tonumber(seconds)
			return RESP.integer(1)
		else
			return RESP.integer(0)
		end
	end,
	DEL = function(page, ...)
		local c = 0
		local args = {...}
		for i, v in ipairs(args) do
			if db[page][v] then
				db[page][v] = nil
				expire[page][v] = nil
				c = c + 1
			end
		end
		return #args == 1 and RESP.simple_string("OK") or RESP.integer(c)
	end,
	GET = function(page, key)
		return RESP.bulk_string(db[page][key])
	end,
	SET = function(page, key, value, seconds)
		db[page][key] = value
		if seconds == nil then
			expire[page][key] = nil
		else
			expire[page][key] = os.time() + tonumber(seconds)
		end
		return RESP.simple_string("OK")
	end,
	INCR = function(page, key)
		db[page][key] = (db[page][key] or 0) + 1
		return RESP.integer(db[page][key])
	end,
	DECR = function(page, key)
		db[page][key] = (db[page][key] or 0) - 1
		return RESP.integer(db[page][key])
	end,
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

local COMMAND_ARGC = {
	PING = 0,
	SAVE = 0,
	SELECT = 1,
	EXPIRE = 2,
	DEL = 1,
	GET = 1,
	SET = 2,
	INCR = 1,
	DECR = 1,
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
			loop.step(0.001)
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
	argc, res = wait_number(skt, res)
	for i = 1, argc do
		argv, res = wait_argv(skt, res)
		args[#args + 1] = argv
	end
	return args
end

local function try_expire(page, key)
	if expire[page][key] and expire[page][key] < os.time() then
		db[page][key] = nil
		expire[page][key] = nil
	end
end

local function handler(skt)
	local page = "0"
	while true do
		local ret, tag
		local args = get_args(skt)
		local cmd = string.upper(table.remove(args, 1))
		if COMMANDS[cmd] then
			if #args >= COMMAND_ARGC[cmd] then
				try_expire(page, args[1])
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
		skt:send(ret)
	end
end

loop.newserver("*", 6379, handler)

loop.start()
