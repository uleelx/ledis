Ledis
===========

Ledis is an alternative of Redis server which provides a subset of Redis commands.<br>
It uses [FlatDB](https://github.com/uleelx/FlatDB) as the database backend that persists on disk.

Usage
==========

Ledis runs very well on LuaJIT 2.1(recommended) and it supports Lua 5.1 to Lua 5.3 though.

Run this:
```
$ luajit ledis-server.lua
```

Then it will listen to the TCP port 6379 and wait for Redis clients to connect.

Warning!!!
==========
Ledis is still in its early stage. It means it may be so buggy that you **SHOULD NOT** use it in production now.

TODO
==========
- More tests
- More commands
- Make it robust enough for production environment

Dependencies
=======

- [lua-MessagePack](https://github.com/fperrad/lua-MessagePack)
- [socket](https://github.com/diegonehab/luasocket)
- [socketloop](https://github.com/luapower/socketloop)
- [glue](https://github.com/luapower/glue)

License
=======

Ledis is distributed under the MIT license.
