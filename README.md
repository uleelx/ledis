Ledis
===========

Ledis is a redis server replacement which provides a very small subset of Redis commands.<br>
It uses [FlatDB](https://github.com/uleelx/FlatDB) as the database backend that persists on disk.

Usage
==========

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
- More testing required
- Implement more commands
- Make it robust enough to serve for productions

Dependencies
=======

- [pp](https://github.com/luapower/pp)
- [lfs](http://keplerproject.github.io/luafilesystem)
- [socketloop](https://github.com/luapower/socketloop)
  - [socket](https://github.com/diegonehab/luasocket)
  - [glue](https://github.com/luapower/glue)

All above libraries can be found on [LuaPower](https://luapower.com/).

License
=======

Ledis is distributed under the MIT license.
