Ledis
===========

Ledis is a Redis-Protocol compatible server which provides a subset of Redis commands.<br>
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

- [lua-MessagePack](https://framagit.org/fperrad/lua-MessagePack)
- [socket](https://github.com/diegonehab/luasocket)
- [socketloop](https://github.com/luapower/socketloop)
- [glue](https://github.com/luapower/glue)

License
=======

This is free and unencumbered software released into the public domain.

Anyone is free to copy, modify, publish, use, compile, sell, or
distribute this software, either in source code form or as a compiled
binary, for any purpose, commercial or non-commercial, and by any
means.

In jurisdictions that recognize copyright laws, the author or authors
of this software dedicate any and all copyright interest in the
software to the public domain. We make this dedication for the benefit
of the public at large and to the detriment of our heirs and
successors. We intend this dedication to be an overt act of
relinquishment in perpetuity of all present and future rights to this
software under copyright law.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
OTHER DEALINGS IN THE SOFTWARE.

For more information, please refer to <https://unlicense.org>
