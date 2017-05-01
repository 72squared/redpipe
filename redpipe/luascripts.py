# -*- coding: utf-8 -*-

"""
redpipe.luascripts
------------------
Some utility lua scripts used to extend some functionality in redis.
It also let's me exercise the eval code path a bit.
"""

lua_restorenx = """
local key = KEYS[1]
local ttl = ARGV[1]
local data = ARGV[2]
local res = redis.call('exists', key)
if res == 0 then
    redis.call('restore', key, ttl, data)
    return 1
else
    return 0
end
"""

lua_object_info = """
local key = KEYS[1]
local subcommand = ARGV[1]
return redis.call('object', subcommand, key)
"""
