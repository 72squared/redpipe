
lua_restorenx = """
local key = KEYS[1]
local pttl = ARGV[1]
local data = ARGV[2]
local res = redis.call('exists', key)
if res == 0 then
    redis.call('restore', key, pttl, data)
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
