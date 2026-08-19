package convredis

import "github.com/redis/go-redis/v9"

var casScript = redis.NewScript(`
local cur = redis.call('GET', KEYS[1])
if ARGV[1] == '0' then
  if cur then return 0 end
  redis.call('SET', KEYS[1], ARGV[3])
  return 1
end
if cur == ARGV[2] then
  redis.call('SET', KEYS[1], ARGV[3])
  return 1
end
return 0
`)

var extendScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return 1
end
return 0
`)

var releaseScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
  redis.call('DEL', KEYS[1])
end
return 1
`)

var claimDueScript = redis.NewScript(`
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[3])
for i = 1, #due do
  redis.call('ZADD', KEYS[1], ARGV[2], due[i])
end
return due
`)

var popDelayedScript = redis.NewScript(`
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
for i = 1, #due do
  redis.call('ZREM', KEYS[1], due[i])
end
return due
`)
