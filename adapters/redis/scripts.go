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
