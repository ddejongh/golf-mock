-- HSET KEYS[1] ARGV[1] ARGV[2] unless the stored value's ts_ms is newer than ARGV[3].
-- Returns 1 if written, 0 if skipped.
local current = redis.call('HGET', KEYS[1], ARGV[1])
if current then
  local current_ts = tonumber(string.match(current, '"ts_ms":(%-?%d+)'))
  if current_ts and current_ts > tonumber(ARGV[3]) then
    return 0
  end
end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
return 1
