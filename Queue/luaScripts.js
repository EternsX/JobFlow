export const nextJobScript = `
    local queueKey = KEYS[1]
    local rateKey = KEYS[2]
    local jobPrefix = ARGV[1]
    local leaseMs = tonumber(ARGV[2])
    local workerId = ARGV[3]
    local max = tonumber(ARGV[4])
    local duration = tonumber(ARGV[5])
    local processing = ARGV[6]
    local completed = ARGV[7]
    local failed = ARGV[8]

    local t = redis.call('TIME')
    local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

    redis.call('ZREMRANGEBYSCORE', rateKey, '-inf', now - duration)

    local count = redis.call('ZCARD', rateKey)
    if count >= max then
        return {'RATE_LIMIT'}
    end

    local jobId = redis.call('ZRANGEBYSCORE', queueKey, '-inf', now, 'LIMIT', 0, 1)[1]
    
    if not jobId then 
        return {'NO_JOBS'}
    end

    local jobKey = jobPrefix .. jobId
    if redis.call('EXISTS', jobKey) == 0 then
        redis.call('ZREM', queueKey, jobId)
        return {'ERROR', 'JOB_DOESNT_EXIST'}
    end
    
    local status = redis.call('HGET', jobKey, 'status')
    if status == completed or status == failed then
        redis.call('ZREM', queueKey, jobId)
        return {'ERROR', 'STATUS_ERROR'}
    end

    local leaseUntil = redis.call('HGET', jobKey, 'leaseUntil')
    if leaseUntil and tonumber(leaseUntil) > now then
        return {'ERROR', 'JOB_TAKEN', workerId, jobId, leaseUntil}
    end

    local newLease = now + leaseMs
    redis.call('HSET', jobKey, 
        'status', processing,
        'startedAt', tostring(now),
        'leaseUntil', tostring(newLease),
        'workerId', workerId
    )

    redis.call('ZADD', queueKey, newLease, jobId)
    redis.call('ZADD', rateKey, now, now .. '-' .. math.random() .. '-' .. jobId)

    return redis.call('HGETALL', jobKey)
`

export const markDoneScript = `
    local queueKey = KEYS[1]
    local statusKey = KEYS[2]
    local jobId = ARGV[1]
    local workerId = ARGV[2]
    local status = ARGV[3]
    local jobKey = ARGV[4]
    local processing = ARGV[5]
    local completed = ARGV[6]

    local t = redis.call('TIME')
    local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

    local worker = redis.call('HGET', jobKey, 'workerId') 
    if worker ~= workerId then
        return {'ERROR', 'WORKER_ERROR', worker, workerId, jobId}
    end

    if redis.call('HGET', jobKey, 'status') ~= processing then
        return {'ERROR', 'STATUS_ERROR'}
    end

    redis.call('HSET', jobKey, 
        'status', status,
        'finishedAt', tostring(now)
    )

    redis.call('ZREM', queueKey, jobId)
    redis.call('ZADD', statusKey, now, jobId)

    redis.call('HDEL', jobKey, 'leaseUntil')
    redis.call('HDEL', jobKey, 'workerId')

    if status == completed then
        redis.call('HDEL', jobKey, 'lastError')
    end

    return {'MARK_DONE_SUCCESS'}
`

export const extendLeaseScript = `
    local queueKey = KEYS[1]
    local jobKey = KEYS[2]
    local jobId = ARGV[1]
    local workerId = ARGV[2]
    local leaseMs = tonumber(ARGV[3])
    local processing = ARGV[4]

    local t = redis.call('TIME')
    local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

    local worker = redis.call('HGET', jobKey, 'workerId') 
    if worker ~= workerId then
        return {'ERROR', 'WORKER_ERROR', worker, workerId, jobId}
    end
    
    if redis.call('HGET', jobKey, 'status') ~= processing then
        return {'ERROR', 'STATUS_ERROR'}
    end

    local currentLease = redis.call("HGET", jobKey, "leaseUntil")
    if not currentLease or tonumber(currentLease) <= now then
        return {'ERROR', 'LEASE_ERROR'}
    end

    local newLease = now + leaseMs
    redis.call('HSET', jobKey,
        'leaseUntil', newLease
    )
    redis.call('ZADD', queueKey, newLease, jobId)

    return {'LEASE_EXTENDED'}
`