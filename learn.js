import Redis from "ioredis";

const PRIORITY_FACTOR = 1000;
const LEASE_MS = 10000;
const JOB_TTL_SEC = 3600;

class RedisQueue {
    constructor() {
        this.redis = new Redis({
            host: "127.0.0.1",
            port: 6379,
        });

        this.keys = {
            queue: "jobs:queue",
            job: (id) => `jobs:${id}`,
            errors: (id) => `jobs:${id}:errors`,
            completed: "jobs:completed",
            failed: "jobs:failed",
            rate: "rate:jobs"
        };
    }


    addJob(job, delay = 0) {
        const pipeline = this.redis.pipeline();

        const runAt = Date.now() + delay;
        const score = (job.priority || 0) * PRIORITY_FACTOR;

        pipeline.zadd(this.keys.queue, score, job.id);
        pipeline.hset(this.keys.job(job.id), {
            ...job,
            status: 'pending'
        })
        pipeline.expire(this.keys.job(job.id), JOB_TTL_SEC);

        pipeline.exec();
    }

    async nextJob(max, duration) {
        const script = `
            local queueKey = KEYS[1]
            local rateKey = KEYS[2]
            local jobPrefix = ARGV[1]
            local max = tonumber(ARGV[2])
            local duration = tonumber(ARGV[3])
            local leaseMs = tonumber(ARGV[4])

            local t = redis.call("TIME")
            local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

            redis.call('ZREMRANGEBYSCORE', rateKey, '-inf', now - duration)
            
            local count = redis.call('ZCARD', rateKey)
            if count >= max then
            return nil
            end

            local jobs = redis.call('ZRANGEBYSCORE', queueKey, '-inf', now, 'LIMIT', 0, 1)
            local jobId = jobs[1]

            if not jobId then
            return nil
            end

            local jobKey = jobPrefix .. jobId
            if redis.call('EXISTS', jobKey) == 0 then
            redis.call('ZREM', queueKey, jobId)
            return nil
            end

            local status = redis.call('HGET', jobKey, 'status')
            if status == 'completed' or status == 'failed' then
            redis.call('ZREM', queueKey, jobId)
            return nil
            end

            local leaseUntil = redis.call('HGET', jobKey, 'leaseUntil')
            if leaseUntil and tonumber(leaseUntil) > now then
            return nil
            end

            local newLease = now + leaseMs
            redis.call('HSET', jobKey, 
                "status", "processing",
                "leaseUntil", tostring(newLease),
                "startedAt", tostring(now)
            )

            redis.call("ZADD", queueKey, newLease, jobId)
            redis.call("ZADD", rateKey, now, tostring(now) .. '-' .. tostring(math.random()))

            return redis.call("HGETALL", jobKey)
        `;

        const result = await this.redis.eval(
            script,
            2,
            this.keys.queue,
            this.keys.rate,
            "jobs:",
            max,
            duration,
            LEASE_MS
        );

        if (!result) return null;

        if (result[0] === "RATE_LIMITED") {
            return { rateLimited: true };
        }

        const job = {};
        for (let i = 0; i < result.length; i += 2) {
            job[result[i]] = result[i + 1];
        }

        const normalized = this.normalizeJob(job);

        normalized.errors = await this.redis.lrange(
            this.keys.errors(normalized.id),
            0,
            -1
        );

        return normalized;
    }
}