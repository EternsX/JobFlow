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
      rate: "rate:jobs",
    };
  }

  computeScore(runAt, priority = 0) {
    return runAt - priority * PRIORITY_FACTOR;
  }

  async addJob(job, delay = 0) {
    const pipeline = this.redis.pipeline();

    const runAt = Date.now() + delay;
    const score = this.computeScore(runAt, job.priority);

    pipeline.zadd(this.keys.queue, score, job.id);

    pipeline.hset(this.keys.job(job.id), {
      id: job.id,
      description: job.description,
      tries: job.tries ?? 0,
      maxRetries: job.maxRetries ?? 3,
      priority: job.priority ?? 0,
      status: "pending",
    });

    pipeline.expire(this.keys.job(job.id), JOB_TTL_SEC);

    await pipeline.exec();
  }

  normalizeJob(job) {
    return {
      ...job,
      id: job.id,
      tries: Number(job.tries),
      maxRetries: Number(job.maxRetries),
      priority: Number(job.priority),
      startedAt: job.startedAt ? Number(job.startedAt) : null,
      leaseUntil: job.leaseUntil ? Number(job.leaseUntil) : null,
    };
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

    redis.call("ZREMRANGEBYSCORE", rateKey, "-inf", now - duration)
    local count = redis.call("ZCARD", rateKey)

    if count >= max then
      return { "RATE_LIMITED" }
    end

    local jobs = redis.call("ZRANGEBYSCORE", queueKey, "-inf", now, "LIMIT", 0, 10)

    for i = 1, #jobs do
      local jobId = jobs[i]
      local jobKey = jobPrefix .. jobId

      if redis.call("EXISTS", jobKey) == 0 then
        redis.call("ZREM", queueKey, jobId)

      else
        local status = redis.call("HGET", jobKey, "status")

        if status == "completed" or status == "failed" then
          redis.call("ZREM", queueKey, jobId)

        else
          local leaseUntil = redis.call("HGET", jobKey, "leaseUntil")

          if not leaseUntil or tonumber(leaseUntil) <= now then
            local newLease = now + leaseMs

            redis.call("HSET", jobKey,
              "status", "processing",
              "startedAt", tostring(now),
              "leaseUntil", tostring(newLease)
            )

            redis.call("ZADD", queueKey, newLease, jobId)
            redis.call("ZADD", rateKey, now, now .. "-" .. math.random())

            return redis.call("HGETALL", jobKey)
          end
        end
      end
    end

    return nil
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

  async markDone(jobId, status = "completed", extra = {}) {
    const job = await this.getJob(jobId);
    if (job?.status !== "processing") return;

    const now = Date.now();
    const pipeline = this.redis.pipeline();

    pipeline.zrem(this.keys.queue, jobId);
    pipeline.zadd(this.keys[status], now, jobId);

    pipeline.hset(this.keys.job(jobId), {
      status,
      finishedAt: now,
      ...extra,
    });

    pipeline.hdel(this.keys.job(jobId), "leaseUntil");

    if (status === "completed") {
      pipeline.hdel(this.keys.job(jobId), "lastError");
    }

    await pipeline.exec();
  }

  async getJob(jobId) {
    const job = await this.redis.hgetall(this.keys.job(jobId));
    if (!job || Object.keys(job).length === 0) return null;
    return this.normalizeJob(job);
  }

  async extendLease(jobId, leaseUntil) {
    const script = `
      local jobKey = KEYS[1]
      local queueKey = KEYS[2]
      local jobId = ARGV[1]
      local leaseUntil = tonumber(ARGV[2])

      local t = redis.call("TIME")
      local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

      local status = redis.call("HGET", jobKey, "status")
      if status ~= "processing" then return 0 end

      local currentLease = redis.call("HGET", jobKey, "leaseUntil")
      if not currentLease or tonumber(currentLease) < now then return 0 end

      redis.call("ZADD", queueKey, leaseUntil, jobId)
      redis.call("HSET", jobKey, "leaseUntil", leaseUntil)

      return 1
    `;

    await this.redis.eval(
      script,
      2,
      this.keys.job(jobId),
      this.keys.queue,
      jobId,
      leaseUntil
    );
  }

  async addError(jobId, message) {
    const pipeline = this.redis.pipeline();
    pipeline.rpush(this.keys.errors(jobId), message);
    pipeline.hset(this.keys.job(jobId), { lastError: message });
    await pipeline.exec();
  }

  async isIdle() {
    return (await this.redis.zcard(this.keys.queue)) === 0;
  }

  async clearAll() {
    const stream = this.redis.scanStream({ match: "jobs:*" });

    for await (const keys of stream) {
      if (keys.length) {
        await this.redis.del(...keys);
      }
    }
  }
}

export default RedisQueue;