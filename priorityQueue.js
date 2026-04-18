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

  async addJob(job, delay = 0) {
    const pipeline = this.redis.pipeline();

    const runAt = Date.now() + delay;
    const priorityWeight = (job.priority || 0) * PRIORITY_FACTOR;
    const score = runAt - priorityWeight;

    pipeline.zadd(this.keys.queue, score, job.id);

    pipeline.hset(this.keys.job(job.id), {
      id: job.id,
      description: job.description,
      status: "pending",
      tries: job.tries,
      maxRetries: job.maxRetries,
      priority: job.priority,
    });

    pipeline.expire(this.keys.job(job.id), JOB_TTL_SEC);

    await pipeline.exec();
  }

  normalizeJob(job) {
    return {
      ...job,
      tries: Number(job.tries),
      maxRetries: Number(job.maxRetries),
      priority: Number(job.priority),
      startedAt: job.startedAt ? Number(job.startedAt) : null,
      leaseUntil: job.leaseUntil ? Number(job.leaseUntil) : null,
    };
  }

  async nextJob() {
    const script = `
    local now = tonumber(redis.call("TIME")[1]) * 1000

    local res = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", now, "LIMIT", 0, 10)
    if #res == 0 then return nil end

    for i = 1, #res do
      local jobId = res[i]
      local jobKey = ARGV[1] .. jobId

      -- remove missing jobs
      if redis.call("EXISTS", jobKey) == 0 then
        redis.call("ZREM", KEYS[1], jobId)

      else
        local status = redis.call("HGET", jobKey, "status")

        -- remove finished jobs
        if status == "completed" or status == "failed" then
          redis.call("ZREM", KEYS[1], jobId)

        else
          local nextAttempt = redis.call("HGET", jobKey, "nextAttemptAt")
          if nextAttempt and tonumber(nextAttempt) > now then

          else
            local lease = redis.call("HGET", jobKey, "leaseUntil")

            -- claim job if lease expired
            if not lease or tonumber(lease) <= now then
              local leaseUntil = now + ${LEASE_MS}

              redis.call("ZADD", KEYS[1], leaseUntil, jobId)

              redis.call("HSET", jobKey,
                "status", "processing",
                "startedAt", tostring(now),
                "leaseUntil", tostring(leaseUntil)
              )

              return redis.call("HGETALL", jobKey)
            end
          end
        end
      end
    end

    return nil
  `;

    const result = await this.redis.eval(
      script,
      1,
      this.keys.queue,
      "jobs:"
    );

    if (!result) return null;

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

  async canProcessJob(max, duration) {
    const script = `
      local key = KEYS[1]
      local max = tonumber(ARGV[1])
      local duration = tonumber(ARGV[2])
      local now = tonumber(ARGV[3])

      redis.call("ZREMRANGEBYSCORE", key, "-inf", now - duration)

      local count = redis.call("ZCARD", key)

      if count < max then
        redis.call("ZADD", key, now, now .. "-" .. math.random())
        redis.call("PEXPIRE", key, duration)
        return 1
      else
        return 0
      end
    `;

    const result = await this.redis.eval(
      script,
      1,
      this.keys.rate,
      max,
      duration,
      Date.now()
    );

    return result === 1;
  }

  async markDone(jobId, status = "completed", extra = {}) {
    const now = Date.now();
    const pipeline = this.redis.pipeline();

    pipeline.zrem(this.keys.queue, jobId);
    pipeline.zadd(this.keys[status], now, jobId);

    pipeline.hset(this.keys.job(jobId), {
      status,
      finishedAt: now,
      ...extra,
    });

    if (status === "completed") {
      pipeline.hdel(this.keys.job(jobId), "lastError");
    }

    pipeline.hdel(this.keys.job(jobId), "leaseUntil");

    await pipeline.exec();
  }

  async getJobsByStatus(status) {
    const jobIds = await this.redis.zrange(this.keys[status], 0, -1);

    const pipeline = this.redis.pipeline();
    jobIds.forEach((id) => {
      pipeline.hgetall(this.keys.job(id));
    });

    const results = await pipeline.exec();

    return results.map(([err, job]) => {
      if (err) console.error(err);
      return this.normalizeJob(job);
    });
  }

  async getJob(jobId) {
    const job = await this.redis.hgetall(this.keys.job(jobId));
    return this.normalizeJob(job);
  }

  async requeueJob(job, delay = 0) {
    const pipeline = this.redis.pipeline();

    const runAt = Date.now() + delay;
    const score = runAt - (job.priority || 0) * 1000;

    pipeline.zadd(this.keys.queue, score, job.id);

    pipeline.hset(this.keys.job(job.id), {
      status: "pending",
      nextAttemptAt: runAt
    });

    pipeline.hdel(this.keys.job(job.id), "leaseUntil");

    await pipeline.exec();
  }

  async recoverStuckJobs() {
    const now = Date.now();

    const jobIds = await this.redis.zrangebyscore(
      this.keys.queue,
      "-inf",
      now
    );

    // batch fetch
    const pipeline = this.redis.pipeline();
    jobIds.forEach((id) => {
      pipeline.hgetall(this.keys.job(id));
    });

    const results = await pipeline.exec();

    for (let i = 0; i < jobIds.length; i++) {
      const jobId = jobIds[i];
      const [err, job] = results[i];
      if (err) {
        console.error("Recover fetch error", err);
        continue;
      }

      if (!job || Object.keys(job).length === 0) {
        await this.redis.zrem(this.keys.queue, jobId);
        continue;
      }

      if (job.status !== "processing") continue;

      const leaseUntil = Number(job.leaseUntil || 0);
      if (leaseUntil > now) continue;

      console.log(`⚠️ Recovering stuck job ${jobId}`);

      const tries = Number(job.tries) + 1;
      const maxRetries = Number(job.maxRetries);

      await this.addError(jobId, "Worker crashed / lease expired");
      await this.redis.hdel(this.keys.job(jobId), "leaseUntil");

      if (tries < maxRetries) {
        const backoff = 1000 * Math.pow(2, tries - 1);

        await this.addJob(
          {
            id: jobId,
            description: job.description,
            tries,
            maxRetries,
            priority: job.priority,
          },
          backoff
        );
      } else {
        await this.markDone(jobId, "failed", {
          lastError: "Worker crashed / lease expired",
        });
      }
    }
  }

  async extendLease(jobId, leaseUntil) {
    const script = `
      local jobKey = KEYS[1]
      local queueKey = KEYS[2]
      local jobId = ARGV[1]
      local leaseUntil = ARGV[2]

      local status = redis.call("HGET", jobKey, "status")

      if status == "completed" or status == "failed" then
        redis.call("ZREM", queueKey, jobId)
        return 0
      end

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

  async getFlakyJobs() {
    const completedIds = await this.redis.zrange(this.keys.completed, 0, -1);

    const pipeline = this.redis.pipeline();
    completedIds.forEach((id) => {
      pipeline.hgetall(this.keys.job(id));
      pipeline.llen(this.keys.errors(id));
    });

    const results = await pipeline.exec();

    const jobs = [];

    for (let i = 0; i < completedIds.length; i++) {
      const job = this.normalizeJob(results[i * 2][1]);
      const errorCount = Number(results[i * 2 + 1][1]);

      if (errorCount > 0) {
        jobs.push({ ...job, errorCount });
      }
    }

    return jobs;
  }

  async isIdle() {
    const totalJobs = await this.redis.zcard(this.keys.queue);
    return totalJobs === 0;
  }

  async addError(jobId, message) {
    const pipeline = this.redis.pipeline();

    pipeline.rpush(this.keys.errors(jobId), message);
    pipeline.hset(this.keys.job(jobId), {
      lastError: message,
    });

    await pipeline.exec();
  }

  async clearAll() {
    const keys = await this.redis.keys("jobs:*");

    const pipeline = this.redis.pipeline();
    keys.forEach((key) => pipeline.del(key));

    await pipeline.exec();
  }
}

export default RedisQueue;