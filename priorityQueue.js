import Redis from "ioredis";

class RedisQueue {

  constructor() {
    this.redis = new Redis({
      host: "127.0.0.1",
      port: 6379,
    });

    this.queueKey = "jobs:queue";
  }

  async addJob(job, delay = 0) {
    const pipeline = this.redis.pipeline();

    const runAt = Date.now() + delay;

    pipeline.zadd(this.queueKey, runAt, job.id);

    pipeline.hset(`job:${job.id}`, {
      id: job.id,
      description: job.description,
      status: "pending",
      tries: job.tries,
      maxRetries: job.maxRetries,
      priority: job.priority,
      result: "",
    });

    pipeline.expire(`job:${job.id}`, 3600);

    await pipeline.exec();
  }

  async nextJob() {
    const script = `
      local now = tonumber(redis.call("TIME")[1]) * 1000
      local leaseUntil = now + 10000

      local res = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", now, "LIMIT", 0, 5)

      if #res == 0 then return nil end

      for i = 1, #res do
        local jobId = res[i]
        local jobKey = ARGV[1] .. jobId

        if redis.call("EXISTS", jobKey) == 0 then
          redis.call("ZREM", KEYS[1], jobId)
        else
          local status = redis.call("HGET", jobKey, "status")

          if status == "completed" or status == "failed" then
            redis.call("ZREM", KEYS[1], jobId)
          else
            local lease = redis.call("HGET", jobKey, "leaseUntil")

            if not lease or tonumber(lease) <= now then
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

      return nil
    `;

    const result = await this.redis.eval(
      script,
      1,
      this.queueKey,
      "job:"
    );

    if (!result) return null;

    const job = {};
    for (let i = 0; i < result.length; i += 2) {
      job[result[i]] = result[i + 1];
    }

    job.errors = await this.redis.lrange(`job:${job.id}:errors`, 0, -1);

    return job;
  }

  async markDone(jobId, status = "completed", extra = {}) {
    const now = Date.now();

    const pipeline = this.redis.pipeline();

    pipeline.zrem(this.queueKey, jobId);

    pipeline.zadd(`jobs:${status}`, now, jobId);

    pipeline.hset(`job:${jobId}`, {
      status,
      finishedAt: now,
      ...extra,
    });

    if (status === "completed") {
      pipeline.hdel(`job:${jobId}`, "lastError");
    }
    pipeline.hdel(`job:${jobId}`, "leaseUntil");



    await pipeline.exec();
  }

  async getJobsByStatus(status) {
    const jobIds = await this.redis.zrange(`jobs:${status}`, 0, -1);

    const pipeline = this.redis.pipeline();

    for (const id of jobIds) {
      pipeline.hgetall(`job:${id}`);
    }

    const results = await pipeline.exec();

    return results.map(([err, job]) => job);
  }


  async getJob(jobId) {
    return this.redis.hgetall(`job:${jobId}`);
  }

  async extendLease(jobId, leaseUntil) {
    const script = `
    local jobKey = KEYS[1]
    local queueKey = KEYS[2]
    local jobId = ARGV[1]
    local leaseUntil = ARGV[2]

    local status = redis.call("HGET", jobKey, "status")

    if status == "completed" or status == "failed" then
      return 0
    end

    redis.call("ZADD", queueKey, leaseUntil, jobId)
    redis.call("HSET", jobKey, "leaseUntil", leaseUntil)

    return 1
  `;

    await this.redis.eval(
      script,
      2,
      `job:${jobId}`,
      this.queueKey,
      jobId,
      leaseUntil
    );
  }


  async getFlakyJobs() {
    const completedIds = await this.redis.zrange("jobs:completed", 0, -1);

    const pipeline = this.redis.pipeline();

    for (const id of completedIds) {
      pipeline.hgetall(`job:${id}`);
      pipeline.llen(`job:${id}:errors`);
    }

    const results = await pipeline.exec();

    const jobs = [];

    for (let i = 0; i < completedIds.length; i++) {
      const job = results[i * 2][1];
      const errorCount = results[i * 2 + 1][1];

      if (Number(errorCount) > 0) {
        jobs.push({ ...job, errorCount });
      }
    }

    return jobs;
  }

  async isIdle() {
    const totalJobs = await this.redis.zcard(this.queueKey);

    return totalJobs === 0;
  }

  async addError(jobId, message) {
    const pipeline = this.redis.pipeline();

    pipeline.rpush(`job:${jobId}:errors`, message);
    pipeline.hset(`job:${jobId}`, {
      lastError: message,
    });

    await pipeline.exec();
  }


  async clearAll() {
    const keys = await this.redis.keys("job:*");

    const pipeline = this.redis.pipeline();

    // delete all job hashes + error lists
    for (const key of keys) {
      pipeline.del(key);
    }

    // delete queue structures
    pipeline.del(this.queueKey);

    // delete history
    pipeline.del("jobs:completed");
    pipeline.del("jobs:failed");

    await pipeline.exec();
  }

}

export default RedisQueue;