import Redis from "ioredis";

class RedisQueue {

  constructor() {
    this.redis = new Redis({
      host: "127.0.0.1",
      port: 6379,
    });

    this.queueKey = "jobs:queue";
    this.activeKey = "jobs:active";
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

      local res = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", now, "LIMIT", 0, 1)

      if #res == 0 then
        return nil
      end

      local jobId = res[1]

      redis.call("ZREM", KEYS[1], jobId)

      local jobKey = ARGV[1] .. jobId

      redis.call("SADD", KEYS[2], jobId)

      redis.call("HSET", jobKey,
        "status", "processing",
        "startedAt", tostring(now)
      )

      return redis.call("HGETALL", jobKey)
    `;

    const result = await this.redis.eval(
      script,
      2,
      this.queueKey,
      this.activeKey,
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

    pipeline.srem(this.activeKey, jobId);

    if (status === "completed" || status === "failed") {
      pipeline.zadd(`jobs:${status}`, now, jobId);
    }

    pipeline.hset(`job:${jobId}`, {
      status,
      finishedAt: now,
      ...extra,
    });

    if (status === "completed") {
      pipeline.hdel(`job:${jobId}`, "lastError");
    }


    await pipeline.exec();
  }

  async addError(jobId, message) {
    await this.redis.rpush(`job:${jobId}:errors`, message);
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

  async isIdle() {
    const queueSize = await this.redis.zcard(this.queueKey);
    const activeSize = await this.redis.scard(this.activeKey);

    return queueSize === 0 && activeSize === 0;
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
    pipeline.del(this.activeKey);

    // delete history
    pipeline.del("jobs:completed");
    pipeline.del("jobs:failed");

    await pipeline.exec();
  }

  async recoverStuckJobs(timeout = 10000) {
    const now = Date.now();

    const activeJobs = await this.redis.smembers(this.activeKey);

    for (const jobId of activeJobs) {
      const job = await this.redis.hgetall(`job:${jobId}`);

      const startedAt = Number(job.startedAt || 0);

      if (now - startedAt > timeout) {
        console.log(`Recovering stuck job ${jobId}`);

        // remove from active
        await this.redis.srem(this.activeKey, jobId);

        // requeue it
        await this.redis.zadd(
          this.queueKey,
          now,
          jobId
        );

        await this.redis.hset(`job:${jobId}`, "status", "pending");
      }
    }
  }

  async getFlakyJobs() {
    const completedIds = await this.redis.zrange("jobs:completed", 0, -1);

    const pipeline = this.redis.pipeline();

    for (const id of completedIds) {
      pipeline.hgetall(`job:${id}`);
      pipeline.llen(`job:${id}:errors`);
    }

    const results = await pipeline.exec();

    const flakyJobs = [];

    for (let i = 0; i < results.length; i += 2) {
      const job = results[i][1];
      const errorCount = results[i + 1][1];

      if (Number(errorCount) > 0) {
        flakyJobs.push({
          ...job,
          errorCount
        });
      }
    }

    return flakyJobs;
  }

}

export default RedisQueue;