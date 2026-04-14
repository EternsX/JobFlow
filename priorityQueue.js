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

  async addJob(job) {
    const pipeline = this.redis.pipeline();

    pipeline.zadd(this.queueKey, job.priority, job.id);

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
      local res = redis.call("ZPOPMIN", KEYS[1], 1)
      if #res == 0 then
        return nil
      end

      local jobId = res[1]
      local jobKey = ARGV[1] .. jobId

      redis.call("SADD", KEYS[2], jobId)

      redis.call("HSET", jobKey,
        "status", "processing",
        "startedAt", tostring(redis.call("TIME")[1])
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

  async markDone(jobId, status) {
    const now = Date.now();

    const pipeline = this.redis.pipeline();

    pipeline.srem(this.activeKey, jobId);
    pipeline.zadd(`jobs:${status}`, now, jobId);
    pipeline.hset(`job:${jobId}`, "status", status, "finishedAt", now);

    await pipeline.exec();
  }

  async addError(jobId, message) {
    await this.redis.rpush(`job:${jobId}:errors`, message);
  }
}

export default RedisQueue;