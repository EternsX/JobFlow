import Redis from "ioredis";
import { nextJobScript, markDoneScript, extendLeaseScript } from "./luaScripts.js";
import { PRIORITY_FACTOR, EXPIRE_MS, LEASE_MS, states } from "./constants.js";

class RedisQueue {
  constructor() {
    this.redis = new Redis({
      host: "127.0.0.1",
      port: 6379,
    });

    this.keys = {
      queue: 'jobs:queue',
      job: (id) => `job:${id}`,
      jobPrefix: 'job:',
      completed: 'jobs:completed',
      errors: (id) => `jobs:${id}:errors`,
      rate: 'jobs:rate',
      dlq: 'jobs:dlq',
    }
  }

  async addJob(job, delay = 0) {
    const pipeline = this.redis.pipeline();

    const runAt = Date.now() + delay;
    const score = runAt - job.priority * PRIORITY_FACTOR;

    pipeline.zadd(this.keys.queue, score, job.id);
    pipeline.hset(this.keys.job(job.id), {
      id: job.id,
      description: job.description,
      priority: job.priority,
      tries: job.tries,
      maxTries: job.maxTries,
      status: 'pending'
    });
    pipeline.hdel(this.keys.job(job.id), 'leaseUntil');

    pipeline.expire(this.keys.job(job.id), EXPIRE_MS);

    await pipeline.exec();
  }

  normalizeJob(job) {
    return {
      ...job,
      id: job.id,
      tries: Number(job.tries),
      maxTries: Number(job.maxTries),
      priority: Number(job.priority),
      startedAt: job.startedAt ? Number(job.startedAt) : null,
      leaseUntil: job.leaseUntil ? Number(job.leaseUntil) : null,
    };
  }

  async nextJob(workerId, rate) {
    const res = await this.redis.eval(
      nextJobScript,
      2,
      this.keys.queue,
      this.keys.rate,
      this.keys.jobPrefix,
      LEASE_MS,
      workerId,
      rate.max,
      rate.duration,
      states.PROCESSING,
      states.COMPLETED,
      states.FAILED
    )
    switch (res[0]) {
      case 'ERROR':
        return { ok: false, type: res[0], error: res };
      case 'RATE_LIMIT':
        return { ok: false, type: res[0] }
      case 'NO_JOBS':
        return { ok: false, type: res[0] }
    }

    const job = {};
    for (let i = 0; i < res.length; i += 2) {
      job[res[i]] = res[i + 1];
    }

    const normalizedJob = this.normalizeJob(job);
    return { ok: true, rawJob: normalizedJob };
  }

  async markDone(jobId, workerId, status) {
    const res = await this.redis.eval(
      markDoneScript,
      2,
      this.keys.queue,
      this.keys[status],
      jobId,
      workerId,
      status,
      this.keys.job(jobId),
      states.PROCESSING,
      states.COMPLETED,
    )

    if (res[0] == 'ERROR') {
      return { ok: false, error: res }
    }
    return { ok: true }
  }

  async moveToDLQ(jobId, workerId, error) {
    const pipeline = this.redis.pipeline();
    pipeline.hset(this.keys.job(jobId), {
      status: 'dlq',
      failedAt: Date.now(),
      failureReason: error,
    })

    pipeline.zrem(this.keys.queue, jobId)
    pipeline.rpush(this.keys.dlq, jobId)

    await pipeline.exec();
  }

  async extendLease(jobId, workerId) {
    const res = await this.redis.eval(
      extendLeaseScript,
      2,
      this.keys.queue,
      this.keys.job(jobId),
      jobId,
      workerId,
      LEASE_MS,
      states.PROCESSING
    )

    if (res[0] == 'ERROR') {
      return { ok: false, error: res }
    }

    return { ok: true }
  }

  async addError(jobId, error) {
    await this.redis.rpush(this.keys.errors(jobId), error);
    await this.redis.hset(this.keys.job(jobId), 'lastError', error);
  }

  async isIdle() {
    return await this.redis.zcard(this.keys.queue) === 0;
  }
}

export default RedisQueue;