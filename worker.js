import RedisQueue from "./redisQueue.js";
import Job from "./job.js";

function delay(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function log(...args) {
  const now = new Date().toISOString().slice(11, 23);
  console.log(`[${now}]`, ...args);
}

const HEARTBEAT_INTERVAL = 3000;

class Worker {
  constructor(concurrency = 3, pollInterval = 500) {
    this.queue = new RedisQueue();
    this.running = false;
    this.pollInterval = pollInterval;
    this.concurrency = concurrency;
    this.heartbeats = new Map();
    this.activeJobs = new Set();

    this.rateLimit = {
      max: 5,
      duration: 1000,
    };
  }

  async addJob(job) {
    await this.queue.addJob(job);
    console.log(`Job ${job.id} added`);
  }

  async processJob(job, workerId) {
    if (!job || !job.id) return;

    log(`Worker ${workerId} processing ${job.id}`);
    this.activeJobs.add(job.id);
    this.startHeartbeat(job.id);

    try {
      const result = await job.perform();
      job.result = result;

      await this.queue.markDone(job.id, "completed");

      console.log(`✅ Worker ${workerId} completed ${job.id}`);
    } catch (error) {
      console.error(`❌ Worker ${workerId} failed ${job.id}`);

      await this.queue.addError(job.id, error.message);

      const tries = job.tries + 1;
      job.tries = tries;

      if (tries < job.maxRetries) {
        const backoff = 1000 * Math.pow(2, tries - 1);

        await this.queue.addJob(
          {
            id: job.id,
            description: job.description,
            tries: job.tries,
            maxRetries: job.maxRetries,
            priority: job.priority,
          },
          backoff
        );
      } else {
        await this.queue.markDone(job.id, "failed");
      }
    } finally {
      this.activeJobs.delete(job.id);
      this.stopHeartbeat(job.id);
    }
  }

  async start() {
    if (this.running) return;
    this.running = true;

    console.log(`Starting ${this.concurrency} workers...`);

    for (let i = 0; i < this.concurrency; i++) {
      this.workerLoop(i + 1);
    }
  }

  async workerLoop(workerId) {
    console.log(`Worker ${workerId} started`);

    while (this.running) {
      try {
        const rawJob = await this.queue.nextJob(
          this.rateLimit.max,
          this.rateLimit.duration
        );

        if (!rawJob) {
          await delay(this.pollInterval);
          continue;
        }

        if (rawJob.rateLimited) {
          const jitter = Math.random() * 200;
          await delay(this.pollInterval + jitter);
          continue;
        }

        const job = new Job(
          rawJob.id,
          rawJob.description,
          rawJob.maxRetries,
          rawJob.priority
        );

        job.tries = rawJob.tries;

        await this.processJob(job, workerId);
      } catch (err) {
        console.error(`Worker ${workerId} crashed job loop`, err);
        await delay(this.pollInterval);
      }
    }
  }

  async waitForIdle() {
    return new Promise((resolve) => {
      const interval = setInterval(async () => {
        const idle = await this.queue.isIdle();

        if (idle && this.activeJobs.size === 0) {
          console.log("All workers are idle. Shutting down...");

          clearInterval(interval);
          this.stop();
          resolve();
        }
      }, 2000);
    });
  }

  async startHeartbeat(jobId) {
    const interval = setInterval(async () => {
      if (!this.activeJobs.has(jobId)) return;

      try {
        await this.queue.extendLease(jobId);
      } catch (err) {
        console.error("Heartbeat failed", err);
      }
    }, HEARTBEAT_INTERVAL);

    this.heartbeats.set(jobId, interval);
  }

  stopHeartbeat(jobId) {
    const interval = this.heartbeats.get(jobId);

    if (interval) {
      clearInterval(interval);
      this.heartbeats.delete(jobId);
    }
  }

  stop({ clear = false } = {}) {
    this.running = false;

    console.log("Stopping workers...");

    if (clear && process.env.NODE_ENV !== "production") {
      this.queue.clearAll();
    }

    for (const [, interval] of this.heartbeats) {
      clearInterval(interval);
    }

    this.heartbeats.clear();
  }
}

export default Worker;