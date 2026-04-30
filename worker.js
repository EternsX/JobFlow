import Job from "./job.js";
import RedisQueue from './redisQueue.js'
import { workerLoopSwitch } from "./workerFunctions.js";
import { POLL_INTERVAL, HEARTBEAT_INTERVAL, states } from "./constants.js";

const delay = (ms) => {
  return new Promise((r) => setTimeout(r, ms));
}

class Worker {
  constructor(concurrency = 1) {
    this.concurrency = concurrency;
    this.queue = new RedisQueue;
    this.rate = {
      max: 5,
      duration: 1000,
    }
    this.active = false;
    this.activeJobs = new Set();
    this.activeWorkers = new Set();
    this.heartbeats = new Map();
  }

  async addJob(job) {
    try {
      await this.queue.addJob(job.toDTO());
      console.log(`Job ${job.id} added successfully!`);
    } catch (e) {
      console.log('Error adding Job', e);
    }
  }

  async processJob(job, workerId) {
    this.activeJobs.add(job.id);
    this.startHeartbeat(job.id, workerId);

    console.log(`Worker ${workerId} processing job ${job.id}`);

    try {
      const result = await job.execute();

      if (result.ok) {
        const res = await this.queue.markDone(job.id, workerId, states.COMPLETED);
        if (!res.ok) return res;

        console.log(`OOOOO -> Worker ${workerId} completed Job ${job.id}`);
        return { ok: true };
      }

      console.log(`XXXXX -> Worker ${workerId} failed Job ${job.id} -> ${result.error}`);

      job.incrementTries();

      if (!job.canRetry()) {
        const res = await this.queue.markDone(job.id, workerId, states.FAILED);
        if (!res.ok) return res;

        return { ok: false, error: result.error };
      }

      const backoffDelay  = job.getBackoffDelay();
      await this.queue.addJob(job.toDTO(), backoffDelay );

      return { ok: false, retry: true };

    } catch (err) {
      console.error(`SYSTEM ERROR Worker ${workerId} Job ${job.id}:`, err.message);

      try {
        await this.queue.markDone(job.id, workerId, states.FAILED);
      } catch (e) {
        console.error('Failed to mark job as failed:', e.message);
      }

      return { ok: false, systemError: true, error: err.message };

    } finally {
      this.activeJobs.delete(job.id);
      this.stopHeartbeat(job.id);
    }
  }

  async workerLoop(workerId) {
    console.log(`Starting worker ${workerId}`);
    this.activeWorkers.add(workerId)

    while (this.active) {
      const res = await this.queue.nextJob(workerId, this.rate);
      if (!res.ok) {
        workerLoopSwitch(res, POLL_INTERVAL, delay)
        continue;
      }

      const rawJob = res.rawJob;

      const job = Job.from(rawJob);
      const result = await this.processJob(job, workerId);
      if (!result.ok) {
        console.log(`Worker: ${workerId}, job: ${job.id} -> `, result.error)
      }
    }

    console.log(`Worker ${workerId} shutting down.`)
    this.activeWorkers.delete(workerId);
  }

  async awaitIdle() {
    return new Promise((resolve) => {
      const interval = setInterval(async () => {
        const isIdle = await this.queue.isIdle();

        if (isIdle && this.activeJobs.size === 0) {
          console.log('All workers stopped shutting down process...')

          clearInterval(interval);
          this.stop();
          resolve();
        }
      }, POLL_INTERVAL)
    })
  }

  async startHeartbeat(jobId, workerId) {
    const interval = setInterval(async () => {
      if (!this.activeJobs.has(jobId)) return;
      try {
        await this.queue.extendLease(jobId, workerId);
      } catch (err) {
        console.error("Heartbeat failed", err);
      }
    }, HEARTBEAT_INTERVAL);

    this.heartbeats.set(jobId, interval);
  }

  async stopHeartbeat(jobId) {
    const interval = this.heartbeats.get(jobId);

    if (interval) {
      clearInterval(interval);
      this.heartbeats.delete(jobId);
    }
  }

  async shutDown() {
    return new Promise((resolve) => {
      const interval = setInterval(async () => {
        if (this.activeWorkers.size === 0) {
          console.log('SYSTEM SHUT DOWN')
          clearInterval(interval);
          resolve();
        }
      }, POLL_INTERVAL)
    })
  }

  start() {
    this.active = true;
    for (let i = 0; i < this.concurrency; i++) {
      this.workerLoop(i + 1);
    }
  }

  stop() {
    this.active = false;
  }
}

export default Worker;