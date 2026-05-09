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
        await this.queue.markDone(job.id, workerId, states.COMPLETED);

        console.log(`OOOOO -> Worker ${workerId} completed Job ${job.id}`);
        return { ok: true };
      }

      console.log(`XXXXX -> Worker ${workerId} failed Job ${job.id} -> ${result.error}`);

      await this.queue.addError(job.id, result.error);


      job.incrementTries();

      if (!job.canRetry()) {
        await this.queue.moveToDLQ(job.id, workerId, result.error);

        return { ok: false, error: result.error };
      }

      const backoffDelay = job.getBackoffDelay();
      await this.queue.addJob(job.toDTO(), backoffDelay);

      return { ok: false, error: result.error };

    } catch (err) {
      console.error(`SYSTEM ERROR Worker ${workerId} Job ${job.id}:`, err.message);

      try {
        const res = await this.queue.moveToDLQ(job.id, workerId, err.message);
        if (!res.ok) {
          console.error(`Failed to mark job as failed:`, res.error);
        }
      } catch (e) {
        console.error('Failed to mark job as failed:', e.message);
      }

      return { ok: false, error: err.message };

    } finally {
      this.activeJobs.delete(job.id);
      this.stopHeartbeat(job.id);
    }
  }

  async workerLoop(workerId) {
    console.log(`Starting worker ${workerId}`);
    this.activeWorkers.add(workerId)

    while (this.active) {
      try {
        const res = await this.queue.nextJob(workerId, this.rate);
        if (!res.ok) {
          await workerLoopSwitch(res, POLL_INTERVAL, delay)
          continue;
        }

        const rawJob = res.rawJob;

        const job = Job.from(rawJob);
        const result = await this.processJob(job, workerId);
      } catch (err) {
        console.error(`Worker ${workerId} encountered an error:`, err);
        await delay(POLL_INTERVAL);
      }
    }

    console.log(`Worker ${workerId} shutting down.`)
    this.activeWorkers.delete(workerId);
  }

  async awaitIdle() {
    return new Promise((resolve, reject) => {
      const interval = setInterval(async () => {
        try {
          const isIdle = await this.queue.isIdle();

          if (isIdle && this.activeJobs.size === 0) {
            console.log('All workers stopped shutting down process...');

            clearInterval(interval);
            this.stop();

            resolve();
          }

        } catch (err) {
          clearInterval(interval);
          reject(err);
        }

      }, POLL_INTERVAL);
    });
  }

  async startHeartbeat(jobId, workerId) {
    const interval = setInterval(async () => {
      if (!this.activeJobs.has(jobId)) return;
      try {
        const res = await this.queue.extendLease(jobId, workerId);
        if (!res.ok) {
          console.error("Heartbeat failed", res.error);
        }
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