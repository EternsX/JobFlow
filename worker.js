import RedisQueue from "./priorityQueue.js";
import Job from "./job.js";

function delay(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

class Worker {
  constructor(concurrency = 3, pollInterval = 500) {
    this.queue = new RedisQueue();
    this.running = false;
    this.pollInterval = pollInterval;
    this.concurrency = concurrency;
    this.heartbeats = new Map();
    this.activeJobs = new Set();
  }

  async addJob(job) {
    await this.queue.addJob(job);
    console.log(`Job ${job.id} added`);
  }

  async processJob(job, workerId) {
    console.log(`👷 Worker ${workerId} processing ${job.id}`);
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

      job.tries += 1;

      if (job.tries < job.maxRetries) {
        const backoff = 1000 * Math.pow(2, job.tries - 1);

        await this.queue.addJob(job, backoff);
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
      const rawJob = await this.queue.nextJob();

      if (!rawJob) {
        await delay(this.pollInterval);
        continue;
      }

      const job = new Job(
        rawJob.id,
        rawJob.description,
        Number(rawJob.maxRetries),
        Number(rawJob.priority)
      );

      job.tries = Number(rawJob.tries);

      try {
        await this.processJob(job, workerId);
      } catch (err) {
        console.error(`Worker ${workerId} crashed job loop`, err);
      }
    }
  }

  async waitForIdle() {
    return new Promise((resolve) => {
      const interval = setInterval(async () => {
        const idle = await this.queue.isIdle();

        if (idle && this.activeJobs.size === 0) {
          console.log("All workers are idle. Shutting down...");

          console.log(await this.queue.getJobsByStatus("completed"));
          console.log(await this.queue.getJobsByStatus("failed"));

          clearInterval(interval);
          this.stop();
          resolve();
        }
      }, 2000);
    });
  }

  async startHeartbeat(jobId) {
    const interval = setInterval(async () => {
      try {
        const newLease = Date.now() + 10000;
        await this.queue.extendLease(jobId, newLease);
      } catch (err) {
        console.error("Heartbeat failed", err);
      }
    }, 3000);

    this.heartbeats.set(jobId, interval);
  }

  stopHeartbeat(jobId) {
    const interval = this.heartbeats.get(jobId);

    if (interval) {
      clearInterval(interval);
      this.heartbeats.delete(jobId);
    }
  }

  stop() {
    this.running = false;
    console.log("Stopping workers...");
    this.queue.clearAll();

    for (const [, interval] of this.heartbeats) {
      clearInterval(interval);
    }
    this.heartbeats.clear();
  }
}

export default Worker;