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
  }

  async addJob(job) {
    await this.queue.addJob(job);
    console.log(`Job ${job.id} added`);
  }

  async processJob(job, workerId) {
    console.log(`👷 Worker ${workerId} processing ${job.id}`);

    try {
      const result = await job.perform();
      job.result = result;

      await this.queue.markDone(job.id, "completed");

      console.log(`✅ Worker ${workerId} completed ${job.id}`);
    } catch (error) {
      console.error(`❌ Worker ${workerId} failed ${job.id}`);

      job.tries += 1;

      await this.queue.addError(job.id, error.message);

      if (job.tries < job.maxRetries) {
        const delay = 1000 * Math.pow(2, job.tries);

        await this.queue.markDone(job.id, "retry", {
          lastError: error.message,
        });

        await this.queue.addJob(job, delay);
      } else {
        await this.queue.markDone(job.id, "failed", {
          lastError: error.message,
        });
      }
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

      await this.processJob(job, workerId);
    }
  }

  async waitForIdle() {
    return new Promise((resolve) => {
      const interval = setInterval(async () => {
        if (await this.queue.isIdle()) {
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

  async recoverStuckJobs() {
    this.recoveryRunning = true;

    while (this.recoveryRunning) {
      await this.queue.recoverStuckJobs(10000);
      await delay(10000);
    }
  }


  stop() {
    this.running = false;
    console.log("Stopping workers...");
    this.queue.clearAll();
  }
}

export default Worker;