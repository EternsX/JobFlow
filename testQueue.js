import Worker from "./worker.js";

class TestJob {
  constructor(id, description, maxRetries = 3, priority = 0) {
    this.id = id;
    this.description = description;
    this.maxRetries = maxRetries;
    this.priority = priority;
    this.tries = 0;
  }

  async perform() {
    console.log(`Running job ${this.id} (try ${this.tries + 1})`);

    // Simulate work
    await new Promise((r) => setTimeout(r, 500));

    // Random failure (to test retries)
    if (Math.random() < 0.3) {
      throw new Error("Random failure");
    }

    return `Done ${this.id}`;
  }
}

async function main() {
  const worker = new Worker(3, 200);

  // Clear old data (dev only)
  await worker.queue.clearAll();

  // Add jobs
  for (let i = 1; i <= 10; i++) {
    const job = new TestJob(
      `job-${i}`,
      `Test job ${i}`,
      3,
      Math.floor(Math.random() * 3) // random priority
    );

    await worker.addJob(job);
  }

  // Start workers
  await worker.start();

  // Wait until everything is done
  await worker.waitForIdle();

  console.log("🎉 TEST COMPLETE");
}

main();