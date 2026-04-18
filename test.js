import Worker from "./worker.js";
import Job from "./job.js";

function randomDelay(max = 3000) {
  return new Promise((r) => setTimeout(r, Math.random() * max));
}

class TestJob extends Job {
  async perform() {
    // simulate work
    await randomDelay();

    // 30% failure rate
    if (Math.random() < 0.3) {
      throw new Error("Random failure");
    }

    return "done";
  }
}

async function main() {
  const worker = new Worker(5); // 5 workers

  // add jobs
  for (let i = 0; i < 50; i++) {
    const job = new TestJob(
      `job-${i}`,
      "test job",
      3, // maxRetries
      Math.floor(Math.random() * 5) // priority
    );

    job.tries = 0;

    await worker.addJob(job);
  }

  await worker.start();

  // simulate random crashes
  setInterval(() => {
    if (Math.random() < 0.1) {
      console.log("💥 Simulating crash...");
      process.exit(1);
    }
  }, 5000);

  await worker.waitForIdle();
}

main();