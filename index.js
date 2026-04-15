import Worker from './worker.js';

const worker = new Worker(3);

worker.start();

worker.recoverStuckJobs();
await worker.waitForIdle();