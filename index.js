import Worker from './worker.js';

const worker = new Worker(3);

worker.start();

await worker.waitForIdle();