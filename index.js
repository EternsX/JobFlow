import Worker from './worker.js';
import dotenv from 'dotenv';

dotenv.config();

const worker = new Worker(3);

worker.start();

// await worker.awaitIdle();
// await worker.shutDown();