import Worker from './worker.js';
import dotenv from 'dotenv';
import { Resend } from 'resend';
import JobFlow from './JobFlow.js';

dotenv.config();

const resend = new Resend(process.env.RESEND_API_KEY);
const worker = new Worker(3);


worker.start();

// await worker.awaitIdle();
// await worker.shutDown();