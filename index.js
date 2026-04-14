import Worker from './worker.js'; // your Worker class

const worker = new Worker(3); // concurrency = 3

worker.start(); // 🔥 THIS is what you were missing/confused about