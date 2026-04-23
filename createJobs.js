import RedisQueue from './redisQueue.js';
import Job from './job.js';

const queue = new RedisQueue();

async function createJobs() {
    for (let i = 1; i <= 20; i++) {
        const job = new Job(
            i,
            `Task number ${i}`,
            3,
            Math.floor(Math.random() * 10)
        );

        await queue.addJob(job);
        console.log(`➕ Added ${job.id}`);
    }

    await queue.redis.quit(); // 🔥 CLOSE CONNECTION
}

createJobs();