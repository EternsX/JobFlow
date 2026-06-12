import RedisQueue from './Queue/redisQueue.js';
import Job from './Job/job.js';

const queue = new RedisQueue();

async function createJobs() {
    for (let i = 1; i <= 50; i++) {
        const job = Job.from({
            id: i,
            description: `Task number ${i}`,
            priority: Math.floor(Math.random() * 10),
            maxTries: 3
        });

        await queue.addJob(job);
        console.log(`➕ Added ${job.id}`);
    }

    await queue.redis.quit(); 
}

createJobs();