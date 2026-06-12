import SendEmailJob from "../Job/emailJob.js";
import Job from "../Job/job.js";

export async function workerLoopSwitch(res, POLL_INTERVAL, delay) {
    switch (res.type) {
        case 'RATE_LIMIT':
        case 'NO_JOBS':
            await delay(POLL_INTERVAL + Math.random() * 200);
            break;
        case 'ERROR':
            console.log(`Worker ${workerId} error: ${res.error[1]}`);
            await delay(POLL_INTERVAL);
    }
}

export function createJobFrom(rawJob) {
    switch (rawJob.jobType) {
        case 'email':
            return SendEmailJob.from(rawJob);
        case 'job':
            return Job.from(rawJob);
        default:
            throw new Error(`Unknown job type: ${rawJob.type}`);
    }
}