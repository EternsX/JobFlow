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