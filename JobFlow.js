import RedisQueue from "./redisQueue.js";
import SendEmailJob from "./emailJob.js";

class JobFlow {
    constructor() {
        this.queue = new RedisQueue();
    }

    sendPasswordResetEmail(from, to, link) {
        const job = new SendEmailJob(from, to, "Password Reset", "password_reset", { resetLink: link });
        this.queue.addJob(job);
    }

    sendWelcomeEmail(from, to, name) {
        const job = new SendEmailJob(from, to, "Welcome", "welcome", { name: name });
        this.queue.addJob(job);
    }

    sendVerificationEmail(from, to, link) {
        const job = new SendEmailJob(from, to, "Email Verification", "email-verification", { verificationLink: link });
        this.queue.addJob(job);
    }
}

export default JobFlow;