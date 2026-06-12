import SendEmailJob from "../../../Job/emailJob.js";
import { queue } from "../queue/queue.js";

export const welcome = async (req, res) => {
    const { from, to, name } = req.body;
    const job = new SendEmailJob(from, to, "Welcome", "welcome", { name: name });
    queue.addJob(job);

    res.status(201).json({ success: true });
};


export const verification = async (req, res) => {
    const { from, to, link } = req.body;
    const job = new SendEmailJob(from, to, "Email Verification", "email-verification", { verificationLink: link });
    queue.addJob(job);

    res.status(201).json({ success: true });
};


export const passwordReset = async (req, res) => {
    const { from, to, link } = req.body;
    const job = new SendEmailJob(from, to, "Password Reset", "password_reset", { resetLink: link });
    queue.addJob(job);

    res.status(201).json({ success: true });
};