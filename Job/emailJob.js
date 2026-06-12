import Job from './job.js';
import EMAIL_CASES from '../constants/email-cases.js';
import resend from '../config/resend.js';
import crypto from "crypto";
import https from 'https';

class SendEmailJob extends Job {
  constructor(from, to, subject, type, content) {
    super({ id: `email-${crypto.randomUUID()}`, priority: 1 });
    this.from = from;
    this.to = to;
    this.subject = subject;
    this.type = type;
    this.content = content;
    this.jobType = 'email'
  }

  static from(raw) {
    const job = new SendEmailJob(
      raw.from,
      raw.to,
      raw.subject,
      raw.type,
      raw.content
    );

    job.id = raw.id;
    job.priority = Number(raw.priority ?? 1);
    job.tries = Number(raw.tries ?? 0);
    job.maxTries = Number(raw.maxTries ?? 3);

    job.validate();
    return job;
  }

  async perform() {

    const data = JSON.stringify({
      from: this.from,
      to: this.to,
      subject: this.subject,
      html: this.renderEmail(this.type, this.content),
    });

    return new Promise((resolve, reject) => {
      const req = https.request({
        hostname: "api.resend.com",
        path: "/emails",
        method: "POST",
        headers: {
          Authorization: `Bearer ${process.env.RESEND_API_KEY}`,
          "Content-Type": "application/json",
          "Content-Length": Buffer.byteLength(data),
        },
      }, (res) => {
        let body = "";

        res.on("data", (chunk) => {
          body += chunk;
        });

        res.on("end", () => {
          const parsed = JSON.parse(body || "{}");

          if (res.statusCode >= 200 && res.statusCode < 300) {
            resolve(parsed);
          } else {
            reject(parsed);
          }
        });
      });

      req.on("error", (err) => {
        reject(err);
      });

      req.write(data);
      req.end();
    });
  }

  renderEmail(type, content) {
    switch (type) {
      case "password_reset":
        return EMAIL_CASES.password_reset(content);
      case "welcome":
        return EMAIL_CASES.welcome(content);
      case "email_verification":
        return EMAIL_CASES.email_verification(content);
    }
  }
}

export default SendEmailJob;