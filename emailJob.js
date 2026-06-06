import Job from './job.js';
import EMAIL_CASES from './constants/email-cases.js';

class SendEmailJob extends Job {
  constructor(from, to, subject, type, content) {
    super({ id: `email-${Date.now()}`, priority: 1 });
    this.from = from;
    this.to = to;
    this.subject = subject;
    this.type = type;
    this.content = content;
  }

  async perform() {
    await resend.emails.send({
      from: this.from,
      to: this.to,
      subject: this.subject,
      html: this.renderEmail(this.type, this.content),
    });
  }

  renderEmail(type, content) {
    switch (type) {
      case "password_reset":
        return EMAIL_CASES.password_reset(content);
      case "welcome":
        return EMAIL_CASES.welcome(content);
      case "email-verification":
        return EMAIL_CASES.email_verification(content);
    }
  }
}

// const job = new SendEmailJob();
// job.execute().then(result => {
//   if (result.ok) {
//     console.log('Email sent successfully!');
//   } else {
//     console.error('Failed to send email:', result.error);
//   }
// });

export default SendEmailJob;