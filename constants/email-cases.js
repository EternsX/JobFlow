const EMAIL_CASES = {
    "password_reset": (content) => 
        `
        <h1>Reset your password</h1>
        <p>Click the button below:</p>
        <a href="${content.resetLink}" 
           style="padding:10px 20px;background:#000;color:#fff;">
          Reset Password
        </a>
      `,
    "welcome": (content) =>
        `
        <h1>Welcome to our service, ${content.name}!</h1>
        <p>We're excited to have you on board. Explore our features and enjoy your experience.</p>
      `,
    "email-verification": (content) => 
        `
        <h1>Verify your email address</h1>
        <p>Click the link below to verify your email:</p>
        <a href="${content.verificationLink}"
          Verify Email
        </a>
      `

}

export default EMAIL_CASES;