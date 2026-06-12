const baseUrl = 'http://localhost:4000';
const emailUrl = `${baseUrl}/email`;
const welcomeUrl = `${emailUrl}/welcome`;
const verificationUrl = `${emailUrl}/verification`;
const passwordResetUrl = `${emailUrl}/password-reset`;

class JobFlow {
    async sendPasswordResetEmail(from, to, link) {
        await fetch(passwordResetUrl, {
            method: "POST",
            body: JSON.stringify({ from, to, link }),
            headers: {
                "Content-Type": "application/json",
            },
        })
        return { success: true }
    }

    async sendWelcomeEmail(from, to, name) {
        console.log(2)
        const res = await fetch(welcomeUrl, {
            method: "POST",
            body: JSON.stringify({ from, to, name }),
            headers: {
                "Content-Type": "application/json",
            },
        })
        console.log(res)
        return { success: true }
    }

    async sendVerificationEmail(from, to, link) {
        await fetch(verificationUrl, {
            method: "POST",
            body: JSON.stringify({ from, to, link }),
            headers: {
                "Content-Type": "application/json",
            },
        })
        return { success: true }
    }
}

export default JobFlow;