import { Resend } from "resend";
import https from "https";

const resend = new Resend('re_97z4XNFA_fmkPxQq6Mqx3jxyyx1mbNNCD');

const data = JSON.stringify({
  from: "WorkoutTracker <noreply@workouttracker.org>",
  to: "btriantafyllidis18@gmail.com",
  subject: "Test",
  html: "Hello"
});

const options = {
  hostname: "api.resend.com",
  path: "/emails",
  method: "POST",
  headers: {
    Authorization: "Bearer re_97z4XNFA_fmkPxQq6Mqx3jxyyx1mbNNCD",
    "Content-Type": "application/json",
    "Content-Length": Buffer.byteLength(data)
  }
};

const req = https.request(options, (res) => {
  console.log("STATUS:", res.statusCode);

  let body = "";

  res.on("data", (chunk) => {
    body += chunk;
  });

  res.on("end", () => {
    console.log("RESPONSE BODY:");
    console.log(body);
  });
});

req.on("error", (err) => {
  console.error("ERROR:", err);
});

req.write(data);
req.end();