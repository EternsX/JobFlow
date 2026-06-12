import express from "express";
import { welcome, verification, passwordReset } from "../controllers/email.controller.js";

const router = express.Router();


router.post("/welcome", welcome);
router.post("/verification", verification);
router.post("/password-reset", passwordReset);

export default router;