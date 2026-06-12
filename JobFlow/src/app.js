import express from 'express';
import cors from 'cors';
import emailRoutes from './routes/email.routes.js'

const app = express();

app.use(cors({
  methods: ['POST']
}));
app.use(express.json());


app.use('/email', emailRoutes);

export default app;

