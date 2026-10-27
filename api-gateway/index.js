const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '../.env') });
const express = require('express');
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');

const def = protoLoader.loadSync(path.join(__dirname, '../proto/auth.proto'),
  { keepCase: true, defaults: true });
const auth = new (grpc.loadPackageDefinition(def).auth.AuthService)(
  'localhost:50051', grpc.credentials.createInsecure());

const HTTP = {
  [grpc.status.INVALID_ARGUMENT]: 400,
  [grpc.status.UNAUTHENTICATED]: 401,
  [grpc.status.PERMISSION_DENIED]: 403,
  [grpc.status.NOT_FOUND]: 404,
  [grpc.status.ALREADY_EXISTS]: 409,
};
const call = (method, body, res, okStatus = 200) =>
  auth[method](body, (err, reply) => {
    if (err) return res.status(HTTP[err.code] || 500).json({ error: err.details });
    res.status(okStatus).json(reply);
  });

const app = express();
app.use(express.json());

app.use((req, _res, next) => {            // logging
  console.log(`${new Date().toISOString()} ${req.method} ${req.url}`);
  next();
});

app.get('/health', (_req, res) => res.json({ status: 'ok' }));
app.post('/api/auth/register', (req, res) => call('Register', req.body, res, 201));
app.post('/api/auth/login', (req, res) => call('Login', req.body, res));

app.listen(8080, () => console.log('API Gateway chạy ở :8080'));
