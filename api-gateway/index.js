const path = require('path');
const crypto = require('crypto');
require('dotenv').config({ path: path.join(__dirname, '../.env') });
const express = require('express');
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const Redis = require('ioredis');

const redis = new Redis({ host: 'localhost', port: 6379 });
redis.on('error', (e) => console.error('Redis lỗi:', e.message));

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
const verifyRpc = (token) =>
  new Promise((resolve, reject) =>
    auth.Verify({ token }, (e, r) => (e ? reject(e) : resolve(r))));

// ---- Rate limit (cửa sổ 60 giây, đếm bằng Redis) ----
const rateLimit = (name, max) => async (req, res, next) => {
  try {
    const key = `rl:${name}:${req.ip}:${Math.floor(Date.now() / 60000)}`;
    const n = await redis.incr(key);
    if (n === 1) await redis.expire(key, 60);
    if (n > max) {
      res.set('Retry-After', '60');
      return res.status(429).json({ error: 'Quá nhiều request, thử lại sau' });
    }
  } catch (e) { console.error('rate limit bỏ qua do Redis lỗi'); }
  next();
};

// ---- Authenticate: cache token ở Redis ----
const authenticate = async (req, res, next) => {
  const h = req.headers.authorization || '';
  const token = h.startsWith('Bearer ') ? h.slice(7) : null;
  if (!token) return res.status(401).json({ error: 'Thiếu token' });
  const key = 'token:' + crypto.createHash('sha256').update(token).digest('hex');
  try {
    const cached = await redis.get(key);
    if (cached) { req.user = JSON.parse(cached); return next(); }
  } catch (e) { /* Redis lỗi thì hỏi thẳng Auth */ }
  try {
    const r = await verifyRpc(token);
    if (!r.valid) return res.status(401).json({ error: 'Token không hợp lệ hoặc đã hết hạn' });
    req.user = { user_id: r.user_id, role: r.role };
    const ttl = Number(r.exp) - Math.floor(Date.now() / 1000);
    if (ttl > 0) redis.set(key, JSON.stringify(req.user), 'EX', ttl).catch(() => {});
    next();
  } catch (e) { res.status(503).json({ error: 'Auth service không khả dụng' }); }
};

// ---- Authorize theo role ----
const authorize = (...roles) => (req, res, next) =>
  roles.includes(req.user.role)
    ? next()
    : res.status(403).json({ error: 'Không đủ quyền' });

const app = express();
app.use(express.json());
app.use((req, _res, next) => {
  console.log(`${new Date().toISOString()} ${req.ip} ${req.method} ${req.url}`);
  next();
});
app.use(rateLimit('global', 100));

app.get('/health', (_req, res) => res.json({ status: 'ok' }));
app.post('/api/auth/register', (req, res) => call('Register', req.body, res, 201));
app.post('/api/auth/login', rateLimit('login', 10), (req, res) => call('Login', req.body, res));

// Route thử, sau này thay bằng route thật
app.get('/api/me', authenticate, (req, res) => res.json(req.user));
app.get('/api/admin/ping', authenticate, authorize('ADMIN'), (_req, res) => res.json({ ok: true }));

app.listen(8080, () => console.log('API Gateway chạy ở :8080'));