const path = require('path');
const crypto = require('crypto');
require('dotenv').config({ path: path.join(__dirname, '../.env') });
const express = require('express');
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const Redis = require('ioredis');

const redis = new Redis({ host: 'localhost', port: 6379 });
redis.on('error', (e) => console.error('Redis lỗi:', e.message));

const load = (file) => grpc.loadPackageDefinition(
  protoLoader.loadSync(path.join(__dirname, '../proto', file), { keepCase: true, defaults: true }));
const insecure = grpc.credentials.createInsecure();
const auth = new (load('auth.proto').auth.AuthService)('localhost:50051', insecure);
const customer = new (load('customer.proto').customer.CustomerService)('localhost:50052', insecure);
const product = new (load('product.proto').product.ProductService)('localhost:50053', insecure);
const order = new (load('order.proto').order.OrderService)('localhost:50054', insecure);
const shipment = new (load('shipment.proto').shipment.ShipmentService)('localhost:50055', insecure);

const HTTP = {
  [grpc.status.INVALID_ARGUMENT]: 400,
  [grpc.status.UNAUTHENTICATED]: 401,
  [grpc.status.PERMISSION_DENIED]: 403,
  [grpc.status.NOT_FOUND]: 404,
  [grpc.status.ALREADY_EXISTS]: 409,
  [grpc.status.ABORTED]: 409,
  [grpc.status.UNAVAILABLE]: 503,
};
const rpc = (client, method, body, res, okStatus = 200) =>
  client[method](body, (err, reply) => {
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

// Chỉ chủ tài khoản hoặc ADMIN được truy cập
const ownerOrAdmin = (req, res, next) =>
  req.user.role === 'ADMIN' || req.user.user_id === req.params.uid
    ? next()
    : res.status(403).json({ error: 'Không đủ quyền' });

// ---- Kiểm tra dữ liệu đầu vào ----
const validPid = (req, res, next) =>
  /^\d{1,9}$/.test(req.params.pid) ? next() : res.status(400).json({ error: 'pid không hợp lệ' });
const validProduct = (req, res, next) => {
  const { pname, price, quantity } = req.body || {};
  const ok = typeof pname === 'string' && pname.trim() !== ''
    && typeof price === 'number' && price >= 0
    && Number.isInteger(quantity) && quantity >= 0;
  return ok ? next()
    : res.status(400).json({ error: 'pname, price (>=0), quantity (số nguyên >=0) là bắt buộc' });
};
const validOrder = (req, res, next) => {
  const items = req.body && req.body.items;
  const ok = Array.isArray(items) && items.length > 0 && items.length <= 50
    && items.every((i) => Number.isInteger(i.pid) && i.pid > 0 && Number.isInteger(i.qty) && i.qty > 0);
  return ok ? next() : res.status(400).json({ error: 'items phải là mảng {pid, qty} với số nguyên dương' });
};
const validOid = (req, res, next) =>
  /^\d{1,9}$/.test(req.params.oid) ? next() : res.status(400).json({ error: 'oid không hợp lệ' });
const validShipid = (req, res, next) =>
  /^\d{1,9}$/.test(req.params.shipid) ? next() : res.status(400).json({ error: 'shipid không hợp lệ' });

const SHIPMENT_STATUSES = ['PENDING', 'SHIPPING', 'DELIVERED', 'CANCELLED'];
const canViewAllShipments = (req) => ['ADMIN', 'SHIPMENT_STAFF'].includes(req.user.role);

const app = express();
app.use(express.json());
app.use((req, _res, next) => {
  console.log(`${new Date().toISOString()} ${req.ip} ${req.method} ${req.url}`);
  next();
});
app.use(rateLimit('global', 100));

app.get('/health', (_req, res) => res.json({ status: 'ok' }));
app.get('/api/health', async (_req, res) => {
  try {
    await redis.ping();
    res.json({ status: 'UP', message: 'E-commerce API is running' });
  } catch (e) { res.status(503).json({ status: 'DOWN', message: 'Redis không khả dụng' }); }
});

// ---- Auth ----
app.post('/api/auth/register', (req, res) => rpc(auth, 'Register', req.body, res, 201));
app.post('/api/auth/login', rateLimit('login', 10), (req, res) => rpc(auth, 'Login', req.body, res));

// Route thử
app.get('/api/me', authenticate, (req, res) => res.json(req.user));
app.get('/api/admin/ping', authenticate, authorize('ADMIN'), (_req, res) => res.json({ ok: true }));

// ---- Customer ----
app.get('/api/customers/:uid', authenticate, ownerOrAdmin,
  (req, res) => rpc(customer, 'GetCustomer', { uid: req.params.uid }, res));

app.put('/api/customers/:uid', authenticate, ownerOrAdmin, (req, res) => {
  const { fullname, mid } = req.body || {};
  if (mid !== undefined && req.user.role !== 'ADMIN')
    return res.status(403).json({ error: 'Chỉ ADMIN được đổi hạng thành viên' });
  rpc(customer, 'UpdateCustomer',
    { uid: req.params.uid, fullname: fullname || '', mid: mid || 0, set_mid: mid !== undefined }, res);
});

// ---- Product ----
app.get('/api/products', authenticate, (_req, res) => rpc(product, 'ListProducts', {}, res));
app.get('/api/products/:pid', authenticate, validPid,
  (req, res) => rpc(product, 'GetProduct', { pid: Number(req.params.pid) }, res));
app.post('/api/products', authenticate, authorize('ADMIN'), validProduct,
  (req, res) => rpc(product, 'CreateProduct', req.body, res, 201));
app.put('/api/products/:pid', authenticate, authorize('ADMIN'), validPid, validProduct,
  (req, res) => rpc(product, 'UpdateProduct', { ...req.body, pid: Number(req.params.pid) }, res));
app.delete('/api/products/:pid', authenticate, authorize('ADMIN'), validPid,
  (req, res) => rpc(product, 'DeleteProduct', { pid: Number(req.params.pid) }, res));

// ---- Order ----
app.post('/api/orders', authenticate, authorize('CUSTOMER'), validOrder, (req, res) => {
  order.CreateOrder(
    { uid: req.user.user_id, items: req.body.items.map(({ pid, qty }) => ({ pid, qty })) },
    (err, reply) => {
      if (err) return res.status(HTTP[err.code] || 500).json({ error: err.details });
      // Shipment được tạo bất đồng bộ qua Kafka nên lúc này luôn là PENDING
      res.status(201).json({ ...reply, shipment: { status: 'PENDING' } });
    });
});

app.get('/api/orders', authenticate, (req, res) =>
  rpc(order, 'ListOrders', { requester_uid: req.user.user_id, is_admin: req.user.role === 'ADMIN' }, res));

app.get('/api/orders/:oid', authenticate, validOid, (req, res) =>
  rpc(order, 'GetOrder',
    { oid: Number(req.params.oid), requester_uid: req.user.user_id, is_admin: req.user.role === 'ADMIN' }, res));

// ---- Shipment ----
// Tra shipment theo đơn hàng: GET /api/shipments?oid=1001
app.get('/api/shipments', authenticate, (req, res) => {
  if (!/^\d{1,9}$/.test(String(req.query.oid || '')))
    return res.status(400).json({ error: 'Cần tham số oid hợp lệ' });
  rpc(shipment, 'GetShipmentByOrder',
    { oid: Number(req.query.oid), requester_uid: req.user.user_id, can_view_all: canViewAllShipments(req) }, res);
});

app.get('/api/shipments/:shipid', authenticate, validShipid, (req, res) =>
  rpc(shipment, 'GetShipment',
    { shipid: Number(req.params.shipid), requester_uid: req.user.user_id, can_view_all: canViewAllShipments(req) }, res));

app.patch('/api/shipments/:shipid/status', authenticate, authorize('ADMIN', 'SHIPMENT_STAFF'), validShipid,
  (req, res) => {
    const status = req.body && req.body.status;
    if (!SHIPMENT_STATUSES.includes(status))
      return res.status(400).json({ error: 'status phải là ' + SHIPMENT_STATUSES.join(', ') });
    rpc(shipment, 'UpdateStatus', { shipid: Number(req.params.shipid), status }, res);
  });

app.listen(8080, () => console.log('API Gateway chạy ở :8080'));