const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '../../.env') });
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const { Pool } = require('pg');
const { Kafka } = require('kafkajs');

const pool = new Pool({
  host: 'localhost', port: 5443,
  user: process.env.DB_USER, password: process.env.DB_PASSWORD,
  database: 'order_db',
});
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

const load = (f) => grpc.loadPackageDefinition(
  protoLoader.loadSync(path.join(__dirname, '../../proto', f), { keepCase: true, defaults: true }));
const insecure = grpc.credentials.createInsecure();
const customerClient = new (load('customer.proto').customer.CustomerService)('localhost:50052', insecure);
const productClient = new (load('product.proto').product.ProductService)('localhost:50053', insecure);
const orderProto = load('order.proto').order;

// Gọi gRPC sang service khác, tối đa 5 giây
const call = (client, method, req) => new Promise((resolve, reject) =>
  client[method](req, { deadline: Date.now() + 5000 }, (e, r) => (e ? reject(e) : resolve(r))));

async function initDb() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS orders (
      oid SERIAL PRIMARY KEY,
      uid UUID NOT NULL,                       -- tham chiếu sang Auth/Customer, không có FK
      discount INT NOT NULL,                   -- phần trăm giảm giá tại thời điểm đặt
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    CREATE TABLE IF NOT EXISTS order_details (
      id SERIAL PRIMARY KEY,
      oid INT NOT NULL REFERENCES orders(oid),
      pid INT NOT NULL,
      qty INT NOT NULL CHECK (qty > 0),
      unit_price NUMERIC(12,2) NOT NULL
    );
    CREATE TABLE IF NOT EXISTS outbox_events (
      id BIGSERIAL PRIMARY KEY,
      aggregate_id INT NOT NULL,
      event_type VARCHAR(50) NOT NULL,
      payload JSONB NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      published_at TIMESTAMPTZ
    );
    CREATE INDEX IF NOT EXISTS idx_outbox_pending ON outbox_events(id) WHERE published_at IS NULL;
  `);
}

const fail = (cb, code, message) => cb({ code, message });
// Chuyển lỗi từ service khác thành lỗi của Order Service
const passThrough = (cb, e) => {
  const known = [grpc.status.NOT_FOUND, grpc.status.ABORTED, grpc.status.INVALID_ARGUMENT];
  if (known.includes(e.code)) return fail(cb, e.code, e.details);
  if (e.code === grpc.status.UNAVAILABLE || e.code === grpc.status.DEADLINE_EXCEEDED)
    return fail(cb, grpc.status.UNAVAILABLE, 'Dịch vụ phụ thuộc không khả dụng');
  console.error(e);
  return fail(cb, grpc.status.INTERNAL, 'Lỗi hệ thống');
};

const rowToOrder = (o, items = []) => ({
  oid: o.oid, uid: o.uid, discount: o.discount,
  created_at: new Date(o.created_at).toISOString(),
  items: items.map((d) => ({ pid: d.pid, qty: d.qty, unit_price: Number(d.unit_price) })),
});

const handlers = {
  async CreateOrder({ request: r }, cb) {
    let reserved = null;
    let committed = false;
    try {
      if (!UUID_RE.test(r.uid)) return fail(cb, grpc.status.INVALID_ARGUMENT, 'uid không hợp lệ');
      // Kiểm tra items, gộp các dòng trùng pid
      const merged = new Map();
      for (const it of r.items) {
        if (!(it.pid > 0) || !(it.qty > 0))
          return fail(cb, grpc.status.INVALID_ARGUMENT, 'items không hợp lệ (pid, qty phải > 0)');
        merged.set(it.pid, (merged.get(it.pid) || 0) + it.qty);
      }
      if (merged.size === 0) return fail(cb, grpc.status.INVALID_ARGUMENT, 'Đơn hàng phải có ít nhất 1 sản phẩm');
      const items = [...merged].map(([pid, qty]) => ({ pid, qty }));

      const cus = await call(customerClient, 'GetCustomer', { uid: r.uid });       // 404 nếu không có
      reserved = await call(productClient, 'ReserveStock', { items });             // 404 / 409 nếu lỗi

      const client = await pool.connect();
      let order;
      try {
        await client.query('BEGIN');
        const o = await client.query(
          'INSERT INTO orders(uid, discount) VALUES ($1, $2) RETURNING oid, created_at', [r.uid, cus.discount]);
        const { oid, created_at } = o.rows[0];
        for (const it of reserved.items)
          await client.query('INSERT INTO order_details(oid,pid,qty,unit_price) VALUES ($1,$2,$3,$4)',
            [oid, it.pid, it.qty, it.unit_price]);
        const payload = {
          oid, uid: r.uid, discount: cus.discount, created_at: new Date(created_at).toISOString(),
          items: reserved.items.map((i) => ({ pid: i.pid, qty: i.qty, unit_price: i.unit_price })),
        };
        await client.query(
          `INSERT INTO outbox_events(aggregate_id, event_type, payload) VALUES ($1, 'OrderCreated', $2)`,
          [oid, JSON.stringify(payload)]);
        await client.query('COMMIT');
        committed = true;
        order = rowToOrder({ oid, uid: r.uid, discount: cus.discount, created_at }, reserved.items);
      } catch (e) {
        await client.query('ROLLBACK').catch(() => {});
        throw e;
      } finally { client.release(); }
      cb(null, order);
    } catch (e) {
      if (reserved && !committed)       // Giao dịch thất bại: hoàn lại tồn kho đã giữ
        await call(productClient, 'ReleaseStock', { items: reserved.items })
          .catch((err) => console.error('HOÀN KHO THẤT BẠI, cần xử lý tay:', err.message));
      passThrough(cb, e);
    }
  },

  async GetOrder({ request: r }, cb) {
    try {
      const o = await pool.query('SELECT * FROM orders WHERE oid = $1', [r.oid]);
      if (!o.rows[0]) return fail(cb, grpc.status.NOT_FOUND, 'Order not found');
      if (!r.is_admin && o.rows[0].uid !== r.requester_uid)
        return fail(cb, grpc.status.PERMISSION_DENIED, 'Không đủ quyền');
      const d = await pool.query('SELECT pid, qty, unit_price FROM order_details WHERE oid = $1 ORDER BY id', [r.oid]);
      cb(null, rowToOrder(o.rows[0], d.rows));
    } catch (e) { console.error(e); fail(cb, grpc.status.INTERNAL, 'Lỗi hệ thống'); }
  },

  async ListOrders({ request: r }, cb) {
    try {
      const q = r.is_admin
        ? await pool.query('SELECT * FROM orders ORDER BY oid DESC LIMIT 200')
        : await pool.query('SELECT * FROM orders WHERE uid = $1 ORDER BY oid DESC LIMIT 200', [r.requester_uid]);
      cb(null, { items: q.rows.map((o) => rowToOrder(o)) });
    } catch (e) { console.error(e); fail(cb, grpc.status.INTERNAL, 'Lỗi hệ thống'); }
  },
};

// ---- Outbox publisher: đọc event chưa gửi, đẩy lên Kafka, đánh dấu đã gửi ----
const producer = new Kafka({ clientId: 'order-service', brokers: ['localhost:9092'] }).producer();
let kafkaReady = false, publishing = false, lastErr = '';
async function publishOutbox() {
  if (publishing) return;
  publishing = true;
  const client = await pool.connect();
  try {
    if (!kafkaReady) { await producer.connect(); kafkaReady = true; console.log('Kafka producer đã kết nối'); }
    await client.query('BEGIN');
    const { rows } = await client.query(
      `SELECT id, payload FROM outbox_events WHERE published_at IS NULL
       ORDER BY id LIMIT 20 FOR UPDATE SKIP LOCKED`);
    for (const ev of rows) {
      await producer.send({
        topic: 'order-created',
        messages: [{ key: String(ev.payload.oid), value: JSON.stringify({ event_id: ev.id, ...ev.payload }) }],
      });
      await client.query('UPDATE outbox_events SET published_at = now() WHERE id = $1', [ev.id]);
      console.log('Đã gửi OrderCreated oid=' + ev.payload.oid);
    }
    await client.query('COMMIT');
    lastErr = '';
  } catch (e) {
    await client.query('ROLLBACK').catch(() => {});
    kafkaReady = false;                       // lần sau thử kết nối lại
    if (e.message !== lastErr) { console.error('Outbox chưa gửi được, sẽ thử lại:', e.message); lastErr = e.message; }
  } finally { client.release(); publishing = false; }
}

(async () => {
  await initDb();
  const server = new grpc.Server();
  server.addService(orderProto.OrderService.service, handlers);
  server.bindAsync('0.0.0.0:50054', grpc.ServerCredentials.createInsecure(), (err) => {
    if (err) throw err;
    console.log('Order service gRPC chạy ở :50054');
  });
  setInterval(publishOutbox, 1000);
})();