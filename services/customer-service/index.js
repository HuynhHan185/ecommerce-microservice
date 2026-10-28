const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '../../.env') });
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const { Pool } = require('pg');
const { Kafka } = require('kafkajs');

const pool = new Pool({
  host: 'localhost', port: 5442,
  user: process.env.DB_USER, password: process.env.DB_PASSWORD,
  database: 'customer_db',
});
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

async function initDb() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS membership (
      mid SERIAL PRIMARY KEY,
      mname VARCHAR(30) UNIQUE NOT NULL,
      min_score INT NOT NULL,
      discount INT NOT NULL            -- phần trăm giảm giá
    );
    CREATE TABLE IF NOT EXISTS customer (
      uid UUID PRIMARY KEY,            -- tham chiếu sang Auth, không có FK
      fullname VARCHAR(100) NOT NULL,
      mid INT NOT NULL REFERENCES membership(mid),
      score INT NOT NULL DEFAULT 0,
      created_at TIMESTAMPTZ DEFAULT now()
    );
    INSERT INTO membership(mname,min_score,discount)
      VALUES ('Bronze',0,0),('Silver',200,5),('Gold',500,10)
      ON CONFLICT (mname) DO NOTHING;
  `);
}

const def = protoLoader.loadSync(path.join(__dirname, '../../proto/customer.proto'),
  { keepCase: true, defaults: true });
const proto = grpc.loadPackageDefinition(def).customer;
const fail = (cb, code, message) => cb({ code, message });

const handlers = {
  async GetCustomer({ request: r }, cb) {
    try {
      if (!UUID_RE.test(r.uid)) return fail(cb, grpc.status.INVALID_ARGUMENT, 'uid không hợp lệ');
      const { rows } = await pool.query(
        `SELECT c.uid,c.fullname,c.mid,c.score,m.mname,m.discount
         FROM customer c JOIN membership m ON m.mid=c.mid WHERE c.uid=$1`, [r.uid]);
      if (!rows[0]) return fail(cb, grpc.status.NOT_FOUND, 'Customer not found');
      cb(null, rows[0]);
    } catch (e) { console.error(e); fail(cb, grpc.status.INTERNAL, 'Lỗi hệ thống'); }
  },
  async UpdateCustomer({ request: r }, cb) {
    try {
      if (!UUID_RE.test(r.uid)) return fail(cb, grpc.status.INVALID_ARGUMENT, 'uid không hợp lệ');
      const name = (r.fullname || '').trim();
      if (!name && !r.set_mid) return fail(cb, grpc.status.INVALID_ARGUMENT, 'Không có dữ liệu để cập nhật');
      if (name.length > 100) return fail(cb, grpc.status.INVALID_ARGUMENT, 'fullname quá dài');
      const res = await pool.query(
        `UPDATE customer SET
           fullname = COALESCE(NULLIF($2::text,''), fullname),
           mid = CASE WHEN $3::boolean THEN $4::int ELSE mid END
         WHERE uid=$1 RETURNING uid`, [r.uid, name, r.set_mid, r.mid]);
      if (!res.rowCount) return fail(cb, grpc.status.NOT_FOUND, 'Customer not found');
      cb(null, { message: 'Customer updated successfully', uid: r.uid });
    } catch (e) {
      if (e.code === '23503') return fail(cb, grpc.status.INVALID_ARGUMENT, 'mid không tồn tại');
      console.error(e); fail(cb, grpc.status.INTERNAL, 'Lỗi hệ thống');
    }
  },
};

// Nhận event user-registered từ Auth Service, tạo Customer ở hạng thấp nhất
async function startConsumer() {
  const kafka = new Kafka({ clientId: 'customer-service', brokers: ['localhost:9092'] });
  const consumer = kafka.consumer({ groupId: 'customer-service' });
  await consumer.connect();
  await consumer.subscribe({ topic: 'user-registered', fromBeginning: true });
  await consumer.run({
    eachMessage: async ({ message }) => {
      let evt;
      try { evt = JSON.parse(message.value.toString()); } catch { return console.warn('Bỏ qua event hỏng'); }
      if (!evt.uid || !UUID_RE.test(evt.uid)) return console.warn('Bỏ qua event thiếu uid');
      await pool.query(
        `INSERT INTO customer(uid,fullname,mid)
         SELECT $1::uuid,$2::text,mid FROM membership ORDER BY min_score LIMIT 1
         ON CONFLICT (uid) DO NOTHING`, [evt.uid, evt.username || 'Customer']);
      console.log('Đã tạo customer', evt.uid);
    },
  });
}

(async () => {
  await initDb();
  const server = new grpc.Server();
  server.addService(proto.CustomerService.service, handlers);
  server.bindAsync('0.0.0.0:50052', grpc.ServerCredentials.createInsecure(), (err) => {
    if (err) throw err;
    console.log('Customer service gRPC chạy ở :50052');
  });
  await startConsumer();
})();