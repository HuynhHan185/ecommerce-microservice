const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '../../.env') });
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const { Pool } = require('pg');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');

const JWT_SECRET = process.env.JWT_SECRET;
if (!JWT_SECRET) throw new Error('Thiếu JWT_SECRET trong .env');
const TOKEN_TTL = 3600;

const pool = new Pool({
  host: 'localhost', port: 5441,
  user: process.env.DB_USER, password: process.env.DB_PASSWORD,
  database: 'auth_db',
});

async function initDb() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS roles (
      id SERIAL PRIMARY KEY, name VARCHAR(30) UNIQUE NOT NULL
    );
    CREATE TABLE IF NOT EXISTS users (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      username VARCHAR(50) UNIQUE NOT NULL,
      email VARCHAR(120) UNIQUE NOT NULL,
      phone VARCHAR(20),
      password_hash TEXT NOT NULL,
      role_id INT NOT NULL REFERENCES roles(id),
      created_at TIMESTAMPTZ DEFAULT now()
    );
    INSERT INTO roles(name) VALUES ('CUSTOMER'),('ADMIN'),('SHIPMENT_STAFF')
      ON CONFLICT DO NOTHING;
  `);
  // Tài khoản admin mặc định cho môi trường dev
  const hash = await bcrypt.hash('Admin@123', 10);
  await pool.query(
    `INSERT INTO users(username,email,password_hash,role_id)
     SELECT 'admin','admin@example.com',$1,id FROM roles WHERE name='ADMIN'
     ON CONFLICT DO NOTHING`, [hash]);
}

const def = protoLoader.loadSync(path.join(__dirname, '../../proto/auth.proto'),
  { keepCase: true, defaults: true });
const authProto = grpc.loadPackageDefinition(def).auth;

const fail = (cb, code, message) => cb({ code, message });

const handlers = {
  async Register({ request: r }, cb) {
    try {
      if (!r.username || !r.email || !r.password || r.password.length < 6)
        return fail(cb, grpc.status.INVALID_ARGUMENT, 'username, email, password (>=6 ký tự) là bắt buộc');
      const hash = await bcrypt.hash(r.password, 10);
      const { rows } = await pool.query(
        `INSERT INTO users(username,email,phone,password_hash,role_id)
         SELECT $1,$2,$3,$4,id FROM roles WHERE name='CUSTOMER'
         RETURNING id`, [r.username, r.email, r.phone || null, hash]);
      cb(null, { user_id: rows[0].id, username: r.username, role: 'CUSTOMER' });
    } catch (e) {
      if (e.code === '23505') return fail(cb, grpc.status.ALREADY_EXISTS, 'username hoặc email đã tồn tại');
      console.error(e); fail(cb, grpc.status.INTERNAL, 'Lỗi hệ thống');
    }
  },
  async Login({ request: r }, cb) {
    try {
      const { rows } = await pool.query(
        `SELECT u.id,u.password_hash,ro.name AS role FROM users u
         JOIN roles ro ON ro.id=u.role_id WHERE u.username=$1`, [r.username]);
      const u = rows[0];
      if (!u || !(await bcrypt.compare(r.password, u.password_hash)))
        return fail(cb, grpc.status.UNAUTHENTICATED, 'Sai username hoặc password');
      const token = jwt.sign({ sub: u.id, role: u.role }, JWT_SECRET, { expiresIn: TOKEN_TTL });
      cb(null, { access_token: token, expires_in: TOKEN_TTL, role: u.role });
    } catch (e) { console.error(e); fail(cb, grpc.status.INTERNAL, 'Lỗi hệ thống'); }
  },
  Verify({ request: r }, cb) {
    try {
      const p = jwt.verify(r.token, JWT_SECRET);
      cb(null, { valid: true, user_id: p.sub, role: p.role, exp: p.exp });
    } catch { cb(null, { valid: false }); }
  },
};

(async () => {
  await initDb();
  const server = new grpc.Server();
  server.addService(authProto.AuthService.service, handlers);
  server.bindAsync('0.0.0.0:50051', grpc.ServerCredentials.createInsecure(), (err) => {
    if (err) throw err;
    console.log('Auth service gRPC chạy ở :50051');
  });
})();
