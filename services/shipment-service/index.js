const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '../../.env') });
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const mongoose = require('mongoose');
const { Kafka } = require('kafkajs');

const STATUSES = ['PENDING', 'SHIPPING', 'DELIVERED', 'CANCELLED'];

const Shipment = mongoose.model('Shipment', new mongoose.Schema({
  shipid: { type: Number, unique: true, required: true },
  oid: { type: Number, unique: true, required: true },   // unique => mỗi đơn chỉ có 1 shipment (idempotent)
  uid: { type: String, default: '' },
  status: { type: String, enum: STATUSES, default: 'PENDING' },
}, { timestamps: true, versionKey: false }));

const FailedEvent = mongoose.model('FailedEvent', new mongoose.Schema({
  reason: String, raw: String, at: { type: Date, default: Date.now },
}, { versionKey: false }));

const Counter = mongoose.model('Counter',
  new mongoose.Schema({ _id: String, seq: Number }, { versionKey: false }));
const nextShipid = async () =>
  5000 + (await Counter.findByIdAndUpdate('shipid', { $inc: { seq: 1 } }, { new: true, upsert: true })).seq;

const toMsg = (s) => ({ shipid: s.shipid, oid: s.oid, status: s.status });

const def = protoLoader.loadSync(path.join(__dirname, '../../proto/shipment.proto'),
  { keepCase: true, defaults: true });
const proto = grpc.loadPackageDefinition(def).shipment;
const fail = (cb, code, message) => cb({ code, message });
const wrap = (fn) => async (call, cb) => {
  try { await fn(call.request, cb); }
  catch (e) { console.error(e); fail(cb, grpc.status.INTERNAL, 'Lỗi hệ thống'); }
};
// Trả shipment nếu được phép xem, ngược lại trả lỗi
const guard = (s, r, cb) => {
  if (!s) { fail(cb, grpc.status.NOT_FOUND, 'Shipment not found'); return false; }
  if (!r.can_view_all && s.uid !== r.requester_uid) {
    fail(cb, grpc.status.PERMISSION_DENIED, 'Không đủ quyền'); return false;
  }
  return true;
};

const handlers = {
  GetShipment: wrap(async (r, cb) => {
    const s = await Shipment.findOne({ shipid: r.shipid }).lean();
    if (guard(s, r, cb)) cb(null, toMsg(s));
  }),
  GetShipmentByOrder: wrap(async (r, cb) => {
    const s = await Shipment.findOne({ oid: r.oid }).lean();
    if (guard(s, r, cb)) cb(null, toMsg(s));
  }),
  UpdateStatus: wrap(async (r, cb) => {
    if (!STATUSES.includes(r.status)) return fail(cb, grpc.status.INVALID_ARGUMENT, 'Trạng thái không hợp lệ');
    const s = await Shipment.findOneAndUpdate({ shipid: r.shipid }, { status: r.status }, { new: true }).lean();
    return s ? cb(null, toMsg(s)) : fail(cb, grpc.status.NOT_FOUND, 'Shipment not found');
  }),
};

// ---- Nhận OrderCreated từ Kafka ----
async function reject(reason, raw) {
  console.warn('Từ chối event:', reason);
  await FailedEvent.create({ reason, raw });
}
async function startConsumer() {
  const kafka = new Kafka({ clientId: 'shipment-service', brokers: ['localhost:9092'] });
  const consumer = kafka.consumer({ groupId: 'shipment-service' });
  await consumer.connect();
  await consumer.subscribe({ topic: 'order-created', fromBeginning: true });
  await consumer.run({
    eachMessage: async ({ message }) => {
      const raw = message.value ? message.value.toString() : '';
      let evt;
      try { evt = JSON.parse(raw); } catch { return reject('JSON không hợp lệ', raw); }
      if (!Number.isInteger(evt.oid) || evt.oid <= 0) return reject('Thiếu oid', raw);
      if (await Shipment.exists({ oid: evt.oid })) return console.log('Bỏ qua event trùng oid=' + evt.oid);
      try {
        const s = await Shipment.create({ shipid: await nextShipid(), oid: evt.oid, uid: evt.uid || '', status: 'PENDING' });
        console.log(`Đã tạo shipment ${s.shipid} cho oid=${evt.oid}`);
      } catch (e) {
        if (e.code === 11000) return console.log('Bỏ qua event trùng (đua) oid=' + evt.oid);
        throw e;                                    // lỗi khác: Kafka sẽ gửi lại event
      }
    },
  });
}

(async () => {
  await mongoose.connect('mongodb://localhost:27019/shipment_db');
  console.log('Đã kết nối MongoDB shipment_db');
  const server = new grpc.Server();
  server.addService(proto.ShipmentService.service, handlers);
  server.bindAsync('0.0.0.0:50055', grpc.ServerCredentials.createInsecure(), (err) => {
    if (err) throw err;
    console.log('Shipment service gRPC chạy ở :50055');
  });
  await startConsumer();
})();