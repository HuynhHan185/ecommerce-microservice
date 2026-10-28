const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '../../.env') });
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const mongoose = require('mongoose');

// ---- Cấu hình (đọc từ biến môi trường, mặc định cho chạy local) ----
const MONGO_URL = process.env.MONGO_URL || 'mongodb://localhost:27018/product_db';
const GRPC_PORT = process.env.PRODUCT_PORT || 50053;

const Product = mongoose.model('Product', new mongoose.Schema({
  pid: { type: Number, unique: true, required: true },
  pname: { type: String, required: true, trim: true, maxlength: 200 },
  price: { type: Number, required: true, min: 0 },
  quantity: { type: Number, required: true, min: 0 },
}, { versionKey: false }));

// pid tự tăng (1, 2, 3...) bằng một bảng đếm riêng
const Counter = mongoose.model('Counter',
  new mongoose.Schema({ _id: String, seq: Number }, { versionKey: false }));
const nextPid = async () =>
  (await Counter.findByIdAndUpdate('pid', { $inc: { seq: 1 } }, { new: true, upsert: true })).seq;

const toMsg = (p) => ({ pid: p.pid, pname: p.pname, price: p.price, quantity: p.quantity });
const validInput = (r) =>
  typeof r.pname === 'string' && r.pname.trim() !== '' && r.price >= 0 && r.quantity >= 0;

const def = protoLoader.loadSync(path.join(__dirname, '../../proto/product.proto'),
  { keepCase: true, defaults: true });
const proto = grpc.loadPackageDefinition(def).product;
const fail = (cb, code, message) => cb({ code, message });
const wrap = (fn) => async (call, cb) => {
  try { await fn(call.request, cb); }
  catch (e) { console.error(e); fail(cb, grpc.status.INTERNAL, 'Lỗi hệ thống'); }
};

const handlers = {
  ListProducts: wrap(async (_r, cb) => {
    const items = await Product.find().sort({ pid: 1 }).lean();
    cb(null, { items: items.map(toMsg) });
  }),
  GetProduct: wrap(async (r, cb) => {
    const p = await Product.findOne({ pid: r.pid }).lean();
    return p ? cb(null, toMsg(p)) : fail(cb, grpc.status.NOT_FOUND, 'Product not found');
  }),
  CreateProduct: wrap(async (r, cb) => {
    if (!validInput(r)) return fail(cb, grpc.status.INVALID_ARGUMENT, 'Dữ liệu sản phẩm không hợp lệ');
    const p = await Product.create({ pid: await nextPid(), pname: r.pname, price: r.price, quantity: r.quantity });
    cb(null, toMsg(p));
  }),
  UpdateProduct: wrap(async (r, cb) => {
    if (!validInput(r)) return fail(cb, grpc.status.INVALID_ARGUMENT, 'Dữ liệu sản phẩm không hợp lệ');
    const p = await Product.findOneAndUpdate({ pid: r.pid },
      { pname: r.pname, price: r.price, quantity: r.quantity }, { runValidators: true });
    return p
      ? cb(null, { message: 'Product updated successfully', pid: r.pid })
      : fail(cb, grpc.status.NOT_FOUND, 'Product not found');
  }),
  DeleteProduct: wrap(async (r, cb) => {
    const res = await Product.deleteOne({ pid: r.pid });
    return res.deletedCount
      ? cb(null, { message: 'Product deleted successfully', pid: r.pid })
      : fail(cb, grpc.status.NOT_FOUND, 'Product not found');
  }),
  // Giữ tồn kho cho cả đơn: được tất cả hoặc không được gì
  ReserveStock: wrap(async (r, cb) => {
    const done = [];
    const undo = () => Promise.all(done.map((d) =>
      Product.updateOne({ pid: d.pid }, { $inc: { quantity: d.qty } })));
    for (const it of r.items) {
      if (!(it.qty > 0)) { await undo(); return fail(cb, grpc.status.INVALID_ARGUMENT, 'qty không hợp lệ'); }
      // Trừ kho nguyên tử: chỉ trừ khi còn đủ hàng, tránh bán vượt khi nhiều đơn đến cùng lúc
      const p = await Product.findOneAndUpdate(
        { pid: it.pid, quantity: { $gte: it.qty } },
        { $inc: { quantity: -it.qty } }, { new: true });
      if (!p) {
        await undo();
        const exists = await Product.exists({ pid: it.pid });
        return exists
          ? fail(cb, grpc.status.ABORTED, `Insufficient stock for pid ${it.pid}`)
          : fail(cb, grpc.status.NOT_FOUND, `Product ${it.pid} not found`);
      }
      done.push({ pid: it.pid, qty: it.qty, unit_price: p.price });
    }
    cb(null, { items: done });
  }),
  // Hoàn kho khi tạo đơn thất bại (bù trừ)
  ReleaseStock: wrap(async (r, cb) => {
    for (const it of r.items)
      await Product.updateOne({ pid: it.pid }, { $inc: { quantity: it.qty } });
    cb(null, { message: 'Stock released', pid: 0 });
  }),
};

// MongoDB có thể chưa sẵn sàng khi container vừa lên: thử lại thay vì sập
async function connectWithRetry() {
  for (;;) {
    try { await mongoose.connect(MONGO_URL); return; }
    catch (e) {
      console.error('Kết nối MongoDB lỗi, thử lại sau 3s:', e.message);
      await new Promise((r) => setTimeout(r, 3000));
    }
  }
}

(async () => {
  await connectWithRetry();
  console.log('Đã kết nối MongoDB product_db');
  if ((await Product.countDocuments()) === 0) {       // dữ liệu mẫu cho dev
    for (const [pname, price, quantity] of [['Laptop', 1500, 10], ['Mouse', 25, 100]])
      await Product.create({ pid: await nextPid(), pname, price, quantity });
    console.log('Đã seed 2 sản phẩm mẫu');
  }
  const server = new grpc.Server();
  server.addService(proto.ProductService.service, handlers);
  server.bindAsync(`0.0.0.0:${GRPC_PORT}`, grpc.ServerCredentials.createInsecure(), (err) => {
    if (err) throw err;
    console.log(`Product service gRPC chạy ở :${GRPC_PORT}`);
  });
})();