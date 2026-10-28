const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '../../.env') });
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const mongoose = require('mongoose');

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
};

(async () => {
  await mongoose.connect('mongodb://localhost:27018/product_db');
  console.log('Đã kết nối MongoDB product_db');
  if ((await Product.countDocuments()) === 0) {       // dữ liệu mẫu cho dev
    for (const [pname, price, quantity] of [['Laptop', 1500, 10], ['Mouse', 25, 100]])
      await Product.create({ pid: await nextPid(), pname, price, quantity });
    console.log('Đã seed 2 sản phẩm mẫu');
  }
  const server = new grpc.Server();
  server.addService(proto.ProductService.service, handlers);
  server.bindAsync('0.0.0.0:50053', grpc.ServerCredentials.createInsecure(), (err) => {
    if (err) throw err;
    console.log('Product service gRPC chạy ở :50053');
  });
})();