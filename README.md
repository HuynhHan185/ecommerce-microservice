# E-commerce MVP - Microservices

Hệ thống thương mại điện tử MVP xây dựng bằng **Node.js** theo kiến trúc **Microservices, DDD, Database-per-Service**. Toàn bộ hệ thống chạy bằng một lệnh Docker Compose.

## Kiến trúc

Client gọi REST qua **API Gateway**. Gateway xác thực JWT, phân quyền, giới hạn tốc độ, ghi log, rồi gọi các service bằng **gRPC**. Order Service phát sự kiện `OrderCreated` qua **Kafka** (mẫu Outbox) để Shipment Service tạo vận đơn bất đồng bộ.

| Thành phần | Vai trò | Cổng | Cơ sở dữ liệu |
|---|---|---|---|
| api-gateway | Routing, JWT, phân quyền, rate limit, cache, log | 8080 (công khai) | Redis |
| auth-service | Tài khoản, vai trò, cấp và kiểm tra JWT | 50051 | PostgreSQL `auth_db` |
| customer-service | Khách hàng, hạng thành viên, score, discount | 50052 | PostgreSQL `customer_db` |
| product-service | Sản phẩm, tồn kho | 50053 | MongoDB `product_db` |
| order-service | Đặt hàng, tính discount, Order và OrderDetail, Outbox | 50054 | PostgreSQL `order_db` |
| shipment-service | Nhận `OrderCreated`, quản lý trạng thái giao hàng | 50055 | MongoDB `shipment_db` |
| Kafka | Message broker (topic `user-registered`, `order-created`) | 9092 | - |

Chỉ API Gateway mở cổng ra ngoài. Các service giao tiếp trong mạng nội bộ của Docker Compose.

### Luồng đặt hàng

1. Client gọi `POST /api/orders` kèm JWT.
2. Gateway xác thực token, gọi Order Service.
3. Order Service lấy thông tin Customer (discount theo hạng thành viên), giữ tồn kho ở Product Service.
4. Order Service lưu Order, OrderDetail và OutboxEvent trong cùng một transaction.
5. Outbox publisher gửi `OrderCreated` lên Kafka.
6. Shipment Service nhận sự kiện, tạo Shipment trạng thái `PENDING` (không tạo trùng khi nhận lại cùng sự kiện).

## Công nghệ

Node.js, Express, gRPC, PostgreSQL, MongoDB, Kafka, Redis, JWT, bcrypt, Docker Compose.

## Chạy hệ thống

Yêu cầu: Docker và Docker Compose.

```bash
cp .env.example .env     # sửa giá trị trong .env nếu cần
docker compose up -d --build
docker compose ps        # chờ tất cả container healthy (khoảng 1 đến 2 phút)
curl localhost:8080/api/health
```

Dừng hệ thống: `docker compose down` (thêm `-v` để xóa dữ liệu).

## Tài khoản mẫu

| Username | Password | Vai trò |
|---|---|---|
| admin | Admin@123 | ADMIN |
| staff | Staff@123 | SHIPMENT_STAFF |

Tài khoản đăng ký mới có vai trò `CUSTOMER` và hạng thành viên thấp nhất (Bronze). Dữ liệu mẫu: hạng Bronze (giảm 0%), Silver (5%), Gold (10%) và 2 sản phẩm (Laptop, Mouse).

## API

Tất cả đi qua `http://localhost:8080`. Các endpoint có (JWT) cần header `Authorization: Bearer <token>`.

| Method | Endpoint | Quyền | Mô tả |
|---|---|---|---|
| GET | /api/health | Công khai | Health check |
| POST | /api/auth/register | Công khai | Đăng ký (201, 409 nếu trùng) |
| POST | /api/auth/login | Công khai | Đăng nhập, trả JWT |
| GET | /api/customers/:uid | JWT (chủ tài khoản hoặc ADMIN) | Lấy thông tin khách hàng và hạng |
| PUT | /api/customers/:uid | JWT (chủ tài khoản hoặc ADMIN) | Cập nhật `fullname`; chỉ ADMIN đổi `mid` |
| GET | /api/products | JWT | Danh sách sản phẩm |
| GET | /api/products/:pid | JWT | Chi tiết sản phẩm |
| POST | /api/products | ADMIN | Tạo sản phẩm |
| PUT | /api/products/:pid | ADMIN | Cập nhật sản phẩm |
| DELETE | /api/products/:pid | ADMIN | Xóa sản phẩm |
| POST | /api/orders | CUSTOMER | Đặt hàng, tự tính discount theo hạng |
| GET | /api/orders | JWT | Đơn hàng của mình (ADMIN: tất cả) |
| GET | /api/orders/:oid | JWT | Chi tiết đơn hàng |
| GET | /api/shipments?oid= | JWT | Tra vận đơn theo đơn hàng |
| GET | /api/shipments/:shipid | JWT | Chi tiết vận đơn |
| PATCH | /api/shipments/:shipid/status | ADMIN, SHIPMENT_STAFF | Cập nhật trạng thái (PENDING, SHIPPING, DELIVERED, CANCELLED) |

## Ví dụ nhanh

```bash
# Đăng nhập
curl -X POST localhost:8080/api/auth/login -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"Admin@123"}'

# Đặt hàng (dùng token của customer)
curl -X POST localhost:8080/api/orders -H "Content-Type: application/json" \
  -H "Authorization: Bearer <token>" -d '{"items":[{"pid":1,"qty":2}]}'
```

## Ghi chú

- Rate limit: 100 request mỗi phút cho toàn hệ thống và 10 lần mỗi phút cho đăng nhập (vượt thì trả `429`). Xóa bộ đếm: `docker exec gateway-redis redis-cli flushall`.
- Sau khi đăng ký, Customer được tạo bất đồng bộ qua Kafka (chờ 2 đến 3 giây trước khi đặt hàng).
- Shipment được tạo bất đồng bộ sau khi đặt hàng, nên chờ 2 đến 3 giây trước khi tra vận đơn.
- Mỗi service dùng cơ sở dữ liệu riêng, không truy cập trực tiếp dữ liệu của service khác.
