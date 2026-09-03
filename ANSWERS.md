# Day 28 Modern Platform Lab - Submission Answers

## 1. Vai trò thực hiện (Cá nhân)
Do thực hiện lab cá nhân (Local-standard), mình đã đi qua toàn bộ các vai trò của dự án:
- **Ingestion & Orchestration (IP01-IP02)**: Thiết lập header chứa khóa chống trùng lặp `idempotency-key` và mã theo dõi `traceparent` tại Gateway để đẩy vào Kafka.
- **Data & ML (IP03-IP04-IP06)**: Cấu hình `dedupe_latest` trong Spark Delta MERGE để giữ lại log mới nhất khi xảy ra Replay. Xây dựng truy vấn Fetch Feature từ Feast Store (`asker_activity_v1`).
- **Serving & Retrieval (IP05-IP07)**: Khởi tạo luồng inference lấy Retrieval từ Qdrant và tính toán thông qua cấu trúc RAG. (Phần GPU vLLM được đánh dấu UNVERIFIED do giới hạn phần cứng máy cá nhân).
- **Platform & Giám sát (IP08-IP10)**: Thiết kế chuẩn kiểm tra sức khoẻ hệ thống (`ready`/`degraded`/`not_ready`) dựa trên `readiness_status`.

## 2. Ghi chú sự cố (Incident Report)
- **Sự cố tạo ra**: Kafka Broker bị mất kết nối mạng tạm thời (mô phỏng) dẫn đến một lô bản tin bị gián đoạn, sau đó Consumer group trigger **Replay** để gửi lại toàn bộ.
- **Dấu hiệu quan sát**: Trên Grafana, throughput của Consumer bỗng nhiên có một Spike (đỉnh nhọn) tăng vọt lượng requests.
- **Nguyên nhân**: Lỗi mạng transient (tạm thời) dẫn đến rebalance và replay lại offset cũ.
- **Cách khôi phục**: Hệ thống tự động phục hồi. Dữ liệu trong Delta Lake **không bị phình to (no data loss & no data duplication)** nhờ cơ chế `dedupe_latest` so sánh `(occurred_at, event_id)` trên `idempotency_key` (IP03).

## 3. Reflection
- **Điều khó nhất**: Đảm bảo tính minh bạch và truyền vết (Traceability). Khi một Request đi qua FastAPI -> Kafka -> Airflow -> Spark -> Delta, việc giữ nguyên vẹn cái `traceparent` (Trace ID) ở IP01/IP10 để Jaeger vẽ được một chuỗi liền mạch là cực kỳ thử thách. Nó đòi hỏi mọi component không được drop W3C headers.
- **Trade-off đã chọn**: 
  - Ở IP07/IP08 (Readiness), mình chọn Trade-off "Trả về Degraded thay vì Not Ready" nếu thành phần không bắt buộc (như vLLM model cache) bị sập. Việc này đánh đổi trải nghiệm trả lời hơi chậm/sai lệch một chút nhưng giữ cho API Gateway vẫn sống và phục vụ được các Request cơ bản (như gửi feedback), giúp tăng Availability của toàn hệ thống (CAP theorem: ưu tiên A và P).
- **Điều sẽ cải tiến**: 
  - Khai báo thêm Circuit Breaker trên API Gateway để tự động ngừng gửi Request sang các services đang báo `degraded` quá lâu (tránh cạn kiệt connection pool).
  - Phân vùng Kafka (Partitions) mịn hơn theo `asker_id` thay vì random, để tránh hoàn toàn race condition khi gửi replay.
