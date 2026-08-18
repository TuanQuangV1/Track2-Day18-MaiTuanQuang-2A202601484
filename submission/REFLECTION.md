# Lakehouse Anti-Patterns Reflection

Trong Top 5 Lakehouse Anti-Patterns, đội ngũ của chúng tôi đối mặt với rủi ro cao nhất ở bẫy **Small-File Ingestion without Automated Maintenance** (Tích tụ file Parquet siêu nhỏ từ streaming ingestion mà thiếu cronjob bảo trì tự động).

### Nguyên nhân và Tác động:
Hệ thống nhận log quan sát LLM qua Kafka với chu kỳ trigger ngắn (5s), tạo ra hàng trăm ngàn file Parquet kích thước chỉ vài KB. Việc này gây ra hai hậu quả nghiêm trọng:
1. **Nghẽn hiệu năng truy vấn**: Engine đọc phải liên tục mở hàng triệu file metadata làm tăng độ trễ truy vấn phi tuyến tính.
2. **Bùng nổ chi phí S3**: Chi phí S3 `GET` request tăng vượt trội so với dung lượng lưu trữ thực tế.

### Rủi ro thứ hai & Giải pháp:
Bên cạnh đó, **Vector Index Lifecycle Skew** cũng là rủi ro pháp lý lớn (Luật PDPL / EU AI Act). Khi dữ liệu cá nhân bị xóa ở Lakehouse (System-of-Record), Vector DB phụ thuộc nếu không được evict sẽ vi phạm quyền được quên (Right-to-Erasure).

### Biện pháp phòng ngừa:
- Đã thiết lập 4 Job Maintenance chạy định kỳ bắt buộc: `Compaction` (gom file đạt 128MB), `Z-ORDER` (clustering theo `user_id`), `Expiry` (thu hồi version cũ) và `Orphan removal`.
- Áp dụng Delta **Change Data Feed (CDF)** để truyền sự kiện `delete` trực tiếp sang Vector Index, đảm bảo đồng bộ vòng đời dữ liệu.
