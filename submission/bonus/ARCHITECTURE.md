# Architecture Strategy Brief: Enterprise High-Throughput LLM Observability & Multimodal RAG Lakehouse

**System Target:** Foundation Model Observability at Scale (1 Billion LLM requests / day, ~12,000 req/s peak, ~5 TB/day raw) & Multimodal RAG Corpus (1 Trillion Tokens).  
**Author / Lead Architect:** Mai Tuấn Quang (MaiTuanQuang-2A202601484)

---

## 1. Problem Statement

Đội ngũ Foundation Model API phục vụ **1 tỷ lượt gọi LLM / ngày** (~12,000 requests/giây ở thời điểm đỉnh điểm). Mỗi request ghi nhận log prompt, completion, usage, latency và metadata với kích thước trung bình 5 KB/request, tạo ra **5 TB dữ liệu thô / ngày**.

### Yêu cầu & Ràng buộc Hệ thống:
1. **Dashboard & Cost Tracking**: Yêu cầu cập nhật chỉ số chi phí & latency theo tenant mỗi 5 phút (SLA < 10s cho ad-hoc analytics).
2. **Lifecycle & Retention**: Giữ prompt/response đầy đủ trong 7 ngày phục vụ Incident Review; sau 7 ngày tự động tổng hợp (aggregate) và lưu trữ bản ghi nén 1 năm.
3. **Bảo mật & Quyền Riêng Tư (PII & PDPL)**: Tokenize/Redact toàn bộ PII (sĐT, CMND/CCCD, email) ngay tại tầng Bronze trước khi dữ liệu được ghi đĩa. Tuân thủ **Luật Bảo vệ Dữ liệu Cá nhân (PDPL 91/2025)** & **EU AI Act Art. 10**.
4. **Trần Chi Phí (FinOps Cap)**: Tổng chi phí lưu trữ S3 + request cost phải $\le \$5,000$ / tháng.

---

## 2. Architecture Diagram (End-to-End Medallion & Control Plane)

```
[ INGESTION LAYER ]
  Clients (12K req/s) ──> API Gateway (PII Redactor) ──> Kafka Cluster (24 Partitions)
                                                              │
                                                              ▼ (Spark Structured Streaming 5s trigger)
[ STORAGE LAYER (Medallion) ]
  S3 Standard ───────> BRONZE LAYER: s3://bronze/llm_calls_raw (Raw JSON + Tokenized PII)
                             │
                             ▼ (Spark Compaction 1h & ROW_NUMBER() Dedup by request_id)
  S3 Standard/IA ────> SILVER LAYER: s3://silver/llm_calls (Partitioned by date, Z-ORDER by tenant_id)
                             │
                             ▼ (Daily Aggregation Cron & Token Cost Join)
  S3 IA/Glacier ─────> GOLD LAYER: s3://gold/llm_daily_metrics (p50/p95 latency, cost_usd, error_rate)

[ CONTROL PLANE & GOVERNANCE ]
  Apache Polaris / Unity REST Catalog API ──> Query Planning, Metadata Caching & Access Control
  Delta Change Data Feed (CDF) ────────────> Real-time Eviction Signal to External Vector DB
```

---

## 3. Các Quyết Định Kiến Trúc Chính & Lựa Chọn Đã Loại Bỏ (Quyết Định & Alternatives)

### 📌 Quyết định 1: Chọn Apache Iceberg / Delta Lake với REST Catalog làm Control Plane
* **Lựa chọn**: **Apache Iceberg / Delta 4.x với Apache Polaris REST Catalog**.
* **Lựa chọn đã loại bỏ**: 
  - *Hive Metastore (HMS)*: Loại vì bị giới hạn bởi RDBMS bottleneck, không hỗ trợ Server-side scan planning và thiếu tính năng hidden partitioning.
  - *Custom S3 Layout không có Catalog*: Loại vì bẫy small files và nguy cơ scan toàn bộ bucket khi quên predicate.
* **Lý do**: REST Catalog chuyển việc lập kế hoạch scan (scan planning) về phía server, giảm 90% lượng metadata phải tải về client, hỗ trợ phân quyền mức cột (column-level access control) và hủy bỏ hoàn toàn bẫy Hive partition drift.

### 📌 Quyết định 2: Chiến lược Lưu trữ Vector In-Table kết hợp Lượng Tử Hóa Int8
* **Lựa chọn**: **Lưu mảng Embedding (dim=768) dạng cột `FixedSizeList` trong bảng Parquet, lượng tử hóa `int8`**.
* **Lựa chọn đã loại bỏ**:
  - *Tách biệt 100% sang Standalone Vector DB (Pinecone/Weaviate)*: Loại vì bẫy **Vector Index Lifecycle Skew** — khi dòng bị xóa ở Lakehouse (System-of-Record), Vector DB độc lập dễ bỏ sót sự kiện xóa, vi phạm quyền được quên (Right-to-Erasure).
  - *Giữ nguyên Float32 Vector trong Parquet*: Loại vì tốn kém gấp $4\times$ dung lượng đĩa.
* **Lý do**: Int8 Quantization giảm $4\times$ dung lượng đĩa ($768$ Bytes/row vs $3,072$ Bytes/row), giữ vững Recall@10 $\ge 88\%$ và Topic Fidelity $\ge 96\%$.

### 📌 Quyết định 3: Phân loại Provenance theo EU AI Act Art. 10 dưới dạng Partition Key
* **Lựa chọn**: **Tạo cột `provenance_bucket` làm Partition Key chính ở tầng Silver**.
* **Lựa chọn đã loại bỏ**:
  - *Quản lý Provenance bằng tài liệu Confluence / File CSV bên ngoài*: Loại vì không thể truy vết khi audit dữ liệu huấn luyện.
* **Lý do**: Khi biến 4 rổ bản quyền (`licensed`, `public_domain`, `scraped_optout_checked`, `synthetic`) thành partition key, việc loại bỏ dữ liệu không hợp lệ (`UNCLASSIFIED`) trở thành thao tác Partition Pruning tĩnh (chỉ cần bỏ qua partition folder), đạt độ trễ 0ms thay vì scan toàn bộ 5TB dữ liệu.

### 📌 Quyết định 4: Lưu trữ Blobs (Images/Audio) dưới dạng URI Pointer thay vì Inline Bytes
* **Lựa chọn**: **Lưu `blob_uri` trỏ tới S3 Object Storage bên ngoài**.
* **Lựa chọn đã loại bỏ**:
  - *Inline Binary Blobs trực tiếp trong cột Parquet*: Loại vì bẫy **Random-Access Row Group Amplification** — khi đọc 1 image 100KB, Parquet buộc phải decompress toàn bộ Row Group 32MB (khuếch đại đọc $\ge 5\times$).
* **Lý do**: Giữ bảng Parquet tinh gọn cho các truy vấn SQL analytics, trong khi GPU trainer tải binary blobs trực tiếp từ S3 bằng `GET` song song.

### 📌 Quyết định 5: Chiến lược Đồng bộ Xóa Dữ Liệu bằng Delta Change Data Feed (CDF)
* **Lựa chọn**: **Kích hoạt `delta.enableChangeDataFeed = true` cho tầng Silver**.
* **Lựa chọn đã loại bỏ**:
  - *Chạy lại Full Batch Re-sync hàng đêm*: Loại vì quá tốn kém compute ($5$ TB scan lại mỗi đêm chỉ để tìm vài dòng bị xóa) và không đáp ứng SLA xóa dữ liệu trong 1 giờ của PDPL.
* **Lý do**: Change Data Feed phát ra chính xác các sự kiện `_change_type = delete` chứa danh sách `doc_id`, cho phép Vector Index đăng ký lắng nghe và eviction lập tức.

---

## 4. Kịch Bản Sự Cố 3 Giờ Sáng (Failure Modes & Rollback Plans)

| # | Sự Cố (Failure Mode) | Nguyên Nhân | Cơ Chế Phát Hiện (Detection) | Kịch Bản Xử Lý & Rollback (Recovery) |
|---|---|---|---|---|
| 1 | **Small-File Storm & S3 Bill Spiking** | Kafka Ingestion job bị dồn ứ, ghi liên tục micro-batches 1KB khiến S3 GET cost tăng vọt. | CloudWatch Alert: Số lượng S3 GET Requests > 10,000/phút hoặc số file trong `_delta_log` > 5,000. | Kích hoạt tức thì khẩn cấp Job 1 Compaction: `dt.optimize.compact(target_size=128MB)`. Điều chỉnh trigger interval của Kafka Ingestion từ 5s lên 60s. |
| 2 | **Vector Index Lifecycle Violation (PDPL Audit Failure)** | Yêu cầu xóa dữ liệu cá nhân (`user_042`) đã xóa ở Delta nhưng pipeline CDF bị ngắt, làm rò rỉ dữ liệu trong Vector DB. | Automated Audit Cron: Chạy query đối chiếu ngẫu nhiên 1,000 IDs giữa System-of-Record và External Vector Index. | Tự động phát hiện chênh lệch (`ex_hits > 0`), kích hoạt `CDF replay` từ `starting_version` chưa đồng bộ để evict triệt để dữ liệu lỗi khỏi Vector Index. |
| 3 | **Bad Data Pipeline Pollution (Dữ liệu lỗi tràn vào Tầng Silver)** | Một bug ở upstream làm ghi âm điểm `score = -1` hoặc null `request_id` cho 10 triệu bản ghi. | Data Quality Assertion Fails trong Job chạy chuyển tầng Silver $\rightarrow$ Gold. | Thực thi tính năng Time-Travel Rollback: `dt.restore(version_clean)` trong < 30 giây. Phiên bản lỗi được loại bỏ hoàn toàn khỏi view hiện tại và ghi nhận lịch sử audit. |

---

## 5. Ước Tính Chi Phí FinOps Back-of-the-Envelope

**Giả định:** 5 TB dữ liệu thô / ngày = 150 TB / tháng.

### A. Chi Phí Storage (Bảng giá S3 List Price):
- **Hot Tier (0-7 ngày)**: $35\text{ TB} \times \$0.023/\text{GB} = \$805/\text{tháng}$.
- **Warm Tier (8-30 ngày)**: $115\text{ TB (Silver đã compact)} \times \$0.0125/\text{GB} = \$1,437.5/\text{tháng}$.
- **Cold Tier (31-365 ngày - Gold Aggregates)**: $5\text{ TB (Gold đã nén)} \times \$0.004/\text{GB} = \$20/\text{tháng}$.
- **Tổng Chi Phí Storage**: **$\$2,262.5$ / tháng**.

### B. Chi Phí S3 Requests (GET / PUT / LIST):
- **Nếu KHÔNG có Compaction**: $200\text{ files/batch} \times 12\text{ batches/phút} \times 1,440 \times 30 = 103.68\text{ triệu PUTs} \times \$0.005/1K = \$518/\text{tháng}$. Lượt GET từ 50,000 queries/ngày $\times 200,000\text{ files} = 10\text{ tỷ GETs} = \$4,000/\text{tháng}$ $\rightarrow$ **VƯỢT TRẦN BUDGET ($>\$6,700$)**.
- **Khi CÓ Compaction (128 MB target)**: Giảm xuống còn 40 files / day $\rightarrow$ 1,200 files total. Lượt GET cost giảm còn **$\$15$ / tháng**.

👉 **TỔNG CHI PHÍ THỰC TẾ:** **$\$2,277.5$ / tháng** (Hoàn toàn nằm dưới trần budget FinOps $\$5,000$/tháng).

---

## 6. Lộ Trình Lập Slice MVP 1 Tuần (One-Week MVP Plan) & PoC Code

### Lộ trình MVP:
- **Ngày 1–2**: Dựng `_lakehouse` Medallion schema, cấu hình PII Redactor tại Bronze.
- **Ngày 3–4**: Viết PySpark / Delta-rs Compaction & Z-ORDER cronjob.
- **Ngày 5–6**: Triển khai Change Data Feed (CDF) propagation cho Vector Index.
- **Ngày 7**: Kiểm thử kịch bản Time-Travel Restore & PDPL Right-to-Erasure.

---

### Code PoC Minh Chứng (Demonstration Spike Script):
File PoC ngắn tại [`submission/bonus/poc/bonus_poc.py`](file:///c:/Users/tuanq/Downloads/VinAI/Phase%202/Track2-Day18-MaiTuanQuang-2A202601484/submission/bonus/poc/bonus_poc.py) chứng minh cơ chế Lượng Tử Hóa Vector Int8, Change Data Feed Eviction và Provenance Partition Pruning:

```python
import polars as pl
import numpy as np
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

# 1. Vector Int8 Quantization Spike
SCALE = 127.0
emb_f32 = np.random.randn(1000, 256).astype("float32")
emb_f32 /= np.linalg.norm(emb_f32, axis=1, keepdims=True)
emb_i8 = np.clip(np.round(emb_f32 * SCALE), -127, 127).astype("int8")

print(f"Float32 Bytes: {emb_f32.nbytes} B  |  Int8 Bytes: {emb_i8.nbytes} B  (4x Reduction)")

# 2. CDF Event Propagation Spike
table_path = "_lakehouse/scratch/poc_cdf_demo"
df = pl.DataFrame({"doc_id": [1, 2, 3], "subject_id": ["user_A", "user_B", "user_A"]})
write_deltalake(table_path, df.to_arrow(), mode="overwrite", configuration={"delta.enableChangeDataFeed": "true"})

# Delete user_A
dt = DeltaTable(table_path)
dt.delete("subject_id = 'user_A'")

# Capture delete events via CDF
cdf = dt.load_cdf(starting_version=1).read_all()
print("CDF Delete Events emitted:", cdf.to_pandas()[["_change_type", "doc_id"]])
```
