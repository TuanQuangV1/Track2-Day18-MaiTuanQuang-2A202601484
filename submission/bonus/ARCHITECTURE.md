# Architecture Strategy Brief: High-Throughput LLM Observability & Multimodal RAG Lakehouse

**System Target:** 1 Billion LLM requests / day (~12,000 req/s peak) & Multimodal RAG Corpus (1 Trillion Tokens).

---

## 1. High-Level Architecture Overview

Hệ thống được thiết kế dựa trên kiến trúc **Lakehouse Medallion Architecture** kết hợp với **Apache Iceberg / Delta Lake Catalog làm Control Plane**:

```
[ Ingestion Layer ] ──> Kafka / Kinesis (5s Micro-batch)
                             │
                             ▼
[ Storage Layer ] ──> Bronze (Raw JSON / Unparsed Blobs)
                             │  (Spark Structured Streaming)
                             ▼
                      Silver (Parsed, Deduplicated by request_id, Partitioned by date)
                             │  (SQL Aggregation + Z-ORDER)
                             ▼
                      Gold (Aggregated Daily Metrics: p50/p95 Latency, Cost USD, Error Rate)
```

### Catalog Control Plane:
Sử dụng **Polaris / Unity Catalog (REST Catalog API)** làm Query Planner và Security Boundary trung tâm. Catalog quản lý scan planning phía server (server-side planning), phân quyền chi tiết tới mức cột (column-level security) và loại bỏ phụ thuộc Hive Metastore.

---

## 2. Chiến Lược Lưu Trữ, Tối Ưu FinOps & Quản Lý Metadata

### A. Phân tầng Lưu trữ (Storage Tiering):
- **Hot Tier (S3 Standard)**: Dữ liệu 0–30 ngày (`Bronze` + `Silver`). Đảm bảo độ trễ đọc dưới 100ms.
- **Warm Tier (S3 Standard-IA)**: Dữ liệu 31–90 ngày (`Silver` đã compact + `Gold`). Giảm 40% chi phí lưu trữ.
- **Cold Tier (S3 Glacier Instant Retrieval)**: Dữ liệu > 90 ngày (`Gold` aggregated metrics). Tuân thủ lưu trữ audit 2 năm.

### B. 4 Job Bảo Trì Bắt Buộc (Table Maintenance Pipeline):
1. **Compaction (`rewrite_data_files` / `OPTIMIZE`)**: Chạy cronjob 1 giờ/lần cho tầng Silver để gom các micro-batches (KB) thành các file Parquet kích thước tối ưu 128 MB–512 MB, giảm số lượng file $\ge 50\times$.
2. **Clustering (`Z-ORDER` / Liquid Clustering)**: Clustering theo `(model, user_id)` để đạt tỷ lệ skip file $\ge 80\%$ cho các truy vấn point-query và dashboard analytics.
3. **Snapshot Expiry (`expire_snapshots` / `VACUUM`)**: Xóa bỏ các snapshot cũ quá 7 ngày (`retention_hours=168`), giải phóng các tombstoned metadata.
4. **Orphan File Removal (`remove_orphan_files`)**: Quét các file parquet không thuộc transaction log (do job crash) bằng phép hiệu tập hợp ($Disk \setminus Log$) với age guard 24 giờ.

---

## 3. Quản Lý Bản Quyền AI (EU AI Act Art. 10) & Quyền Xóa Dữ Liệu (PDPL / GDPR)

### A. Provenance Buckets (EU AI Act Art. 10):
Toàn bộ dữ liệu huấn luyện RAG và Trajectory được phân loại bắt buộc vào 4 phân vùng bản quyền:
1. `licensed`: Hợp đồng thương mại / bản quyền mua lại.
2. `public_domain`: Giấy phép mở (CC-BY-4.0, MIT, Apache 2.0).
3. `scraped_optout_checked`: Data thu thập đã qua bộ lọc từ chối (Opt-out list).
4. `synthetic`: Dữ liệu tổng hợp (Ghi rõ `generator` và `seed`).
- Dữ liệu thuộc nhãn `UNCLASSIFIED` bị tự động cách ly (isoinate) khỏi pipeline huấn luyện.

### B. Quyền Xóa Dữ Liệu Cá Nhân (PDPL Luật 91/2025 & GDPR Right-to-Erasure):
- Khi nhận yêu cầu xóa dữ liệu của cá nhân (`subject_id`), hệ thống thực hiện `DELETE` trên bảng System-of-Record.
- Kích hoạt **Delta Change Data Feed (CDF)** để tự động phát sự kiện `delete` sang Vector DB / External Index, giải quyết triệt để bẫy **Vector Index Lifecycle Skew**.

---

## 4. Multimodal Vector Storage & Lượng Tử Hóa (Quantization)

- **Vector In-Table**: Mảng Embedding (dim=768 / 1536) được lưu dưới dạng cột `FixedSizeList` trực tiếp trong bảng Parquet/Lance.
- **Int8 Quantization**: Lượng tử hóa vector từ `float32` sang `int8` (Symmetric scaling $[-127, 127]$), giảm dung lượng lưu trữ $4\times$, giữ vững Recall@10 $\ge 85\%$ và Topic Fidelity $\ge 95\%$.
- **Pointer vs Inline Blob**: Lưu ảnh/video raw dưới dạng URI pointer (`blob_uri`), giữ bảng Parquet gọn nhẹ để truy vấn phân tích không bị ảnh hưởng bởi Row Group IO amplification.
