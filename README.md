

# ESG Lakehouse System

## Tổng quan

ESG Lakehouse System là nền tảng quản lý, xử lý và phân tích dữ liệu ESG (Environmental, Social, Governance) toàn diện, ứng dụng kiến trúc Lakehouse hiện đại. Hệ thống tích hợp các công nghệ: Apache Spark, Airflow, dbt, Trino, Flask API, Streamlit, phục vụ cho pipeline ETL, phân tích dữ liệu, và cung cấp API cũng như dashboard trực quan.

---

## Cấu trúc thư mục chính

```
airflow/           # Quản lý pipeline ETL với Airflow
configs/           # Cấu hình hệ thống
datasets/          # Dữ liệu đầu vào (csv, pdf, xlsx)
dbt-profiles/      # Cấu hình cho dbt
dbt-project/       # Dự án dbt (models, macros, ...)
docs/              # Tài liệu dự án
init-scripts/      # Script khởi tạo môi trường
IoT_data_ingestion/# Thành phần ingest dữ liệu IoT (nếu có)
logs/              # Log hệ thống
metricflow/        # Flask API server truy vấn dữ liệu ESG qua Trino
notebooks/         # Notebook phân tích, thử nghiệm
output/            # Kết quả đầu ra
scraper/           # Thành phần thu thập dữ liệu (crawler)
scripts/           # Script tiện ích
spark-apps/        # Ứng dụng Spark xử lý dữ liệu (bronze, silver, gold)
streamlit-app/     # Ứng dụng dashboard Streamlit
trino/             # Cấu hình Trino
utils/             # Tiện ích dùng chung
.env               # Biến môi trường
docker-compose.yml # Khởi tạo các dịch vụ bằng Docker
README.md          # Tài liệu mô tả dự án
```

---

## Luồng dữ liệu tổng thể

1. **Thu thập dữ liệu**: Dữ liệu ESG được thu thập từ nhiều nguồn (csv, pdf, xlsx, web scraping) và lưu vào thư mục `datasets/`.
2. **Xử lý dữ liệu với Spark**: Các script trong `spark-apps/bronze`, `silver`, `gold` thực hiện ingest, làm sạch, chuẩn hóa, tổng hợp dữ liệu ESG.
3. **Orchestration với Airflow**: Các DAG trong `airflow/dags/` tự động hóa pipeline ETL, gọi Spark, dbt, kiểm thử dữ liệu.
4. **Quản lý mô hình dữ liệu với dbt**: Định nghĩa các dimension, fact, semantic model trong `dbt-project/` và kiểm thử chất lượng dữ liệu.
5. **Lưu trữ & truy vấn với Trino**: Dữ liệu được lưu trên MinIO (S3), truy vấn qua Trino.
6. **API truy vấn dữ liệu ESG**: Flask API (`metricflow/api_server.py`) cung cấp endpoint RESTful cho truy vấn động.
7. **Dashboard trực quan**: Streamlit app (`streamlit-app/`) hiển thị dashboard ESG cho người dùng cuối.

---

## Thành phần chính

- **datasets/**: Dữ liệu ESG đầu vào (csv, pdf, xlsx)
- **spark-apps/**: Xử lý dữ liệu với Spark (bronze: ingest, silver: clean, gold: tổng hợp)
- **airflow/**: Orchestrate pipeline ETL, tự động hóa các bước xử lý
- **dbt-project/**: Quản lý mô hình dữ liệu, semantic layer, kiểm thử dữ liệu
- **metricflow/api_server.py**: Flask API truy vấn dữ liệu ESG qua Trino
- **streamlit-app/**: Dashboard trực quan hóa dữ liệu ESG
- **scraper/**: Script thu thập dữ liệu ESG từ web
- **trino/**: Cấu hình truy vấn dữ liệu dạng SQL trên Lakehouse

---

## Cơ sở dữ liệu ở tầng Gold
![alt text](<Fact_Dim (2).jpg>)

## Dịch vụ Docker Compose

Hệ thống hỗ trợ khởi tạo nhanh toàn bộ stack qua `docker-compose.yml`:

- **minio**: Lưu trữ dữ liệu dạng S3 (bronze, silver, gold)
- **spark-master, spark-worker, spark-submit**: Cụm Spark xử lý dữ liệu lớn
- **airflow**: Orchestrate pipeline ETL
- **dbt**: Chạy các model dbt
- **trino**: Truy vấn dữ liệu dạng SQL trên Lakehouse
- **metricflow**: Flask API truy vấn dữ liệu ESG
- **streamlit**: Dashboard trực quan hóa ESG
- **postgres**: Lưu metadata cho Airflow, dbt
- **jupyter**: Notebook phân tích dữ liệu
- **chrome**: Selenium Chrome cho scraping

---

## Hướng dẫn cài đặt & sử dụng

### 1. Cài đặt thư viện Python cần thiết (nếu chạy local)

```bash
pip install -r requirements.txt
pip install requests beautifulsoup4 pandas lxml openpyxl unstructured
```

### 2. Khởi động toàn bộ hệ thống với Docker Compose

```bash
docker-compose up -d
```

### 3. Chạy pipeline ETL
- Truy cập Airflow UI tại http://localhost:8080 để kích hoạt DAG `esg_gold_layer_pipeline` hoặc các DAG khác.

### 4. Truy vấn dữ liệu ESG qua API
- Chạy Flask API server (nếu không dùng Docker):
  ```bash
  python metricflow/api_server.py
  ```
- Gửi request tới các endpoint như `/api/v1/query`, `/api/v1/metrics`, `/api/v1/dimensions`.

### 5. Xem dashboard ESG
- Chạy ứng dụng Streamlit (nếu không dùng Docker):
  ```bash
  streamlit run streamlit-app/app.py
  ```

---

## Đóng góp

- Đoàn Quang Lâm
- Nguyễn Thị Hồng Thơ

## Giấy phép

Các nguồn dữ liệu sử dụng trong dự án đến từ các nguồn công khai và tuân thủ các điều khoản sử dụng tương ứng. Vui lòng tham khảo tài liệu nguồn dữ liệu để biết thêm chi tiết về giấy phép và quyền sử dụng dữ liệu.

Các chỉ số ESG được tính toán và trình bày trong hệ thống này nhằm mục đích nghiên cứu và giáo dục. Người dùng nên tham khảo ý kiến chuyên gia trước khi sử dụng các chỉ số này cho mục đích đầu tư hoặc ra quyết định kinh doanh.

Các chỉ số lấy từ báo cáo bền vững đến từ nhiều công ty công khai.