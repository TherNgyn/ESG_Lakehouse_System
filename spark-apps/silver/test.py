from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 1. Khởi tạo Spark Session (giữ nguyên config giống file chính)
spark = SparkSession.builder \
    .appName("ESG-Test-Bradesco-Output") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "admin123456") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

OUTPUT_PATH = "s3a://silver/staging_companies_mapping"
#df_pdf = spark.read.parquet("s3a://silver/clean_kpi_pdf")
def test_output():

    # 2. Đọc dữ liệu từ bảng Delta
    try:
        df = spark.read.format("parquet").load(OUTPUT_PATH)
    except Exception as e:
        print(f"Lỗi: Không tìm thấy hoặc không đọc được bảng Delta tại {OUTPUT_PATH}")
        return

    count = df.count()
    if count == 0:
        print(">>> KẾT QUẢ: Không tìm thấy bất kỳ dòng dữ liệu nào của Bradesco trong bảng!")
        # Thử in ra danh sách các công ty đang có trong bảng để debug
        print("\nDanh sách các công ty hiện có trong bảng:")
        df.select("name").distinct().show(truncate=False)
    else:
        print(f">>> KẾT QUẢ: Tìm thấy {count} dòng dữ liệu của Bradesco.")

       
        # In mẫu dữ liệu chi tiết
        print("[4] Mẫu 20 dòng dữ liệu chi tiết:")
        df.show(2000, truncate=True)
    print("="*60 + "\n")

if __name__ == "__main__":
    test_output()
    spark.stop()
