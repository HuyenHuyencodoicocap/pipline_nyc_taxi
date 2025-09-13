------------------------------- Các phần em đã làm được trong dự án----------------------------
Data Ingestion & CDC (Change Data Capture):
Dữ liệu ban đầu về các chuyến taxi NYC được nhập vào cơ sở dữ liệu PostgreSQL. Dự án sử dụng Kafka Connect với Debezium connector để triển khai CDC, cho phép theo dõi và ghi nhận các thay đổi (inserts, updates, deletes) trong thời gian thực. Các thay đổi này sau đó được đẩy lên các topic tương ứng trên Kafka, phục vụ cho các quy trình xử lý downstream. File data_generator.py được sử dụng để tự động tạo ra các thay đổi giả lập ( insert, updates, deletes).
Data Lake & Storage:
Dữ liệu đã được ingest được lưu trữ trong một data lake sử dụng MinIO. Tại đây, dữ liệu được tổ chức theo cấu trúc star schema nhằm tối ưu hóa cho các truy vấn phân tích. Các tệp dữ liệu được lưu dưới định dạng Parquet, một định dạng cột (columnar format) hiệu quả, giúp giảm dung lượng lưu trữ và tăng tốc độ đọc dữ liệu.
ETL & Processing:
Quá trình ETL được thực hiện bằng các script Python trong thư mục src/ETL/streaming_etl_app.py. Dữ liệu từ Kafka được xử lý (thường là stream processing) và chuyển đổi để phù hợp với star schema. 
SQL & Schema Management:
Dự án sử dụng các tệp .sql để quản lý schema và thực hiện các truy vấn dữ liệu. Cụ thể:
schema_trips.sql: Định nghĩa schema cho bảng dữ liệu thô trên postgres.
gold_zone_schema.sql: Định nghĩa star schema cho Gold Zone
check_wal.sql: Một script kiểm tra log WAL (Write-Ahead Logging) của PostgreSQL, cần thiết cho quá trình CDC.
clickhouse.sql: Mô tả schema hoặc các truy vấn liên quan đến ClickHouse, một cơ sở dữ liệu cột có thể được sử dụng cho các tác vụ phân tích OLAP (Online Analytical Processing).

# ----------------------------- Các lệnh theo thứ tự để chạy dự án-------------------------------

# Lệnh khởi động dự án
docker compose up -d
# kiểm tra Postgres đã sẵn sàng
docker compose exec -T postgres pg_isready -U postgres -d nyc_taxi
# tạo cấu trúc bảng dữ liệu và thêm dữ liệu
docker compose exec -T postgres psql -U postgres -d nyc_taxi -f /sql/schema_trips.sql
<!-- docker compose exec -T postgres psql -U postgres -d nyc_taxi -f /sql/seed_trips.sql -->
python ".\src\Postgres\load_parquet_to_postgres.py"
# test xem dữ liệu đã tồn tại chưa
docker compose exec -T postgres psql -U postgres -d nyc_taxi -c "SELECT COUNT(*) FROM public.yellow_trips;"
# tạo connector postgres vs debezium
curl.exe -X POST http://localhost:8083/connectors `
  -H "Content-Type: application/json" `
  --data-binary "@src/Connector/pg-nyc-source.json"
# kiểm tra trạng thái của connector
curl.exe http://localhost:8083/connectors/pg-nyc-connector/status
# Lệnh kiểm tra các csdl trên clickhouse
docker compose exec -T clickhouse clickhouse-client -q "SHOW DATABASES;"
# tạo connector với clickhouse
curl.exe -X POST http://localhost:8083/connectors `
  -H "Content-Type: application/json" `
  --data-binary "@src/Connector/clickhouse-sink.json"
# kiểm tra trạng thái connector với clickhouse
curl.exe http://localhost:8083/connectors/clickhouse-sink/status
# Chạy file data_generator.py để tự động insert, update và delete dữ liệu
# Tạo vùng goldzone trên minIO ( tạo bucket nyc-taxi-warehouse trước)
docker exec spark-master spark-sql --packages org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.2.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.hive=org.apache.iceberg.spark.SparkSessionCatalog --conf spark.sql.catalog.hive.uri=thrift://hive-metastore:9083 --conf spark.sql.catalog.hive.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.sql.catalog.hive.s3.endpoint=http://minio:9000 --conf spark.sql.catalog.hive.s3.access-key=minioadmin --conf spark.sql.catalog.hive.s3.secret-key=minioadmin --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 --conf spark.hadoop.fs.s3a.access.key=minioadmin --conf spark.hadoop.fs.s3a.secret.key=minioadmin --conf spark.hadoop.fs.s3a.path.style.access=true --conf spark.sql.warehouse.dir=s3a://nyc-taxi-warehouse/ -f /app/Sql/gold_zone_schema.sql
# Lệnh kiểm tra xem csdl và các bảng đã đucợ tạo thành công chưa
docker exec spark-master spark-sql --packages org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.2.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.hive=org.apache.iceberg.spark.SparkSessionCatalog --conf spark.sql.catalog.hive.uri=thrift://hive-metastore:9083 --conf spark.sql.catalog.hive.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.sql.catalog.hive.s3.endpoint=http://minio:9000 --conf spark.sql.catalog.hive.s3.access-key=minioadmin --conf spark.sql.catalog.hive.s3.secret-key=minioadmin --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 --conf spark.hadoop.fs.s3a.access.key=minioadmin --conf spark.hadoop.fs.s3a.secret.key=minioadmin --conf spark.hadoop.fs.s3a.path.style.access=true --conf spark.sql.warehouse.dir=s3a://nyc-taxi-warehouse/ -e "SHOW DATABASES; USE nyc_taxi; SHOW TABLES;"
# chạy file ETL streaming_etl_app.py
docker exec spark-master spark-submit --master spark://spark-master:7077 --jars "/opt/bitnami/spark/jars/iceberg-spark-runtime-3.3_2.12-1.2.0.jar,/opt/bitnami/spark/jars/iceberg-aws-1.3.0.jar,/opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar,/opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.262.jar,/opt/bitnami/spark/jars/spark-hive_2.12-3.3.2.jar,/opt/bitnami/spark/jars/hive-exec-3.1.3.jar,/opt/bitnami/spark/jars/hive-metastore-3.1.3.jar" --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0" --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" --conf "spark.sql.catalog.iceberg_catalog=org.apache.iceberg.spark.SparkCatalog" --conf "spark.sql.catalog.iceberg_catalog.type=hive" --conf "spark.sql.catalog.iceberg_catalog.uri=thrift://hive-metastore:9083" --conf "spark.sql.catalog.iceberg_catalog.warehouse=s3a://nyc-taxi-warehouse/" --conf "spark.sql.catalog.iceberg_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO" --conf "spark.sql.catalog.iceberg_catalog.s3.endpoint=http://minio:9000" --conf "spark.sql.catalog.iceberg_catalog.s3.access-key=minioadmin" --conf "spark.sql.catalog.iceberg_catalog.s3.secret-key=minioadmin" --conf "spark.hadoop.fs.s3a.endpoint=http://minio:9000" --conf "spark.hadoop.fs.s3a.access.key=minioadmin" --conf "spark.hadoop.fs.s3a.secret.key=minioadmin" --conf "spark.hadoop.fs.s3a.path.style.access=true" --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" /app/ETL/streaming_etl_app.py


