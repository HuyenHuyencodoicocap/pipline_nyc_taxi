from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_date, hour, dayofmonth, month, year
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    DoubleType,
    IntegerType,
)


def create_spark_session():
    """Tạo và cấu hình Spark Session."""
    return (
        SparkSession.builder.appName("NYC Taxi Streaming ETL")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        # Sử dụng SparkCatalog thay vì SparkSessionCatalog để cấu hình rõ ràng và ổn định hơn
        # .config("spark.sql.catalog.hive", "org.apache.iceberg.spark.SparkCatalog")
        # .config("spark.sql.catalog.hive.type", "hive") 

        # .config("spark.sql.catalog.hive.uri", "thrift://hive-metastore:9083")
        # .config(
        #     "spark.sql.catalog.hive.io-impl", "org.apache.iceberg.aws.s3.S3FileIO"
        # )
        # .config("spark.sql.catalog.hive.s3.endpoint", "http://minio:9000")
        # .config("spark.sql.catalog.hive.s3.access-key", "minioadmin")
        # .config("spark.sql.catalog.hive.s3.secret-key", "minioadmin")
        # .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        # .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        # .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        # .config("spark.hadoop.fs.s3a.path.style.access", "true")
        # .config("spark.sql.warehouse.dir", "s3a://nyc-taxi-warehouse/")
        # .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.2.0,org.apache.iceberg:iceberg-aws-bundle:1.3.0")
        .getOrCreate()
            )

def main():
    spark = create_spark_session()

    # Định nghĩa schema cho dữ liệu JSON đến từ Kafka
    kafka_schema = StructType([
        StructField("vendor_id", IntegerType(), True),
        StructField("tpep_pickup_datetime", TimestampType(), True),
        StructField("tpep_dropoff_datetime", TimestampType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("ratecode_id", IntegerType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("PULocationID", IntegerType(), True),
        StructField("DOLocationID", IntegerType(), True),
        StructField("payment_type", IntegerType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("congestion_surcharge", DoubleType(), True),
        StructField("airport_fee", DoubleType(), True),
        StructField("lpep_pickup_datetime", TimestampType(), True),
        StructField("lpep_dropoff_datetime", TimestampType(), True),
    ])

    # === SỬA LỖI 2: Bổ sung bước làm giàu dữ liệu ===
    # # Tạo DataFrame tĩnh cho bảng đối chiếu
    payment_type_data = [
        (1, "Credit card"), (2, "Cash"), (3, "No charge"),
        (4, "Dispute"), (5, "Unknown"), (6, "Voided trip")
    ]
    payment_type_df = spark.createDataFrame(payment_type_data, ["payment_type", "payment_type_description"])

    ratecode_data = [
        (1, "Standard rate"), (2, "JFK"), (3, "Newark"),
        (4, "Nassau or Westchester"), (5, "Negotiated fare"), (6, "Group ride")
    ]
    ratecode_df = spark.createDataFrame(ratecode_data, ["ratecode_id", "ratecode_description"])

    # Đọc luồng dữ liệu từ Kafka
    kafka_stream_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "nyc-taxi-topic")
        .load()
    )

    # Phân tích dữ liệu JSON
    data_stream = kafka_stream_df.selectExpr("CAST(value AS STRING) AS json")
    parsed_stream = data_stream.select(from_json(col("json"), kafka_schema).alias("data")).select("data.*")
    
    # JOIN với bảng tĩnh để có thêm cột description
    enriched_stream = parsed_stream.join(
        payment_type_df, on="payment_type", how="left_outer"
    ).join(
        ratecode_df, on="ratecode_id", how="left_outer"
    )
    print("Đọc và làm giàu dữ liệu thành công")
    
    # === Bắt đầu các bước TRANSFORM (trên dữ liệu đã được làm giàu) ===
    transformed_stream = enriched_stream.withColumn(
        "tpep_pickup_date", to_date(col("tpep_pickup_datetime"))
    ).withColumn(
        "tpep_dropoff_date", to_date(col("tpep_dropoff_datetime"))
    )

    transformed_stream = transformed_stream.filter(
        (col("passenger_count") >= 0) & (col("trip_distance") >= 0) &
        (col("fare_amount") >= 0) & (col("tip_amount") >= 0) &
        (col("tolls_amount") >= 0)
    )

    transformed_stream = transformed_stream.fillna({
        "tip_amount": 0, "tolls_amount": 0, "fare_amount": 0
    })

   
    # transformed_stream = transformed_stream.dropDuplicates() 
    print("transform dữ liệu thành công")

    # === Bước LOAD dữ liệu vào các bảng chiều (Dimension Tables) ===
    # 1. Tải dữ liệu vào dim_datetime
    dim_datetime_stream = transformed_stream.select(
        "tpep_pickup_datetime", "tpep_dropoff_datetime",
        hour("tpep_pickup_datetime").alias("pickup_hour"),
        dayofmonth("tpep_pickup_datetime").alias("pickup_day"),
        month("tpep_pickup_datetime").alias("pickup_month"),
        year("tpep_pickup_datetime").alias("pickup_year"),
        hour("tpep_dropoff_datetime").alias("dropoff_hour"),
        dayofmonth("tpep_dropoff_datetime").alias("dropoff_day"),
        month("tpep_dropoff_datetime").alias("dropoff_month"),
        year("tpep_dropoff_datetime").alias("dropoff_year"),
    )
    # spark.sql("CREATE DATABASE iceberg_catalog.nyc_taxi LOCATION 's3a://nyc-taxi-warehouse/nyc_taxi.db';")
    query_dim_datetime = (
        dim_datetime_stream.writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", "s3a://nyc-taxi-warehouse/nyc_taxi.db/dim_datetime/_checkpoints")
        .toTable("iceberg_catalog.nyc_taxi.dim_datetime")
    )
    
    # 2. Tải dữ liệu vào dim_passenger_count
    dim_passenger_count_stream = transformed_stream.select("passenger_count")
    query_dim_passenger = (
        dim_passenger_count_stream.writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", "s3a://nyc-taxi-warehouse/dim_passenger_count")
        .toTable("iceberg_catalog.nyc_taxi.dim_passenger_count")
    )

    # 3. Tải dữ liệu vào dim_ratecode
    dim_ratecode_stream = transformed_stream.select("ratecode_id", "ratecode_description")
    query_dim_ratecode = (
        dim_ratecode_stream.writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", "s3a://nyc-taxi-warehouse/dim_ratecode")
        .toTable("iceberg_catalog.nyc_taxi.dim_ratecode")
    )
    
    # 4. Tải dữ liệu vào dim_payment_type
    dim_payment_type_stream = transformed_stream.select("payment_type", "payment_type_description")
    query_dim_payment = (
        dim_payment_type_stream.writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", "s3a://nyc-taxi-warehouse/dim_payment_type")
        .toTable("iceberg_catalog.nyc_taxi.dim_payment_type")
    )

    # 5. Tải dữ liệu vào dim_trip_distance
    dim_trip_distance_stream = transformed_stream.select("trip_distance")
    query_dim_distance = (
        dim_trip_distance_stream.writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", "s3a://nyc-taxi-warehouse/dim_trip_distance")
        .toTable("iceberg_catalog.nyc_taxi.dim_trip_distance")
    )
    
    # === Kích hoạt lại việc ghi vào Bảng Fact ===
    fact_stream = transformed_stream.select(
        "vendor_id", "tpep_pickup_datetime", "tpep_dropoff_datetime",
        "passenger_count", "trip_distance", "ratecode_id", "store_and_fwd_flag",
        "PULocationID", "DOLocationID", "payment_type", "fare_amount", "extra",
        "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge",
        "total_amount", "congestion_surcharge", "airport_fee",
        "lpep_pickup_datetime", "lpep_dropoff_datetime"
    )
    query_fact_trips = (
        fact_stream.writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", "s3a://nyc-taxi-warehouse/fact_trips")
        .toTable("iceberg_catalog.nyc_taxi.fact_trips")
    )

    # Bắt đầu tất cả các luồng ghi
    query_dim_datetime.start()
    query_dim_passenger.start()
    query_dim_ratecode.start()
    query_dim_payment.start()
    query_dim_distance.start()
    query_fact_trips.start()
    
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()