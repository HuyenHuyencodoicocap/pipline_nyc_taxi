from pyspark.sql import SparkSession
# KHẮC PHỤC 1: Thêm các hàm cần thiết để tạo hash và xử lý merge
from pyspark.sql.functions import from_json, col, to_date, hour, dayofmonth, month, year, sha2, concat_ws
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
        .config("spark.sql.catalog.hive", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.hive.type", "hive")
        .config("spark.sql.catalog.hive.uri", "thrift://hive-metastore:9083")
        .config(
            "spark.sql.catalog.hive.io-impl", "org.apache.iceberg.aws.s3.S3FileIO"
        )
        .config("spark.sql.catalog.hive.s3.endpoint", "http://minio:9000")
        .config("spark.sql.catalog.hive.s3.access-key", "minioadmin")
        .config("spark.sql.catalog.hive.s3.secret-key", "minioadmin")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.sql.warehouse.dir", "s3a://nyc-taxi-warehouse/")
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.2.0,org.apache.iceberg:iceberg-aws-bundle:1.3.0")
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

    kafka_stream_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "nyc-taxi-topic")
        .load()
    )

    data_stream = kafka_stream_df.selectExpr("CAST(value AS STRING) AS json")
    parsed_stream = data_stream.select(from_json(col("json"), kafka_schema).alias("data")).select("data.*")
    
    enriched_stream = parsed_stream.join(
        payment_type_df, on="payment_type", how="left_outer"
    ).join(
        ratecode_df, on="ratecode_id", how="left_outer"
    )
    
    transformed_stream = enriched_stream.filter(
        (col("passenger_count") >= 0) & (col("trip_distance") >= 0) &
        (col("fare_amount") >= 0) & (col("tip_amount") >= 0) &
        (col("tolls_amount") >= 0) &
        # Đảm bảo các cột timestamp không bị null để tạo hash
        (col("tpep_pickup_datetime").isNotNull()) &
        (col("tpep_dropoff_datetime").isNotNull())
    )

    transformed_stream = transformed_stream.fillna({
        "tip_amount": 0, "tolls_amount": 0, "fare_amount": 0
    })

    # KHẮC PHỤC 2: Tạo cột datetime_hash để khớp với schema của dim_datetime và fact_trips
    # Hash được tạo từ 2 cột timestamp để đảm bảo tính duy nhất
    transformed_stream_with_hash = transformed_stream.withColumn(
        "datetime_hash",
        sha2(concat_ws("||", col("tpep_pickup_datetime"), col("tpep_dropoff_datetime")), 256)
    )
    
    # KHẮC PHỤC 3: Sử dụng forEachBatch để xử lý ghi dữ liệu một cách linh hoạt và tối ưu
    # Điều này cho phép chúng ta thực hiện logic MERGE INTO cho các bảng chiều
    def upsert_to_dimensions_and_append_to_facts(micro_batch_df, batch_id):
        print(f"Bắt đầu xử lý batch {batch_id}...")

        # Cache micro-batch để tái sử dụng
        micro_batch_df.cache()

        # 1. Tải dữ liệu vào dim_datetime (đã có hash)
        dim_datetime_df = micro_batch_df.select(
            "datetime_hash", # <--- Thêm cột hash
            "tpep_pickup_datetime", "tpep_dropoff_datetime",
            hour("tpep_pickup_datetime").alias("pickup_hour"),
            dayofmonth("tpep_pickup_datetime").alias("pickup_day"),
            month("tpep_pickup_datetime").alias("pickup_month"),
            year("tpep_pickup_datetime").alias("pickup_year"),
            hour("tpep_dropoff_datetime").alias("dropoff_hour"),
            dayofmonth("tpep_dropoff_datetime").alias("dropoff_day"),
            month("tpep_dropoff_datetime").alias("dropoff_month"),
            year("tpep_dropoff_datetime").alias("dropoff_year"),
        ).dropDuplicates(["datetime_hash"])

        dim_datetime_df.write.format("iceberg").mode("append").save("nyc_taxi.dim_datetime")
        print(f"Đã ghi vào dim_datetime cho batch {batch_id}")

        # 2. Tải dữ liệu vào dim_passenger_count (sử dụng MERGE)
        dim_passenger_count_df = micro_batch_df.select("passenger_count").distinct()
        dim_passenger_count_df.createOrReplaceTempView("source_passenger_count")
        spark.sql("""
            MERGE INTO nyc_taxi.dim_passenger_count t
            USING source_passenger_count s
            ON t.passenger_count = s.passenger_count
            WHEN NOT MATCHED THEN INSERT *
        """)
        print(f"Đã merge vào dim_passenger_count cho batch {batch_id}")


        # 3. Tải dữ liệu vào dim_ratecode (sử dụng MERGE)
        dim_ratecode_df = micro_batch_df.select("ratecode_id", "ratecode_description").distinct()
        dim_ratecode_df.createOrReplaceTempView("source_ratecode")
        spark.sql("""
            MERGE INTO nyc_taxi.dim_ratecode t
            USING source_ratecode s
            ON t.ratecode_id = s.ratecode_id
            WHEN NOT MATCHED THEN INSERT *
        """)
        print(f"Đã merge vào dim_ratecode cho batch {batch_id}")


        # 4. Tải dữ liệu vào dim_payment_type (sử dụng MERGE)
        dim_payment_type_df = micro_batch_df.select("payment_type", "payment_type_description").distinct()
        dim_payment_type_df.createOrReplaceTempView("source_payment_type")
        spark.sql("""
            MERGE INTO nyc_taxi.dim_payment_type t
            USING source_payment_type s
            ON t.payment_type = s.payment_type
            WHEN NOT MATCHED THEN INSERT *
        """)
        print(f"Đã merge vào dim_payment_type cho batch {batch_id}")


        # 5. Tải dữ liệu vào dim_trip_distance (sử dụng MERGE)
        dim_trip_distance_df = micro_batch_df.select("trip_distance").distinct()
        dim_trip_distance_df.createOrReplaceTempView("source_trip_distance")
        spark.sql("""
            MERGE INTO nyc_taxi.dim_trip_distance t
            USING source_trip_distance s
            ON t.trip_distance = s.trip_distance
            WHEN NOT MATCHED THEN INSERT *
        """)
        print(f"Đã merge vào dim_trip_distance cho batch {batch_id}")

        # 6. Tải dữ liệu vào fact_trips (luôn là append)
        # KHẮC PHỤC 4: Chọn đúng các cột cho bảng fact, sử dụng datetime_hash
        fact_df = micro_batch_df.select(
            "vendor_id",
            "datetime_hash", # <--- Dùng hash thay vì timestamp
            "passenger_count",
            "trip_distance",
            "ratecode_id",
            "store_and_fwd_flag",
            "PULocationID",
            "DOLocationID",
            "payment_type",
            "fare_amount",
            "extra",
            "mta_tax",
            "tip_amount",
            "tolls_amount",
            "improvement_surcharge",
            "total_amount",
            "congestion_surcharge",
            "airport_fee",
            "lpep_pickup_datetime",
            "lpep_dropoff_datetime"
        )
        fact_df.write.format("iceberg").mode("append").save("nyc_taxi.fact_trips")
        print(f"Đã ghi vào fact_trips cho batch {batch_id}")
        
        micro_batch_df.unpersist()


    query = (
        transformed_stream_with_hash.writeStream
        .foreachBatch(upsert_to_dimensions_and_append_to_facts)
        .outputMode("update")
        .option("checkpointLocation", "s3a://nyc-taxi-warehouse/checkpoints/main_etl")
        .start()
    )

    query.awaitTermination()

if __name__ == "__main__":
    main()