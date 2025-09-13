-- Tạo database nyc_taxi nếu nó chưa tồn tại.
CREATE DATABASE IF NOT EXISTS nyc_taxi;

USE nyc_taxi;

-- Bảng chiều datetime. Khóa chính là một giá trị hash tạo từ 2 cột timestamp.
CREATE TABLE IF NOT EXISTS dim_datetime
(
    datetime_hash STRING, -- Khóa chính xác định
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    pickup_hour INT,
    pickup_day INT,
    pickup_month INT,
    pickup_year INT,
    dropoff_hour INT,
    dropoff_day INT,
    dropoff_month INT,
    dropoff_year INT
)
USING iceberg;

-- Bảng chiều passenger_count. Khóa chính là chính cột passenger_count.
CREATE TABLE IF NOT EXISTS dim_passenger_count
(
    passenger_count INT
)
USING iceberg;

-- Bảng chiều payment_type. Khóa chính là cột payment_type.
CREATE TABLE IF NOT EXISTS dim_payment_type
(
    payment_type INT,
    payment_type_description STRING
)
USING iceberg;

-- Bảng chiều ratecode. Khóa chính là cột ratecode_id.
CREATE TABLE IF NOT EXISTS dim_ratecode
(
    ratecode_id INT,
    ratecode_description STRING
)
USING iceberg;

-- Bảng chiều trip_distance. Khóa chính là cột trip_distance.
-- LƯU Ý: Dùng DOUBLE làm khóa có thể không tối ưu, nhưng vẫn khả thi.
CREATE TABLE IF NOT EXISTS dim_trip_distance
(
    trip_distance DOUBLE
)
USING iceberg;

-- Bảng sự kiện. Các cột khóa ngoại giờ đây chính là các khóa tự nhiên từ bảng dimension.
CREATE TABLE IF NOT EXISTS fact_trips
(
    vendor_id INT,
    datetime_hash STRING, -- Khóa ngoại trỏ đến dim_datetime
    passenger_count INT,  -- Khóa ngoại trỏ đến dim_passenger_count
    ratecode_id INT,      -- Khóa ngoại trỏ đến dim_ratecode
    payment_type INT,     -- Khóa ngoại trỏ đến dim_payment_type
    trip_distance DOUBLE, -- Khóa ngoại trỏ đến dim_trip_distance
    PULocationID INT,
    DOLocationID INT,
    store_and_fwd_flag STRING,
    fare_amount DOUBLE,
    extra DOUBLE,
    mta_tax DOUBLE,
    tip_amount DOUBLE,
    tolls_amount DOUBLE,
    improvement_surcharge DOUBLE,
    total_amount DOUBLE,
    congestion_surcharge DOUBLE,
    airport_fee DOUBLE,
    lpep_pickup_datetime TIMESTAMP,
    lpep_dropoff_datetime TIMESTAMP
)
USING iceberg;