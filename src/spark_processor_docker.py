from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, stddev
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType
import time

# Configuration - use container hostnames inside Docker
KAFKA_BOOTSTRAP_SERVERS = 'kafka:9092'
KAFKA_TOPIC = 'crypto-realtime'
CLICKHOUSE_HOST = 'clickhouse'
CLICKHOUSE_PORT = 8123

# Define Schema corresponding to producer payload
schema = StructType([
    StructField("timestamp", LongType()),
    StructField("symbol", StringType()),
    StructField("price", FloatType()),
    StructField("volume", FloatType())
])

def get_spark_session():
    return SparkSession.builder \
        .appName("CryptoAnomalyDetection") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

def write_to_clickhouse(batch_df, batch_id):
    """
    Function to write micro-batch to ClickHouse using HTTP interface
    """
    if batch_df.isEmpty():
        return
        
    print(f"Processing Batch ID: {batch_id} with {batch_df.count()} records")
    
    rows = batch_df.collect()
    
    data_to_insert = []
    for row in rows:
        current_vol = row['volume']
        mean_vol = row['avg_volume']
        std_vol = row['stddev_volume']
        
        threshold = mean_vol + (3 * std_vol) if std_vol else mean_vol * 1.5
        is_anomaly = 1 if current_vol > threshold else 0
        
        event_time = row['window']['end']
        
        data_to_insert.append({
            'event_time': event_time,
            'symbol': row['symbol'],
            'price': row['avg_price'],
            'volume': current_vol,
            'is_anomaly': is_anomaly
        })
    
    if data_to_insert:
        import urllib.request
        import json
        
        for record in data_to_insert:
            event_time_str = record['event_time'].strftime('%Y-%m-%d %H:%M:%S')
            query = f"INSERT INTO crypto.market_data VALUES ('{event_time_str}', '{record['symbol']}', {record['price']}, {record['volume']}, {record['is_anomaly']}, 0)"
            
            url = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/"
            req = urllib.request.Request(url, data=query.encode('utf-8'))
            try:
                urllib.request.urlopen(req)
            except Exception as e:
                print(f"Error inserting to ClickHouse: {e}")
        
        print(f"Inserted {len(data_to_insert)} records to ClickHouse")

def main():
    print("Starting Spark Crypto Processor...")
    print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}, Topic: {KAFKA_TOPIC}")
    print(f"ClickHouse: {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}")
    
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # 1. Read from Kafka
    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # 2. Parse JSON
    df_parsed = df_raw.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    # 3. Add Timestamp for Windowing
    df_with_ts = df_parsed.withColumn("timestamp", (col("timestamp") / 1000).cast("timestamp"))

    # 4. Watermark to handle late data
    df_watermarked = df_with_ts.withWatermark("timestamp", "1 minute")

    # 5. Aggregation with Sliding Window
    df_agg = df_watermarked.groupBy(
        window(col("timestamp"), "1 minute", "30 seconds"),
        col("symbol")
    ).agg(
        avg("price").alias("avg_price"),
        avg("volume").alias("avg_volume"),
        stddev("volume").alias("stddev_volume"),
        col("avg_volume").alias("volume")
    )

    # 6. Write to ClickHouse
    query = df_agg.writeStream \
        .outputMode("update") \
        .foreachBatch(write_to_clickhouse) \
        .option("checkpointLocation", "/tmp/spark-checkpoint") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
