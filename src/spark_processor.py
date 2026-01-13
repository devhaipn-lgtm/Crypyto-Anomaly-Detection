from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, stddev, unix_timestamp
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType
import clickhouse_connect
import time
import os
# Force Spark to download the Kafka dependency
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 pyspark-shell'
os.environ['HADOOP_HOME'] = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'hadoop')

# Configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:29092' # Use localhost if running outside docker, or kafka:9092 if inside
KAFKA_TOPIC = 'crypto-realtime'
CLICKHOUSE_HOST = 'localhost'

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
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
        .config("spark.hadoop.io.nativeio.enabled", "false") \
        .getOrCreate()

def write_to_clickhouse(batch_df, batch_id):
    """
    Function to write micro-batch to ClickHouse using native driver
    """
    if batch_df.isEmpty():
        return
        
    print(f"Processing Batch ID: {batch_id} with {batch_df.count()} records")
    
    # Collect data to driver (acceptable for aggregated results)
    rows = batch_df.collect()
    
    # Prepare data for ClickHouse
    data_to_insert = []
    for row in rows:
        # Calculate Anomaly: Rule based
        # Here we assume the input dataframe already has avg and stddev calculated
        current_vol = row['volume'] # This is sum(volume) in the window
        mean_vol = row['avg_volume']
        std_vol = row['stddev_volume']
        
        # Simple Logic: If stddev is 0 (first record), assume no anomaly.
        # Otherwise, check if volume > mean + 3 * stddev
        threshold = mean_vol + (3 * std_vol) if std_vol else mean_vol * 1.5
        
        is_anomaly = 1 if current_vol > threshold else 0
        
        # Format for ClickHouse DateTime (seconds)
        event_time = row['window']['end']
        
        data_to_insert.append({
            'event_time': event_time,
            'symbol': row['symbol'],
            'price': row['avg_price'],
            'volume': current_vol,
            'is_anomaly': is_anomaly
        })
    
    if data_to_insert:
        client = clickhouse_connect.get_client(host=CLICKHOUSE_HOST)
        client.insert(
            'crypto.market_data',
            [[d['event_time'], d['symbol'], d['price'], d['volume'], d['is_anomaly']] for d in data_to_insert],
            column_names=['event_time', 'symbol', 'price', 'volume', 'is_anomaly']
        )
        print(f"Inserted {len(data_to_insert)} records to ClickHouse")

def main():
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

    # 3. Add Timestamp for Windowing (Convert ms to TimestampType)
    df_with_ts = df_parsed.withColumn("timestamp", (col("timestamp") / 1000).cast("timestamp"))

    # 4. Watermark to handle late data (allow 1 minute delay)
    df_watermarked = df_with_ts.withWatermark("timestamp", "1 minute")

    # 5. Aggregation (Windowing)
    # Calculate Sum Volume, Avg Price, Avg Volume over Sliding Window
    # Window: 1 minute, Slide: 30 seconds
    df_agg = df_watermarked.groupBy(
        window(col("timestamp"), "1 minute", "30 seconds"),
        col("symbol")
    ).agg(
        avg("price").alias("avg_price"),
        avg("volume").alias("avg_volume"), # Mean of volume in this window
        stddev("volume").alias("stddev_volume"), # Stddev of volume
        # We use sum(volume) as the metric to compare against historical average, 
        # but for this simple demo, we compare "current avg volume" vs "historical distribution".
        # A better approach requires joining with a static historical stats table, 
        # but to keep it streaming-only, we check deviation within the window itself or accumulating state.
        # SIMPLIFICATION: We output the window metrics and decide anomaly in the writer based on statistical outlier in that window context.
        col("avg_volume").alias("volume") 
    )

    # 6. Write to ClickHouse with checkpoint
    checkpoint_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'checkpoint')
    os.makedirs(checkpoint_dir, exist_ok=True)
    
    query = df_agg.writeStream \
        .outputMode("update") \
        .foreachBatch(write_to_clickhouse) \
        .option("checkpointLocation", checkpoint_dir) \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()