"""
Spark Processor with Sentiment Integration (Docker version).
Reads from both crypto-realtime and crypto-sentiment Kafka topics.
Correlates trade anomalies with sentiment data for enhanced detection.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType, LongType, IntegerType, DoubleType
)
import time

# Configuration - use container hostnames inside Docker
KAFKA_BOOTSTRAP_SERVERS = 'kafka:9092'
TRADE_TOPIC = 'crypto-realtime'
SENTIMENT_TOPIC = 'crypto-sentiment'
CLICKHOUSE_HOST = 'clickhouse'
CLICKHOUSE_PORT = 8123

# Trade data schema
trade_schema = StructType([
    StructField("timestamp", LongType()),
    StructField("symbol", StringType()),
    StructField("price", FloatType()),
    StructField("volume", FloatType())
])

# Sentiment data schema
sentiment_schema = StructType([
    StructField("timestamp", LongType()),
    StructField("symbol", StringType()),
    StructField("source", StringType()),
    StructField("post_id", StringType()),
    StructField("title", StringType()),
    StructField("kind", StringType()),
    StructField("sentiment_score", DoubleType()),
    StructField("sentiment_label", StringType()),
    StructField("votes_positive", IntegerType()),
    StructField("votes_negative", IntegerType()),
    StructField("votes_important", IntegerType()),
    StructField("url", StringType()),
    StructField("published_at", StringType()),
])


def get_spark_session():
    return SparkSession.builder \
        .appName("CryptoAnomalyDetectionWithSentiment") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()


# --- Combined Anomaly Score Weights ---
VOLUME_WEIGHT = 0.7
SENTIMENT_WEIGHT = 0.3
COMBINED_THRESHOLD = 1.0  # Combined score above this = anomaly
SENTIMENT_LOOKBACK_MINUTES = 60  # How far back to look for sentiment data
VOLUME_LOOKBACK_MINUTES = 30  # How far back to compute volume baseline


def fetch_volume_baseline(symbol):
    """
    Query ClickHouse for the historical mean and stddev of avg volume
    for a symbol over the last VOLUME_LOOKBACK_MINUTES minutes.
    Returns (hist_mean, hist_stddev). Falls back to (0, 0) if no data.
    """
    import urllib.request
    import json

    query = (
        f"SELECT avg(volume) AS hist_mean, stddevPop(volume) AS hist_stddev, count() AS cnt "
        f"FROM crypto.market_data "
        f"WHERE symbol = '{symbol}' "
        f"AND event_time >= now() - INTERVAL {VOLUME_LOOKBACK_MINUTES} MINUTE "
        f"FORMAT JSON"
    )

    url = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/"
    req = urllib.request.Request(url, data=query.encode('utf-8'))

    try:
        response = urllib.request.urlopen(req, timeout=5)
        result = json.loads(response.read().decode('utf-8'))
        rows = result.get('data', [])
        if rows and int(rows[0].get('cnt', 0)) >= 3:  # Need at least 3 data points
            hist_mean = float(rows[0]['hist_mean'])
            hist_stddev = float(rows[0]['hist_stddev'])
            return (hist_mean, hist_stddev)
    except Exception as e:
        print(f"Warning: Could not fetch volume baseline for {symbol}: {e}")

    return (0.0, 0.0)


def fetch_recent_sentiment(symbol):
    """
    Query ClickHouse for the average sentiment score of a symbol
    over the last SENTIMENT_LOOKBACK_MINUTES minutes.
    Returns a value in [-1.0, 1.0] (0.0 if no data).
    """
    import urllib.request
    import json

    query = (
        f"SELECT avg(sentiment_score) AS avg_score, count() AS cnt "
        f"FROM crypto.sentiment_data "
        f"WHERE symbol = '{symbol}' "
        f"AND event_time >= now() - INTERVAL {SENTIMENT_LOOKBACK_MINUTES} MINUTE "
        f"FORMAT JSON"
    )

    url = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/"
    req = urllib.request.Request(url, data=query.encode('utf-8'))

    try:
        response = urllib.request.urlopen(req, timeout=5)
        result = json.loads(response.read().decode('utf-8'))
        rows = result.get('data', [])
        if rows and int(rows[0].get('cnt', 0)) > 0:
            return float(rows[0]['avg_score'])
    except Exception as e:
        print(f"Warning: Could not fetch sentiment for {symbol}: {e}")

    return 0.0  # Neutral if no sentiment data available


def compute_volume_score(current_vol, hist_mean, hist_stddev):
    """
    Compute a normalized volume anomaly score (Z-score).
    Compares the current window's average volume against the
    historical baseline (mean and stddev from previous windows).
    Returns 0.0 for normal, >1.0 for elevated, >3.0 for extreme.
    """
    if hist_stddev and hist_stddev > 0:
        z_score = (current_vol - hist_mean) / hist_stddev
        return max(0.0, z_score)  # Only care about positive deviations
    else:
        # Not enough historical data — use simple ratio
        if hist_mean > 0:
            ratio = current_vol / hist_mean
            return max(0.0, ratio - 1.0)
        return 0.0


def compute_sentiment_anomaly_score(sentiment_avg):
    """
    Convert sentiment [-1, 1] to an anomaly contribution score [0, ~3.3].
    Strongly bearish sentiment → high anomaly score.
    Bullish sentiment → 0 (reduces anomaly likelihood).
    
    Mapping:
      -1.0 (very bearish)  → 3.33 (max contribution)
      -0.3 (mildly bearish) → 1.0
       0.0 (neutral)        → 0.0
      +1.0 (very bullish)   → 0.0 (no anomaly contribution)
    """
    if sentiment_avg >= 0:
        return 0.0
    # Linear scale: -1 → 3.33, 0 → 0
    return abs(sentiment_avg) * 3.33


def write_trades_to_clickhouse(batch_df, batch_id):
    """
    Write trade micro-batch to ClickHouse with combined anomaly scoring.
    
    Combined Score = volume_score * 0.7 + sentiment_anomaly_score * 0.3
    is_anomaly = 1 if combined_score > COMBINED_THRESHOLD
    """
    if batch_df.isEmpty():
        return

    print(f"[Trade] Processing Batch ID: {batch_id} with {batch_df.count()} records")

    rows = batch_df.collect()

    # Collect unique symbols in this batch and fetch their sentiment + volume baseline
    symbols = set(row['symbol'] for row in rows)
    sentiment_cache = {}
    volume_baseline_cache = {}
    for symbol in symbols:
        sentiment_cache[symbol] = fetch_recent_sentiment(symbol)
        volume_baseline_cache[symbol] = fetch_volume_baseline(symbol)
        if sentiment_cache[symbol] != 0.0:
            print(f"  Sentiment for {symbol}: {sentiment_cache[symbol]:+.4f}")
        hist_mean, hist_std = volume_baseline_cache[symbol]
        if hist_mean > 0:
            print(f"  Volume baseline for {symbol}: mean={hist_mean:.6f}, std={hist_std:.6f}")

    data_to_insert = []
    for row in rows:
        current_vol = row['avg_volume']  # Use avg volume of current window
        symbol = row['symbol']
        event_time = row['window']['end']

        # 1. Volume Z-score (compare against historical baseline)
        hist_mean, hist_stddev = volume_baseline_cache.get(symbol, (0.0, 0.0))
        vol_score = compute_volume_score(current_vol, hist_mean, hist_stddev)

        # 2. Sentiment anomaly score
        sent_avg = sentiment_cache.get(symbol, 0.0)
        sent_score = compute_sentiment_anomaly_score(sent_avg)

        # 3. Combined score
        combined = (vol_score * VOLUME_WEIGHT) + (sent_score * SENTIMENT_WEIGHT)

        # 4. Anomaly decision
        is_anomaly = 1 if combined > COMBINED_THRESHOLD else 0

        if is_anomaly:
            print(f"  ANOMALY {symbol}: vol_score={vol_score:.2f}, "
                  f"sent_score={sent_score:.2f}, combined={combined:.2f}")

        data_to_insert.append({
            'event_time': event_time,
            'symbol': symbol,
            'price': row['avg_price'],
            'volume': row['avg_volume'],
            'is_anomaly': is_anomaly,
            'volume_score': round(vol_score, 4),
            'sentiment_avg': round(sent_avg, 4),
            'combined_score': round(combined, 4),
        })

    if data_to_insert:
        import urllib.request

        for record in data_to_insert:
            event_time_str = record['event_time'].strftime('%Y-%m-%d %H:%M:%S')
            query = (
                f"INSERT INTO crypto.market_data "
                f"(event_time, symbol, price, volume, is_anomaly, is_alerted, "
                f"volume_score, sentiment_avg, combined_score) VALUES "
                f"('{event_time_str}', '{record['symbol']}', {record['price']}, "
                f"{record['volume']}, {record['is_anomaly']}, 0, "
                f"{record['volume_score']}, {record['sentiment_avg']}, "
                f"{record['combined_score']})"
            )

            url = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/"
            req = urllib.request.Request(url, data=query.encode('utf-8'))
            try:
                urllib.request.urlopen(req)
            except Exception as e:
                print(f"Error inserting trade to ClickHouse: {e}")

        anomaly_count = sum(1 for r in data_to_insert if r['is_anomaly'])
        print(f"[Trade] Inserted {len(data_to_insert)} records "
              f"({anomaly_count} anomalies) to ClickHouse")


def write_raw_trades_to_clickhouse(batch_df, batch_id):
    """Write raw trade ticks to crypto.raw_trades for price history."""
    if batch_df.isEmpty():
        return

    rows = batch_df.collect()
    if not rows:
        return

    import urllib.request

    inserted = 0
    for row in rows:
        event_time_ms = row['timestamp']
        # Convert to DateTime64(3) string format
        from datetime import datetime as dt
        ts = dt.utcfromtimestamp(event_time_ms.timestamp())
        event_time_str = ts.strftime('%Y-%m-%d %H:%M:%S.') + f'{ts.microsecond // 1000:03d}'

        query = (
            f"INSERT INTO crypto.raw_trades VALUES ("
            f"'{event_time_str}', '{row['symbol']}', "
            f"{row['price']}, {row['volume']})"
        )

        url = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/"
        req = urllib.request.Request(url, data=query.encode('utf-8'))
        try:
            urllib.request.urlopen(req)
            inserted += 1
        except Exception as e:
            print(f"Error inserting raw trade to ClickHouse: {e}")

    if inserted > 0:
        print(f"[RawTrades] Inserted {inserted} ticks to ClickHouse")


def write_sentiment_to_clickhouse(batch_df, batch_id):
    """Write sentiment micro-batch to ClickHouse."""
    if batch_df.isEmpty():
        return

    print(f"[Sentiment] Processing Batch ID: {batch_id} with {batch_df.count()} records")

    rows = batch_df.collect()

    if rows:
        import urllib.request

        for row in rows:
            event_time_str = row['event_time'].strftime('%Y-%m-%d %H:%M:%S')
            # Escape single quotes in title
            title_escaped = str(row['title']).replace("'", "\\'")

            query = (
                f"INSERT INTO crypto.sentiment_data VALUES ("
                f"'{event_time_str}', "
                f"'{row['symbol']}', "
                f"'{row['source']}', "
                f"'{row['post_id']}', "
                f"'{title_escaped}', "
                f"'{row['kind']}', "
                f"{row['sentiment_score']}, "
                f"'{row['sentiment_label']}', "
                f"{row['votes_positive']}, "
                f"{row['votes_negative']}, "
                f"{row['votes_important']}, "
                f"'{row['url']}', "
                f"'{row['published_at']}')"
            )

            url = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/"
            req = urllib.request.Request(url, data=query.encode('utf-8'))
            try:
                urllib.request.urlopen(req)
            except Exception as e:
                print(f"Error inserting sentiment to ClickHouse: {e}")

        print(f"[Sentiment] Inserted {len(rows)} records to ClickHouse")


def main():
    print("Starting Spark Crypto Processor with Sentiment...")
    print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Trade Topic: {TRADE_TOPIC}, Sentiment Topic: {SENTIMENT_TOPIC}")
    print(f"ClickHouse: {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}")

    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # ==========================================
    # Stream 1: Trade data (same as original)
    # ==========================================
    df_trade_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", TRADE_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    df_trade_parsed = df_trade_raw.select(
        from_json(col("value").cast("string"), trade_schema).alias("data")
    ).select("data.*")

    df_trade_ts = df_trade_parsed.withColumn(
        "timestamp", (col("timestamp") / 1000).cast("timestamp")
    )

    df_trade_watermarked = df_trade_ts.withWatermark("timestamp", "1 minute")

    df_trade_agg = df_trade_watermarked.groupBy(
        window(col("timestamp"), "1 minute", "30 seconds"),
        col("symbol")
    ).agg(
        avg("price").alias("avg_price"),
        avg("volume").alias("avg_volume")
    )

    # ==========================================
    # Stream 2: Sentiment data
    # ==========================================
    df_sentiment_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", SENTIMENT_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    df_sentiment_parsed = df_sentiment_raw.select(
        from_json(col("value").cast("string"), sentiment_schema).alias("data")
    ).select("data.*")

    df_sentiment = df_sentiment_parsed.withColumn(
        "event_time", (col("timestamp") / 1000).cast("timestamp")
    )

    # ==========================================
    # Stream 3: Raw trades (tick-by-tick for price history)
    # ==========================================
    df_raw_trades = df_trade_ts.withWatermark("timestamp", "10 seconds")

    # ==========================================
    # Write all three streams
    # ==========================================
    trade_query = df_trade_agg.writeStream \
        .outputMode("update") \
        .foreachBatch(write_trades_to_clickhouse) \
        .option("checkpointLocation", "/tmp/spark-checkpoint-trades") \
        .queryName("trade_stream") \
        .start()

    raw_trade_query = df_raw_trades.writeStream \
        .outputMode("append") \
        .foreachBatch(write_raw_trades_to_clickhouse) \
        .option("checkpointLocation", "/tmp/spark-checkpoint-raw-trades") \
        .queryName("raw_trade_stream") \
        .start()

    sentiment_query = df_sentiment.writeStream \
        .outputMode("append") \
        .foreachBatch(write_sentiment_to_clickhouse) \
        .option("checkpointLocation", "/tmp/spark-checkpoint-sentiment") \
        .queryName("sentiment_stream") \
        .start()

    print("All three streams started. Waiting for termination...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
