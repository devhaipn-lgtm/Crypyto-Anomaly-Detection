"""
Simple Kafka to ClickHouse processor without Spark.
Processes crypto trades and writes aggregated data to ClickHouse.
"""
import json
import time
from datetime import datetime, timedelta
from collections import defaultdict
from kafka import KafkaConsumer
import clickhouse_connect
import threading

# Configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:29092'
KAFKA_TOPIC = 'crypto-realtime'
CLICKHOUSE_HOST = 'localhost'

# Window settings
WINDOW_SIZE_SECONDS = 60
SLIDE_INTERVAL_SECONDS = 30

class WindowAggregator:
    def __init__(self):
        self.windows = defaultdict(lambda: {'prices': [], 'volumes': []})
        self.lock = threading.Lock()
    
    def add_trade(self, symbol, price, volume, timestamp_ms):
        """Add a trade to the current window"""
        timestamp = datetime.fromtimestamp(timestamp_ms / 1000)
        window_key = self._get_window_key(timestamp)
        
        with self.lock:
            key = (symbol, window_key)
            self.windows[key]['prices'].append(price)
            self.windows[key]['volumes'].append(volume)
    
    def _get_window_key(self, timestamp):
        """Get window key based on timestamp"""
        seconds = int(timestamp.timestamp())
        window_start = seconds - (seconds % SLIDE_INTERVAL_SECONDS)
        return window_start
    
    def get_completed_windows(self):
        """Get windows that are ready to be flushed"""
        current_time = int(time.time())
        cutoff = current_time - WINDOW_SIZE_SECONDS - SLIDE_INTERVAL_SECONDS
        
        completed = []
        with self.lock:
            keys_to_remove = []
            for key, data in self.windows.items():
                symbol, window_start = key
                if window_start < cutoff and len(data['prices']) > 0:
                    prices = data['prices']
                    volumes = data['volumes']
                    
                    avg_price = sum(prices) / len(prices)
                    avg_volume = sum(volumes) / len(volumes)
                    
                    # Calculate stddev
                    if len(volumes) > 1:
                        mean = avg_volume
                        variance = sum((v - mean) ** 2 for v in volumes) / len(volumes)
                        stddev = variance ** 0.5
                    else:
                        stddev = 0
                    
                    # Anomaly detection: volume > mean + 3*stddev
                    threshold = avg_volume + (3 * stddev) if stddev > 0 else avg_volume * 1.5
                    is_anomaly = 1 if avg_volume > threshold else 0
                    
                    window_end = datetime.fromtimestamp(window_start + WINDOW_SIZE_SECONDS)
                    
                    completed.append({
                        'event_time': window_end,
                        'symbol': symbol,
                        'price': avg_price,
                        'volume': avg_volume,
                        'is_anomaly': is_anomaly
                    })
                    keys_to_remove.append(key)
            
            for key in keys_to_remove:
                del self.windows[key]
        
        return completed

def write_to_clickhouse(records):
    """Write records to ClickHouse"""
    if not records:
        return
    
    try:
        client = clickhouse_connect.get_client(host=CLICKHOUSE_HOST)
        data = [[r['event_time'], r['symbol'], r['price'], r['volume'], r['is_anomaly']] for r in records]
        client.insert(
            'crypto.market_data',
            data,
            column_names=['event_time', 'symbol', 'price', 'volume', 'is_anomaly']
        )
        print(f"Inserted {len(records)} records to ClickHouse")
    except Exception as e:
        print(f"Error writing to ClickHouse: {e}")

def flush_worker(aggregator):
    """Background worker to flush completed windows"""
    while True:
        time.sleep(SLIDE_INTERVAL_SECONDS)
        completed = aggregator.get_completed_windows()
        if completed:
            write_to_clickhouse(completed)

def main():
    print("Starting Simple Crypto Processor...")
    print(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
    
    # Initialize aggregator
    aggregator = WindowAggregator()
    
    # Start flush worker
    flush_thread = threading.Thread(target=flush_worker, args=(aggregator,), daemon=True)
    flush_thread.start()
    
    # Initialize Kafka consumer
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest',
        enable_auto_commit=True
    )
    
    print(f"Connected! Listening to topic: {KAFKA_TOPIC}")
    
    try:
        for message in consumer:
            data = message.value
            aggregator.add_trade(
                symbol=data['symbol'],
                price=data['price'],
                volume=data['volume'],
                timestamp_ms=data['timestamp']
            )
            print(f"Received: {data['symbol']} @ {data['price']:.2f}")
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
