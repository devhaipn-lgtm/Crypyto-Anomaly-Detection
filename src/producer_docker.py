import json
import time
import websocket
from kafka import KafkaProducer
from datetime import datetime

# Configuration - use container hostname inside Docker
KAFKA_BOOTSTRAP_SERVERS = 'kafka:9092'
KAFKA_TOPIC = 'crypto-realtime'

# Multiple trading pairs
TRADING_PAIRS = ['btcusdt', 'ethusdt', 'solusdt', 'dogeusdt', 'bnbusdt', 'xrpusdt']
STREAMS = '/'.join([f'{pair}@trade' for pair in TRADING_PAIRS])
BINANCE_WS_URL = f"wss://stream.binance.com:9443/stream?streams={STREAMS}"

# Initialize Kafka Producer with retry
def create_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print(f"Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
            return producer
        except Exception as e:
            print(f"Waiting for Kafka... {e}")
            time.sleep(5)

producer = create_producer()

def on_message(ws, message):
    msg = json.loads(message)
    
    # Combined streams format: {"stream": "btcusdt@trade", "data": {...}}
    data = msg.get('data', msg)
    
    # Extract relevant fields
    payload = {
        "timestamp": data['T'],
        "symbol": data['s'],
        "price": float(data['p']),
        "volume": float(data['q'])
    }
    
    # Send to Kafka
    producer.send(KAFKA_TOPIC, payload)
    print(f"Sent: {payload['symbol']} @ {payload['price']:.2f}")

def on_error(ws, error):
    print(f"Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("### Connection Closed ###")

def on_open(ws):
    print("### Connected to Binance WebSocket ###")
    print(f"Streaming pairs: {', '.join([p.upper() for p in TRADING_PAIRS])}")

if __name__ == "__main__":
    print("Starting Crypto Producer...")
    
    ws = websocket.WebSocketApp(
        BINANCE_WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    
    while True:
        try:
            ws.run_forever()
        except Exception as e:
            print(f"Exception: {e}. Reconnecting in 5 seconds...")
            time.sleep(5)
