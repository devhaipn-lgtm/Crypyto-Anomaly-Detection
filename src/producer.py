import json
import time
import websocket
from kafka import KafkaProducer
from datetime import datetime

# Configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:29092'
KAFKA_TOPIC = 'crypto-realtime'

# Multiple trading pairs
TRADING_PAIRS = ['btcusdt', 'ethusdt', 'solusdt', 'dogeusdt', 'bnbusdt', 'xrpusdt']
STREAMS = '/'.join([f'{pair}@trade' for pair in TRADING_PAIRS])
BINANCE_WS_URL = f"wss://stream.binance.com:9443/stream?streams={STREAMS}"

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def on_message(ws, message):
    msg = json.loads(message)
    
    # Combined streams format: {"stream": "btcusdt@trade", "data": {...}}
    data = msg.get('data', msg)
    
    # Extract relevant fields
    # Binance format: e: event type, s: symbol, p: price, q: quantity, T: trade time
    payload = {
        "timestamp": data['T'],  # Milliseconds
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

if __name__ == "__main__":
    # Create WebSocket connection
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