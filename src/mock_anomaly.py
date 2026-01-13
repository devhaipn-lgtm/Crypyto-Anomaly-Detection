"""
Mock Anomaly Generator - Inserts fake anomaly records into ClickHouse to test alerts
"""
import os
import clickhouse_connect
from datetime import datetime

CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'localhost')

def insert_mock_anomaly(symbol='BTCUSDT', price=99999.99, volume=999999.99):
    """Insert a mock anomaly record into ClickHouse"""
    client = clickhouse_connect.get_client(host=CLICKHOUSE_HOST)
    
    event_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    query = f"""
        INSERT INTO crypto.market_data (event_time, symbol, price, volume, is_anomaly, is_alerted)
        VALUES ('{event_time}', '{symbol}', {price}, {volume}, 1, 0)
    """
    
    print(f"Inserting mock anomaly:")
    print(f"  Symbol: {symbol}")
    print(f"  Price: ${price:,.2f}")
    print(f"  Volume: {volume:,.2f}")
    print(f"  Time: {event_time}")
    print(f"  is_anomaly: 1")
    
    client.command(query)
    print("\n✅ Mock anomaly inserted! Alert bot should detect it within CHECK_INTERVAL seconds.")

if __name__ == "__main__":
    import sys
    
    # Default values
    symbol = 'BTCUSDT'
    price = 99999.99
    volume = 999999.99
    
    # Parse command line args
    if len(sys.argv) > 1:
        symbol = sys.argv[1].upper()
    if len(sys.argv) > 2:
        price = float(sys.argv[2])
    if len(sys.argv) > 3:
        volume = float(sys.argv[3])
    
    print("=" * 50)
    print("🧪 MOCK ANOMALY GENERATOR")
    print("=" * 50)
    print(f"Usage: python mock_anomaly.py [SYMBOL] [PRICE] [VOLUME]")
    print("=" * 50)
    
    insert_mock_anomaly(symbol, price, volume)
