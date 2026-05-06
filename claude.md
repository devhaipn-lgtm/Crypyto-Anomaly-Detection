# Crypto Anomaly Detection Pipeline

## Project Overview

Real-time Big Data pipeline for cryptocurrency market anomaly detection. Ingests live trade data from Binance, processes it with Spark Structured Streaming, detects anomalies via the 3-Sigma rule, stores results in ClickHouse, visualizes in Grafana, and sends email alerts.

**Course:** Big Data Storage and Processing (HUST - SoICT)  
**Supervisor:** Dr. Ta Duy Hoang

## Architecture (Kappa Architecture)

```
Binance WebSocket API → Python Producer → Apache Kafka → Spark Structured Streaming → ClickHouse → Grafana
                                                                                          ↓
                                                                                     Alert Bot (Email)
```

**Layers:**
- **Data Source:** Binance WebSocket API (Market Streams, push-based)
- **Ingestion:** Python Producer + Apache Kafka (topic: `crypto-realtime`)
- **Processing:** Spark Structured Streaming (1-min window, 30s slide, 10s watermark)
- **Storage:** ClickHouse (hot path, OLAP) — MinIO mentioned in report as cold path but not implemented in code
- **Serving:** Grafana dashboards + Python SMTP email alert bot

## Tech Stack

| Component          | Technology                          | Version   |
|--------------------|-------------------------------------|-----------|
| Message Broker     | Apache Kafka (Confluent)            | 7.5.0     |
| Stream Processing  | Apache Spark (Structured Streaming) | 3.5.1     |
| Database           | ClickHouse                          | latest    |
| Visualization      | Grafana                             | latest    |
| Alerting           | Python SMTP                         | -         |
| Containerization   | Docker / Docker Compose             | 20.10+    |
| Language           | Python                              | 3.9+      |

## Key Source Files

### `src/producer.py` / `src/producer_docker.py`
- Connects to Binance WebSocket for 6 trading pairs: BTC, ETH, SOL, DOGE, BNB, XRP
- Extracts `{timestamp, symbol, price, volume}` from trade events
- Sends JSON to Kafka topic `crypto-realtime`
- Docker version uses `kafka:9092` with retry logic; local version uses `localhost:29092`

### `src/spark_processor.py` / `src/spark_processor_docker.py`
- Reads from Kafka, parses JSON, converts ms timestamp to Spark timestamp
- Applies watermark (1 min in local, 1 min in docker)
- Sliding window aggregation: 1-min window, 30s slide
- Computes `avg_price`, `avg_volume`, `stddev_volume`
- Anomaly rule: `volume > avg_volume + 3 * stddev_volume` (falls back to `avg_volume * 1.5` if stddev is 0)
- Local version writes via `clickhouse_connect`; Docker version writes via HTTP (`urllib.request`)

### `src/simple_processor.py`
- Lightweight alternative to Spark — pure Python with `KafkaConsumer`
- Custom `WindowAggregator` class with threading for window-based aggregation
- Same anomaly detection logic (3-sigma)

### `src/alert_bot.py`
- Polls ClickHouse every `CHECK_INTERVAL` seconds (default 5)
- Queries for `is_anomaly = 1 AND is_alerted = 0`
- Sends email via SMTP (configurable: Gmail, MailerSend, SMTP2GO, SendGrid)
- Marks sent alerts with `is_alerted = 1` via `ALTER TABLE UPDATE`
- Stops after `MAX_ALERTS` emails (default 1)
- Supports `--test` flag for testing email config

### `src/mock_anomaly.py`
- Inserts fake anomaly records into ClickHouse for testing
- Usage: `python mock_anomaly.py [SYMBOL] [PRICE] [VOLUME]`

### `src/clickhouse_init.sql`
- Creates `crypto` database and `market_data` table
- Engine: `ReplacingMergeTree()`, partitioned by month, ordered by `(symbol, event_time)`
- Columns: `event_time`, `symbol`, `price`, `volume`, `is_anomaly`, `is_alerted`

## Infrastructure

### Docker Compose Services (7 total)
1. **Zookeeper** — Kafka coordination (port 2181)
2. **Kafka** — Message broker (port 29092 external, 9092 internal)
3. **ClickHouse** — Analytics DB (ports 8123 HTTP, 9000 native)
4. **Grafana** — Dashboards (port 3000, admin/admin)
5. **Spark Processor** — Runs `spark_processor_docker.py` via `spark-submit`
6. **Producer** — Custom image from `Dockerfile.producer`
7. **Alert Bot** — Custom image from `Dockerfile.alertbot`

### Dockerfiles
- `Dockerfile.producer`: Python 3.11-slim, installs `kafka-python-ng` + `websocket-client`
- `Dockerfile.alertbot`: Python 3.11-slim, installs `clickhouse-connect`

## Anomaly Detection Algorithm

**3-Sigma Rule (Z-Score):**
```
threshold = mean_volume + 3 * stddev_volume
is_anomaly = 1 if current_volume > threshold else 0
```
- If `stddev == 0` (insufficient data), fallback: `threshold = mean_volume * 1.5`
- Applied per symbol within each sliding window

## Database Schema

```sql
CREATE TABLE crypto.market_data (
    event_time DateTime,
    symbol String,
    price Float64,
    volume Float64,
    is_anomaly UInt8,
    is_alerted UInt8 DEFAULT 0
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(event_time)
ORDER BY (symbol, event_time);
```

## Quick Start

```bash
docker-compose up -d
docker exec -i crypto-bigdata-project-clickhouse-1 clickhouse-client < src/clickhouse_init.sql
# Access Grafana at http://localhost:3000 (admin/admin)
```

## Known Limitations
- Cold start: statistical models inaccurate until enough data fills the window
- Spark micro-batch latency (~100ms–1s), not suitable for HFT
- Single Kafka partition (demo scope)
- ClickHouse `ALTER TABLE UPDATE` is expensive (used for `is_alerted` flag)
- MinIO cold path mentioned in report but not implemented in code
