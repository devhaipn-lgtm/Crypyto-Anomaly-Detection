# Crypto Anomaly Detection Pipeline

A real-time Big Data pipeline for cryptocurrency market anomaly detection using Apache Kafka, Spark Structured Streaming, ClickHouse, and Grafana with automated email alerting.

![Pipeline Status](https://img.shields.io/badge/status-active-brightgreen)
![Docker](https://img.shields.io/badge/docker-required-blue)
![Python](https://img.shields.io/badge/python-3.9+-yellow)

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Components](#components)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Grafana Dashboard](#grafana-dashboard)
- [Anomaly Detection](#anomaly-detection)
- [Email Alerts](#email-alerts)
- [Testing](#testing)
- [Troubleshooting](#troubleshooting)
- [Project Structure](#project-structure)
- [Tech Stack](#tech-stack)

---

## Overview

This project implements a **real-time streaming pipeline** that:

1. **Ingests** live cryptocurrency trade data from Binance WebSocket API
2. **Streams** data through Apache Kafka message broker
3. **Processes** data using Spark Structured Streaming with windowed aggregations
4. **Detects** anomalies using statistical methods (3-sigma rule)
5. **Stores** processed data in ClickHouse time-series database
6. **Visualizes** data in real-time Grafana dashboards
7. **Alerts** via email when anomalies are detected

### Key Features

- ✅ Real-time data ingestion from Binance
- ✅ Support for multiple trading pairs (BTC, ETH, SOL, DOGE, BNB, XRP)
- ✅ Windowed aggregations (1-minute windows, 30-second slides)
- ✅ Statistical anomaly detection (volume spikes)
- ✅ Time-series storage with ClickHouse
- ✅ Live Grafana dashboards
- ✅ Automated email alerts
- ✅ Fully containerized with Docker

---

## Architecture

### High-Level Architecture

```
┌─────────────────┐     ┌─────────────┐     ┌──────────────────┐     ┌─────────────┐
│  Binance API    │────▶│   Producer  │────▶│   Apache Kafka   │────▶│    Spark    │
│  (WebSocket)    │     │  (Python)   │     │  (Message Queue) │     │  Processor  │
└─────────────────┘     └─────────────┘     └──────────────────┘     └──────┬──────┘
                                                                            │
                        ┌─────────────┐     ┌──────────────────┐            │
                        │  Alert Bot  │◀────│   ClickHouse     │◀───────────┘
                        │   (Email)   │     │   (Database)     │
                        └─────────────┘     └────────┬─────────┘
                                                     │
                                            ┌────────▼─────────┐
                                            │     Grafana      │
                                            │   (Dashboard)    │
                                            └──────────────────┘
```

### Data Flow

```
1. Binance WebSocket API
   └── Streams: btcusdt@trade, ethusdt@trade, solusdt@trade, ...
   
2. Producer (producer_docker.py)
   └── Extracts: timestamp, symbol, price, volume
   └── Sends to Kafka topic: crypto-realtime
   
3. Kafka (Message Broker)
   └── Topic: crypto-realtime
   └── Partitions: 1, Replication: 1
   
4. Spark Processor (spark_processor_docker.py)
   └── Window: 1 minute, Slide: 30 seconds
   └── Calculates: avg_price, avg_volume, stddev_volume
   └── Anomaly: volume > mean + 3*stddev
   
5. ClickHouse (Analytics Database)
   └── Database: crypto
   └── Table: market_data
   
6. Grafana (Visualization)
   └── Real-time price charts
   └── Anomaly markers
   
7. Alert Bot (alert_bot.py)
   └── Polls ClickHouse for new anomalies
   └── Sends email via SMTP
```

---

## Components

| Component | Image/Technology | Description | Port |
|-----------|------------------|-------------|------|
| **Zookeeper** | `confluentinc/cp-zookeeper:7.5.0` | Kafka coordination service | 2181 |
| **Kafka** | `confluentinc/cp-kafka:7.5.0` | Distributed message broker | 29092 |
| **ClickHouse** | `clickhouse/clickhouse-server:latest` | Column-oriented OLAP database | 8123, 9000 |
| **Grafana** | `grafana/grafana:latest` | Metrics visualization | 3000 |
| **Spark** | `apache/spark:3.5.1-python3` | Stream processing engine | - |
| **Producer** | Custom Python | Binance WebSocket → Kafka | - |
| **Alert Bot** | Custom Python | Anomaly email notifications | - |

### Trading Pairs Monitored

| Symbol | Pair | Description |
|--------|------|-------------|
| BTCUSDT | BTC/USDT | Bitcoin |
| ETHUSDT | ETH/USDT | Ethereum |
| SOLUSDT | SOL/USDT | Solana |
| DOGEUSDT | DOGE/USDT | Dogecoin |
| BNBUSDT | BNB/USDT | Binance Coin |
| XRPUSDT | XRP/USDT | Ripple |

---

## Prerequisites

- **Docker** (v20.10+)
- **Docker Compose** (v2.0+)
- **Python** (v3.9+) - for local testing
- **Git**
- **8GB+ RAM** recommended

### Required Ports

Ensure these ports are available:

| Port | Service |
|------|---------|
| 2181 | Zookeeper |
| 29092 | Kafka |
| 8123 | ClickHouse HTTP |
| 9000 | ClickHouse Native |
| 3000 | Grafana |

---

## Installation

### 1. Clone the Repository

```bash
git clone <repository-url>
cd crypto-bigdata-project
```

### 2. Start All Services

```bash
docker-compose up -d
```

### 3. Wait for Services to Initialize

```bash
# Check all services are running
docker-compose ps
```

### 4. Initialize ClickHouse Schema

```bash
docker exec -i crypto-bigdata-project-clickhouse-1 clickhouse-client < src/clickhouse_init.sql
```

### 5. Verify Data Flow

```bash
# Check producer logs
docker-compose logs -f producer

# Check Spark processor logs
docker-compose logs -f spark-processor

# Query ClickHouse for data
docker exec crypto-bigdata-project-clickhouse-1 clickhouse-client --query "SELECT count() FROM crypto.market_data"
```

### 6. Access Grafana

- **URL**: http://localhost:3000
- **Username**: `admin`
- **Password**: `admin`

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `CLICKHOUSE_HOST` | ClickHouse server hostname | `localhost` |
| `CHECK_INTERVAL` | Alert check interval (seconds) | `5` |
| `MAX_ALERTS` | Maximum alerts to send | `1` |
| `SMTP_SERVER` | SMTP server hostname | `smtp.gmail.com` |
| `SMTP_PORT` | SMTP server port | `587` |
| `SMTP_USER` | SMTP username (if different from sender) | - |
| `SENDER_EMAIL` | Email sender address | - |
| `SENDER_PASSWORD` | SMTP password/API key | - |
| `RECEIVER_EMAIL` | Alert recipient email | - |

### Email Alerts Configuration

Configure in `docker-compose.yml` under `alert-bot` service:

```yaml
environment:
  - SMTP_SERVER=smtp.mailersend.net
  - SMTP_PORT=2525
  - SMTP_USER=your_smtp_user
  - SENDER_EMAIL=your_sender@domain.com
  - SENDER_PASSWORD=your_password
  - RECEIVER_EMAIL=recipient@example.com
  - MAX_ALERTS=1
  - CHECK_INTERVAL=5
```

#### Supported Email Providers

| Provider | SMTP Server | Port | Notes |
|----------|-------------|------|-------|
| **MailerSend** | `smtp.mailersend.net` | 587/2525 | Requires domain verification |
| **Gmail** | `smtp.gmail.com` | 587 | Requires App Password |
| **SMTP2GO** | `mail.smtp2go.com` | 2525 | SMTP_USER required |
| **SendGrid** | `smtp.sendgrid.net` | 587 | Use API key as password |

### Adding New Trading Pairs

Edit `src/producer_docker.py`:

```python
TRADING_PAIRS = ['btcusdt', 'ethusdt', 'solusdt', 'dogeusdt', 'bnbusdt', 'xrpusdt', 'newpair']
```

---

## Grafana Dashboard Setup

### 1. Add ClickHouse Data Source

1. Go to **Configuration** → **Data Sources** → **Add data source**
2. Search for **ClickHouse**
3. Configure:
   - **Server address**: `clickhouse`
   - **Server port**: `9000`
   - **Protocol**: `Native`
   - **Database**: `crypto`
4. Click **Save & Test**

### 2. Create Price Chart Panel

**Query for price data:**

```sql
SELECT 
    event_time as time, 
    price 
FROM crypto.market_data 
WHERE symbol = 'ETHUSDT' 
ORDER BY time
```

### 3. Create Anomaly Markers Panel

**Query for anomaly points:**

```sql
SELECT 
    event_time as time, 
    price 
FROM crypto.market_data 
WHERE symbol = 'ETHUSDT' AND is_anomaly = 1 
ORDER BY time
```

### 4. Dashboard Settings

- **Time Range**: Last 15 minutes
- **Refresh**: 5 seconds
- **Timezone**: Browser or Asia/Bangkok

### Sample Dashboard JSON

Import this JSON to create a basic dashboard:

```json
{
  "title": "Crypto Anomaly Detection",
  "panels": [
    {
      "title": "ETH/USDT Price",
      "type": "timeseries",
      "datasource": "ClickHouse",
      "targets": [
        {
          "rawSql": "SELECT event_time as time, price FROM crypto.market_data WHERE symbol = 'ETHUSDT' ORDER BY time"
        }
      ]
    }
  ]
}
```

---

## Anomaly Detection

### Algorithm: 3-Sigma Rule

The Spark processor uses statistical anomaly detection based on the **3-sigma (standard deviation) rule**:

```
Anomaly Condition: volume > mean_volume + (3 × stddev_volume)
```

### Processing Parameters

| Parameter | Value | Description |
|-----------|-------|-------------|
| **Window Size** | 1 minute | Time window for aggregation |
| **Slide Interval** | 30 seconds | How often windows are evaluated |
| **Watermark** | 10 seconds | Late data tolerance |
| **Threshold** | 3σ | Standard deviations above mean |

### Anomaly Detection Flow

```
1. Receive trade data from Kafka
2. Parse JSON: {timestamp, symbol, price, volume}
3. Apply watermark for late data handling
4. Group by symbol with sliding window (1min window, 30s slide)
5. Calculate aggregations:
   - avg_price = average(price)
   - avg_volume = average(volume)
   - stddev_volume = stddev(volume)
   - current_volume = last(volume)
6. Apply anomaly rule:
   - threshold = avg_volume + (3 × stddev_volume)
   - is_anomaly = 1 if current_volume > threshold else 0
7. Write to ClickHouse
```

### Why 3-Sigma?

- In a normal distribution, 99.7% of data falls within 3 standard deviations
- Values beyond 3σ are statistically rare (0.3% probability)
- Effective for detecting sudden volume spikes in trading

---

## Email Alerts

### Alert Bot Behavior

1. Polls ClickHouse every `CHECK_INTERVAL` seconds
2. Queries for anomalies where `is_alerted = 0`
3. Sends email for each new anomaly
4. Marks anomaly as `is_alerted = 1` in database
5. Stops after `MAX_ALERTS` emails (prevents spam)

### Email Format

```
Subject: 🚨 Anomaly Detected: ETHUSDT

Body:
Anomaly Detected in Crypto Market

Pair: ETHUSDT
Price: $3,110.00
Volume: 999,999.99
Time: 2026-01-13 11:30:00

Check your dashboard for more details.

Alert 1 of 1
```

### Testing Email Configuration

```bash
# Test from inside container
docker exec crypto-bigdata-project-alert-bot-1 python /app/src/alert_bot.py --test

# Or run locally
python src/alert_bot.py --test
```

---

## Services Management

### Start/Stop Commands

```bash
# Start all services
docker-compose up -d

# Start specific service
docker-compose up -d producer

# Stop all services
docker-compose down

# Stop and remove volumes
docker-compose down -v

# Restart a service
docker-compose restart alert-bot

# Rebuild and restart
docker-compose up -d --build alert-bot
```

### View Logs

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f spark-processor

# Last N lines
docker-compose logs --tail=50 alert-bot
```

### Check Status

```bash
# Service status
docker-compose ps

# Resource usage
docker stats
```

---

## Testing

### Insert Mock Anomaly

```bash
# Usage: python src/mock_anomaly.py [SYMBOL] [PRICE] [VOLUME]
python src/mock_anomaly.py ETHUSDT 3110.00 999999.99
python src/mock_anomaly.py BTCUSDT 95000.00 500000.00
```

### Test Email Alert

```bash
docker exec crypto-bigdata-project-alert-bot-1 python /app/src/alert_bot.py --test
```

### ClickHouse Queries

```bash
# Count all records
docker exec crypto-bigdata-project-clickhouse-1 clickhouse-client \
  --query "SELECT count() FROM crypto.market_data"

# View recent data
docker exec crypto-bigdata-project-clickhouse-1 clickhouse-client \
  --query "SELECT * FROM crypto.market_data ORDER BY event_time DESC LIMIT 10"

# View anomalies
docker exec crypto-bigdata-project-clickhouse-1 clickhouse-client \
  --query "SELECT * FROM crypto.market_data WHERE is_anomaly = 1 ORDER BY event_time DESC"

# Count by symbol
docker exec crypto-bigdata-project-clickhouse-1 clickhouse-client \
  --query "SELECT symbol, count() as cnt FROM crypto.market_data GROUP BY symbol ORDER BY cnt DESC"

# Delete all anomalies
docker exec crypto-bigdata-project-clickhouse-1 clickhouse-client \
  --query "ALTER TABLE crypto.market_data DELETE WHERE is_anomaly = 1"

# Describe table schema
docker exec crypto-bigdata-project-clickhouse-1 clickhouse-client \
  --query "DESCRIBE crypto.market_data"
```

### Kafka Commands

```bash
# List topics
docker exec crypto-bigdata-project-kafka-1 kafka-topics --list --bootstrap-server localhost:9092

# Consume messages
docker exec crypto-bigdata-project-kafka-1 kafka-console-consumer \
  --bootstrap-server localhost:9092 --topic crypto-realtime --from-beginning --max-messages 5
```

---

## Database Schema

### Table: `crypto.market_data`

```sql
CREATE DATABASE IF NOT EXISTS crypto;

CREATE TABLE IF NOT EXISTS crypto.market_data (
    event_time DateTime,      -- Timestamp of the trade
    symbol String,            -- Trading pair (e.g., BTCUSDT)
    price Float64,            -- Trade price in USDT
    volume Float64,           -- Trade volume
    is_anomaly UInt8,         -- 1 = anomaly, 0 = normal
    is_alerted UInt8 DEFAULT 0 -- 1 = alert sent, 0 = not alerted
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_time)
ORDER BY (symbol, event_time);
```

### Column Descriptions

| Column | Type | Description |
|--------|------|-------------|
| `event_time` | DateTime | UTC timestamp of the aggregated window |
| `symbol` | String | Trading pair symbol (e.g., BTCUSDT) |
| `price` | Float64 | Average price during the window |
| `volume` | Float64 | Current volume (last value in window) |
| `is_anomaly` | UInt8 | 1 if volume exceeded threshold, else 0 |
| `is_alerted` | UInt8 | 1 if email alert was sent, else 0 |

---

## Project Structure

```
crypto-bigdata-project/
├── README.md                    # This file
├── docker-compose.yml           # Docker services configuration
├── Dockerfile.producer          # Producer container build
├── Dockerfile.alertbot          # Alert bot container build
├── requirements.txt             # Python dependencies
│
├── clickhouse_config/
│   └── users.xml                # ClickHouse user configuration
│
└── src/
    ├── producer.py              # Local Binance WebSocket producer
    ├── producer_docker.py       # Docker-compatible producer
    ├── producer_docker.py
    ├── spark_processor.py
    ├── spark_processor_docker.py
    ├── alert_bot.py
    ├── mock_anomaly.py
    └── clickhouse_init.sql
```

## Troubleshooting

### Common Issues

#### 1. Spark Native IO Error on Windows

**Error**: `NativeIO$Windows.access0` error when running Spark locally

**Solution**: Run Spark inside Docker instead of locally. The `spark-processor` service in docker-compose handles this automatically.

#### 2. Email Rate Limiting

**Error**: `(429, 'Too many requests')` or `(554, 'Too many requests')`

**Solution**: 
- Wait 2-5 minutes for rate limit to reset
- Reduce `CHECK_INTERVAL` to slow down polling
- The alert bot now tracks sent alerts to prevent duplicates

#### 3. ClickHouse Column Not Found

**Error**: `Column 'is_alerted' not found`

**Solution**: Add the missing column:
```bash
docker exec crypto-bigdata-project-clickhouse-1 clickhouse-client \
  --query "ALTER TABLE crypto.market_data ADD COLUMN IF NOT EXISTS is_alerted UInt8 DEFAULT 0"
```

#### 4. Kafka Connection Refused

**Error**: `Connection refused` when producer tries to connect

**Solution**: 
- Ensure Kafka is running: `docker-compose ps kafka`
- Wait for Kafka to fully start (30-60 seconds)
- Check Kafka logs: `docker-compose logs kafka`

#### 5. ClickHouse Insert Error (400 Bad Request)

**Error**: `HTTP Error 400: Bad Request`

**Solution**: Schema mismatch. Ensure INSERT matches table columns:
```sql
-- Check current schema
DESCRIBE crypto.market_data
```

#### 6. Grafana Can't Connect to ClickHouse

**Solution**:
- Use hostname `clickhouse` (not `localhost`) inside Docker network
- Ensure ClickHouse is running
- Check port: 9000 for native, 8123 for HTTP

#### 7. No Data in Grafana

**Checklist**:
1. Check producer is running: `docker-compose logs producer`
2. Check Spark processor: `docker-compose logs spark-processor`
3. Query ClickHouse directly to verify data exists
4. Check Grafana time range (set to "Last 15 minutes")

#### 8. Alert Bot Sending Duplicate Emails

**Solution**: 
- Delete existing anomalies: `ALTER TABLE crypto.market_data DELETE WHERE is_anomaly = 1`
- Rebuild alert-bot: `docker-compose up -d --build alert-bot`

### Useful Debug Commands

```bash
# Check all container status
docker-compose ps

# View all logs
docker-compose logs -f

# Restart everything
docker-compose down && docker-compose up -d

# Check resource usage
docker stats

# Enter ClickHouse CLI
docker exec -it crypto-bigdata-project-clickhouse-1 clickhouse-client

# Check Kafka topics
docker exec crypto-bigdata-project-kafka-1 kafka-topics --list --bootstrap-server localhost:9092
```

---

## Tech Stack

| Category | Technology | Version |
|----------|------------|---------|
| **Data Source** | Binance WebSocket API | - |
| **Message Broker** | Apache Kafka | 7.5.0 |
| **Stream Processing** | Apache Spark | 3.5.1 |
| **Database** | ClickHouse | Latest |
| **Visualization** | Grafana | Latest |
| **Alerting** | Python SMTP | - |
| **Container Runtime** | Docker | 20.10+ |
| **Language** | Python | 3.9+ |

### Python Dependencies

```
kafka-python-ng
websocket-client
clickhouse-connect
pyspark
```

---

## Performance Considerations

- **Kafka**: Single partition setup for simplicity; scale partitions for higher throughput
- **Spark**: 2 shuffle partitions configured; increase for larger datasets
- **ClickHouse**: MergeTree engine with monthly partitioning; efficient for time-series queries
- **Memory**: Recommend 8GB+ RAM for running all services

---

## Future Improvements

- [ ] Add more anomaly detection algorithms (Isolation Forest, LSTM)
- [ ] Support for more trading pairs
- [ ] Telegram/Slack alert integration
- [ ] Historical data backfill
- [ ] Kubernetes deployment
- [ ] Prometheus metrics export
- [ ] Multi-exchange support (Coinbase, Kraken)

---

## License

MIT License

---

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

---

## Contact

For questions or issues, please open a GitHub issue.
