# Grafana Dashboard Setup Guide

## 1. Access Grafana

- **URL:** http://localhost:3000
- **Username:** `admin`
- **Password:** `admin`
- Skip the password change prompt

---

## 2. Install ClickHouse Plugin

1. Go to **⚙ Administration** (left sidebar) → **Plugins and data** → **Plugins**
2. Search for **ClickHouse**
3. Click **Install**

---

## 3. Add ClickHouse Data Source

1. Go to **⚙ Connections** → **Data Sources** → **Add data source**
2. Select **ClickHouse**
3. Configure:

| Setting | Value |
|---------|-------|
| Server address | `clickhouse` |
| Server port | `9000` |
| Protocol | `Native` |
| Username | `default` |
| Password | *(leave empty)* |
| Database | `crypto` |

4. Click **Save & Test** → should show ✅ success

---

## 4. Create Dashboard

1. Click **+** (top-right) → **New dashboard** → **Add visualization**
2. Select your **ClickHouse** data source
3. Switch to **SQL Editor** mode (toggle at bottom of query editor)

---

## 5. Panel Queries

### Panel 1: Real-time Price Chart (Tick-by-Tick)

- **Visualization:** Time series
- **Description:** High-resolution price line from every individual trade
- **Query:**

```sql
SELECT event_time AS time, symbol, price
FROM crypto.raw_trades
WHERE $__timeFilter(event_time)
ORDER BY time
```

### Panel 2: OHLC Candlestick (1-Minute Candles)

- **Visualization:** Candlestick (or Time series)
- **Description:** Standard candlestick chart showing open/high/low/close per minute
- **Query:**

```sql
SELECT
    toStartOfMinute(event_time) AS time,
    symbol,
    argMin(price, event_time) AS open,
    max(price) AS high,
    min(price) AS low,
    argMax(price, event_time) AS close,
    sum(volume) AS total_volume
FROM crypto.raw_trades
WHERE $__timeFilter(event_time) AND symbol = 'BTCUSDT'
GROUP BY time, symbol
ORDER BY time
```

> Change `'BTCUSDT'` to any symbol, or remove the symbol filter for all coins.

### Panel 3: Volume Over Time

- **Visualization:** Time series
- **Query:**

```sql
SELECT event_time AS time, symbol, volume
FROM crypto.market_data
WHERE $__timeFilter(event_time)
ORDER BY time
```

### Panel 4: Anomaly Points

- **Visualization:** Table or Time series
- **Query:**

```sql
SELECT event_time AS time, symbol, price,
       volume_score, sentiment_avg, combined_score
FROM crypto.market_data
WHERE is_anomaly = 1 AND $__timeFilter(event_time)
ORDER BY time DESC
```

### Panel 5: Sentiment Feed

- **Visualization:** Table
- **Query:**

```sql
SELECT event_time AS time, symbol,
       sentiment_score, sentiment_label, source, title
FROM crypto.sentiment_data
WHERE $__timeFilter(event_time)
ORDER BY time DESC
LIMIT 50
```

### Panel 6: Anomaly Count by Symbol

- **Visualization:** Pie chart or Bar chart
- **Query:**

```sql
SELECT symbol, count() AS anomaly_count
FROM crypto.market_data
WHERE is_anomaly = 1 AND $__timeFilter(event_time)
GROUP BY symbol
ORDER BY anomaly_count DESC
```

### Panel 7: Sentiment Distribution

- **Visualization:** Pie chart
- **Query:**

```sql
SELECT sentiment_label, count() AS cnt
FROM crypto.sentiment_data
WHERE $__timeFilter(event_time)
GROUP BY sentiment_label
```

---

## 6. Dashboard Settings

1. Click ⚙ (dashboard settings icon, top-right)
2. Configure:

| Setting | Value |
|---------|-------|
| Time range | Last 30 minutes |
| Auto-refresh | 5s |
| Timezone | Asia/Bangkok (UTC+7) |

3. Click 💾 **Save dashboard**

---

## 7. Troubleshooting

### Panels show empty / no data

Verify data exists in ClickHouse:

```bash
docker exec crypto-bigdata-project-clickhouse-1 clickhouse-client --query "SELECT count() FROM crypto.market_data"
docker exec crypto-bigdata-project-clickhouse-1 clickhouse-client --query "SELECT count() FROM crypto.raw_trades"
docker exec crypto-bigdata-project-clickhouse-1 clickhouse-client --query "SELECT count() FROM crypto.sentiment_data"
```

If counts are 0, check Spark processor:

```bash
docker compose logs --tail=20 spark-processor
```

### ClickHouse data source test fails

- Ensure ClickHouse is running: `docker compose ps clickhouse`
- Use hostname `clickhouse` (not `localhost`) — Grafana runs inside the Docker network
- Verify port `9000` (native), not `8123` (HTTP)

### Plugin not found

- Ensure Grafana container has internet access to download the ClickHouse plugin
- Alternatively, restart Grafana: `docker compose restart grafana`
