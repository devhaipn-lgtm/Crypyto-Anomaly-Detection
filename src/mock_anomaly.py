"""
Mock Anomaly Generator - Simulates all alert scenarios for the combined
anomaly scoring system (volume + sentiment).

Scenarios match those defined in BUSINESS_LOGIC.md:
  1. normal          - Normal trading, no news            → NORMAL
  2. volume_spike    - Volume spike (3σ), neutral news    → ANOMALY
  3. volume_bullish  - Volume spike (3σ), bullish news    → ANOMALY
  4. combined        - Moderate volume + bearish news     → ANOMALY
  5. sentiment_only  - Normal volume + very bearish news  → ANOMALY
  6. borderline      - Slightly elevated + mildly bearish → NORMAL (boundary)
  7. custom          - Custom values via CLI arguments
  8. all             - Run all scenarios 1-6 sequentially

Usage:
  python mock_anomaly.py <scenario>
  python mock_anomaly.py custom BTCUSDT 95000 500000 3.5 -0.8 2.95
  python mock_anomaly.py all
"""
import os
import sys
import time
import clickhouse_connect
from datetime import datetime, timedelta

CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'localhost')


def get_client():
    return clickhouse_connect.get_client(host=CLICKHOUSE_HOST)


def insert_market_record(symbol, price, volume, is_anomaly, volume_score,
                         sentiment_avg, combined_score, time_offset_sec=0):
    """Insert a record into crypto.market_data with all scoring columns."""
    client = get_client()
    event_time = (datetime.now() + timedelta(seconds=time_offset_sec)).strftime('%Y-%m-%d %H:%M:%S')

    query = f"""
        INSERT INTO crypto.market_data
        (event_time, symbol, price, volume, is_anomaly, is_alerted,
         volume_score, sentiment_avg, combined_score)
        VALUES ('{event_time}', '{symbol}', {price}, {volume}, {is_anomaly}, 0,
                {volume_score}, {sentiment_avg}, {combined_score})
    """
    client.command(query)

    status = "ANOMALY" if is_anomaly else "NORMAL"
    print(f"  [{status}] {symbol} | price=${price:,.2f} vol={volume:,.2f} "
          f"| vol_score={volume_score:.2f} sent_avg={sentiment_avg:+.2f} "
          f"combined={combined_score:.2f}")
    return event_time


def insert_sentiment_record(symbol, score, label, title):
    """Insert a record into crypto.sentiment_data."""
    client = get_client()
    event_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    post_id = f"mock_{int(time.time())}_{symbol}"
    title_escaped = title.replace("'", "\\'")

    query = f"""
        INSERT INTO crypto.sentiment_data
        (event_time, symbol, source, post_id, title, kind,
         sentiment_score, sentiment_label, votes_positive, votes_negative,
         votes_important, url, published_at)
        VALUES ('{event_time}', '{symbol}', 'cryptonews_mock', '{post_id}',
                '{title_escaped}', 'news', {score}, '{label}',
                {max(0, int((1+score)*25))}, {max(0, int((1-score)*25))}, 0,
                '', '{event_time}')
    """
    client.command(query)
    print(f"  [SENTIMENT] {symbol} | score={score:+.2f} ({label}) | {title}")


# =====================================================================
#  PREDEFINED SCENARIOS
# =====================================================================

def scenario_normal():
    """Scenario 1: Normal trading, neutral sentiment → NORMAL"""
    print("\n📊 Scenario 1: NORMAL TRADING (no anomaly expected)")
    print("-" * 60)
    insert_sentiment_record('ETHUSDT', 0.1, 'neutral',
                            'Ethereum network upgrade proceeds as planned')
    insert_market_record(
        symbol='ETHUSDT', price=3100.00, volume=150.00,
        is_anomaly=0, volume_score=0.5, sentiment_avg=0.1, combined_score=0.35
    )


def scenario_volume_spike():
    """Scenario 2: Extreme volume spike (3σ), neutral sentiment → ANOMALY"""
    print("\n🔴 Scenario 2: VOLUME SPIKE (3-sigma, triggers alert)")
    print("-" * 60)
    insert_sentiment_record('BTCUSDT', 0.0, 'neutral',
                            'Bitcoin market remains stable ahead of Fed meeting')
    insert_market_record(
        symbol='BTCUSDT', price=95000.00, volume=500000.00,
        is_anomaly=1, volume_score=3.0, sentiment_avg=0.0, combined_score=2.10
    )


def scenario_volume_bullish():
    """Scenario 3: Volume spike + bullish sentiment → ANOMALY"""
    print("\n🔴 Scenario 3: VOLUME SPIKE + BULLISH NEWS (still triggers)")
    print("-" * 60)
    insert_sentiment_record('SOLUSDT', 0.7, 'bullish',
                            'Solana TVL hits new all-time high, institutional interest surges')
    insert_market_record(
        symbol='SOLUSDT', price=185.00, volume=800000.00,
        is_anomaly=1, volume_score=3.0, sentiment_avg=0.7, combined_score=2.10
    )


def scenario_combined():
    """Scenario 4: Moderate volume (2σ) + bearish sentiment → ANOMALY"""
    print("\n🔴 Scenario 4: MODERATE VOLUME + BEARISH NEWS (combined triggers)")
    print("-" * 60)
    insert_sentiment_record('ETHUSDT', -0.6, 'bearish',
                            'Major Ethereum whale dumps 50K ETH amid regulatory fears')
    time.sleep(1)
    insert_market_record(
        symbol='ETHUSDT', price=2850.00, volume=300000.00,
        is_anomaly=1, volume_score=2.0, sentiment_avg=-0.6, combined_score=2.00
    )


def scenario_sentiment_only():
    """Scenario 5: Normal volume + very bearish sentiment → ANOMALY"""
    print("\n🔴 Scenario 5: SENTIMENT-DRIVEN ANOMALY (volume normal, news very bearish)")
    print("-" * 60)
    insert_sentiment_record('XRPUSDT', -1.0, 'bearish',
                            'SEC files new lawsuit against Ripple, XRP delisting fears spread')
    time.sleep(1)
    insert_market_record(
        symbol='XRPUSDT', price=0.48, volume=120.00,
        is_anomaly=1, volume_score=0.5, sentiment_avg=-1.0, combined_score=1.35
    )


def scenario_borderline():
    """Scenario 6: Slightly elevated volume + mild bearish → NORMAL (boundary)"""
    print("\n📊 Scenario 6: BORDERLINE (just below threshold, no anomaly)")
    print("-" * 60)
    insert_sentiment_record('DOGEUSDT', -0.3, 'bearish',
                            'Dogecoin community debates upcoming protocol changes')
    time.sleep(1)
    insert_market_record(
        symbol='DOGEUSDT', price=0.15, volume=200.00,
        is_anomaly=0, volume_score=1.0, sentiment_avg=-0.3, combined_score=1.00
    )


def scenario_custom(symbol, price, volume, volume_score, sentiment_avg, combined_score):
    """Scenario 7: Fully custom values."""
    is_anomaly = 1 if combined_score > 1.0 else 0
    print(f"\n🔧 Scenario 7: CUSTOM ({symbol})")
    print("-" * 60)
    if sentiment_avg < 0:
        label = 'bearish'
    elif sentiment_avg > 0.3:
        label = 'bullish'
    else:
        label = 'neutral'
    insert_sentiment_record(symbol, sentiment_avg, label,
                            f'Custom mock sentiment for {symbol}')
    time.sleep(1)
    insert_market_record(
        symbol=symbol, price=price, volume=volume,
        is_anomaly=is_anomaly, volume_score=volume_score,
        sentiment_avg=sentiment_avg, combined_score=combined_score
    )


def scenario_all():
    """Run all predefined scenarios sequentially."""
    print("=" * 60)
    print("🧪 RUNNING ALL SCENARIOS")
    print("=" * 60)
    scenario_normal()
    time.sleep(1)
    scenario_volume_spike()
    time.sleep(1)
    scenario_volume_bullish()
    time.sleep(1)
    scenario_combined()
    time.sleep(1)
    scenario_sentiment_only()
    time.sleep(1)
    scenario_borderline()
    print("\n" + "=" * 60)
    print("✅ All 6 scenarios inserted.")
    print("   4 ANOMALY records (scenarios 2,3,4,5) → alert bot will detect these")
    print("   2 NORMAL records  (scenarios 1,6)     → no alerts expected")
    print("=" * 60)


SCENARIOS = {
    'normal': scenario_normal,
    'volume_spike': scenario_volume_spike,
    'volume_bullish': scenario_volume_bullish,
    'combined': scenario_combined,
    'sentiment_only': scenario_sentiment_only,
    'borderline': scenario_borderline,
    'all': scenario_all,
}


def print_usage():
    print("=" * 60)
    print("🧪 MOCK ANOMALY GENERATOR (Combined Scoring)")
    print("=" * 60)
    print()
    print("Usage:")
    print("  python mock_anomaly.py <scenario>")
    print("  python mock_anomaly.py custom <SYMBOL> <PRICE> <VOLUME> <VOL_SCORE> <SENT_AVG> <COMBINED>")
    print()
    print("Scenarios:")
    print("  normal          Normal trading, neutral news              → NORMAL")
    print("  volume_spike    Volume spike (3σ), neutral news           → ANOMALY")
    print("  volume_bullish  Volume spike (3σ), bullish news           → ANOMALY")
    print("  combined        Moderate volume (2σ) + bearish news       → ANOMALY")
    print("  sentiment_only  Normal volume + very bearish news         → ANOMALY")
    print("  borderline      Slightly elevated + mildly bearish        → NORMAL")
    print("  custom          Fully custom (provide all values)         → depends")
    print("  all             Run all predefined scenarios sequentially")
    print()
    print("Examples:")
    print("  python mock_anomaly.py volume_spike")
    print("  python mock_anomaly.py all")
    print("  python mock_anomaly.py custom BTCUSDT 95000 500000 3.5 -0.8 2.95")
    print("=" * 60)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print_usage()
        sys.exit(0)

    scenario_name = sys.argv[1].lower()

    if scenario_name == 'custom':
        if len(sys.argv) < 8:
            print("ERROR: custom requires 6 args: SYMBOL PRICE VOLUME VOL_SCORE SENT_AVG COMBINED")
            print("Example: python mock_anomaly.py custom BTCUSDT 95000 500000 3.5 -0.8 2.95")
            sys.exit(1)
        scenario_custom(
            symbol=sys.argv[2].upper(),
            price=float(sys.argv[3]),
            volume=float(sys.argv[4]),
            volume_score=float(sys.argv[5]),
            sentiment_avg=float(sys.argv[6]),
            combined_score=float(sys.argv[7]),
        )
    elif scenario_name in SCENARIOS:
        SCENARIOS[scenario_name]()
    else:
        print(f"ERROR: Unknown scenario '{scenario_name}'")
        print_usage()
        sys.exit(1)
