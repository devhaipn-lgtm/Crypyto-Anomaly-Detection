"""
Sentiment Producer - Fetches crypto news sentiment from cryptonews-api.com
and pushes to Kafka topic for correlation with trade data.

API docs: https://cryptonews-api.com/documentation
Free trial: 5 days, 100 API calls, all features included.
Register at: https://cryptonews-api.com/register
"""
import json
import time
import os
import requests
from kafka import KafkaProducer
from datetime import datetime

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092')
KAFKA_TOPIC = 'crypto-sentiment'

# CryptoNews API
# Register at: https://cryptonews-api.com/register
CRYPTONEWS_API_TOKEN = 'bbxncv56th6jaucotx0enougb5pivrmiaadxkiwi '
CRYPTONEWS_BASE_URL = 'https://cryptonews-api.com/api/v1'

# Currencies to monitor (must match trading pairs in producer)
CURRENCIES = ['BTC', 'ETH', 'SOL', 'DOGE', 'BNB', 'XRP']

# Number of news items per request (1-100)
ITEMS_PER_REQUEST = 3

# Polling interval in seconds
POLL_INTERVAL = 800

# Track already-seen news IDs to avoid duplicates
seen_posts = set()
MAX_SEEN_CACHE = 10000


def create_kafka_producer():
    """Create Kafka producer with retry logic."""
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


def article_to_sentiment_score(article):
    """
    Calculate sentiment score from a CryptoNews API article.
    
    The API provides a 'sentiment' field ('Positive', 'Negative', 'Neutral').
    We compute a numeric score in [-1.0, 1.0] using:
      - Base: +0.6 for Positive, -0.6 for Negative, 0.0 for Neutral
      - Source trust boost: articles from major sources get a ±0.2 amplification
      - Ticker relevance: if article mentions only 1 ticker, stronger signal (±0.1)
    
    Clamped to [-1.0, 1.0].
    """
    sentiment_label = article.get('sentiment', 'Neutral')
    
    # Base score from API sentiment classification
    if sentiment_label == 'Positive':
        base = 0.6
    elif sentiment_label == 'Negative':
        base = -0.6
    else:
        base = 0.0

    # Source trust boost — major outlets carry more weight
    trusted_sources = {
        'Coindesk', 'Cointelegraph', 'Bloomberg', 'Reuters', 'CNBC',
        'Forbes', 'Decrypt', 'The Block', 'CoinMarketCap', 'Yahoo Finance'
    }
    source = article.get('source_name', '')
    if source in trusted_sources:
        base += 0.2 if base > 0 else (-0.2 if base < 0 else 0)

    # Single-ticker articles are more targeted signals
    tickers = article.get('tickers', [])
    if isinstance(tickers, list) and len(tickers) == 1:
        base += 0.1 if base > 0 else (-0.1 if base < 0 else 0)

    return max(-1.0, min(1.0, round(base, 4)))


def classify_sentiment(score):
    """Classify numeric score into label."""
    if score > 0.3:
        return 'bullish'
    elif score < -0.3:
        return 'bearish'
    else:
        return 'neutral'


def fetch_sentiment(currency):
    """
    Fetch latest news for a given currency from cryptonews-api.com.
    Returns a list of sentiment records matching the Kafka schema.
    """
    if not CRYPTONEWS_API_TOKEN:
        print("WARNING: CRYPTONEWS_API_TOKEN not set. Using mock data.")
        return generate_mock_sentiment(currency)

    url = f"{CRYPTONEWS_BASE_URL}?tickers={currency}&items={ITEMS_PER_REQUEST}&token={CRYPTONEWS_API_TOKEN}"

    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        data = response.json()

        articles = data.get('data', [])
        records = []

        for article in articles:
            news_id = article.get('news_id', '')

            # Skip already-seen
            if news_id in seen_posts:
                continue
            seen_posts.add(news_id)

            # Evict oldest entries if cache is too large
            if len(seen_posts) > MAX_SEEN_CACHE:
                seen_posts.clear()

            # Calculate our own sentiment score
            score = article_to_sentiment_score(article)
            api_sentiment = article.get('sentiment', 'Neutral')

            # Map API sentiment to positive/negative vote equivalents
            votes_positive = 1 if api_sentiment == 'Positive' else 0
            votes_negative = 1 if api_sentiment == 'Negative' else 0

            record = {
                'timestamp': int(datetime.utcnow().timestamp() * 1000),
                'symbol': f'{currency}USDT',
                'source': 'cryptonews-api',
                'post_id': str(news_id),
                'title': article.get('title', '')[:500],
                'kind': article.get('type', 'Article').lower(),
                'sentiment_score': score,
                'sentiment_label': classify_sentiment(score),
                'votes_positive': votes_positive,
                'votes_negative': votes_negative,
                'votes_important': 0,
                'url': article.get('news_url', ''),
                'published_at': article.get('date', ''),
            }
            records.append(record)

        return records

    except requests.exceptions.RequestException as e:
        print(f"Error fetching sentiment for {currency}: {e}")
        return []


def generate_mock_sentiment(currency):
    """
    Generate mock sentiment data when API token is not configured.
    Useful for testing the pipeline without API credentials.
    """
    import random

    score = round(random.uniform(-1.0, 1.0), 4)
    return [{
        'timestamp': int(datetime.utcnow().timestamp() * 1000),
        'symbol': f'{currency}USDT',
        'source': 'cryptonews_mock',
        'post_id': f'mock_{int(time.time())}_{currency}',
        'title': f'Mock {currency} sentiment news article',
        'kind': 'news',
        'sentiment_score': score,
        'sentiment_label': classify_sentiment(score),
        'votes_positive': random.randint(0, 1),
        'votes_negative': random.randint(0, 1),
        'votes_important': 0,
        'url': '',
        'published_at': datetime.utcnow().isoformat() + 'Z',
    }]


def main():
    print("Starting Crypto Sentiment Producer (cryptonews-api.com)...")
    print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}, Topic: {KAFKA_TOPIC}")
    print(f"Currencies: {', '.join(CURRENCIES)}")
    print(f"Poll interval: {POLL_INTERVAL}s")
    print(f"Items per request: {ITEMS_PER_REQUEST}")
    print(f"API Token configured: {'Yes' if CRYPTONEWS_API_TOKEN else 'No (using mock data)'}")

    kafka_producer = create_kafka_producer()

    while True:
        try:
            total_sent = 0
            for currency in CURRENCIES:
                records = fetch_sentiment(currency)
                for record in records:
                    kafka_producer.send(KAFKA_TOPIC, record)
                    total_sent += 1
                    print(f"Sent: {record['symbol']} | {record['sentiment_label']} "
                          f"({record['sentiment_score']:+.4f}) | {record['title'][:60]}")

                # Small delay between currencies to respect rate limits
                time.sleep(2)

            if total_sent == 0:
                print(f"[{datetime.utcnow().isoformat()}] No new sentiment data")

            kafka_producer.flush()
            print(f"--- Sleeping {POLL_INTERVAL}s ---")
            time.sleep(POLL_INTERVAL)

        except KeyboardInterrupt:
            print("\nStopping sentiment producer...")
            break
        except Exception as e:
            print(f"Error in main loop: {e}")
            time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
