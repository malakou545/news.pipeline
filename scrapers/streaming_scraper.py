import requests
from bs4 import BeautifulSoup
import json
import uuid
import time
from datetime import datetime
from kafka import KafkaProducer
import os

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
TOPIC_NAME = 'news_events'

def get_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
    )

# ──────────────────────────────────────────────
# 1. Hespress English (Maroc)
# ──────────────────────────────────────────────
def scrape_hespress():
    url = "https://en.hespress.com/"
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(response.content, 'html.parser')
    except Exception as e:
        print(f"Error fetching Hespress: {e}")
        return []

    articles = []
    for item in soup.find_all('div', class_='card')[:5]:
        title_tag = item.find('h3')
        if not title_tag:
            continue
        title = title_tag.get_text(strip=True)
        link_tag = item.find('a')
        link = link_tag['href'] if link_tag else ""

        articles.append({
            "id": str(uuid.uuid4()),
            "title": title,
            "author": "Hespress Writer",
            "published_date": datetime.utcnow().isoformat(),
            "category": "General",
            "content": f"Hespress streaming content for: {title}",
            "source": "Hespress",
            "country": "Maroc",
            "url": link
        })
    return articles

# ──────────────────────────────────────────────
# 2. Akhbarona (Maroc)
# ──────────────────────────────────────────────
def scrape_akhbarona():
    url = "https://www.akhbarona.com/"
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(response.content, 'html.parser')
    except Exception as e:
        print(f"Error fetching Akhbarona: {e}")
        return []

    articles = []
    for item in soup.find_all('h2')[:5]:
        title = item.get_text(strip=True)
        if not title:
            continue
        link_tag = item.find('a') or item.find_parent('a')
        link = link_tag['href'] if link_tag and link_tag.get('href') else ""

        articles.append({
            "id": str(uuid.uuid4()),
            "title": title,
            "author": "Akhbarona",
            "published_date": datetime.utcnow().isoformat(),
            "category": "General",
            "content": f"Akhbarona article: {title}",
            "source": "Akhbarona",
            "country": "Maroc",
            "url": link
        })
    return articles

# ──────────────────────────────────────────────
# Streaming Loop
# ──────────────────────────────────────────────
def run_streaming():
    print("=" * 50)
    print("Starting multi-source streaming scraper...")
    print("=" * 50)

    try:
        producer = get_kafka_producer()
    except Exception as e:
        print(f"Error connecting to Kafka: {e}")
        return

    scrapers = [
        (scrape_hespress,  "Hespress"),
        (scrape_akhbarona, "Akhbarona"),
    ]

    while True:
        for scraper_fn, source_name in scrapers:
            print(f"\n[{source_name}] Fetching articles...")
            try:
                articles = scraper_fn()
                for article in articles:
                    producer.send(TOPIC_NAME, article)
                    print(f"  → Sent to Kafka: {article['title'][:60]}...")
                producer.flush()
                print(f"  ✅ {len(articles)} articles sent from {source_name}.")
            except Exception as e:
                print(f"  ❌ Error streaming {source_name}: {e}")

        print("\n⏳ Sleeping for 60 seconds before next cycle...\n")
        time.sleep(60)

if __name__ == "__main__":
    run_streaming()
