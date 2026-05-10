import requests
from bs4 import BeautifulSoup
import json
import uuid
from datetime import datetime
from minio import Minio
import os
import io

MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'minio:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'admin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'password')
BUCKET_NAME = 'bronze'

def init_minio():
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)
    return client

def save_to_minio(client, articles, source_name):
    for article in articles:
        file_name = f"{source_name}_{article['id']}.json"
        content = json.dumps(article, ensure_ascii=False).encode('utf-8')
        try:
            client.put_object(
                BUCKET_NAME,
                file_name,
                data=io.BytesIO(content),
                length=len(content),
                content_type='application/json'
            )
            print(f"Saved {file_name} to MinIO")
        except Exception as e:
            print(f"Error saving {file_name} to MinIO: {e}")

# ──────────────────────────────────────────────
# 1. Al Jazeera (International — Qatar)
# ──────────────────────────────────────────────
def scrape_aljazeera():
    url = "https://www.aljazeera.com/"
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(response.content, 'html.parser')
    except Exception as e:
        print(f"Error fetching Al Jazeera: {e}")
        return []

    articles = []
    for article in soup.find_all('article')[:10]:
        title_tag = article.find('h3')
        if not title_tag:
            continue
        title = title_tag.get_text(strip=True)
        link_tag = article.find('a')
        link = "https://www.aljazeera.com" + link_tag['href'] if link_tag and link_tag.get('href', '').startswith('/') else ""

        articles.append({
            "id": str(uuid.uuid4()),
            "title": title,
            "author": "Al Jazeera Staff",
            "published_date": datetime.utcnow().isoformat(),
            "category": "News",
            "content": f"Article preview: {title}...",
            "source": "Al Jazeera",
            "country": "International",
            "url": link
        })
    return articles

# ──────────────────────────────────────────────
# 2. BBC News (International — Royaume-Uni)
# ──────────────────────────────────────────────
def scrape_bbc():
    url = "https://www.bbc.com/news"
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(response.content, 'html.parser')
    except Exception as e:
        print(f"Error fetching BBC: {e}")
        return []

    articles = []
    for item in soup.find_all('h3')[:10]:
        title = item.get_text(strip=True)
        if not title:
            continue
        link_tag = item.find_parent('a')
        link = ""
        if link_tag and link_tag.get('href'):
            href = link_tag['href']
            link = "https://www.bbc.com" + href if href.startswith('/') else href

        articles.append({
            "id": str(uuid.uuid4()),
            "title": title,
            "author": "BBC News Staff",
            "published_date": datetime.utcnow().isoformat(),
            "category": "News",
            "content": f"BBC News article: {title}...",
            "source": "BBC News",
            "country": "International",
            "url": link
        })
    return articles

# ──────────────────────────────────────────────
# 3. Hespress English (Maroc — International)
# ──────────────────────────────────────────────
def scrape_hespress_batch():
    url = "https://en.hespress.com/"
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(response.content, 'html.parser')
    except Exception as e:
        print(f"Error fetching Hespress: {e}")
        return []

    articles = []
    for item in soup.find_all('div', class_='card')[:8]:
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
            "content": f"Hespress article: {title}...",
            "source": "Hespress",
            "country": "Maroc",
            "url": link
        })
    return articles

# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────
def run_batch():
    print("=" * 50)
    print("Starting multi-source batch scraping...")
    print("=" * 50)

    try:
        client = init_minio()
    except Exception as e:
        print(f"Error initializing MinIO: {e}")
        return

    scrapers = [
        (scrape_aljazeera,     "aljazeera"),
        (scrape_bbc,           "bbc"),
        (scrape_hespress_batch, "hespress"),
    ]

    total = 0
    for scraper_fn, source_name in scrapers:
        print(f"\n[{source_name.upper()}] Scraping...")
        articles = scraper_fn()
        print(f"  → Found {len(articles)} articles.")
        save_to_minio(client, articles, source_name)
        total += len(articles)

    print(f"\n✅ Batch scraping complete. Total articles saved: {total}")

if __name__ == "__main__":
    run_batch()
