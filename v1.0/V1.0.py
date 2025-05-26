import feedparser
import json
import csv
import os
import requests
import logging
from langdetect import detect
from datetime import datetime
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

# Configure logging
logging.basicConfig(
    filename='news_fetch.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Ensure data directory exists
DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

# List of RSS feeds with country tags
rss_feeds = {
    "BBC (UK)": ("http://feeds.bbci.co.uk/news/rss.xml", "United Kingdom"),
    "CNN (USA)": ("http://rss.cnn.com/rss/edition.rss", "United States"),
    "ABC (Australia)": ("https://www.abc.net.au/news/feed/51120/rss.xml", "Australia"),
    "Al Jazeera (Middle East)": ("https://www.aljazeera.com/xml/rss/all.xml", "Middle East"),
    "Times of India (India)": ("https://timesofindia.indiatimes.com/rssfeeds/-2128936835.cms", "India"),
    "NHK (Japan)": ("https://www3.nhk.or.jp/rss/news/cat0.xml", "Japan"),
    "The Straits Times (Singapore)": ("https://www.straitstimes.com/news/singapore/rss.xml", "Singapore"),
    "DW (Germany)": ("https://rss.dw.com/rdf/rss-en-all", "Germany"),
    "ANSA (Italy)": ("https://www.ansa.it/sito/ansait_rss.xml", "Italy"),
    "El País (Spain)": ("https://feeds.elpais.com/mrss-s/pages/ep/site/elpais.com/portada", "Spain"),
    "China Daily (China)": ("http://www.chinadaily.com.cn/rss/china_rss.xml", "China"),
    "Yonhap (South Korea)": ("https://en.yna.co.kr/Service/RSS/main.xml", "South Korea"),
    "Globo (Brazil)": ("https://g1.globo.com/rss/g1/", "Brazil"),
    "Punch (Nigeria)": ("https://punchng.com/feed/", "Nigeria"),
    "Hurriyet Daily News (Turkey)": ("https://www.hurriyetdailynews.com/rss", "Turkey"),
    "El Tiempo (Colombia)": ("https://www.eltiempo.com/rss/colombia.xml", "Colombia"),
    "The Star (Malaysia)": ("https://www.thestar.com.my/rss", "Malaysia"),
    "Dawn (Pakistan)": ("https://www.dawn.com/feeds/home", "Pakistan"),
    "VNExpress (Vietnam)": ("https://vnexpress.net/rss", "Vietnam"),
    "Tehran Times (Iran)": ("https://www.tehrantimes.com/rss", "Iran"),
    "Jerusalem Post (Israel)": ("https://www.jpost.com/Rss/RssFeedsHeadlines.aspx", "Israel"),
    "Ekathimerini (Greece)": ("https://www.ekathimerini.com/rss/news/", "Greece"),
    "NL Times (Netherlands)": ("https://nltimes.nl/rss", "Netherlands"),
}

# Function to get historical snapshots from Wayback Machine
def get_wayback_snapshots(feed_url, from_date, to_date):
    try:
        api_url = f"http://web.archive.org/cdx/search/cdx?url={feed_url}&output=json&from={from_date}&to={to_date}"
        response = requests.get(api_url, timeout=10)
        response.raise_for_status()
        data = response.json()
        return [item[1] for item in data[1:]] if len(data) > 1 else []
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch snapshots for {feed_url}: {e}")
        return []

# Function to fetch and parse archived RSS feed
def fetch_archived_feed(feed_url, timestamp):
    archived_url = f"http://web.archive.org/web/{timestamp}/{feed_url}"
    return feedparser.parse(archived_url)

# Main function to fetch and process news
# Function to process a single RSS feed
def process_feed(source, url, country, seen_titles, news_data, language_data):
    try:
        logging.info(f"Fetching historical data for: {source}...")
        snapshots = get_wayback_snapshots(url, "20240101", "20250201")
        
        if not snapshots:
            logging.warning(f"No snapshots found for {source}")
            return
        
        for timestamp in snapshots:
            logging.info(f"Fetching snapshot from {timestamp} for {source}...")
            feed = fetch_archived_feed(url, timestamp)
            
            for entry in feed.entries:
                title = entry.get("title", "N/A").strip()
                if title in seen_titles:
                    continue  # Skip duplicate news titles
                seen_titles.add(title)
                
                text_to_detect = entry.get("summary", "") or entry.get("title", "")
                language = detect(text_to_detect) if text_to_detect else "Unknown"
                
                news_item = {
                    "title": title,
                    "publication_date": entry.get("published", "N/A"),
                    "source": source,
                    "country": country,
                    "summary": entry.get("summary", "N/A"),
                    "url": entry.get("link", "N/A"),
                    "language": language
                }
                
                news_data.append(news_item)
                language_data[language].append(news_item)
    except Exception as e:
        logging.error(f"Error processing {source}: {e}")

# Function to fetch news in parallel
def fetch_news_parallel():
    news_data = []
    language_data = defaultdict(list)
    seen_titles = set()
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        for source, (url, country) in rss_feeds.items():
            executor.submit(process_feed, source, url, country, seen_titles, news_data, language_data)
    
    return news_data, language_data


#Save Functions        
def save_language_data(language_data):
    for lang, articles in language_data.items():
        json_filename = os.path.join(DATA_DIR, f"news_{lang}.json")
        csv_filename = os.path.join(DATA_DIR, f"news_{lang}.csv")
        
        with open(json_filename, "w", encoding="utf-8") as file:
            json.dump(articles, file, ensure_ascii=False, indent=4)
        logging.info(f"Data saved to {json_filename} for language: {lang}")
        
        keys = ["title", "publication_date", "source", "country", "summary", "url", "language"]
        with open(csv_filename, "w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=keys)
            writer.writeheader()
            writer.writerows([{k: v for k, v in article.items() if k in keys} for article in articles])
        logging.info(f"Data saved to {csv_filename} for language: {lang}")


def save_to_json(data, filename="news_data.json"):
    filepath = os.path.join(DATA_DIR, filename)
    with open(filepath, "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=4)
    logging.info(f"Data saved to {filepath}")

def save_to_csv(data, filename="news_data.csv"):
    filepath = os.path.join(DATA_DIR, filename)
    keys = ["title", "publication_date", "source", "country", "summary", "url", "language"]
    with open(filepath, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=keys)
        writer.writeheader()
        writer.writerows([{k: v for k, v in item.items() if k in keys} for item in data])
    logging.info(f"Data saved to {filepath}")


def main():
    logging.info("Fetching historical news data...")
    news_data, language_data = fetch_news_parallel()
    save_to_json(news_data)
    save_to_csv(news_data)
    save_language_data(language_data)
    logging.info("News fetching complete!")


if __name__ == "__main__":
    main()
