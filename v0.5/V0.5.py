import feedparser
import json
import csv
import os
import time
import schedule
import requests
from langdetect import detect
from datetime import datetime
from collections import defaultdict

# Ensure data directory exists
DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

# List of RSS feeds with country tags
rss_feeds = {
    "BBC (UK)": ("http://feeds.bbci.co.uk/news/rss.xml", "United Kingdom"),
    "CNN (USA)": ("http://rss.cnn.com/rss/edition.rss", "United States"),
    "ABC (Australia)": ("https://www.abc.net.au/news/feed/51120/rss.xml", "Australia"),
    "Times of India (India)": ("https://timesofindia.indiatimes.com/rssfeeds/-2128936835.cms", "India"),
    "NHK (Japan)": ("https://www3.nhk.or.jp/rss/news/cat0.xml", "Japan"),
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


def fetch_news():
    news_data = []
    seen_titles = set()  # To avoid duplicate titles
    language_data = defaultdict(list)  # To categorize articles by language
    
    for source, (url, country) in rss_feeds.items():
        try:
            print(f"Fetching: {source}...")
            response = requests.get(url, timeout=10)  # Fetch RSS feed with timeout
            response.raise_for_status()  # Raise exception for HTTP errors
            
            feed = feedparser.parse(response.text)
            for entry in feed.entries:
                title = entry.get("title", "N/A").strip()
                
                if title in seen_titles:
                    continue  # Skip duplicate news titles
                
                seen_titles.add(title)
                # Detect language from summary if available
                language = detect(entry.get("summary", "")) if entry.get("summary") else "Unknown"
                
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
        except requests.exceptions.Timeout:
            print(f"⏳ Skipping {source}: Took too long to respond!")
        except requests.exceptions.RequestException as e:
            print(f"❌ Error fetching {source}: {e}")
    
    return news_data, language_data

def save_language_data(language_data):
    # Save articles categorized by language into JSON and CSV files
    for lang, articles in language_data.items():
        json_filename = os.path.join(DATA_DIR, f"news_{lang}.json")
        csv_filename = os.path.join(DATA_DIR, f"news_{lang}.csv")

        # Convert `_id` fields before saving JSON
        cleaned_articles = [{k: v if k != "_id" else str(v) for k, v in article.items()} for article in articles]

        # Save as JSON
        with open(json_filename, "w", encoding="utf-8") as file:
            json.dump(cleaned_articles, file, ensure_ascii=False, indent=4)
        print(f"Data saved to {json_filename} for language: {lang}")

        # Save as CSV
        keys = ["title", "publication_date", "source", "country", "summary", "url", "language"]
        with open(csv_filename, "w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=keys)
            writer.writeheader()
            writer.writerows([{k: v for k, v in article.items() if k in keys} for article in cleaned_articles])
        print(f"Data saved to {csv_filename} for language: {lang}")

def save_to_json(data, filename="news_data.json"):
    # Save overall data to JSON file
    filepath = os.path.join(DATA_DIR, filename)
    with open(filepath, "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=4)
    print(f"Data saved to {filepath}")

def save_to_csv(data, filename="news_data.csv"):
    # Save overall data to CSV file
    filepath = os.path.join(DATA_DIR, filename)
    keys = ["title", "publication_date", "source", "country", "summary", "url", "language"]
    with open(filepath, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=keys)
        writer.writeheader()
        writer.writerows([{k: v for k, v in item.items() if k in keys} for item in data])
    print(f"Data saved to {filepath}")

def main():
    print("Fetching news...")
    news_data, language_data = fetch_news()  # Fetch and process news
    save_to_json(news_data)  # Save data to JSON
    save_to_csv(news_data)  # Save data to CSV
    save_language_data(language_data)  # Save data categorized by language
    print("News fetching complete!")

if __name__ == "__main__":
    main()
