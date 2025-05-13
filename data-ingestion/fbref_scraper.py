import os
import pandas as pd
from time import sleep
from kafka import KafkaProducer
from pymongo import MongoClient
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup, Comment
import json
from io import StringIO

# Lig ve sezon bilgileri
leagues = {
    "Premier League": {
        "comp": 9,
        "seasons": {
            "2022-2023": "10728",
            "2023-2024": "16544",
            "2024-2025": "17452"
        }
    },
    "Championship": {
        "comp": 10,
        "seasons": {
            "2022-2023": "10730",
            "2023-2024": "16545",
            "2024-2025": "17453"
        }
    }
}


table_types = {
    "standard": "stats",
    "shooting": "shooting",
    "passing": "passing",
    "passing_types": "passing_types",
    "defense": "defense",
}

# Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Selenium Chrome ayarları
chrome_options = Options()
chrome_options.add_argument('--headless')
chrome_options.add_argument('--disable-gpu')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--window-size=1920x1080')
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")

# Chrome WebDriver
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

def get_fbref_url(league_name, comp_id, season_id, table_key):
    return f"https://fbref.com/en/comps/{comp_id}/{season_id}/{table_types[table_key]}/{season_id}-{league_name.replace(' ', '-')}-{table_types[table_key].capitalize()}"

def scrape_and_publish(league, season, table_key):
    comp_id = leagues[league]["comp"]
    season_id = leagues[league]["seasons"][season]
    url = get_fbref_url(league, comp_id, season_id, table_key)

    print(f"🔄 Fetching: {league} | {season} | {table_key} → {url}")

    try:
        driver.get(url)
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "table")))
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        tables = []

        # Yorum içindeki tablolar
        comments = soup.find_all(string=lambda text: isinstance(text, Comment))
        for comment in comments:
            comment_soup = BeautifulSoup(comment, 'html.parser')
            for table in comment_soup.find_all("table", id=lambda x: x and x.startswith("stats_")):
                tables.append(table)

        # Açık tablolar
        for table in soup.find_all("table", id=lambda x: x and x.startswith("stats_")):
            tables.append(table)

        valid_found = False

        for table in tables:
            if not table.find("thead"):
                continue

            try:
                df_list = pd.read_html(StringIO(table.prettify()), header=1)
                df = df_list[0]
            except Exception as read_err:
                print(f"⚠️ read_html failed on table: {read_err}")
                continue

            if 'Player' not in df.columns:
                print("⏭️ Skipping table without 'Player' column.")
                continue
            
            df.columns = [col.replace('.', '_') for col in df.columns]
            df = df[df['Player'].apply(lambda x: isinstance(x, str) and x.strip() != '' and 'Total' not in x and len(x) > 1)]
            df = df[~df['Player'].str.contains("Squad Total|Opponent Total|Team Totals|Opponent|Squad|Team", na=False)]
            valid_found = True

            for _, row in df.iterrows():
                record = row.to_dict()
                record['league'] = league
                record['season'] = season
                record['table'] = table_key
                producer.send('player_stats', record)

            producer.flush()
            print(f"✅ {df.shape[0]} records published to Kafka")

        if not valid_found:
            print("⏭️ No valid player table found on page.")
        return None

    except Exception as e:
        print(f"❌ Failed to fetch {url}: {e}")
        return None

# Dummy verileri silmek için MongoDB temizliği
client = MongoClient("mongodb://localhost:27017")
db = client["bigscout"]
db.players.delete_many({})
print("🧹 MongoDB players koleksiyonundaki önceki veriler silindi.")

# Tüm kombinasyonları çalıştır
for league in leagues:
    for season in leagues[league]["seasons"]:
        for table_key in table_types:
            scrape_and_publish(league, season, table_key)
            sleep(5)

# WebDriver kapat
driver.quit()
