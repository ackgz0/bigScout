from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from bs4 import BeautifulSoup
from webdriver_manager.chrome import ChromeDriverManager
import re
import time

target_leagues = {
    "Premier League": "9",
    "Championship": "10"
}

target_seasons = ["2022-2023", "2023-2024", "2024-2025"]

chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--window-size=1920x1080")

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
driver.get("https://fbref.com/en/comps/")
time.sleep(5)  # biraz daha uzun bekle

soup = BeautifulSoup(driver.page_source, "html.parser")
all_links = soup.find_all("a", href=True)

season_ids = {}

for link in all_links:
    href = link["href"]
    text = link.text.strip()

    for league_name, comp_id in target_leagues.items():
        if f"/en/comps/{comp_id}/" in href:
            for season_name in target_seasons:
                if season_name in text:
                    match = re.search(r"/en/comps/\d+/(\d+)", href)
                    if match:
                        season_id = match.group(1)
                        if league_name not in season_ids:
                            season_ids[league_name] = {}
                        season_ids[league_name][season_name] = season_id
                        print(f"✅ Found: {league_name} | {season_name} → {season_id}")

driver.quit()

print("\n📌 Final Season ID Mapping:\n")
for league, seasons in season_ids.items():
    print(f"{league}:")
    for season, sid in seasons.items():
        print(f"  {season}: {sid}")
