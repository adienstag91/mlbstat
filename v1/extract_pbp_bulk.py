import time
from datetime import datetime
import pandas as pd
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

def get_team_game_urls(team_abbr="NYY", year=2025):
    url = f"https://www.baseball-reference.com/teams/{team_abbr}/{year}-schedule-scores.shtml"
    today = datetime.today()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url)
        html = page.content()
        browser.close()

    soup = BeautifulSoup(html, "html.parser")
    base_url = "https://www.baseball-reference.com"
    box_score_urls = []

    for row in soup.select("table#team_schedule tbody tr"):
        date_cell = row.find("td", {"data-stat": "date_game"})
        boxscore_link = row.find("td", {"data-stat": "boxscore"}).find("a") if row.find("td", {"data-stat": "boxscore"}) else None

        if not date_cell or not boxscore_link:
            continue

        date_str = date_cell.get("csk")
        if not date_str:
            continue

        game_date = datetime.strptime(date_str, "%Y-%m-%d")
        if game_date >= today:
            continue

        href = boxscore_link.get("href")
        if href:
            box_score_urls.append(base_url + href)

    return box_score_urls

def extract_play_descriptions(url):
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        page.goto(url)
        html = page.content()
        browser.close()

    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", id="play_by_play")
    if not table:
        print(f"❌ Skipping (no play-by-play): {url}")
        return []

    try:
        df = pd.read_html(str(table))[0]
    except Exception as e:
        print(f"❌ Error reading table for {url}: {e}")
        return []

    # Keep only real at-bat rows: e.g., t1, b5, etc.
    df_cleaned = df[df["Inn"].astype(str).str.match(r"^[tb]\d+$", na=False)]

    return df_cleaned["Play Description"].dropna().tolist()


if __name__ == "__main__":
    team = "NYY"
    year = 2025
    print(f"🔎 Fetching game URLs for {team} in {year}...")
    urls = get_team_game_urls(team, year)
    print(f"✅ Found {len(urls)} completed games.\n")

    all_results = []
    for idx, url in enumerate(urls):
        print(f"🔄 [{idx+1}/{len(urls)}] Scraping: {url}")
        try:
            results = extract_play_descriptions(url)
            all_results.extend(results)
        except Exception as e:
            print(f"⚠️ Failed on {url}: {e}")
        time.sleep(3)  # Rate limit buffer

    # Write to file
    with open("all_result_descriptions.txt", "w") as f:
        for line in all_results:
            f.write(line.strip() + "\n")

    print(f"\n🎯 Total descriptions: {len(all_results)}")
    print("📄 Saved to all_result_descriptions.txt")
