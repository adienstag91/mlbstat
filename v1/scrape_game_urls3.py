from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

def get_yankees_game_urls(year=2025):
    url = f"https://www.baseball-reference.com/teams/NYY/{year}-schedule-scores.shtml"

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url)
        html = page.content()
        browser.close()

    soup = BeautifulSoup(html, "html.parser")
    base_url = "https://www.baseball-reference.com"
    box_score_urls = []

    for link in soup.select("td[data-stat='boxscore'] a"):
        href = link.get("href")
        if href:
            box_score_urls.append(base_url + href)

    return box_score_urls

if __name__ == "__main__":
    urls = get_yankees_game_urls()
    print(f"✅ Found {len(urls)} game URLs.")
    for url in urls:
        print(url)
