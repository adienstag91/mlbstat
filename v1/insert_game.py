from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
import sqlite3

boxscore_url = "https://www.baseball-reference.com/boxes/NYA/NYA202503270.shtml"

def fetch_boxscore_html(url):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url)
        html = page.content()
        browser.close()
    return html

def parse_basic_game_info(html):
    soup = BeautifulSoup(html, "html.parser")
    date_node = soup.select_one("div.scorebox div.scorebox_meta div")
    game_date = date_node.text.strip() if date_node else "Unknown"
    teams = soup.select("div.scorebox strong a")
    if len(teams) < 2:
        return None
    away_team = teams[0].text.strip()
    home_team = teams[1].text.strip()
    return {
        "date": game_date,
        "home_team": home_team,
        "away_team": away_team
    }

def insert_game_into_db(game_info):
    conn = sqlite3.connect("mlbstat.db")
    cursor = conn.cursor()
    cursor.execute("INSERT OR IGNORE INTO teams (name, abbreviation) VALUES (?, ?)",
                   (game_info["home_team"], game_info["home_team"][:3].upper()))
    cursor.execute("INSERT OR IGNORE INTO teams (name, abbreviation) VALUES (?, ?)",
                   (game_info["away_team"], game_info["away_team"][:3].upper()))
    cursor.execute("SELECT team_id FROM teams WHERE name = ?", (game_info["home_team"],))
    home_id = cursor.fetchone()[0]
    cursor.execute("SELECT team_id FROM teams WHERE name = ?", (game_info["away_team"],))
    away_id = cursor.fetchone()[0]
    cursor.execute("""
        INSERT INTO games (date, home_team_id, away_team_id, venue)
        VALUES (?, ?, ?, ?)
    """, (game_info["date"], home_id, away_id, "Yankee Stadium"))
    conn.commit()
    game_id = cursor.lastrowid
    conn.close()
    return game_id

if __name__ == "__main__":
    html = fetch_boxscore_html(boxscore_url)
    game_info = parse_basic_game_info(html)
    print("Game Info:", game_info)
    game_id = insert_game_into_db(game_info)
    print("✅ Inserted game with ID:", game_id)
