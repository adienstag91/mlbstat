import pandas as pd
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
from io import StringIO
import re

def normalize_name(name):
    if not isinstance(name, str):
        return name
    name = re.sub(r"\s+(P|C|1B|2B|3B|SS|LF|CF|RF|DH|PH|PR|SP|RP|[A-Z]{1,2}(?:-[A-Z]{1,2})*)$", "", name.strip())
    return name.replace("\xa0", " ")

def extract_box_score_batters(url):
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        page.goto(url)
        soup = BeautifulSoup(page.content(), "html.parser")
        browser.close()

    batter_names = []
    for tbl in soup.find_all("table"):
        table_id = tbl.get("id", "")
        if not table_id.endswith("batting"):
            continue
        df = pd.read_html(StringIO(str(tbl)))[0]
        batter_col = [col for col in df.columns if "Batting" in col or "Player" in col]
        if not batter_col:
            continue
        df.rename(columns={batter_col[0]: "batter"}, inplace=True)
        df = df[df['batter'].notna()]
        df['batter'] = df['batter'].apply(normalize_name)
        batter_names.extend(df['batter'].tolist())

    return pd.Series(batter_names, name="batter").drop_duplicates().reset_index(drop=True)

# Replace with your test URL
url = "https://www.baseball-reference.com/boxes/NYA/NYA202506080.shtml"
print(extract_box_score_batters(url))
