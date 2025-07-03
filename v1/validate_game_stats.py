import re
import pandas as pd
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
from io import StringIO

# --- Normalize Player Names ---
def normalize_name(name):
    if not isinstance(name, str):
        return name
    # Remove position suffix (e.g. "Aaron Judge RF")
    name = re.sub(r"\s+((?:[A-Z0-9]{1,3})(?:-[A-Z0-9]{1,3})*)$", "", name.strip())
    return name.replace("\xa0", " ")

# --- Parse Result (Rules Engine) ---
def parse_result_expanded(description):
    desc = description.lower()
    result = {
        "is_plate_appearance": True,
        "is_hit": False, "hit_type": None, "bases_earned": 0,
        "is_home_run": False, "is_out": False, "out_type": None,
        "is_walk": False, "is_strikeout": False, "is_hbp": False,
        "is_reached_on_error": False, "is_fielder_choice": False,
        "is_sacrifice": False, "rbi": 0, "runs_scored": 0,
        "runners_advanced": [], "notes": [], "event_type": None,
        "runner_name": None, "SB": 0, "CS": 0, "2B": 0, "3B": 0, "SF": 0
    }

    # Non-plate-appearance events (e.g. stolen bases)
    non_pa_keywords = [
        "stolen base", "caught stealing", "picked off", "defensive indifference",
        "wild pitch", "passed ball", "balk", "pickoff"
    ]
    if any(k in desc for k in non_pa_keywords):
        result["is_plate_appearance"] = False
        result["notes"].append("non_plate_appearance_event")
        if "stolen base" in desc:
            result["SB"] = 1
        elif "caught stealing" in desc:
            result["CS"] = 1
        return result

    # Hits
    if "home run" in desc:
        result.update({"is_hit": True, "hit_type": "home_run", "is_home_run": True, "bases_earned": 4})
    elif "triple" in desc:
        result.update({"is_hit": True, "hit_type": "triple", "bases_earned": 3, "3B": 1})
    elif "double" in desc and "double play" not in desc:
        result.update({"is_hit": True, "hit_type": "double", "bases_earned": 2, "2B": 1})
    elif "single" in desc:
        result.update({"is_hit": True, "hit_type": "single", "bases_earned": 1})

    # Walks and HBP
    elif "walk" in desc and "intent" not in desc:
        result["is_walk"] = True
    elif "hit by pitch" in desc or "hbp" in desc:
        result["is_hbp"] = True

    # Strikeouts
    elif "strikeout swinging" in desc:
        result.update({"is_strikeout": True, "is_out": True, "out_type": "strikeout_swinging"})
    elif "strikeout looking" in desc:
        result.update({"is_strikeout": True, "is_out": True, "out_type": "strikeout_looking"})

    # Sacrifices
    if "sac fly" in desc or "sacrifice fly" in desc:
        result.update({"is_sacrifice": True, "is_out": True, "out_type": "sacrifice_fly", "SF": 1})
    elif "sac bunt" in desc or "sacrifice bunt" in desc:
        result.update({"is_sacrifice": True, "is_out": True, "out_type": "sacrifice_bunt"})

    # Other outs
    elif "groundout" in desc:
        result.update({"is_out": True, "out_type": "groundout"})
    elif "flyout" in desc or "flyball" in desc:
        result.update({"is_out": True, "out_type": "flyout"})
    elif "lineout" in desc:
        result.update({"is_out": True, "out_type": "lineout"})
    elif "popup" in desc or "popfly" in desc:
        result.update({"is_out": True, "out_type": "popup"})
    elif "double play" in desc:
        result.update({"is_out": True, "out_type": "double_play"})
    elif "forceout" in desc:
        result.update({"is_out": True, "out_type": "forceout"})

    # Reached base
    if "reaches on e" in desc or "safe on e" in desc:
        result["is_reached_on_error"] = True
    if "fielder's choice" in desc:
        result["is_fielder_choice"] = True

    # RBI scoring
    runners_scored = len(re.findall(r"to home|scores", desc))
    if result["is_home_run"]:
        runners_scored += 1  # include the batter
    result["runs_scored"] = runners_scored
    result["runners_advanced"] = list(set(re.findall(r"to (\d+b|2b|3b|home)", desc)))

    if "rbi" in desc:
        match = re.search(r"(\d) rbi", desc)
        if match:
            result["rbi"] = int(match.group(1))
    elif runners_scored > 0 and not result["is_reached_on_error"] and not result["is_fielder_choice"]:
        result["rbi"] = runners_scored

    return result

# --- Extract parsed stats from play-by-play ---
def extract_parsed_stats(url):
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        page.goto(url)
        soup = BeautifulSoup(page.content(), "html.parser")
        browser.close()

    table = soup.find("table", id="play_by_play")
    if table is None:
        return pd.DataFrame()

    df = pd.read_html(StringIO(str(table)))[0]  # Read HTML table into dataframe (grabs first table which we know is PBP)
    df = df[df['Inn'].notna() & df['Play Description'].notna() & df['Batter'].notna()]  # Remove rows with missing required fields
    df = df[~df['Batter'].str.contains("Top of the|Bottom of the|inning", case=False, na=False)]  # remove header rows
    df = df[~df['Batter'].str.contains("Team Totals", case=False, na=False)]  # remove summary rows
    df['Batter'] = df['Batter'].apply(normalize_name)  # clean player names

    stats = []
    for _, row in df.iterrows():
        parsed = parse_result_expanded(row['Play Description'])
        if not parsed['is_plate_appearance']:
            continue
        stats.append({
            "batter": row['Batter'],
            "AB": int(not parsed['is_walk'] and not parsed['is_hbp'] and not parsed['is_sacrifice']),
            "H": int(parsed['is_hit']),
            "HR": int(parsed['is_home_run']),
            "RBI": parsed['rbi'],
            "BB": int(parsed['is_walk']),
            "SO": int(parsed['is_strikeout']),
            "SB": parsed['SB'],
            "CS": parsed['CS'],
            "2B": parsed['2B'],
            "3B": parsed['3B'],
            "SF": parsed['SF']
        })

    if not stats:
        return pd.DataFrame(columns=["batter", "AB", "H", "HR", "RBI", "BB", "SO", "SB", "CS", "2B", "3B", "SF"])

    return pd.DataFrame(stats).groupby("batter").sum(numeric_only=True).reset_index()

# Helper Function to parse details section of box score
def parse_details_column(details_str):
    stats = {"HR": 0, "2B": 0, "3B": 0, "SB": 0, "CS": 0, "SF": 0}
    if pd.isna(details_str):
        return stats

    parts = [p.strip() for p in str(details_str).split(",")]
    for part in parts:
        match = re.match(r"(\d+)·(HR|2B|3B|SB|CS|SF|GDP)", part)
        if match:
            count, stat = match.groups()
            stats[stat] += int(count)
        elif part in stats:
            stats[part] += 1
    return stats

# --- Extract official stats from box score ---
def extract_box_score_stats(url):
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        page.goto(url)
        soup = BeautifulSoup(page.content(), "html.parser")
        browser.close()

    all_dfs = []
    for tbl in soup.find_all("table"):
        table_id = tbl.get("id", "")
        if not table_id.endswith("batting"):
            continue

        df = pd.read_html(StringIO(str(tbl)))[0]
        df = df[df['Batting'].notna()]
        df = df[~df['Batting'].str.contains("Team Totals", na=False)]
        df['batter'] = df['Batting'].apply(normalize_name)

        # Extract additional stats from 'Details' column if present
        if 'Details' in df.columns:
            parsed_stats = df['Details'].apply(parse_details_column).apply(pd.Series)
            for stat in ['SB', 'CS', '2B', '3B', 'SF', 'HR', 'GDP']:
                df[stat] = parsed_stats.get(stat, 0)
        else:
            for stat in ['SB', 'CS', '2B', '3B', 'SF', 'HR', 'GDP']:
                df[stat] = 0

        all_dfs.append(df)

    combined = pd.concat(all_dfs, ignore_index=True)
    cols_to_keep = ['batter', 'AB', 'H', 'RBI', 'BB', 'SO', 'HR', 'SB', 'CS', '2B', '3B', 'SF', 'GDP']
    for col in cols_to_keep[1:]:
        if col not in combined.columns:
            combined[col] = 0

    combined = combined[cols_to_keep].copy()
    combined.columns = ["batter"] + [f"{col}_official" for col in combined.columns if col != "batter"]
    return combined.reset_index(drop=True)

# --- Compare parsed and official stats ---
def validate_game(url):
    parsed_df = extract_parsed_stats(url)
    official_df = extract_box_score_stats(url)
    merged = pd.merge(parsed_df, official_df, how="inner", on="batter")

    for col in ['AB', 'H', 'RBI', 'BB', 'SO', 'HR', 'SB', 'CS', '2B', '3B', 'SF']:
        merged[f"{col}_parsed"] = pd.to_numeric(merged.get(col), errors='coerce').fillna(0)
        merged[f"{col}_official"] = pd.to_numeric(merged.get(f"{col}_official"), errors='coerce').fillna(0)
        merged[f"{col}_diff"] = merged[f"{col}_parsed"] - merged[f"{col}_official"]

    return merged[[
        "batter",
        "AB_parsed", "AB_official", "AB_diff",
        "H_parsed", "H_official", "H_diff",
        "RBI_parsed", "RBI_official", "RBI_diff",
        "BB_parsed", "BB_official", "BB_diff",
        "SO_parsed", "SO_official", "SO_diff",
        "HR_parsed", "HR_official", "HR_diff",
        "SB_parsed", "SB_official", "SB_diff",
        "CS_parsed", "CS_official", "CS_diff",
        "2B_parsed", "2B_official", "2B_diff",
        "3B_parsed", "3B_official", "3B_diff",
        "SF_parsed", "SF_official", "SF_diff"
    ]]

# --- Run the validation ---
if __name__ == "__main__":
    game_url = "https://www.baseball-reference.com/boxes/NYA/NYA202506080.shtml"
    validation_df = validate_game(game_url)
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", None)
    pd.set_option("display.max_rows", None)
    print("\n🧾 VALIDATION REPORT:")
    print(validation_df)

    import streamlit as st
    st.title("MLB Stat Validation")
    st.dataframe(validation_df)
