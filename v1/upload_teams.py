import sqlite3

# Connect to the database
conn = sqlite3.connect("mlbstat.db")
cursor = conn.cursor()

# List of all 30 MLB teams by league and division
teams = [
    # American League East
    ("Baltimore Orioles", "BAL", "AL", "East"),
    ("Boston Red Sox", "BOS", "AL", "East"),
    ("New York Yankees", "NYY", "AL", "East"),
    ("Tampa Bay Rays", "TBR", "AL", "East"),
    ("Toronto Blue Jays", "TOR", "AL", "East"),

    # American League Central
    ("Chicago White Sox", "CHW", "AL", "Central"),
    ("Cleveland Guardians", "CLE", "AL", "Central"),
    ("Detroit Tigers", "DET", "AL", "Central"),
    ("Kansas City Royals", "KCR", "AL", "Central"),
    ("Minnesota Twins", "MIN", "AL", "Central"),

    # American League West
    ("Houston Astros", "HOU", "AL", "West"),
    ("Los Angeles Angels", "LAA", "AL", "West"),
    ("Oakland Athletics", "OAK", "AL", "West"),
    ("Seattle Mariners", "SEA", "AL", "West"),
    ("Texas Rangers", "TEX", "AL", "West"),

    # National League East
    ("Atlanta Braves", "ATL", "NL", "East"),
    ("Miami Marlins", "MIA", "NL", "East"),
    ("New York Mets", "NYM", "NL", "East"),
    ("Philadelphia Phillies", "PHI", "NL", "East"),
    ("Washington Nationals", "WSN", "NL", "East"),

    # National League Central
    ("Chicago Cubs", "CHC", "NL", "Central"),
    ("Cincinnati Reds", "CIN", "NL", "Central"),
    ("Milwaukee Brewers", "MIL", "NL", "Central"),
    ("Pittsburgh Pirates", "PIT", "NL", "Central"),
    ("St. Louis Cardinals", "STL", "NL", "Central"),

    # National League West
    ("Arizona Diamondbacks", "ARI", "NL", "West"),
    ("Colorado Rockies", "COL", "NL", "West"),
    ("Los Angeles Dodgers", "LAD", "NL", "West"),
    ("San Diego Padres", "SDP", "NL", "West"),
    ("San Francisco Giants", "SFG", "NL", "West"),
]

# Insert all 30 teams into the teams table
cursor.executemany(
    "INSERT OR IGNORE INTO teams (name, abbreviation, league, division) VALUES (?, ?, ?, ?)",
    teams
)

# Commit changes and close connection
conn.commit()
conn.close()

print("✅ All 30 MLB teams inserted into mlbstat.db")
