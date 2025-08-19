import sqlite3

# Connect to (or create) the SQLite database
conn = sqlite3.connect("mlbstat.db")
cursor = conn.cursor()

# Drop old tables if they exist
cursor.executescript("""
DROP TABLE IF EXISTS pitches;
DROP TABLE IF EXISTS at_bats;
DROP TABLE IF EXISTS appearances;
DROP TABLE IF EXISTS players;
DROP TABLE IF EXISTS games;
DROP TABLE IF EXISTS teams;
DROP TABLE IF EXISTS runners;
""")

# Create updated tables

cursor.execute("""
CREATE TABLE teams (
    team_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    abbreviation TEXT NOT NULL UNIQUE,
    league TEXT CHECK (league IN ('AL', 'NL')),
    division TEXT
);
""")

cursor.execute("""
CREATE TABLE players (
    player_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    UNIQUE(name)
);
""")

cursor.execute("""
CREATE TABLE games (
    game_id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL,
    home_team_id INTEGER NOT NULL,
    away_team_id INTEGER NOT NULL,
    FOREIGN KEY (home_team_id) REFERENCES teams(team_id),
    FOREIGN KEY (away_team_id) REFERENCES teams(team_id)
);
""")

cursor.execute("""
CREATE TABLE appearances (
    appearance_id INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id INTEGER NOT NULL,
    player_id INTEGER NOT NULL,
    team_id INTEGER NOT NULL,
    is_starting BOOLEAN,
    batting_order INTEGER,
    position TEXT,
    FOREIGN KEY (game_id) REFERENCES games(game_id),
    FOREIGN KEY (player_id) REFERENCES players(player_id),
    FOREIGN KEY (team_id) REFERENCES teams(team_id)
);
""")

cursor.execute("""
CREATE TABLE at_bats (
    at_bat_id INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id INTEGER NOT NULL,
    batter_id INTEGER NOT NULL,
    pitcher_id INTEGER,
    team_id INTEGER NOT NULL,
    inning INTEGER,
    result TEXT,
    rbi INTEGER,
    is_hit BOOLEAN,
    is_home_run BOOLEAN,
    bases_earned INTEGER,
    FOREIGN KEY (game_id) REFERENCES games(game_id),
    FOREIGN KEY (batter_id) REFERENCES players(player_id),
    FOREIGN KEY (pitcher_id) REFERENCES players(player_id),
    FOREIGN KEY (team_id) REFERENCES teams(team_id)
);
""")



# Commit and close
conn.commit()
conn.close()

print("✅ Database schema has been reset and updated in mlbstat.db")
