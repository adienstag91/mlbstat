import sqlite3

conn = sqlite3.connect("mlbstat.db")
cursor = conn.cursor()

# Insert teams
cursor.execute("INSERT OR IGNORE INTO teams (name, abbreviation, league, division) VALUES (?, ?, ?, ?)",
               ("New York Yankees", "NYY", "AL", "East"))
cursor.execute("INSERT OR IGNORE INTO teams (name, abbreviation, league, division) VALUES (?, ?, ?, ?)",
               ("Boston Red Sox", "BOS", "AL", "East"))

# Get team IDs
cursor.execute("SELECT team_id FROM teams WHERE abbreviation = 'NYY'")
yankees_id = cursor.fetchone()[0]
cursor.execute("SELECT team_id FROM teams WHERE abbreviation = 'BOS'")
redsox_id = cursor.fetchone()[0]

# Insert players
players = [
    ("Aaron Judge", "R", "R", "RF", yankees_id),
    ("Gerrit Cole", "R", "R", "SP", yankees_id),
    ("Rafael Devers", "L", "R", "3B", redsox_id),
    ("Brayan Bello", "R", "R", "SP", redsox_id),
]

cursor.executemany("""
    INSERT INTO players (name, bats, throws, position, team_id)
    VALUES (?, ?, ?, ?, ?)
""", players)

# Insert a game
cursor.execute("""
    INSERT INTO games (date, home_team_id, away_team_id, venue, game_time, weather)
    VALUES (?, ?, ?, ?, ?, ?)
""", ("2024-04-10", yankees_id, redsox_id, "Yankee Stadium", "7:05 PM", "Clear, 60°F"))

# Get game_id
cursor.execute("SELECT game_id FROM games WHERE date = '2024-04-10'")
game_id = cursor.fetchone()[0]

# Insert appearances
cursor.execute("SELECT player_id FROM players WHERE name = 'Aaron Judge'")
judge_id = cursor.fetchone()[0]
cursor.execute("SELECT player_id FROM players WHERE name = 'Gerrit Cole'")
cole_id = cursor.fetchone()[0]
cursor.execute("SELECT player_id FROM players WHERE name = 'Rafael Devers'")
devers_id = cursor.fetchone()[0]
cursor.execute("SELECT player_id FROM players WHERE name = 'Brayan Bello'")
bello_id = cursor.fetchone()[0]

appearances = [
    (game_id, judge_id, yankees_id, True, 2, 'RF', False),
    (game_id, cole_id, yankees_id, True, None, 'SP', True),
    (game_id, devers_id, redsox_id, True, 3, '3B', False),
    (game_id, bello_id, redsox_id, True, None, 'SP', True),
]

cursor.executemany("""
    INSERT INTO appearances (game_id, player_id, team_id, is_starting, batting_order, position, is_pitcher)
    VALUES (?, ?, ?, ?, ?, ?, ?)
""", appearances)

# Insert at-bats
at_bats = [
    (game_id, judge_id, bello_id, 1, 'Bottom', 'Home Run', 2, 4, 'Ball,Strike,Foul,Hit'),
    (game_id, devers_id, cole_id, 2, 'Top', 'Strikeout', 0, 3, 'Strike,Strike,Strike'),
]

cursor.executemany("""
    INSERT INTO at_bats (game_id, batter_id, pitcher_id, inning, half_inning, result, rbi, pitch_count, pitches)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
""", at_bats)

conn.commit()
conn.close()
