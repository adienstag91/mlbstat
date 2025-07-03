import sqlite3

conn = sqlite3.connect("mlbstat.db")
cursor = conn.cursor()

cursor.execute("""
    SELECT b.name AS batter, p.name AS pitcher, ab.result, ab.rbi
    FROM at_bats ab
    JOIN players b ON ab.batter_id = b.player_id
    JOIN players p ON ab.pitcher_id = p.player_id
    WHERE ab.result = 'Home Run'
""")

for row in cursor.fetchall():
    print(row)

conn.close()