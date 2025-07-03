import sqlite3

conn = sqlite3.connect("mlbstat.db")
cursor = conn.cursor()

# List all tables
print("Tables in DB:")
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
print(cursor.fetchall())

# View schema for 'teams' table
print("\nSchema for 'appearances':")
cursor.execute("PRAGMA table_info(appearances);")
print(cursor.fetchall())

# Preview contents of 'teams'
print("\nSample contents:")
cursor.execute("SELECT * FROM appearances LIMIT 10;")
print(cursor.fetchall())

conn.close()
