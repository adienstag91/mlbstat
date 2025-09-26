# database/query_database.py
"""
MLB Database Query Script
========================

Explore and validate what's stored in your MLB database.
Run various queries to confirm data integrity and structure.
"""

import sqlite3
import pandas as pd
from typing import Dict, Any
import sys
import os

class MLBDatabaseExplorer:
    """Explore and query your MLB database"""
    
    def __init__(self, db_path: str = "test_mlb_separate_tables_NEW.db"):
        self.db_path = db_path
        
    def get_table_info(self) -> Dict[str, Any]:
        """Get basic information about all tables"""
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Get all table names
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            
            table_info = {}
            
            for table in tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                row_count = cursor.fetchone()[0]
                
                cursor.execute(f"PRAGMA table_info({table})")
                columns = [col[1] for col in cursor.fetchall()]
                
                table_info[table] = {
                    'row_count': row_count,
                    'columns': columns
                }
        
        return table_info
    
    def print_database_overview(self):
        """Print a comprehensive overview of the database"""
        
        print("MLB DATABASE OVERVIEW")
        print("=" * 50)
        
        try:
            table_info = self.get_table_info()
            
            for table_name, info in table_info.items():
                print(f"\n{table_name.upper()} TABLE:")
                print(f"  Records: {info['row_count']}")
                print(f"  Columns: {', '.join(info['columns'])}")
        
        except Exception as e:
            print(f"Error accessing database: {e}")
            return
    
    def query_games(self):
        """Show all games in the database"""
        
        print("\nGAMES IN DATABASE:")
        print("-" * 30)
        
        with sqlite3.connect(self.db_path) as conn:
            df = pd.read_sql_query("""
                SELECT game_id, date, home_team, away_team, venue, attendance
                FROM games 
                ORDER BY date DESC
            """, conn)
            
            if df.empty:
                print("No games found")
                return
                
            for _, game in df.iterrows():
                print(f"{game['game_id']}: {game['away_team']} @ {game['home_team']}")
                if game['date']:
                    print(f"  Date: {game['date']}")
                if game['venue']:
                    print(f"  Venue: {game['venue']}")
                if game['attendance']:
                    print(f"  Attendance: {game['attendance']:,}")
                print()
    
    def query_batting_summary(self, limit: int = 10):
        """Show batting performance summary"""
        
        print(f"\nTOP BATTING PERFORMANCES (Top {limit}):")
        print("-" * 40)
        
        with sqlite3.connect(self.db_path) as conn:
            df = pd.read_sql_query(f"""
                SELECT 
                    player_name,
                    team,
                    batting_order,
                    PA, AB, H, HR, RBI, BB, SO,
                    CASE WHEN AB > 0 THEN ROUND(CAST(H AS FLOAT) / AB, 3) ELSE 0 END as AVG
                FROM batting_appearances 
                WHERE PA > 0
                ORDER BY H DESC, HR DESC
                LIMIT {limit}
            """, conn)
            
            if df.empty:
                print("No batting data found")
                return
                
            print(df.to_string(index=False))
    
    def query_pitching_summary(self, limit: int = 10):
        """Show pitching performance summary"""
        
        print(f"\nPITCHING PERFORMANCES (Top {limit}):")
        print("-" * 35)
        
        with sqlite3.connect(self.db_path) as conn:
            df = pd.read_sql_query(f"""
                SELECT 
                    player_name,
                    team,
                    is_starter,
                    decisions,
                    BF, H_allowed, BB_allowed, SO_pitched, HR_allowed
                FROM pitching_appearances 
                WHERE BF > 0
                ORDER BY BF DESC
                LIMIT {limit}
            """, conn)
            
            if df.empty:
                print("No pitching data found")
                return
                
            print(df.to_string(index=False))
    
    def query_play_by_play_sample(self, limit: int = 15):
        """Show sample play-by-play events"""
        
        print(f"\nPLAY-BY-PLAY EVENTS SAMPLE (First {limit}):")
        print("-" * 45)
        
        with sqlite3.connect(self.db_path) as conn:
            df = pd.read_sql_query(f"""
                SELECT 
                    event_order,
                    inning,
                    inning_half,
                    batter_id,
                    pitcher_id,
                    description,
                    is_hit,
                    hit_type,
                    is_walk,
                    is_strikeout
                FROM at_bats 
                ORDER BY event_order
                LIMIT {limit}
            """, conn)
            
            if df.empty:
                print("No play-by-play data found")
                return
            
            for _, event in df.iterrows():
                inning_desc = f"{event['inning_half'].title()} {event['inning']}"
                outcome = []
                if event['is_hit']: outcome.append(f"Hit ({event['hit_type']})")
                if event['is_walk']: outcome.append("Walk")
                if event['is_strikeout']: outcome.append("Strikeout")
                outcome_str = ", ".join(outcome) if outcome else "Other"
                
                print(f"{event['event_order']:2d}. {inning_desc:8s} | {event['batter_id'][:15]:<15s} vs {event['pitcher_id'][:15]:<15s}")
                print(f"     {outcome_str:<15s} | {event['description'][:50]}")
                print()
    
    def query_two_way_players(self):
        """Find players who both hit and pitched (like Ohtani)"""
        
        print("\nTWO-WAY PLAYERS (Appeared in Both Batting and Pitching):")
        print("-" * 55)
        
        with sqlite3.connect(self.db_path) as conn:
            df = pd.read_sql_query("""
                SELECT DISTINCT
                    b.player_name,
                    b.team,
                    b.PA, b.AB, b.H, b.HR,
                    p.BF, p.H_allowed, p.SO_pitched, p.decisions
                FROM batting_appearances b
                INNER JOIN pitching_appearances p 
                    ON b.player_id = p.player_id 
                    AND b.game_id = p.game_id
                ORDER BY b.PA DESC
            """, conn)
            
            if df.empty:
                print("No two-way players found in this game")
                return
            
            for _, player in df.iterrows():
                print(f"{player['player_name']} ({player['team']}):")
                print(f"  Batting: {player['PA']} PA, {player['AB']} AB, {player['H']} H, {player['HR']} HR")
                print(f"  Pitching: {player['BF']} BF, {player['H_allowed']} H allowed, {player['SO_pitched']} SO")
                if player['decisions']:
                    print(f"  Decisions: {player['decisions']}")
                print()
    
    def query_validation_accuracy(self):
        """Show validation results for data quality"""
        
        print("\nVALIDATION ACCURACY:")
        print("-" * 20)
        
        with sqlite3.connect(self.db_path) as conn:
            df = pd.read_sql_query("""
                SELECT 
                    game_id,
                    validation_type,
                    status,
                    accuracy_percentage,
                    total_official,
                    total_calculated,
                    discrepancies_count
                FROM validation_reports 
                ORDER BY game_id, validation_type
            """, conn)
            
            if df.empty:
                print("No validation data found")
                return
            
            print(df.to_string(index=False))
    
    def query_player_stats_aggregated(self):
        """Show aggregated player stats across all games"""
        
        print("\nPLAYER STATS AGGREGATED (All Games):")
        print("-" * 35)
        
        with sqlite3.connect(self.db_path) as conn:
            # Batting aggregation
            batting_df = pd.read_sql_query("""
                SELECT 
                    player_name,
                    COUNT(DISTINCT game_id) as games,
                    SUM(PA) as total_PA,
                    SUM(AB) as total_AB,
                    SUM(H) as total_H,
                    SUM(BB) as total_BB,
                    SUM(HR) as total_HR,
                    SUM(RBI) as total_RBI,
                    SUM(R) as total_R,
                    CASE WHEN SUM(AB) > 0 THEN ROUND(CAST(SUM(H) AS FLOAT) / SUM(AB), 3) ELSE 0 END as AVG
                FROM batting_appearances 
                WHERE PA > 0
                GROUP BY player_name
                HAVING SUM(PA) >= 3
                ORDER BY total_PA DESC
                LIMIT 15
            """, conn)
            
            if not batting_df.empty:
                print("BATTING LEADERS:")
                print(batting_df.to_string(index=False))
            
            print()
            
            # Pitching aggregation  
            pitching_df = pd.read_sql_query("""
                SELECT 
                    player_name,
                    COUNT(DISTINCT game_id) as games,
                    SUM(BF) as total_BF,
                    SUM(H_allowed) as total_H_allowed,
                    SUM(BB_allowed) as total_BB_allowed,
                    SUM(SO_pitched) as total_SO_pitched
                FROM pitching_appearances 
                WHERE BF > 0
                GROUP BY player_name
                ORDER BY total_BF DESC
                LIMIT 10
            """, conn)
            
            if not pitching_df.empty:
                print("PITCHING LEADERS:")
                print(pitching_df.to_string(index=False))
    
    def query_custom(self, sql_query: str):
        """Run a custom SQL query"""
        
        print(f"\nCUSTOM QUERY RESULTS:")
        print("-" * 20)
        print(f"Query: {sql_query}")
        print()
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                df = pd.read_sql_query(sql_query, conn)
                
                if df.empty:
                    print("No results found")
                else:
                    print(df.to_string(index=False))
        
        except Exception as e:
            print(f"Query error: {e}")

def main():
    """Main function to run database exploration"""
    
    # Initialize explorer
    explorer = MLBDatabaseExplorer("test_mlb_separate_tables_NEW.db")
    
    print("MLB DATABASE EXPLORER")
    print("=" * 60)
    
    # 1. Database overview
    explorer.print_database_overview()
    
    # 2. Games information
    explorer.query_games()
    
    # 3. Batting summary
    explorer.query_batting_summary(limit=15)
    
    # 4. Pitching summary
    explorer.query_pitching_summary(limit=10)
    
    # 5. Play-by-play sample
    explorer.query_play_by_play_sample(limit=10)
    
    # 6. Two-way players (Ohtani test)
    explorer.query_two_way_players()
    
    # 7. Validation accuracy
    explorer.query_validation_accuracy()
    
    # 8. Aggregated stats (if multiple games)
    explorer.query_player_stats_aggregated()
    
    print("\n" + "=" * 60)
    print("DATABASE EXPLORATION COMPLETE")
    
    # Interactive mode
    print("\nEnter custom SQL queries (or 'quit' to exit):")
    while True:
        query = input("SQL> ").strip()
        if query.lower() in ['quit', 'exit', 'q']:
            break
        if query:
            explorer.query_custom(query)
            print()

if __name__ == "__main__":
    main()