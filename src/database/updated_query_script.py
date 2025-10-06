#!/usr/bin/env python3
"""
MLB Database Query Script - Updated for Clean Processor Schema
============================================================

Explore and validate what's stored in your MLB database.
Compatible with the clean MLBGameProcessor schema.
"""

import sqlite3
import pandas as pd
from typing import Dict, Any
import sys
import os

class MLBDatabaseExplorer:
    """Explore and query your MLB database"""
    
    def __init__(self, db_path: str = "mlb_games.db"):
        self.db_path = db_path
        
    def get_table_info(self) -> Dict[str, Any]:
        """Get basic information about all tables"""
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Get all table names
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tables = [row[0] for row in cursor.fetchall()]
                
                table_info = {}
                
                for table in tables:
                    try:
                        cursor.execute(f"SELECT COUNT(*) FROM {table}")
                        row_count = cursor.fetchone()[0]
                        
                        cursor.execute(f"PRAGMA table_info({table})")
                        columns = [col[1] for col in cursor.fetchall()]
                        
                        table_info[table] = {
                            'row_count': row_count,
                            'columns': columns
                        }
                    except Exception as e:
                        table_info[table] = {
                            'row_count': f"Error: {e}",
                            'columns': []
                        }
            
            return table_info
            
        except Exception as e:
            print(f"Database connection error: {e}")
            return {}
    
    def print_database_overview(self):
        """Print a comprehensive overview of the database"""
        
        print("MLB DATABASE OVERVIEW")
        print("=" * 50)
        
        if not os.path.exists(self.db_path):
            print(f"Database file not found: {self.db_path}")
            return
        
        try:
            table_info = self.get_table_info()
            
            if not table_info:
                print("No tables found or database is empty")
                return
            
            total_records = 0
            for table_name, info in table_info.items():
                print(f"\n{table_name.upper()} TABLE:")
                if isinstance(info['row_count'], int):
                    print(f"  Records: {info['row_count']:,}")
                    total_records += info['row_count']
                else:
                    print(f"  Records: {info['row_count']}")
                print(f"  Columns: {', '.join(info['columns'])}")
            
            print(f"\nTOTAL RECORDS ACROSS ALL TABLES: {total_records:,}")
        
        except Exception as e:
            print(f"Error accessing database: {e}")
            return
    
    def query_games(self, limit:int = 10):
        """Show all games in the database"""
        
        print("\nGAMES IN DATABASE:")
        print("-" * 30)
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                df = pd.read_sql_query(f"""
                    SELECT game_id, game_date, venue, home_team, away_team, runs_home_team, runs_away_team, winner, loser
                    FROM games 
                    ORDER BY game_date DESC
                    LIMIT {limit}
                """, conn)
                
                if df.empty:
                    print("No games found")
                    return
                    
                for _, game in df.iterrows():
                    print(f"{game['game_id']}: {game['away_team']} @ {game['home_team']}")
                    if game['game_date']:
                        print(f"  Date: {game['game_date']}")
                    if game['venue']:
                        print(f"  Venue: {game['venue']}")
                    print(f"  Score: {game['away_team']}: {game['runs_away_team']} - {game['home_team']}: {game['runs_home_team']}")
                    print()
        except Exception as e:
            print(f"Error querying games: {e}")
    
    def query_batting_summary(self, limit: int = 10):
        """Show batting performance summary"""
        
        print(f"\nTOP BATTING PERFORMANCES (Top {limit}):")
        print("-" * 40)
        
        try:
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
        except Exception as e:
            print(f"Error querying batting data: {e}")
    
    def query_pitching_summary(self, limit: int = 10):
        """Show pitching performance summary"""
        
        print(f"\nPITCHING PERFORMANCES (Top {limit}):")
        print("-" * 35)
        
        try:
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
        except Exception as e:
            print(f"Error querying pitching data: {e}")
    
    def query_play_by_play_sample(self, limit: int = 15):
        """Show sample play-by-play events"""
        
        print(f"\nPLAY-BY-PLAY EVENTS SAMPLE (First {limit}):")
        print("-" * 45)
        
        try:
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
        except Exception as e:
            print(f"Error querying play-by-play data: {e}")
    
    def query_two_way_players(self):
        """Find players who both hit and pitched (like Ohtani)"""
        
        print("\nTWO-WAY PLAYERS (Appeared in Both Batting and Pitching):")
        print("-" * 55)
        
        try:
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
                    print("No two-way players found in this dataset")
                    return
                
                for _, player in df.iterrows():
                    print(f"{player['player_name']} ({player['team']}):")
                    print(f"  Batting: {player['PA']} PA, {player['AB']} AB, {player['H']} H, {player['HR']} HR")
                    print(f"  Pitching: {player['BF']} BF, {player['H_allowed']} H allowed, {player['SO_pitched']} SO")
                    if player['decisions']:
                        print(f"  Decisions: {player['decisions']}")
                    print()
        except Exception as e:
            print(f"Error querying two-way players: {e}")
    
    def query_validation_accuracy(self, limit: int = 10):
        """Show validation results for data quality"""
        
        print("\nVALIDATION ACCURACY:")
        print("-" * 20)
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                df = pd.read_sql_query(f"""
                    SELECT 
                        game_id,
                        validation_type,
                        status,
                        accuracy_percentage,
                        total_official,
                        total_calculated,
                        discrepancies_count
                    FROM validation_reports 
                    ORDER BY accuracy_percentage, discrepancies_count
                    LIMIT {limit}
                """, conn)
                
                if df.empty:
                    print("No validation data found")
                    return
                
                print(df.to_string(index=False))
        except Exception as e:
            print(f"Error querying validation data: {e}")
    
    def query_player_stats_aggregated(self):
        """Show aggregated player stats across all games"""
        
        print("\nPLAYER STATS AGGREGATED (All Games):")
        print("-" * 35)
        
        try:
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
                else:
                    print("No batting data available for aggregation")
                
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
                else:
                    print("No pitching data available for aggregation")
        except Exception as e:
            print(f"Error querying aggregated stats: {e}")
    
    def query_database_health(self):
        """Check database health and data integrity"""
        
        print("\nDATABASE HEALTH CHECK:")
        print("-" * 25)
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                health_issues = []
                
                # Check for orphaned records
                orphaned_batting = pd.read_sql_query("""
                    SELECT COUNT(*) as count 
                    FROM batting_appearances b
                    LEFT JOIN games g ON b.game_id = g.game_id
                    WHERE g.game_id IS NULL
                """, conn).iloc[0]['count']
                
                if orphaned_batting > 0:
                    health_issues.append(f"Orphaned batting records: {orphaned_batting}")
                
                orphaned_pitching = pd.read_sql_query("""
                    SELECT COUNT(*) as count 
                    FROM pitching_appearances p
                    LEFT JOIN games g ON p.game_id = g.game_id
                    WHERE g.game_id IS NULL
                """, conn).iloc[0]['count']
                
                if orphaned_pitching > 0:
                    health_issues.append(f"Orphaned pitching records: {orphaned_pitching}")
                
                # Check for missing player names
                missing_names = pd.read_sql_query("""
                    SELECT COUNT(*) as count 
                    FROM players 
                    WHERE full_name IS NULL OR full_name = ''
                """, conn).iloc[0]['count']
                
                if missing_names > 0:
                    health_issues.append(f"Players with missing names: {missing_names}")
                
                # Check validation accuracy
                low_accuracy = pd.read_sql_query("""
                    SELECT COUNT(*) as count 
                    FROM validation_reports 
                    WHERE accuracy_percentage < 95.0
                """, conn).iloc[0]['count']
                
                if low_accuracy > 0:
                    health_issues.append(f"Games with <95% validation accuracy: {low_accuracy}")
                
                if health_issues:
                    print("ISSUES FOUND:")
                    for issue in health_issues:
                        print(f"  ⚠️  {issue}")
                else:
                    print("✅ Database appears healthy - no major issues detected")
                    
        except Exception as e:
            print(f"Error checking database health: {e}")
    
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

def test_query_tools(db_path: str = "mlb_games.db"):
    """Main function to run database exploration"""
    
    # Initialize explorer
    explorer = MLBDatabaseExplorer(db_path)
    
    print("MLB DATABASE EXPLORER")
    print("=" * 60)
    print(f"Database: {db_path}")
    print()
    
    # 1. Database overview
    explorer.print_database_overview()
    
    # 2. Database health check
    explorer.query_database_health()
    
    # 3. Games information
    explorer.query_games()
    
    # 4. Batting summary
    explorer.query_batting_summary(limit=15)
    
    # 5. Pitching summary
    explorer.query_pitching_summary(limit=10)
    
    # 6. Play-by-play sample
    explorer.query_play_by_play_sample(limit=10)
    
    # 7. Two-way players (Ohtani test)
    #explorer.query_two_way_players()
    
    # 8. Validation accuracy
    explorer.query_validation_accuracy()
    
    # 9. Aggregated stats (if multiple games)
    explorer.query_player_stats_aggregated()
    
    print("\n" + "=" * 60)
    print("DATABASE EXPLORATION COMPLETE")
    
    # Interactive mode
    print(f"\nInteractive Mode - Database: {db_path}")
    print("Commands:")
    print("  - Enter SQL queries directly")
    print("  - 'tables' - show all tables")
    print("  - 'help' - show this help")
    print("  - 'quit' - exit")
    print()
    
    while True:
        try:
            query = input("SQL> ").strip()
            
            if query.lower() in ['quit', 'exit', 'q']:
                break
            elif query.lower() == 'help':
                print("Available commands:")
                print("  tables - show all table names")
                print("  quit/exit/q - exit interactive mode")
                print("  Any SQL query - execute directly")
            elif query.lower() == 'tables':
                table_info = explorer.get_table_info()
                print("Available tables:")
                for table_name, info in table_info.items():
                    print(f"  {table_name} ({info['row_count']} records)")
            elif query:
                explorer.query_custom(query)
                print()
        except KeyboardInterrupt:
            print("\nExiting...")
            break
        except EOFError:
            print("\nExiting...")
            break

if __name__ == "__main__":
        # Get database path from command line or use default
    if len(sys.argv) > 1:
        db_path = sys.argv[1]
    else:
        db_path = "mlb_games.db"
    test_query_tools(db_path)
