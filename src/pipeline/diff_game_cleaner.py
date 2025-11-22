#!/usr/bin/env python3
"""
Find and Clean Games with Validation Diffs - FIXED VERSION
==========================================================

Fixed to match YOUR actual database schema:
- DB params: POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
- Table: validation_reports
- Columns: validation_type, accuracy_percentage, total_official, total_calculated

Usage:
    python find_and_clean_diff_games_FIXED.py [--min-accuracy 99.0] [--dry-run]
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2
from psycopg2 import sql
import pandas as pd
from typing import List, Dict
from dotenv import load_dotenv
import argparse
from datetime import datetime
from sqlalchemy import create_engine

load_dotenv()

class DiffGameCleaner:
    """Find and clean games with validation accuracy issues"""
    
    def __init__(self):
        self.conn = None
        self.engine = None
        self.connect()
    
    def connect(self):
        """Connect to PostgreSQL database using YOUR env variables"""
        try:
            # psycopg2 connection for operations
            self.conn = psycopg2.connect(
                host=os.getenv('POSTGRES_HOST', 'localhost'),
                port=os.getenv('POSTGRES_PORT', '5432'),
                database=os.getenv('POSTGRES_DB', 'mlb_analytics'),
                user=os.getenv('POSTGRES_USER', 'postgres'),
                password=os.getenv('POSTGRES_PASSWORD')
            )
            
            # SQLAlchemy engine for pandas (eliminates warning)
            db_url = (
                f"postgresql://{os.getenv('POSTGRES_USER', 'postgres')}"
                f":{os.getenv('POSTGRES_PASSWORD')}"
                f"@{os.getenv('POSTGRES_HOST', 'localhost')}"
                f":{os.getenv('POSTGRES_PORT', '5432')}"
                f"/{os.getenv('POSTGRES_DB', 'mlb_analytics')}"
            )
            self.engine = create_engine(db_url)
            
            print("✅ Connected to PostgreSQL database")
        except Exception as e:
            print(f"❌ Failed to connect to database: {e}")
            print("\nCheck your .env file has these variables:")
            print("  POSTGRES_HOST")
            print("  POSTGRES_PORT")
            print("  POSTGRES_DB")
            print("  POSTGRES_USER")
            print("  POSTGRES_PASSWORD")
            sys.exit(1)
    
    def find_games_with_diffs(self, min_accuracy: float = 100.0) -> pd.DataFrame:
        """
        Find all games where batting or pitching accuracy is below threshold
        
        Uses YOUR schema:
        - Column: accuracy_percentage (not 'accuracy')
        - Column: validation_type (not 'stat_type')
        - Column: total_official (not 'players_compared')
        
        Args:
            min_accuracy: Minimum accuracy threshold (default: 100.0)
            
        Returns:
            DataFrame with game_id, batting_accuracy, pitching_accuracy, game_date
        """
        query = """
            SELECT DISTINCT
                vr.game_id,
                g.game_date,
                g.home_team,
                g.away_team,
                vr.validation_type,
                vr.accuracy_percentage,
                vr.total_official,
                vr.total_calculated,
                vr.discrepancies_count,
                vr.created_at
            FROM validation_reports vr
            JOIN games g ON vr.game_id = g.game_id
            WHERE vr.accuracy_percentage < %(min_accuracy)s
            ORDER BY g.game_date DESC, vr.game_id, vr.validation_type
        """
        
        # Use SQLAlchemy engine to avoid pandas warning
        df = pd.read_sql(query, self.engine, params={'min_accuracy': min_accuracy})
        
        # Pivot to get batting and pitching in same row
        if not df.empty:
            # First, create separate dataframes for batting and pitching
            batting_df = df[df['validation_type'] == 'batting'].set_index(['game_id', 'game_date', 'home_team', 'away_team'])
            pitching_df = df[df['validation_type'] == 'pitching'].set_index(['game_id', 'game_date', 'home_team', 'away_team'])
            
            # Rename columns with prefixes
            batting_df = batting_df.rename(columns={
                'accuracy_percentage': 'batting_accuracy',
                'total_official': 'batting_total_official',
                'total_calculated': 'batting_total_calculated',
                'discrepancies_count': 'batting_discrepancies'
            })
            
            pitching_df = pitching_df.rename(columns={
                'accuracy_percentage': 'pitching_accuracy',
                'total_official': 'pitching_total_official',
                'total_calculated': 'pitching_total_calculated',
                'discrepancies_count': 'pitching_discrepancies'
            })
            
            # Select only the renamed columns (drop validation_type and created_at)
            batting_df = batting_df[['batting_accuracy', 'batting_total_official', 'batting_total_calculated', 'batting_discrepancies']]
            pitching_df = pitching_df[['pitching_accuracy', 'pitching_total_official', 'pitching_total_calculated', 'pitching_discrepancies']]
            
            # Join them together
            result_df = batting_df.join(pitching_df, how='outer').reset_index()
            
            return result_df
        
        return df
    
    def find_null_accuracy_games(self) -> List[str]:
        """Find games where validation reports have NULL or 0% accuracy"""
        query = """
            SELECT DISTINCT game_id
            FROM validation_reports
            WHERE accuracy_percentage IS NULL 
               OR accuracy_percentage = 0
            ORDER BY game_id
        """
        
        with self.conn.cursor() as cur:
            cur.execute(query)
            results = cur.fetchall()
        
        return [row[0] for row in results]
    
    def get_game_url(self, game_id: str) -> str:
        """
        Convert game_id to Baseball Reference URL
        
        Format: TBA201809270 -> https://www.baseball-reference.com/boxes/TBA/TBA201809270.shtml
        """
        team_code = game_id[:3]
        return f"https://www.baseball-reference.com/boxes/{team_code}/{game_id}.shtml"
    
    def delete_game_records(self, game_id: str, dry_run: bool = False) -> Dict:
        """
        Delete all records for a game from all tables
        
        Order matters due to foreign key constraints:
        1. at_bats (references batting_appearances, pitching_appearances)
        2. batting_appearances (references games, player)
        3. pitching_appearances (references games, player)
        4. validation_reports (references games)
        5. games (parent table)
        
        Args:
            game_id: Game ID to delete
            dry_run: If True, only count records without deleting
            
        Returns:
            Dict with counts of deleted records per table
        """
        tables = [
            'at_bats',
            'batting_appearances',
            'pitching_appearances',
            'validation_reports',
            'games'
        ]
        
        deleted_counts = {}
        
        with self.conn.cursor() as cur:
            for table in tables:
                # Count records
                cur.execute(f"SELECT COUNT(*) FROM {table} WHERE game_id = %s", (game_id,))
                count = cur.fetchone()[0]
                deleted_counts[table] = count
                
                if not dry_run and count > 0:
                    # Delete records
                    cur.execute(f"DELETE FROM {table} WHERE game_id = %s", (game_id,))
        
        if not dry_run:
            self.conn.commit()
        
        return deleted_counts
    
    def generate_reprocessing_report(self, games_df: pd.DataFrame, 
                                     output_dir: str = "reprocessing_reports") -> Dict[str, str]:
        """
        Generate comprehensive reports for games to reprocess
        
        Returns:
            Dict with paths to generated files
        """
        os.makedirs(output_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 1. Game details CSV
        details_file = os.path.join(output_dir, f"games_to_reprocess_{timestamp}.csv")
        games_df.to_csv(details_file, index=False)
        
        # 2. URL list for batch processing
        game_ids = games_df['game_id'].unique()
        urls = [self.get_game_url(gid) for gid in game_ids]
        
        urls_file = os.path.join(output_dir, f"urls_to_reprocess_{timestamp}.txt")
        with open(urls_file, 'w') as f:
            f.write('\n'.join(urls))
        
        # 3. Python list for easy copy-paste
        python_file = os.path.join(output_dir, f"urls_python_list_{timestamp}.py")
        with open(python_file, 'w') as f:
            f.write("# URLs to reprocess\n")
            f.write("# Copy this list into your batch processor\n\n")
            f.write("game_urls = [\n")
            for url in urls:
                f.write(f'    "{url}",\n')
            f.write("]\n")
        
        # 4. Summary report
        summary_file = os.path.join(output_dir, f"reprocessing_summary_{timestamp}.txt")
        with open(summary_file, 'w') as f:
            f.write("=" * 70 + "\n")
            f.write("GAMES WITH VALIDATION DIFFERENCES - REPROCESSING REPORT\n")
            f.write("=" * 70 + "\n\n")
            f.write(f"Generated: {timestamp}\n")
            f.write(f"Total games to reprocess: {len(game_ids)}\n\n")
            
            # Accuracy statistics
            if 'batting_accuracy' in games_df.columns:
                f.write(f"Batting Accuracy Range:\n")
                f.write(f"  Min: {games_df['batting_accuracy'].min():.2f}%\n")
                f.write(f"  Max: {games_df['batting_accuracy'].max():.2f}%\n")
                f.write(f"  Avg: {games_df['batting_accuracy'].mean():.2f}%\n\n")
            
            if 'pitching_accuracy' in games_df.columns:
                f.write(f"Pitching Accuracy Range:\n")
                f.write(f"  Min: {games_df['pitching_accuracy'].min():.2f}%\n")
                f.write(f"  Max: {games_df['pitching_accuracy'].max():.2f}%\n")
                f.write(f"  Avg: {games_df['pitching_accuracy'].mean():.2f}%\n\n")
            
            # Date range
            f.write(f"Date Range:\n")
            f.write(f"  Earliest: {games_df['game_date'].min()}\n")
            f.write(f"  Latest: {games_df['game_date'].max()}\n\n")
            
            # Team distribution
            f.write(f"Games by Team:\n")
            teams = pd.concat([games_df['home_team'], games_df['away_team']]).value_counts()
            for team, count in teams.head(10).items():
                f.write(f"  {team}: {count} games\n")
        
        print(f"\n📊 Reports Generated:")
        print(f"  📄 Details: {details_file}")
        print(f"  📄 URLs: {urls_file}")
        print(f"  📄 Python list: {python_file}")
        print(f"  📄 Summary: {summary_file}")
        
        return {
            'details': details_file,
            'urls': urls_file,
            'python_list': python_file,
            'summary': summary_file
        }
    
    def close(self):
        """Close database connections"""
        if self.conn:
            self.conn.close()
        if self.engine:
            self.engine.dispose()


def main():
    """Main execution"""
    parser = argparse.ArgumentParser(
        description='Find and clean games with validation differences'
    )
    parser.add_argument(
        '--min-accuracy',
        type=float,
        default=100.0,
        help='Minimum accuracy threshold (default: 100.0)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be deleted without actually deleting'
    )
    parser.add_argument(
        '--include-null',
        action='store_true',
        help='Also include games with NULL or 0% accuracy'
    )
    parser.add_argument(
        '--delete',
        action='store_true',
        help='Actually delete the game records (requires confirmation)'
    )
    
    args = parser.parse_args()
    
    cleaner = DiffGameCleaner()
    
    print("\n" + "=" * 70)
    print("FINDING GAMES WITH VALIDATION DIFFERENCES")
    print("=" * 70)
    
    # Find games with diffs
    print(f"\n🔍 Searching for games with accuracy_percentage < {args.min_accuracy}%...")
    games_df = cleaner.find_games_with_diffs(args.min_accuracy)
    
    # Find games with NULL/0 accuracy
    null_games = []
    if args.include_null:
        print(f"🔍 Searching for games with NULL or 0% accuracy...")
        null_games = cleaner.find_null_accuracy_games()
        print(f"   Found {len(null_games)} games with NULL/0% accuracy")
    
    if games_df.empty and not null_games:
        print("\n✅ No games found with validation differences!")
        print(f"   All games have accuracy >= {args.min_accuracy}%")
        cleaner.close()
        return
    
    # Combine results
    all_game_ids = set(games_df['game_id'].tolist() if not games_df.empty else [])
    all_game_ids.update(null_games)
    
    print(f"\n📊 Found {len(all_game_ids)} games to reprocess:")
    print(f"   Games with accuracy < {args.min_accuracy}%: {len(games_df)}")
    print(f"   Games with NULL/0% accuracy: {len(null_games)}")
    
    # Show sample
    if not games_df.empty:
        print(f"\n📋 Sample games with diffs:")
        print(games_df.head(10).to_string(index=False))
    
    # Generate reports
    print(f"\n📝 Generating reprocessing reports...")
    
    # Create full DataFrame for all games
    if null_games and not games_df.empty:
        # Add NULL games to DataFrame
        null_df = pd.DataFrame({
            'game_id': null_games,
            'game_date': None,
            'home_team': None,
            'away_team': None
        })
        # Get additional info for null games
        for idx, gid in enumerate(null_games):
            with cleaner.conn.cursor() as cur:
                cur.execute(
                    "SELECT game_date, home_team, away_team FROM games WHERE game_id = %s",
                    (gid,)
                )
                result = cur.fetchone()
                if result:
                    null_df.at[idx, 'game_date'] = result[0]
                    null_df.at[idx, 'home_team'] = result[1]
                    null_df.at[idx, 'away_team'] = result[2]
        
        games_df = pd.concat([games_df, null_df], ignore_index=True)
    elif null_games:
        games_df = pd.DataFrame({
            'game_id': null_games,
            'game_date': None,
            'home_team': None,
            'away_team': None
        })
    
    report_files = cleaner.generate_reprocessing_report(games_df)
    
    # Deletion logic
    if args.delete:
        print(f"\n⚠️  DELETE MODE ENABLED")
        print(f"   This will delete {len(all_game_ids)} games from the database")
        
        if args.dry_run:
            print(f"   DRY RUN - No actual deletion will occur")
        else:
            response = input("\n   Are you sure you want to delete these games? (yes/no): ")
            if response.lower() != 'yes':
                print("   ❌ Deletion cancelled")
                cleaner.close()
                return
        
        print(f"\n🗑️  Deleting game records...")
        total_deleted = {
            'games': 0,
            'batting_appearances': 0,
            'pitching_appearances': 0,
            'at_bats': 0,
            'validation_reports': 0
        }
        
        for i, game_id in enumerate(all_game_ids, 1):
            print(f"   Deleting {game_id} ({i}/{len(all_game_ids)})...", end='')
            deleted = cleaner.delete_game_records(game_id, dry_run=args.dry_run)
            
            for table, count in deleted.items():
                total_deleted[table] += count
            
            print(f" ✓")
        
        print(f"\n📊 Deletion Summary:")
        for table, count in total_deleted.items():
            print(f"   {table}: {count} records {'would be ' if args.dry_run else ''}deleted")
        
        if not args.dry_run:
            print(f"\n✅ All game records deleted successfully!")
    else:
        print(f"\n💡 To delete these games from the database, run:")
        print(f"   python find_and_clean_diff_games_FIXED.py --min-accuracy {args.min_accuracy} --delete")
        print(f"   (Add --dry-run to see what would be deleted without deleting)")
    
    print(f"\n🚀 Next Steps:")
    print(f"   1. Review the generated reports in reprocessing_reports/")
    print(f"   2. Copy the URLs from {report_files['urls']}")
    print(f"   3. Run through your batch processor to reprocess")
    print(f"   4. Run through your debugger to analyze patterns")
    
    cleaner.close()


if __name__ == "__main__":
    main()