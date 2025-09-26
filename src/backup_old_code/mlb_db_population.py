import sqlite3
import pandas as pd
from typing import Dict, Any, Set
import logging

class MLBDatabasePopulator:
    def __init__(self, db_path: str = "mlb_stats.db"):
        self.db_path = db_path
        self.conn = None
        self.setup_logging()
    
    def setup_logging(self):
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
    
    def connect(self):
        """Connect to SQLite database"""
        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute("PRAGMA foreign_keys = ON")  # Enable foreign key constraints
        return self.conn.cursor()
    
    def create_schema(self):
        """Create all database tables"""
        cursor = self.connect()
        
        # Drop existing tables (careful in production!)
        cursor.executescript("""
            DROP TABLE IF EXISTS events;
            DROP TABLE IF EXISTS appearances;
            DROP TABLE IF EXISTS games; 
            DROP TABLE IF EXISTS players;
        """)
        
        # Create tables in dependency order
        cursor.execute("""
            CREATE TABLE players (
                player_id TEXT PRIMARY KEY,
                full_name TEXT NOT NULL,
                handedness TEXT
            );
        """)
        
        cursor.execute("""
            CREATE TABLE games (
                game_id TEXT PRIMARY KEY,
                date TEXT NOT NULL,
                home_team TEXT,
                away_team TEXT,
                stadium TEXT
            );
        """)
        
        cursor.execute("""
            CREATE TABLE appearances (
                appearance_id INTEGER PRIMARY KEY AUTOINCREMENT,
                game_id TEXT NOT NULL,
                player_id TEXT NOT NULL,
                team TEXT,
                
                -- Batting stats
                batting_order INTEGER,
                PA INTEGER DEFAULT 0,
                AB INTEGER DEFAULT 0,
                R INTEGER DEFAULT 0,
                H INTEGER DEFAULT 0,
                HR INTEGER DEFAULT 0,
                RBI INTEGER DEFAULT 0,
                SB INTEGER DEFAULT 0,
                CS INTEGER DEFAULT 0,
                BB INTEGER DEFAULT 0,
                SO INTEGER DEFAULT 0,
                GDP INTEGER DEFAULT 0,
                
                -- Pitching stats
                decision TEXT,
                IP REAL DEFAULT 0,
                BF INTEGER DEFAULT 0,
                H_allowed INTEGER DEFAULT 0,
                R_allowed INTEGER DEFAULT 0,
                ER INTEGER DEFAULT 0,
                BB_allowed INTEGER DEFAULT 0,
                SO_pitched INTEGER DEFAULT 0,
                
                FOREIGN KEY (game_id) REFERENCES games(game_id),
                FOREIGN KEY (player_id) REFERENCES players(player_id),
                UNIQUE(game_id, player_id)
            );
        """)
        
        cursor.execute("""
            CREATE TABLE events (
                event_id TEXT PRIMARY KEY,
                game_id TEXT NOT NULL,
                date TEXT NOT NULL,
                home_team TEXT,
                away_team TEXT,
                inning INTEGER,
                inning_half TEXT,
                
                -- Players
                batter_id TEXT,
                pitcher_id TEXT,
                
                -- Outcome flags
                is_plate_appearance BOOLEAN,
                is_at_bat BOOLEAN,
                is_hit BOOLEAN,
                is_home_run BOOLEAN,
                is_walk BOOLEAN,
                is_strikeout BOOLEAN,
                
                -- Metrics
                bases_reached INTEGER,
                rbi INTEGER,
                outs_recorded INTEGER,
                hit_type TEXT,
                
                FOREIGN KEY (batter_id) REFERENCES players(player_id),
                FOREIGN KEY (pitcher_id) REFERENCES players(player_id),
                FOREIGN KEY (game_id) REFERENCES games(game_id)
            );
        """)
        
        # Create indexes for your sample queries
        cursor.execute("CREATE INDEX idx_events_batter ON events(batter_id);")
        cursor.execute("CREATE INDEX idx_events_pitcher ON events(pitcher_id);")
        cursor.execute("CREATE INDEX idx_events_game ON events(game_id);")
        cursor.execute("CREATE INDEX idx_events_date ON events(date);")
        cursor.execute("CREATE INDEX idx_appearances_player ON appearances(player_id);")
        
        self.conn.commit()
        self.logger.info("✅ Database schema created successfully")
    
    def populate_from_game_data(self, game_data: Dict[str, Any]):
        """
        Populate database from your unified game data structure:
        {
            'game_id': 'NYA202507120',
            'unified_events': DataFrame,
            'official_batting': DataFrame,
            'official_pitching': DataFrame,
            'batting_validation': {...},
            'pitching_validation': {...}
        }
        """
        cursor = self.connect()
        game_id = game_data['game_id']
        events_df = game_data['unified_events']
        batting_df = game_data['official_batting']
        pitching_df = game_data['official_pitching']
        
        self.logger.info(f"Processing game {game_id}")
        
        try:
            # 1. Extract and insert unique players
            self._populate_players(events_df, batting_df, pitching_df, cursor)
            
            # 2. Insert game record
            self._populate_game(game_id, events_df, cursor)
            
            # 3. Insert appearances from official stats
            self._populate_appearances(game_id, batting_df, pitching_df, cursor)
            
            # 4. Insert events
            self._populate_events(events_df, cursor)
            
            self.conn.commit()
            self.logger.info(f"✅ Successfully populated {game_id}")
            
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"❌ Failed to populate {game_id}: {e}")
            raise
    
    def _populate_players(self, events_df: pd.DataFrame, batting_df: pd.DataFrame, 
                         pitching_df: pd.DataFrame, cursor):
        """Extract all unique players and insert into players table"""
        all_players = set()
        
        # From events
        all_players.update(events_df['batter_id'].dropna().unique())
        all_players.update(events_df['pitcher_id'].dropna().unique())
        
        # From official stats (these have the actual names)
        player_names = {}
        
        # Get names from batting stats
        if 'Name' in batting_df.columns and 'player_id' in batting_df.columns:
            for _, row in batting_df.iterrows():
                player_names[row['player_id']] = row['Name']
        
        # Get names from pitching stats  
        if 'Name' in pitching_df.columns and 'player_id' in pitching_df.columns:
            for _, row in pitching_df.iterrows():
                player_names[row['player_id']] = row['Name']
        
        # Insert all players
        for player_id in all_players:
            full_name = player_names.get(player_id, f"Unknown Player ({player_id})")
            cursor.execute("""
                INSERT OR IGNORE INTO players (player_id, full_name) 
                VALUES (?, ?)
            """, (player_id, full_name))
    
    def _populate_game(self, game_id: str, events_df: pd.DataFrame, cursor):
        """Insert game record"""
        # Extract game info from events
        if not events_df.empty:
            first_event = events_df.iloc[0]
            date = first_event.get('date', '')
            home_team = first_event.get('home_team', '')
            away_team = first_event.get('away_team', '')
            
            cursor.execute("""
                INSERT OR IGNORE INTO games (game_id, date, home_team, away_team)
                VALUES (?, ?, ?, ?)
            """, (game_id, date, home_team, away_team))
    
    def _populate_appearances(self, game_id: str, batting_df: pd.DataFrame, 
                            pitching_df: pd.DataFrame, cursor):
        """Insert appearances from official batting/pitching stats"""
        
        # Batting appearances
        for _, row in batting_df.iterrows():
            if 'player_id' not in row:
                continue
                
            # Map your DataFrame columns to database columns
            appearance_data = {
                'game_id': game_id,
                'player_id': row['player_id'],
                'team': row.get('team', ''),
                'batting_order': row.get('batting_order', None),
                'PA': int(row.get('PA', 0) or 0),
                'AB': int(row.get('AB', 0) or 0),
                'R': int(row.get('R', 0) or 0),
                'H': int(row.get('H', 0) or 0),
                'HR': int(row.get('HR', 0) or 0),
                'RBI': int(row.get('RBI', 0) or 0),
                'SB': int(row.get('SB', 0) or 0),
                'CS': int(row.get('CS', 0) or 0),
                'BB': int(row.get('BB', 0) or 0),
                'SO': int(row.get('SO', 0) or 0),
                'GDP': int(row.get('GDP', 0) or 0)
            }
            
            cursor.execute("""
                INSERT OR REPLACE INTO appearances (
                    game_id, player_id, team, batting_order, PA, AB, R, H, HR, 
                    RBI, SB, CS, BB, SO, GDP
                ) VALUES (
                    :game_id, :player_id, :team, :batting_order, :PA, :AB, :R, :H, :HR,
                    :RBI, :SB, :CS, :BB, :SO, :GDP
                )
            """, appearance_data)
        
        # Pitching appearances
        for _, row in pitching_df.iterrows():
            if 'player_id' not in row:
                continue
                
            pitching_data = {
                'game_id': game_id,
                'player_id': row['player_id'],
                'team': row.get('team', ''),
                'decision': row.get('decision', ''),
                'IP': float(row.get('IP', 0) or 0),
                'BF': int(row.get('BF', 0) or 0),
                'H_allowed': int(row.get('H', 0) or 0),
                'R_allowed': int(row.get('R', 0) or 0),
                'ER': int(row.get('ER', 0) or 0),
                'BB_allowed': int(row.get('BB', 0) or 0),
                'SO_pitched': int(row.get('SO', 0) or 0)
            }
            
            cursor.execute("""
                INSERT OR REPLACE INTO appearances (
                    game_id, player_id, team, decision, IP, BF, H_allowed, 
                    R_allowed, ER, BB_allowed, SO_pitched
                ) VALUES (
                    :game_id, :player_id, :team, :decision, :IP, :BF, :H_allowed,
                    :R_allowed, :ER, :BB_allowed, :SO_pitched
                )
            """, pitching_data)
    
    def _populate_events(self, events_df: pd.DataFrame, cursor):
        """Insert all events from unified events DataFrame"""
        for _, row in events_df.iterrows():
            # Map your DataFrame columns to database columns
            event_data = {
                'event_id': row.get('event_id', ''),
                'game_id': row.get('game_id', ''),
                'date': row.get('date', ''),
                'home_team': row.get('home_team', ''),
                'away_team': row.get('away_team', ''),
                'inning': int(row.get('inning', 0) or 0),
                'inning_half': row.get('inning_half', ''),
                'batter_id': row.get('batter_id', None),
                'pitcher_id': row.get('pitcher_id', None),
                'is_plate_appearance': bool(row.get('is_plate_appearance', False)),
                'is_at_bat': bool(row.get('is_at_bat', False)),
                'is_hit': bool(row.get('is_hit', False)),
                'is_home_run': bool(row.get('is_home_run', False)),
                'is_walk': bool(row.get('is_walk', False)),
                'is_strikeout': bool(row.get('is_strikeout', False)),
                'bases_reached': int(row.get('bases_reached', 0) or 0),
                'rbi': int(row.get('rbi', 0) or 0),
                'outs_recorded': int(row.get('outs_recorded', 0) or 0),
                'hit_type': row.get('hit_type', '')
            }
            
            cursor.execute("""
                INSERT OR REPLACE INTO events (
                    event_id, game_id, date, home_team, away_team, inning, inning_half,
                    batter_id, pitcher_id, is_plate_appearance, is_at_bat, is_hit,
                    is_home_run, is_walk, is_strikeout, bases_reached, rbi, 
                    outs_recorded, hit_type
                ) VALUES (
                    :event_id, :game_id, :date, :home_team, :away_team, :inning, :inning_half,
                    :batter_id, :pitcher_id, :is_plate_appearance, :is_at_bat, :is_hit,
                    :is_home_run, :is_walk, :is_strikeout, :bases_reached, :rbi,
                    :outs_recorded, :hit_type
                )
            """, event_data)
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()

# Usage example
if __name__ == "__main__":
    # Initialize database
    db = MLBDatabasePopulator("mlb_stats.db")
    db.create_schema()
    
    # Example: Process a single game from your pipeline
    """
    # Your existing game data from the parser
    game_data = {
        'game_id': 'NYA202507120',
        'unified_events': events_df,  # Your unified events DataFrame
        'official_batting': batting_df,  # Official batting stats
        'official_pitching': pitching_df,  # Official pitching stats
        'batting_validation': {'accuracy': 100.0},
        'pitching_validation': {'accuracy': 99.98}
    }
    
    # Populate database
    db.populate_from_game_data(game_data)
    """
    
    # Example: Process multiple games
    """
    game_data_list = [...]  # List of game data from your pipeline
    
    for game_data in game_data_list:
        try:
            db.populate_from_game_data(game_data)
        except Exception as e:
            print(f"Failed to process {game_data['game_id']}: {e}")
    """
    
    db.close()
    print("✅ Database population script ready!")
