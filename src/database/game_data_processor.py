# database/game_data_processor_v2.py
"""
Updated MLB Game Data Processing Pipeline
========================================

Handles separate batting and pitching appearances tables.
Supports Ohtani-style two-way players without data loss.
"""

import sqlite3
import pandas as pd
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
from enum import Enum
import logging
from datetime import datetime
import time
import os

# Import your updated parsing modules
from parsing.appearances_parser import (
    parse_batting_appearances, parse_pitching_appearances,
    get_batting_stats_for_validation, get_pitching_stats_for_validation
)
from parsing.events_parser import parse_play_by_play_events
from parsing.game_metadata_parser import extract_game_metadata
from parsing.parsing_utils import extract_game_id
from validation.stat_validator import validate_batting_stats, validate_pitching_stats
from utils.url_cacher import HighPerformancePageFetcher

class ValidationResult(Enum):
    PASS = "pass"
    FAIL = "fail" 
    PARTIAL = "partial"

@dataclass
class ValidationReport:
    """Structured validation results"""
    status: ValidationResult
    accuracy_percentage: float
    missing_stats: List[str]
    discrepancies: Dict[str, Any]
    total_official: int
    total_calculated: int

class GameDataProcessor:
    """
    Updated game data processor for separate tables approach
    """
    
    def __init__(self, db_path: str = "mlb_games.db", validation_threshold: float = 95.0):
        self.db_path = db_path
        self.validation_threshold = validation_threshold
        self.logger = self._setup_logging()

        cache_dir = "cache"
        self.fetcher = HighPerformancePageFetcher(
            cache_dir=cache_dir,
            max_cache_size_mb=500
        )

        # Initialize database with updated schema
        self._init_database()
        
    def process_game(self, game_url: str, halt_on_validation_failure: bool = True) -> Dict[str, Any]:
        print("DEBUG: Starting process_game...")
        print(f"DEBUG: Database path: {self.db_path}")
        
        # Check if database file exists and is accessible
        if os.path.exists(self.db_path):
            print("DEBUG: Database file exists")
            try:
                # Quick test connection
                test_conn = sqlite3.connect(self.db_path, timeout=5.0)
                test_conn.close()
                print("DEBUG: Test connection successful")
            except Exception as e:
                print(f"DEBUG: Test connection failed: {e}")
        else:
            print("DEBUG: Database file does not exist yet")
        
        self.logger.info(f"Processing game: {game_url}")
        start_time = time.time()
        
        try:
            # STEP 1: Parse all data types
            parsing_results = self._parse_all_data(game_url)
            
            # STEP 2: Validate stats accuracy 
            validation_results = self._validate_stats(parsing_results)
            
            # STEP 3: Database integration decision
            if self._should_store_data(validation_results, halt_on_validation_failure):
                db_results = self._store_to_database(parsing_results, validation_results)
            else:
                db_results = {"status": "skipped", "reason": "validation_failed"}
                
            processing_time = time.time() - start_time
                
            return {
                "game_url": game_url,
                "game_id": parsing_results["game_id"],
                "timestamp": datetime.now().isoformat(),
                "processing_time": processing_time,
                "parsing_results": parsing_results,
                "validation_results": validation_results,
                "database_results": db_results,
                "processing_status": "success"
            }
            
        except Exception as e:
            processing_time = time.time() - start_time
            self.logger.error(f"Processing failed: {str(e)}")
            return {
                "game_url": game_url,
                "timestamp": datetime.now().isoformat(),
                "processing_time": processing_time,
                "processing_status": "error",
                "error_message": str(e)
            }
    
    def _parse_all_data(self, game_url: str) -> Dict[str, Any]:
        """Parse all required data types from game URL (Updated)"""
        
        print("DEBUG: Starting _parse_all_data...")
        
        # Get HTML soup
        print("DEBUG: Fetching page...")
        soup = self.fetcher.fetch_page(game_url)
        print("DEBUG: Extracting game_id...")
        game_id = extract_game_id(game_url)
        
        # Parse game metadata
        print("DEBUG: Parsing game metadata...")
        game_metadata = extract_game_metadata(soup, game_url)
        
        # Parse appearances separately (no more tuples!)
        print("DEBUG: Parsing batting appearances...")
        batting_appearances = parse_batting_appearances(soup, game_id)
        print("DEBUG: Parsing pitching appearances...")
        pitching_appearances = parse_pitching_appearances(soup, game_id)
        
        # Parse play-by-play events
        print("DEBUG: Parsing play-by-play events...")
        play_by_play_events = parse_play_by_play_events(soup, game_id)
        print(f"DEBUG: Events parsed: {len(play_by_play_events)}")
    
        # If no events, debug why
        if play_by_play_events.empty:
            print("DEBUG: No events found - checking for play-by-play table...")
            pbp_table = soup.find("table", id="play_by_play")
            if pbp_table:
                print("DEBUG: Play-by-play table found")
                try:
                    import pandas as pd
                    from io import StringIO
                    df = pd.read_html(StringIO(str(pbp_table)))[0]
                    print(f"DEBUG: Raw table has {len(df)} rows")
                    print(f"DEBUG: Table columns: {list(df.columns)}")
                    print(f"DEBUG: First few rows:")
                    print(df.head())
                except Exception as e:
                    print(f"DEBUG: Failed to parse table: {e}")
            else:
                print("DEBUG: No play-by-play table found in soup")
        
        # Extract unique players from both appearance tables
        print("DEBUG: Extracting unique players...")
        players_encountered = self._extract_unique_players(batting_appearances, pitching_appearances)
        
        print("DEBUG: _parse_all_data complete")
        
        return {
            "game_id": game_id,
            "game_metadata": game_metadata,
            "batting_appearances": batting_appearances,
            "pitching_appearances": pitching_appearances,
            "play_by_play_events": play_by_play_events,
            "players_encountered": players_encountered
        }
    
    def _validate_stats(self, parsing_results: Dict) -> Dict[str, ValidationReport]:
        """Compare official stats vs calculated play-by-play stats (Updated)"""
        
        validation_results = {}
        
        # Extract validation data from appearance DataFrames
        batting_for_validation = get_batting_stats_for_validation(parsing_results["batting_appearances"])
        pitching_for_validation = get_pitching_stats_for_validation(parsing_results["pitching_appearances"])

        print(f"DEBUG: Batting validation data: {len(batting_for_validation)} records")
        if not batting_for_validation.empty:
            print(f"DEBUG: Batting columns: {list(batting_for_validation.columns)}")
            print(f"DEBUG: First batting record: {batting_for_validation.iloc[0].to_dict()}")
    
        print(f"DEBUG: Pitching validation data: {len(pitching_for_validation)} records")
        if not pitching_for_validation.empty:
            print(f"DEBUG: Pitching columns: {list(pitching_for_validation.columns)}")
            print(f"DEBUG: First pitching record: {pitching_for_validation.iloc[0].to_dict()}")
        
        print(f"DEBUG: Play-by-play events: {len(parsing_results['play_by_play_events'])} records")
        
        
        # Batting validation
        batting_validation = self._validate_batting_accuracy(
            batting_for_validation,
            parsing_results["play_by_play_events"]
        )
        validation_results["batting"] = batting_validation
        
        # Pitching validation  
        pitching_validation = self._validate_pitching_accuracy(
            pitching_for_validation,
            parsing_results["play_by_play_events"]
        )
        validation_results["pitching"] = pitching_validation
        
        # Overall validation status
        overall_status = ValidationResult.PASS
        min_accuracy = min(batting_validation.accuracy_percentage, 
                          pitching_validation.accuracy_percentage)
        
        if min_accuracy < 80.0:
            overall_status = ValidationResult.FAIL
        elif min_accuracy < self.validation_threshold:
            overall_status = ValidationResult.PARTIAL
            
        validation_results["overall"] = ValidationReport(
            status=overall_status,
            accuracy_percentage=min_accuracy,
            missing_stats=[],
            discrepancies={},
            total_official=batting_validation.total_official + pitching_validation.total_official,
            total_calculated=batting_validation.total_calculated + pitching_validation.total_calculated
        )
        
        return validation_results
    
    def _store_to_database(self, parsing_results: Dict, validation_results: Dict) -> Dict[str, Any]:
        """Store all data to separate database tables"""
        
        try:
            with sqlite3.connect(self.db_path, timeout=30.0) as conn:
                cursor = conn.cursor()
                
                # Store in dependency order: players → games → appearances → at_bats
                
                # 1. Store players (referenced by all other tables)
                players_stored = self._store_players(
                    parsing_results["players_encountered"], cursor
                )
                
                # 2. Store game metadata
                game_stored = self._store_game_metadata(
                    parsing_results["game_metadata"], cursor
                )
                
                # 3. Store batting appearances
                batting_stored = self._store_batting_appearances(
                    parsing_results["batting_appearances"], cursor
                )
                
                # 4. Store pitching appearances  
                pitching_stored = self._store_pitching_appearances(
                    parsing_results["pitching_appearances"], cursor
                )
                
                # 5. Store play-by-play events
                events_stored = self._store_play_by_play_events(
                    parsing_results["play_by_play_events"], cursor
                )
                
                # 6. Store validation report
                self._store_validation_report(
                    parsing_results["game_id"], validation_results, cursor
                )
                
                # Commit transaction
                conn.commit()
                
                return {
                    "status": "success",
                    "records_stored": {
                        "players": players_stored,
                        "games": game_stored,  
                        "batting_appearances": batting_stored,
                        "pitching_appearances": pitching_stored,
                        "events": events_stored
                    },
                    "validation_metadata": {
                        "batting_accuracy": validation_results["batting"].accuracy_percentage,
                        "pitching_accuracy": validation_results["pitching"].accuracy_percentage
                    }
                }
                
        except Exception as e:
            self.logger.error(f"Database storage failed: {str(e)}")
            return {"status": "error", "error_message": str(e)}
    
    def _store_batting_appearances(self, batting_df: pd.DataFrame, cursor) -> int:
        """Store batting appearances to dedicated table"""
        if batting_df.empty:
            return 0
            
        stored_count = 0
        for _, row in batting_df.iterrows():
            cursor.execute("""
                INSERT OR REPLACE INTO batting_appearances 
                (game_id, player_id, player_name, team, batting_order, positions_played, 
                 is_starter, is_substitute, PA, AB, H, R, RBI, BB, SO, HR, 
                 doubles, triples, SB, CS, HBP, GDP, SF, SH)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row.get('game_id'), row.get('player_id'), row.get('player_name'),
                row.get('team'), row.get('batting_order'), row.get('positions_played'),
                row.get('is_starter', False), row.get('is_substitute', False),
                row.get('PA', 0), row.get('AB', 0), row.get('H', 0), row.get('R', 0), 
                row.get('RBI', 0), row.get('BB', 0), row.get('SO', 0), row.get('HR', 0),
                row.get('2B', 0), row.get('3B', 0), row.get('SB', 0), row.get('CS', 0),
                row.get('HBP', 0), row.get('GDP', 0), row.get('SF', 0), row.get('SH', 0)
            ))
            stored_count += cursor.rowcount
        return stored_count
    
    def _store_pitching_appearances(self, pitching_df: pd.DataFrame, cursor) -> int:
        """Store pitching appearances to dedicated table"""
        if pitching_df.empty:
            return 0
            
        stored_count = 0
        for _, row in pitching_df.iterrows():
            cursor.execute("""
                INSERT OR REPLACE INTO pitching_appearances
                (game_id, player_id, player_name, team, is_starter, pitching_order, 
                 decisions, BF, H_allowed, R_allowed, ER, BB_allowed, SO_pitched, 
                 HR_allowed, IP, pitches_thrown)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row.get('game_id'), row.get('player_id'), row.get('player_name'),
                row.get('team'), row.get('is_starter', False), row.get('pitching_order'),
                row.get('decisions', ''), row.get('BF', 0), row.get('H_allowed', 0),
                row.get('R_allowed', 0), row.get('ER', 0), row.get('BB_allowed', 0),
                row.get('SO_pitched', 0), row.get('HR_allowed', 0), row.get('IP', 0.0),
                row.get('pitches_thrown', 0)
            ))
            stored_count += cursor.rowcount
        return stored_count
    
    def _extract_unique_players(self, batting_df: pd.DataFrame, pitching_df: pd.DataFrame) -> List[Dict]:
        """Extract unique players from both appearance tables"""
        players = {}
        
        # From batting appearances
        if not batting_df.empty:
            for _, row in batting_df.iterrows():
                if row.get('player_id'):
                    players[row['player_id']] = {
                        'player_id': row['player_id'],
                        'full_name': row.get('player_name', ''),
                        'team': row.get('team', '')
                    }
        
        # From pitching appearances
        if not pitching_df.empty:
            for _, row in pitching_df.iterrows():
                if row.get('player_id'):
                    players[row['player_id']] = {
                        'player_id': row['player_id'],
                        'full_name': row.get('player_name', ''),
                        'team': row.get('team', '')
                    }
        
        return list(players.values())
    
    def _validate_batting_accuracy(self, official_stats: pd.DataFrame, 
                                 events: pd.DataFrame) -> ValidationReport:
        """Compare official batting stats vs calculated from play-by-play"""
        
        if official_stats.empty or events.empty:
            return ValidationReport(
                status=ValidationResult.FAIL,
                accuracy_percentage=0.0,
                missing_stats=["no_data"],
                discrepancies={},
                total_official=0,
                total_calculated=0
            )
        
        # Use your existing validation function
        validation_result = validate_batting_stats(official_stats, events)
        
        accuracy = validation_result.get('accuracy', 0.0)
        
        # Determine status
        if accuracy >= self.validation_threshold:
            status = ValidationResult.PASS
        elif accuracy >= 80.0:
            status = ValidationResult.PARTIAL  
        else:
            status = ValidationResult.FAIL
            
        return ValidationReport(
            status=status,
            accuracy_percentage=accuracy,
            missing_stats=[],
            discrepancies=validation_result.get('differences', []),
            total_official=len(official_stats),
            total_calculated=validation_result.get('players_compared', 0)
        )
    
    def _validate_pitching_accuracy(self, official_stats: pd.DataFrame,
                                  events: pd.DataFrame) -> ValidationReport:
        """Compare official pitching stats vs calculated from play-by-play"""
        
        if official_stats.empty or events.empty:
            return ValidationReport(
                status=ValidationResult.FAIL,
                accuracy_percentage=0.0,
                missing_stats=["no_data"],
                discrepancies={},
                total_official=0,
                total_calculated=0
            )
        
        # Use your existing validation function
        validation_result = validate_pitching_stats(official_stats, events)
        
        accuracy = validation_result.get('accuracy', 0.0)
        
        # Determine status
        if accuracy >= self.validation_threshold:
            status = ValidationResult.PASS
        elif accuracy >= 80.0:
            status = ValidationResult.PARTIAL  
        else:
            status = ValidationResult.FAIL
            
        return ValidationReport(
            status=status,
            accuracy_percentage=accuracy,
            missing_stats=[],
            discrepancies=validation_result.get('differences', []),
            total_official=len(official_stats),
            total_calculated=validation_result.get('players_compared', 0)
        )
    
    def _store_players(self, players: List[Dict], cursor) -> int:
        """Store unique players to players table"""
        stored_count = 0
        for player in players:
            cursor.execute("""
                INSERT OR IGNORE INTO players (player_id, full_name, team)
                VALUES (?, ?, ?)
            """, (
                player.get('player_id'), 
                player.get('full_name'), 
                player.get('team', '')
            ))
            stored_count += cursor.rowcount
        return stored_count
    
    def _store_game_metadata(self, game_metadata: Dict, cursor) -> int:
        """Store game to games table"""
        cursor.execute("""
            INSERT OR REPLACE INTO games 
            (game_id, date, home_team, away_team, venue, attendance)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            game_metadata.get('game_id'),
            game_metadata.get('date'), 
            game_metadata.get('home_team'),
            game_metadata.get('away_team'),
            game_metadata.get('venue', ''),
            game_metadata.get('attendance', 0)
        ))
        return cursor.rowcount
    
    def _store_play_by_play_events(self, events: pd.DataFrame, cursor) -> int:
        """Store play-by-play events to at_bats table"""
        if events.empty:
            return 0
        
        print(f"DEBUG: Storing {len(events)} events to database")
        print(f"DEBUG: Events columns: {list(events.columns)}")
        
        # Check if event_order exists and has values
        if 'event_order' in events.columns:
            print(f"DEBUG: event_order column exists")
            print(f"DEBUG: event_order values: {events['event_order'].tolist()[:5]}")  # First 5 values
            print(f"DEBUG: event_order null count: {events['event_order'].isnull().sum()}")
        else:
            print(f"DEBUG: event_order column MISSING from events DataFrame")
        
        stored_count = 0
        for i, (_, event) in enumerate(events.iterrows()):
            if i < 3:  # Debug first few records
                print(f"DEBUG: Event {i}: event_order = {event.get('event_order', 'MISSING')}")
            
            cursor.execute("""
                INSERT OR REPLACE INTO at_bats
                (event_id, game_id, inning, inning_half, batter_id, pitcher_id,
                 description, is_at_bat, is_hit, hit_type, is_walk, is_strikeout,
                 is_out, outs_recorded, bases_reached, event_order)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                event.get('event_id'), event.get('game_id'),
                event.get('inning'), event.get('inning_half'),
                event.get('batter_id'), event.get('pitcher_id'),
                event.get('description'), event.get('is_at_bat', False),
                event.get('is_hit', False), event.get('hit_type'),
                event.get('is_walk', False), event.get('is_strikeout', False),
                event.get('is_out', False), event.get('outs_recorded', 0),
                event.get('bases_reached', 0), event.get('event_order', 0)  # Default to 0 if missing
            ))
            stored_count += cursor.rowcount
        return stored_count
    
    def _store_validation_report(self, game_id: str, validation_results: Dict, cursor):
        """Store validation results for quality tracking"""
        
        for validation_type, report in validation_results.items():
            if validation_type == "overall":
                continue
                
            cursor.execute("""
                INSERT OR REPLACE INTO validation_reports
                (game_id, validation_type, status, accuracy_percentage, 
                 total_official, total_calculated, discrepancies_count)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                game_id,
                validation_type,
                report.status.value,
                report.accuracy_percentage,
                report.total_official,
                report.total_calculated,
                len(report.discrepancies)
            ))
    
    def _should_store_data(self, validation_results: Dict, halt_on_failure: bool) -> bool:
        """Determine if data should be stored based on validation results"""
        
        overall_validation = validation_results["overall"]
        
        if halt_on_failure:
            # Strict mode: only store if validation passes completely
            return overall_validation.status == ValidationResult.PASS
        else:
            # Lenient mode: store unless validation completely fails
            return overall_validation.status != ValidationResult.FAIL
    
    def _init_database(self):
        """Initialize database with individual table creation"""
        try:
            conn = sqlite3.connect(self.db_path, timeout=30.0)
            cursor = conn.cursor()
            
            # Create tables individually instead of executescript
            self._create_tables_individually(cursor)
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            self.logger.error(f"Database initialization failed: {e}")
            raise e
    
    def _create_tables_individually(self, cursor):
        """Create each table separately to avoid locking issues"""

        # Players table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS players (
                player_id VARCHAR(100) PRIMARY KEY,
                full_name VARCHAR(200) NOT NULL,
                first_name VARCHAR(100),
                last_name VARCHAR(100), 
                team VARCHAR(10),
                position VARCHAR(20),
                bats VARCHAR(1),
                throws VARCHAR(1),
                birth_date DATE,
                debut_date DATE,
                height_inches INTEGER,
                weight_lbs INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Games table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS games (
                game_id VARCHAR(50) PRIMARY KEY,
                date DATE NOT NULL,
                home_team VARCHAR(10) NOT NULL,
                away_team VARCHAR(10) NOT NULL, 
                home_score INTEGER DEFAULT 0,
                away_score INTEGER DEFAULT 0,
                venue VARCHAR(100),
                attendance INTEGER,
                weather VARCHAR(100),
                start_time TIME,
                duration_minutes INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Batting appearances table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS batting_appearances (
                batting_appearance_id INTEGER PRIMARY KEY AUTOINCREMENT,
                game_id VARCHAR(50) NOT NULL,
                player_id VARCHAR(100) NOT NULL,
                player_name VARCHAR(200) NOT NULL,
                team VARCHAR(10) NOT NULL,
                
                -- Batting metadata
                batting_order INTEGER,
                positions_played VARCHAR(50),
                is_starter BOOLEAN DEFAULT FALSE,
                is_substitute BOOLEAN DEFAULT FALSE,
                
                -- Batting statistics
                PA INTEGER DEFAULT 0,
                AB INTEGER DEFAULT 0,
                H INTEGER DEFAULT 0,
                R INTEGER DEFAULT 0,
                RBI INTEGER DEFAULT 0,
                BB INTEGER DEFAULT 0,
                SO INTEGER DEFAULT 0,
                HR INTEGER DEFAULT 0,
                doubles INTEGER DEFAULT 0,
                triples INTEGER DEFAULT 0,
                SB INTEGER DEFAULT 0,
                CS INTEGER DEFAULT 0,
                HBP INTEGER DEFAULT 0,
                GDP INTEGER DEFAULT 0,
                SF INTEGER DEFAULT 0,
                SH INTEGER DEFAULT 0,
                
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                FOREIGN KEY (game_id) REFERENCES games(game_id),
                FOREIGN KEY (player_id) REFERENCES players(player_id)
            )
        """)

        # Pitching appearances table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS pitching_appearances (
                pitching_appearance_id INTEGER PRIMARY KEY AUTOINCREMENT,
                game_id VARCHAR(50) NOT NULL,
                player_id VARCHAR(100) NOT NULL,
                player_name VARCHAR(200) NOT NULL,
                team VARCHAR(10) NOT NULL,
                
                -- Pitching metadata
                is_starter BOOLEAN DEFAULT FALSE,
                pitching_order INTEGER,
                decisions VARCHAR(20),
                
                -- Pitching statistics
                BF INTEGER DEFAULT 0,
                H_allowed INTEGER DEFAULT 0,
                R_allowed INTEGER DEFAULT 0,
                ER INTEGER DEFAULT 0,
                BB_allowed INTEGER DEFAULT 0,
                SO_pitched INTEGER DEFAULT 0,
                HR_allowed INTEGER DEFAULT 0,
                IP DECIMAL(4,1) DEFAULT 0.0,
                pitches_thrown INTEGER DEFAULT 0,
                
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                FOREIGN KEY (game_id) REFERENCES games(game_id),
                FOREIGN KEY (player_id) REFERENCES players(player_id)
            )
        """)

        # At-bats table (unchanged)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS at_bats (
                event_id VARCHAR(50) PRIMARY KEY,
                game_id VARCHAR(50) NOT NULL,
                inning INTEGER NOT NULL,
                inning_half VARCHAR(10) NOT NULL,
                batter_id VARCHAR(100) NOT NULL,
                pitcher_id VARCHAR(100) NOT NULL,
                
                description TEXT NOT NULL,
                pitch_sequence VARCHAR(50),
                pitch_count VARCHAR(10),
                
                is_plate_appearance BOOLEAN DEFAULT TRUE,
                is_at_bat BOOLEAN DEFAULT FALSE,
                is_hit BOOLEAN DEFAULT FALSE,
                hit_type VARCHAR(20),
                is_walk BOOLEAN DEFAULT FALSE,
                is_strikeout BOOLEAN DEFAULT FALSE,
                is_sacrifice_fly BOOLEAN DEFAULT FALSE,
                is_sacrifice_hit BOOLEAN DEFAULT FALSE,
                is_hit_by_pitch BOOLEAN DEFAULT FALSE,
                is_out BOOLEAN DEFAULT FALSE,
                
                outs_before INTEGER DEFAULT 0,
                outs_after INTEGER DEFAULT 0,
                outs_recorded INTEGER DEFAULT 0,
                score_home INTEGER DEFAULT 0,
                score_away INTEGER DEFAULT 0,
                
                runner_1b VARCHAR(100),
                runner_2b VARCHAR(100),
                runner_3b VARCHAR(100),
                bases_reached INTEGER DEFAULT 0,
                
                runs_scored INTEGER DEFAULT 0,
                rbi INTEGER DEFAULT 0,
                
                event_order INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                FOREIGN KEY (game_id) REFERENCES games(game_id),
                FOREIGN KEY (batter_id) REFERENCES players(player_id),
                FOREIGN KEY (pitcher_id) REFERENCES players(player_id)
            )
        """)

        # Validation reports table (unchanged)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS validation_reports (
                report_id INTEGER PRIMARY KEY AUTOINCREMENT,
                game_id VARCHAR(50) NOT NULL,
                validation_type VARCHAR(20) NOT NULL,
                
                status VARCHAR(20) NOT NULL,
                accuracy_percentage DECIMAL(5,2),
                total_official INTEGER DEFAULT 0,
                total_calculated INTEGER DEFAULT 0,
                discrepancies_count INTEGER DEFAULT 0,
                
                discrepancies TEXT,
                missing_stats TEXT,
                
                parsing_duration_ms INTEGER,
                validation_threshold DECIMAL(5,2) DEFAULT 95.0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                FOREIGN KEY (game_id) REFERENCES games(game_id)
            )
        """)

        # Indexes
        #cursor.execute("""
            #CREATE INDEX IF NOT EXISTS idx_players_name ON players(full_name);
            #CREATE INDEX IF NOT EXISTS idx_players_team ON players(team);
            #CREATE INDEX IF NOT EXISTS idx_games_date ON games(date);
            #CREATE INDEX IF NOT EXISTS idx_games_teams ON games(home_team, away_team);
            #CREATE INDEX IF NOT EXISTS idx_batting_game ON batting_appearances(game_id);
            #CREATE INDEX IF NOT EXISTS idx_batting_player ON batting_appearances(player_id);
            #CREATE INDEX IF NOT EXISTS idx_pitching_game ON pitching_appearances(game_id);
            #CREATE INDEX IF NOT EXISTS idx_pitching_player ON pitching_appearances(player_id);
            #CREATE INDEX IF NOT EXISTS idx_at_bats_game ON at_bats(game_id);
            #CREATE INDEX IF NOT EXISTS idx_at_bats_batter ON at_bats(batter_id);
            #CREATE INDEX IF NOT EXISTS idx_at_bats_pitcher ON at_bats(pitcher_id);
            #CREATE INDEX IF NOT EXISTS idx_validation_game ON validation_reports(game_id)
        #""")
    
    def _setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(__name__)

# Test function
def test_separate_tables_processor():
    """Test the updated processor with separate tables"""
    
    processor = GameDataProcessor(
        db_path="test_mlb_separate_tables_NEW.db",
        validation_threshold=95.0
    )
    
    # Test with a known good game
    test_url = "https://www.baseball-reference.com/boxes/LAN/LAN202509160.shtml"
    
    print(f"Testing Updated GameDataProcessor with: {test_url}")
    print("=" * 80)
    
    # Process game
    results = processor.process_game(
        game_url=test_url,
        halt_on_validation_failure=True
    )
    
    # Print results
    if results["processing_status"] == "success":
        print(f"✅ Game processed successfully!")
        print(f"   Game ID: {results['game_id']}")
        print(f"   Processing time: {results['processing_time']:.2f}s")
        
        validation = results["validation_results"]
        print(f"   Batting accuracy: {validation['batting'].accuracy_percentage:.1f}%")
        print(f"   Pitching accuracy: {validation['pitching'].accuracy_percentage:.1f}%")
        print(f"   Overall status: {validation['overall'].status.value}")
        
        if results["database_results"]["status"] == "success":
            records = results["database_results"]["records_stored"]
            print(f"   Database records stored:")
            print(f"     Players: {records['players']}")
            print(f"     Games: {records['games']}")
            print(f"     Batting appearances: {records['batting_appearances']}")
            print(f"     Pitching appearances: {records['pitching_appearances']}")
            print(f"     Events: {records['events']}")
        else:
            print(f"   Database: {results['database_results']['status']}")
            
    else:
        print(f"❌ Processing failed: {results.get('error_message', 'Unknown error')}")
    
    return results

if __name__ == "__main__":
    test_separate_tables_processor()