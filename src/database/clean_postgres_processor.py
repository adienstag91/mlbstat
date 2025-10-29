#!/usr/bin/env python3
"""
MLB Game Data Processor - PostgreSQL Production Version
=======================================================

Processes MLB games with separate batting/pitching tables.
Supports batch processing and full season builds.
"""

import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv
import pandas as pd
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
from enum import Enum
import logging
from datetime import datetime
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import your parsing modules
from parsing.appearances_parser import (
    parse_batting_appearances, parse_pitching_appearances,
    get_batting_stats_for_validation, get_pitching_stats_for_validation
)
from parsing.events_parser import parse_play_by_play_events
from parsing.game_metadata_parser import extract_game_metadata
from parsing.player_bio_parser import parse_player_bio
from parsing.parsing_utils import extract_game_id
from parsing.name_to_id_mapper import build_player_id_mapping, add_player_ids_to_events
from validation.stat_validator import validate_batting_stats, validate_pitching_stats
from utils.url_cacher import HighPerformancePageFetcher, SimpleFetcher
from pipeline.game_url_fetcher import get_games_full_season, get_games_by_team

load_dotenv()

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

class MLBGameProcessor:
    """
    Production MLB game data processor with batch capabilities
    """
    
    def __init__(self, validation_threshold: float = 95.0, 
                 max_workers: int = 2, cache_size_mb: int = 500):
        self.validation_threshold = float(validation_threshold)
        self.max_workers = max_workers
        self.logger = self._setup_logging()

        # PostgreSQL connection parameters from .env
        self.db_params = {
            'host': os.getenv('POSTGRES_HOST', 'localhost'),
            'port': os.getenv('POSTGRES_PORT', '5432'),
            'database': os.getenv('POSTGRES_DB', 'mlb_analytics'),
            'user': os.getenv('POSTGRES_USER', 'postgres'),
            'password': os.getenv('POSTGRES_PASSWORD')
        }
        
        # Initialize fetcher with caching
        self.fetcher = SimpleFetcher()
        
        # Thread lock for database operations
        self.db_lock = threading.Lock()
        
        # Initialize database
        self._init_database()
        
        # Processing stats
        self.stats = {
            'games_processed': 0,
            'games_failed': 0,
            'total_events': 0,
            'avg_batting_accuracy': 0,
            'avg_pitching_accuracy': 0,
            'start_time': None,
            'errors': []
        }
        
    def process_single_game(self, game_url: str, halt_on_validation_failure: bool = True) -> Dict[str, Any]:
        """Process a single game with full validation and storage"""

        # Check if game already exists in database
        if self._game_exists(game_url):
            game_id = extract_game_id(game_url)
            self.logger.info(f"Skipping {game_id} - already in database")
            return {
                "game_url": game_url,
                "game_id": game_id,
                "processing_status": "skipped"
            }

        self.logger.info(f"Processing game: {game_url}")
        start_time = time.time()
        
        try:
            # Parse all data types
            parsing_results = self._parse_all_data(game_url)
            
            # Validate stats accuracy 
            validation_results = self._validate_stats(parsing_results)
            
            # Store to database if validation passes
            if self._should_store_data(validation_results, halt_on_validation_failure):
                with self.db_lock:
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
            self.logger.error(f"Processing failed for {game_url}: {str(e)}")
            return {
                "game_url": game_url,
                "timestamp": datetime.now().isoformat(),
                "processing_time": processing_time,
                "processing_status": "error",
                "error_message": str(e)
            }
    
    def process_multiple_games(self, game_urls: List[str], halt_on_failure: bool = False) -> Dict[str, Any]:
        """Process multiple games with threading and progress tracking"""
        self.stats['start_time'] = time.time()
        self.stats['games_processed'] = 0
        self.stats['games_failed'] = 0
        self.stats['errors'] = []
        
        results = {}
        batting_accuracies = []
        pitching_accuracies = []
        total_events = 0
        
        self.logger.info(f"Starting batch processing: {len(game_urls)} games with {self.max_workers} workers")
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all jobs
            future_to_url = {
                executor.submit(self.process_single_game, url, halt_on_failure): url 
                for url in game_urls
            }
            
            # Process completed jobs
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                
                try:
                    result = future.result()
                    game_id = result.get('game_id', self._extract_game_id_from_url(url))
                    results[game_id] = result
                    
                    if result['processing_status'] == 'success':
                        self.stats['games_processed'] += 1
                        
                        # Collect accuracy metrics
                        validation = result.get('validation_results', {})
                        if 'batting' in validation:
                            batting_accuracies.append(validation['batting'].accuracy_percentage)
                        if 'pitching' in validation:
                            pitching_accuracies.append(validation['pitching'].accuracy_percentage)
                        
                        # Count events
                        events = result.get('parsing_results', {}).get('play_by_play_events', pd.DataFrame())
                        if not events.empty:
                            total_events += len(events)
                        
                        self.logger.info(f"Success: {game_id}")
                        print(f"{game_id} processed in {result['processing_time']:.2f} seconds")
                        print(f"Progress: {self.stats['games_processed']}/{len(game_urls)} games ({self.stats['games_processed']/len(game_urls)*100:.1f}%)")
                        time.sleep(1)
                    elif result['processing_status'] == 'skipped':
                        self.logger.info(f"Skipped: {game_id} (already in DB)")
                        continue  # Don't count as success or failure

                    else:
                        self.stats['games_failed'] += 1
                        self.stats['errors'].append({
                            'game_id': game_id,
                            'error': result.get('error_message', 'Unknown error')
                        })
                        self.logger.error(f"Failed: {game_id} - {result.get('error_message', 'Unknown error')}")
                        
                except Exception as e:
                    game_id = self._extract_game_id_from_url(url)
                    self.stats['games_failed'] += 1
                    self.stats['errors'].append({
                        'game_id': game_id,
                        'error': str(e)
                    })
                    self.logger.error(f"Exception: {game_id} - {str(e)}")
        
        # Calculate final stats
        processing_time = time.time() - self.stats['start_time']
        self.stats['avg_batting_accuracy'] = sum(batting_accuracies) / len(batting_accuracies) if batting_accuracies else 0
        self.stats['avg_pitching_accuracy'] = sum(pitching_accuracies) / len(pitching_accuracies) if pitching_accuracies else 0
        self.stats['total_events'] = total_events
        
        # Summary report
        total_games = len(game_urls)
        success_rate = (self.stats['games_processed'] / total_games * 100) if total_games > 0 else 0
        
        summary = {
            'total_games': total_games,
            'successful_games': self.stats['games_processed'],
            'failed_games': self.stats['games_failed'],
            'success_rate': success_rate,
            'total_events': total_events,
            'avg_batting_accuracy': self.stats['avg_batting_accuracy'],
            'avg_pitching_accuracy': self.stats['avg_pitching_accuracy'],
            'processing_time_minutes': processing_time / 60,
            'games_per_minute': total_games / (processing_time / 60) if processing_time > 0 else 0,
            'error_count': len(self.stats['errors'])
        }
        
        self.logger.info(f"Batch complete: {self.stats['games_processed']}/{total_games} successful ({success_rate:.1f}%)")
        
        return {
            'results': results,
            'summary': summary,
            'errors': self.stats['errors']
        }
    
    def get_database_summary(self) -> Dict[str, Any]:
        """Get comprehensive database statistics from PostgreSQL"""
        conn = psycopg2.connect(**self.db_params)
        summary = {}
        
        # Table counts
        tables = ['players', 'games', 'batting_appearances', 'pitching_appearances', 'at_bats', 'validation_reports']
        for table in tables:
            try:
                count = pd.read_sql(f"SELECT COUNT(*) as count FROM {table}", conn).iloc[0]['count']
                summary[f"{table}_count"] = count
            except Exception:
                summary[f"{table}_count"] = 0
        
        # Date range
        try:
            date_range = pd.read_sql("""
                SELECT MIN(game_date) as earliest, MAX(game_date) as latest
                FROM games WHERE game_date IS NOT NULL
            """, conn)
            if not date_range.empty:
                summary['earliest_game'] = date_range.iloc[0]['earliest']
                summary['latest_game'] = date_range.iloc[0]['latest']
        except Exception:
            summary['earliest_game'] = None
            summary['latest_game'] = None
        
        # Validation averages
        try:
            validation = pd.read_sql("""
                SELECT 
                    AVG(CASE WHEN validation_type = 'batting' THEN accuracy_percentage END) as avg_batting,
                    AVG(CASE WHEN validation_type = 'pitching' THEN accuracy_percentage END) as avg_pitching
                FROM validation_reports
            """, conn)
            if not validation.empty:
                summary['avg_batting_accuracy'] = round(validation.iloc[0]['avg_batting'] or 0, 1)
                summary['avg_pitching_accuracy'] = round(validation.iloc[0]['avg_pitching'] or 0, 1)
        except Exception:
            summary['avg_batting_accuracy'] = 0
            summary['avg_pitching_accuracy'] = 0
        
        conn.close()
        return summary
    
    def _parse_all_data(self, game_url: str) -> Dict[str, Any]:
        """Parse all required data types from game URL"""
        soup = self.fetcher.fetch_page(game_url)
        game_id = extract_game_id(game_url)
        
        # Parse game metadata
        game_metadata = extract_game_metadata(soup, game_url)
        
        # Parse appearances separately
        batting_appearances = parse_batting_appearances(soup, game_id)
        pitching_appearances = parse_pitching_appearances(soup, game_id)

        # Build name-to-ID mapping
        name_to_id_mapping = build_player_id_mapping(batting_appearances, pitching_appearances)
        
        # Parse play-by-play events
        play_by_play_events = parse_play_by_play_events(soup, game_id)

         # Add IDs to events using the mapping
        play_by_play_events = add_player_ids_to_events(play_by_play_events, name_to_id_mapping)
        
        # Extract unique players from both appearance tables
        players_encountered = self._extract_unique_players(batting_appearances, pitching_appearances)
        
        return {
            "game_id": game_id,
            "game_metadata": game_metadata,
            "batting_appearances": batting_appearances,
            "pitching_appearances": pitching_appearances,
            "play_by_play_events": play_by_play_events,
            "players_encountered": players_encountered
        }
    
    def _validate_stats(self, parsing_results: Dict) -> Dict[str, ValidationReport]:
        """Compare official stats vs calculated play-by-play stats"""
        validation_results = {}
        
        # Extract validation data from appearance DataFrames
        batting_for_validation = get_batting_stats_for_validation(parsing_results["batting_appearances"])
        pitching_for_validation = get_pitching_stats_for_validation(parsing_results["pitching_appearances"])
        
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
        conn = None
        try:
            conn = psycopg2.connect(**self.db_params)
            cursor = conn.cursor()

            game_id = parsing_results['game_id']
            
            # Store in dependency order
            #print("DEBUG: Storing players...")
            players_stored = self._store_players(parsing_results["players_encountered"], cursor)
            #print(f"DEBUG: Players stored: {players_stored}")
            
            #print("DEBUG: Storing game metadata...")
            game_stored = self._store_game_metadata(game_id, parsing_results["game_metadata"], cursor)
            #print(f"DEBUG: Game metadata stored: {game_stored}")
            
            #print("DEBUG: Storing batting appearances...")
            batting_stored = self._store_batting_appearances(game_id, parsing_results["batting_appearances"], cursor)
            #print(f"DEBUG: Batting appearances stored: {batting_stored}")
            
            #print("DEBUG: Storing pitching appearances...")
            pitching_stored = self._store_pitching_appearances(game_id, parsing_results["pitching_appearances"], cursor)
            #print(f"DEBUG: Pitching appearances stored: {pitching_stored}")
            
            #print("DEBUG: Storing play-by-play events...")
            events_stored = self._store_play_by_play_events(game_id, parsing_results["play_by_play_events"], cursor)
            #print(f"DEBUG: Events stored: {events_stored}")
            
            # Store validation report
            self._store_validation_report(game_id, validation_results, cursor)
            
            conn.commit()
            cursor.close()
            conn.close()
            
            return {
                "status": "success",
                "records_stored": {
                    "players": players_stored,
                    "games": game_stored,  
                    "batting_appearances": batting_stored,
                    "pitching_appearances": pitching_stored,
                    "events": events_stored
                }
            }
                
        except Exception as e:
            self.logger.error(f"Database storage failed: {str(e)}")
            if conn:
                conn.rollback()
                conn.close()
            return {"status": "error", "error_message": str(e)}
    
    def _validate_batting_accuracy(self, official_stats: pd.DataFrame, events: pd.DataFrame) -> ValidationReport:
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
        
        # Ensure numeric columns are actually numeric
        numeric_cols = ['PA', 'AB', 'H', 'R', 'RBI', 'BB', 'SO', 'HR', '2B', '3B']
        for col in numeric_cols:
            if col in official_stats.columns:
                official_stats[col] = pd.to_numeric(official_stats[col], errors='coerce').fillna(0)
        
        validation_result = validate_batting_stats(official_stats, events)
        accuracy = validation_result.get('accuracy', 0.0)
        
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
    
    def _validate_pitching_accuracy(self, official_stats: pd.DataFrame, events: pd.DataFrame) -> ValidationReport:
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
        
        # Ensure numeric columns are actually numeric
        numeric_cols = ['IP', 'BF', 'H', 'R', 'ER', 'BB', 'SO', 'HR']
        for col in numeric_cols:
            if col in official_stats.columns:
                official_stats[col] = pd.to_numeric(official_stats[col], errors='coerce').fillna(0)
        
        validation_result = validate_pitching_stats(official_stats, events)
        accuracy = validation_result.get('accuracy', 0.0)
        
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
    
    def _store_players(self, players: List[Dict], cursor) -> int:
        """Store players, fetching bio data only for new players"""
        
        stored_count = 0
        fetch_count = 0
        
        for player in players:
            player_id = player.get('player_id')
            player_name = player.get('full_name')
            
            # Check if player exists in database
            cursor.execute(
                "SELECT player_id FROM players WHERE player_id = %s", 
                (player_id,)
            )
            exists = cursor.fetchone()
            
            if not exists:
                # New player - fetch bio data
                self.logger.info(f"Fetching bio for new player: {player_name} ({player_id})")
                bio_data = parse_player_bio(player_id, self.fetcher)
                
                # Insert new player with bio data
                cursor.execute("""
                    INSERT INTO players 
                    (player_id, full_name, bats, throws, birth_date, debut_date, 
                     height_inches, weight_lbs)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    player_id,
                    bio_data.get('full_name') or player_name,
                    bio_data.get('bats'),
                    bio_data.get('throws'),
                    bio_data.get('birth_date'),
                    bio_data.get('debut_date'),
                    bio_data.get('height_inches'),
                    bio_data.get('weight_lbs')
                ))
                
                stored_count += cursor.rowcount
                fetch_count += 1
        
        if fetch_count > 0:
            self.logger.info(f"Fetched bio data for {fetch_count} new players")
        
        return stored_count
    
    def _store_game_metadata(self, game_id: str, game_metadata: Dict, cursor) -> int:
        """Store game to games table using PostgreSQL UPSERT"""
        stored_count = 0
        cursor.execute("""
            INSERT INTO games 
            (game_id, game_date, game_time, home_team, away_team, runs_home_team, 
             runs_away_team, winner, loser, venue, is_playoff, playoff_round, innings_played)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (game_id) DO UPDATE SET
                game_date = EXCLUDED.game_date,
                game_time = EXCLUDED.game_time,
                home_team = EXCLUDED.home_team,
                away_team = EXCLUDED.away_team,
                runs_home_team = EXCLUDED.runs_home_team,
                runs_away_team = EXCLUDED.runs_away_team,
                winner = EXCLUDED.winner,
                loser = EXCLUDED.loser,
                venue = EXCLUDED.venue,
                is_playoff = EXCLUDED.is_playoff,
                playoff_round = EXCLUDED.playoff_round,
                innings_played = EXCLUDED.innings_played
        """, (
            game_metadata.get('game_id'),
            game_metadata.get('game_date'), 
            game_metadata.get('game_time'),
            game_metadata.get('home_team'),
            game_metadata.get('away_team'),
            game_metadata.get('runs_home_team'),
            game_metadata.get('runs_away_team'),
            game_metadata.get('winner'),
            game_metadata.get('loser'),
            game_metadata.get('venue', ''),
            game_metadata.get('is_playoff'),
            game_metadata.get('playoff_round'),
            game_metadata.get('innings_played')
        ))
        stored_count += cursor.rowcount
        return stored_count
    
    def _store_batting_appearances(self, game_id: str, batting_df: pd.DataFrame, cursor) -> int:
        """Store batting appearances to dedicated table"""
        if batting_df.empty:
            return 0
            
        stored_count = 0
        for _, row in batting_df.iterrows():
            # Check if exists
            cursor.execute("""
                SELECT COUNT(*) FROM batting_appearances 
                WHERE game_id = %s AND player_id = %s
            """, (game_id, row['player_id']))
            
            if cursor.fetchone()[0] > 0:
                continue  # Skip duplicate

            # DEBUG: Print the values being inserted
            #print(f"Inserting batting for {row.get('player_name')}: "
            #   f"PA={row.get('PA')}, AB={row.get('AB')}, ")

            cursor.execute("""
                INSERT INTO batting_appearances 
                (game_id, player_id, player_name, team, batting_order, positions_played, 
                 is_starter, is_substitute, PA, AB, H, R, RBI, BB, SO, HR, 
                 doubles, triples, SB, CS, HBP, GDP, SF, SH)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (game_id, player_id) DO NOTHING
            """, (
                row.get('game_id'), row.get('player_id'), row.get('player_name'),
                row.get('team'), row.get('batting_order') if pd.notna(row.get('batting_order')) else None, 
                row.get('positions_played'), row.get('is_starter', False), row.get('is_substitute', False),
                int(row.get('PA', 0)), int(row.get('AB', 0)), int(row.get('H', 0)), int(row.get('R', 0)), 
                int(row.get('RBI', 0)), int(row.get('BB', 0)), int(row.get('SO', 0)), int(row.get('HR', 0)),
                int(row.get('2B', 0)), int(row.get('3B', 0)), int(row.get('SB', 0)), int(row.get('CS', 0)),
                int(row.get('HBP', 0)), int(row.get('GDP', 0)), int(row.get('SF', 0)), int(row.get('SH', 0))
            ))
            stored_count += cursor.rowcount
        return stored_count
    
    def _store_pitching_appearances(self, game_id: str, pitching_df: pd.DataFrame, cursor) -> int:
        """Store pitching appearances to dedicated table"""
        if pitching_df.empty:
            return 0
            
        stored_count = 0
        for _, row in pitching_df.iterrows():
            # Check if exists
            cursor.execute("""
                SELECT COUNT(*) FROM pitching_appearances 
                WHERE game_id = %s AND player_id = %s
            """, (game_id, row['player_id']))
            
            if cursor.fetchone()[0] > 0:
                continue  # Skip duplicate

            cursor.execute("""
                INSERT INTO pitching_appearances
                (game_id, player_id, player_name, team, is_starter, pitching_order, 
                 decisions, BF, H_allowed, R_allowed, ER, BB_allowed, SO_pitched, 
                 HR_allowed, IP, pitches_thrown)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (game_id, player_id) DO NOTHING
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
    
    def _store_play_by_play_events(self, game_id: str, events: pd.DataFrame, cursor) -> int:
        """Store play-by-play events to at_bats table"""
        if events.empty:
            return 0
        
        stored_count = 0
        for _, event in events.iterrows():
            cursor.execute("""
                INSERT INTO at_bats
                (event_id, game_id, inning, inning_half, batter_id, batter_name, pitcher_id, pitcher_name,
                 description, is_at_bat, is_hit, hit_type, is_walk, is_strikeout,
                 is_out, outs_recorded, bases_reached, event_order)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (event_id) DO NOTHING
            """, (
                event.get('event_id'), event.get('game_id'),
                event.get('inning'), event.get('inning_half'),
                event.get('batter_id'), event.get('batter_name'), 
                event.get('pitcher_id'), event.get('pitcher_name'),
                event.get('description'), event.get('is_at_bat', False),
                event.get('is_hit', False), event.get('hit_type'),
                event.get('is_walk', False), event.get('is_strikeout', False),
                event.get('is_out', False), event.get('outs_recorded', 0),
                event.get('bases_reached', 0), event.get('event_order', 0)
            ))
            stored_count += cursor.rowcount
        return stored_count
    
    def _store_validation_report(self, game_id: str, validation_results: Dict, cursor):
        """Store validation results for quality tracking"""
        for validation_type, report in validation_results.items():
            if validation_type == "overall":
                continue
                
            cursor.execute("""
                INSERT INTO validation_reports
                (game_id, validation_type, status, accuracy_percentage, 
                 total_official, total_calculated, discrepancies_count)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
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
            return overall_validation.status == ValidationResult.PASS
        else:
            return overall_validation.status != ValidationResult.FAIL

    def _game_exists(self, game_url: str) -> bool:
        """Check if game is already in database"""
        try:
            game_id = extract_game_id(game_url)
            conn = psycopg2.connect(**self.db_params)
            cursor = conn.cursor()
            
            cursor.execute("SELECT game_id FROM games WHERE game_id = %s", (game_id,))
            exists = cursor.fetchone() is not None
            
            conn.close()
            return exists
        except Exception as e:
            self.logger.warning(f"Could not check if game exists: {e}")
            return False
    
    def _extract_game_id_from_url(self, url: str) -> str:
        """Extract game ID from URL for error reporting"""
        try:
            return extract_game_id(url)
        except:
            return url.split('/')[-1].replace('.shtml', '')
    
    def _init_database(self):
        """Initialize PostgreSQL database with schema"""
        try:
            conn = psycopg2.connect(**self.db_params)
            cursor = conn.cursor()
            
            # Create tables if they don't exist
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS players (
                    player_id VARCHAR(20) PRIMARY KEY,
                    full_name VARCHAR(100),
                    bats VARCHAR(1),
                    throws VARCHAR(1),
                    birth_date DATE,
                    debut_date DATE,
                    height_inches INTEGER,
                    weight_lbs INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS games (
                    game_id VARCHAR(50) PRIMARY KEY,
                    game_date DATE,
                    game_time TIME,
                    home_team VARCHAR(50),
                    away_team VARCHAR(50),
                    runs_home_team INTEGER,
                    runs_away_team INTEGER,
                    winner VARCHAR(50),
                    loser VARCHAR(50),
                    venue VARCHAR(200),
                    is_playoff BOOLEAN,
                    playoff_round VARCHAR(50),
                    innings_played INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS batting_appearances (
                    id SERIAL PRIMARY KEY,
                    game_id VARCHAR(50) REFERENCES games(game_id),
                    player_id VARCHAR(20) REFERENCES players(player_id),
                    player_name VARCHAR(100),
                    team VARCHAR(50),
                    batting_order INTEGER,
                    positions_played VARCHAR(50),
                    is_starter BOOLEAN,
                    is_substitute BOOLEAN,
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
                    UNIQUE(game_id, player_id)
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS pitching_appearances (
                    id SERIAL PRIMARY KEY,
                    game_id VARCHAR(50) REFERENCES games(game_id),
                    player_id VARCHAR(20) REFERENCES players(player_id),
                    player_name VARCHAR(100),
                    team VARCHAR(50),
                    is_starter BOOLEAN,
                    pitching_order INTEGER,
                    decisions VARCHAR(10),
                    BF INTEGER DEFAULT 0,
                    H_allowed INTEGER DEFAULT 0,
                    R_allowed INTEGER DEFAULT 0,
                    ER INTEGER DEFAULT 0,
                    BB_allowed INTEGER DEFAULT 0,
                    SO_pitched INTEGER DEFAULT 0,
                    HR_allowed INTEGER DEFAULT 0,
                    IP NUMERIC(4,1) DEFAULT 0.0,
                    pitches_thrown INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(game_id, player_id)
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS at_bats (
                    event_id VARCHAR(100) PRIMARY KEY,
                    game_id VARCHAR(50) REFERENCES games(game_id),
                    inning INTEGER,
                    inning_half VARCHAR(10),
                    batter_id VARCHAR(20) REFERENCES players(player_id),
                    batter_name VARCHAR(100),
                    pitcher_id VARCHAR(20) REFERENCES players(player_id),
                    pitcher_name VARCHAR(100),
                    description TEXT,
                    is_at_bat BOOLEAN,
                    is_hit BOOLEAN,
                    hit_type VARCHAR(20),
                    is_walk BOOLEAN,
                    is_strikeout BOOLEAN,
                    is_out BOOLEAN,
                    outs_recorded INTEGER DEFAULT 0,
                    bases_reached INTEGER DEFAULT 0,
                    event_order INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS validation_reports (
                    id SERIAL PRIMARY KEY,
                    game_id VARCHAR(50) REFERENCES games(game_id),
                    validation_type VARCHAR(20),
                    status VARCHAR(20),
                    accuracy_percentage NUMERIC(5,2),
                    total_official INTEGER,
                    total_calculated INTEGER,
                    discrepancies_count INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(game_id, validation_type)
                )
            """)
            
            conn.commit()
            cursor.close()
            conn.close()
            self.logger.info("✅ Database schema initialized")
            
        except Exception as e:
            self.logger.error(f"Database initialization failed: {e}")
            raise e
    
    def _setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(__name__)


# Convenience functions for common use cases
def process_single_game(game_url: str, validation_threshold: float = 99.0) -> Dict[str, Any]:
    """Process a single game with default settings"""
    processor = MLBGameProcessor(validation_threshold)
    return processor.process_single_game(game_url)

def process_game_list(game_urls: List[str], max_workers: int = 2, validation_threshold: float = 99.0) -> Dict[str, Any]:
    """Process a list of games with batch processing"""
    processor = MLBGameProcessor(validation_threshold, max_workers)
    return processor.process_multiple_games(game_urls)

def get_processing_summary() -> Dict[str, Any]:
    """Get database processing summary"""
    processor = MLBGameProcessor()
    return processor.get_database_summary()


# Example usage and testing
def batch_processor(test_urls: List[str]):
    """Demonstrate the game processor capabilities"""
    
    print("MLB Game Data Processor")
    print("=" * 50)
    
    # Initialize processor
    processor = MLBGameProcessor(
        validation_threshold=95.0,
        max_workers=2
    )
    
    print(f"Processing {len(test_urls)} games...")
    
    # Process multiple games
    results = processor.process_multiple_games(test_urls)
    
    # Print summary
    summary = results['summary']
    print(f"\nBatch Processing Summary:")
    print(f"  Total Games: {summary['total_games']}")
    print(f"  Successful: {summary['successful_games']} ({summary['success_rate']:.1f}%)")
    print(f"  Failed: {summary['failed_games']}")
    print(f"  Total Events: {summary['total_events']:,}")
    print(f"  Avg Batting Accuracy: {summary['avg_batting_accuracy']:.1f}%")
    print(f"  Avg Pitching Accuracy: {summary['avg_pitching_accuracy']:.1f}%")
    print(f"  Processing Time: {summary['processing_time_minutes']:.1f} minutes")
    print(f"  Processing Rate: {summary['games_per_minute']:.1f} games/minute")
    
    if results['errors']:
        print(f"\nErrors ({len(results['errors'])}):")
        for error in results['errors']:
            print(f"  {error['game_id']}: {error['error']}")
    
    # Database summary
    db_summary = processor.get_database_summary()
    print(f"\nDatabase Summary:")
    for key, value in db_summary.items():
        print(f"  {key}: {value}")
    
    return results

def full_season_processor(year: str):
    """Demonstrate season building"""
    
    print("\nFull Season Batch Processing")
    print("=" * 30)
    
    # Get all games for the season
    season_urls = get_games_full_season(year)
    
    season_results = batch_processor(season_urls)
    
    return season_results


if __name__ == "__main__":

    # Test URLs
    test_urls = [
        "https://www.baseball-reference.com/boxes/KCA/KCA202503290.shtml",
        "https://www.baseball-reference.com/boxes/NYA/NYA202505050.shtml",
        "https://www.baseball-reference.com/boxes/TEX/TEX202505180.shtml"
    ]

    # Run demos
    print("1. Single Game Processing Demo")
    #single_result = process_single_game("https://www.baseball-reference.com/boxes/NYN/NYN202410080.shtml")
    #if single_result['processing_status'] == 'success':
    #    print(f"   Success: {single_result['game_id']}")
    #    validation = single_result['validation_results']
    #    print(f"   Batting: {validation['batting'].accuracy_percentage:.1f}%")
    #    print(f"   Pitching: {validation['pitching'].accuracy_percentage:.1f}%")
    #else:
    #    print(f"   Failed: {single_result.get('error_message', 'Unknown error')}")
    
    print("\n2. Batch Processing Demo")
    # Uncomment to run batch processing
    #batch_results = batch_processor(test_urls)
    
    print("\n3. Season Building Demo")
    # Uncomment to run full season
    season_results = full_season_processor("2021")
    
    print("\n✅ Clean MLB Game Data Processor ready for production use!")