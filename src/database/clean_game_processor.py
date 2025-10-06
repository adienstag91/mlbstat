#!/usr/bin/env python3
"""
MLB Game Data Processor - Clean Production Version
=================================================

Processes MLB games with separate batting/pitching tables.
Supports batch processing and full season builds.
"""

import sqlite3
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
from utils.url_cacher import HighPerformancePageFetcher
from pipeline.game_url_fetcher import get_games_full_season, get_games_by_team

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
    
    def __init__(self, db_path: str = "mlb_games.db", validation_threshold: float = 95.0, 
                 max_workers: int = 3, cache_size_mb: int = 500):
        self.db_path = db_path
        self.validation_threshold = validation_threshold
        self.max_workers = max_workers
        self.db_timeout = 30.0
        self.logger = self._setup_logging()
        
        # Initialize fetcher with caching
        self.fetcher = HighPerformancePageFetcher(
            cache_dir="cache",
            max_cache_size_mb=cache_size_mb
        )
        
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
    
    def build_season(self, game_urls: List[str], season_name: str = "Season") -> Dict[str, Any]:
        """Build a complete season dataset with progress reporting"""
        self.logger.info(f"Building {season_name} with {len(game_urls)} games")
        
        # Process all games
        batch_results = self.process_multiple_games(game_urls, halt_on_failure=False)
        
        # Generate season report
        season_report = self._generate_season_report(batch_results, season_name)
        
        return {
            'season_name': season_name,
            'batch_results': batch_results,
            'season_report': season_report
        }
    
    def get_database_summary(self) -> Dict[str, Any]:
        """Get comprehensive database statistics"""
        with sqlite3.connect(self.db_path) as conn:
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
                    SELECT MIN(date) as earliest, MAX(date) as latest
                    FROM games WHERE date IS NOT NULL
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
        try:
            with sqlite3.connect(self.db_path, timeout=30.0) as conn:
                cursor = conn.cursor()
                
                # Store in dependency order
                players_stored = self._store_players(parsing_results["players_encountered"], cursor)
                game_stored = self._store_game_metadata(parsing_results["game_metadata"], cursor)
                batting_stored = self._store_batting_appearances(parsing_results["batting_appearances"], cursor)
                pitching_stored = self._store_pitching_appearances(parsing_results["pitching_appearances"], cursor)
                events_stored = self._store_play_by_play_events(parsing_results["play_by_play_events"], cursor)
                
                # Store validation report
                self._store_validation_report(parsing_results["game_id"], validation_results, cursor)
                
                conn.commit()
                
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
            return {"status": "error", "error_message": str(e)}
    
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
        import time
        
        stored_count = 0
        fetch_count = 0
        
        for player in players:
            player_id = player.get('player_id')
            player_name = player.get('full_name')
            
            # Check if player exists in database
            cursor.execute(
                "SELECT player_id FROM players WHERE player_id = ?", 
                (player_id,)
            )
            exists = cursor.fetchone()
            
            if exists:
                # Player exists - skip, no action needed
                continue
            else:
                # New player - fetch bio data
                self.logger.info(f"Fetching bio for new player: {player_name} ({player_id})")
                
                bio_data = parse_player_bio(player_id, self.fetcher)
                
                # Insert new player with bio data
                cursor.execute("""
                    INSERT INTO players 
                    (player_id, full_name, bats, throws, birth_date, debut_date, 
                     height_inches, weight_lbs)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
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
    
    def _store_game_metadata(self, game_metadata: Dict, cursor) -> int:
        """Store game to games table"""
        cursor.execute("""
            INSERT OR REPLACE INTO games 
            (game_id, game_date, game_time, home_team, away_team, runs_home_team, runs_away_team, winner, loser, venue, is_playoff, playoff_round, innings_played)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        return cursor.rowcount
    
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
    
    def _store_play_by_play_events(self, events: pd.DataFrame, cursor) -> int:
        """Store play-by-play events to at_bats table"""
        if events.empty:
            return 0
        
        stored_count = 0
        for _, event in events.iterrows():
            cursor.execute("""
                INSERT OR REPLACE INTO at_bats
                (event_id, game_id, inning, inning_half, batter_id, batter_name, pitcher_id, pitcher_name,
                 description, is_at_bat, is_hit, hit_type, is_walk, is_strikeout,
                 is_out, outs_recorded, bases_reached, event_order)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            return overall_validation.status == ValidationResult.PASS
        else:
            return overall_validation.status != ValidationResult.FAIL
    
    def _generate_season_report(self, batch_results: Dict, season_name: str) -> Dict[str, Any]:
        """Generate comprehensive season report"""
        summary = batch_results['summary']
        
        return {
            'season_name': season_name,
            'total_games': summary['total_games'],
            'successful_games': summary['successful_games'],
            'success_rate': summary['success_rate'],
            'total_events': summary['total_events'],
            'avg_batting_accuracy': summary['avg_batting_accuracy'],
            'avg_pitching_accuracy': summary['avg_pitching_accuracy'],
            'processing_time_hours': summary['processing_time_minutes'] / 60,
            'games_per_hour': summary['games_per_minute'] * 60,
            'quality_grade': self._calculate_quality_grade(summary),
            'recommendations': self._generate_recommendations(summary, batch_results['errors'])
        }
    
    def _calculate_quality_grade(self, summary: Dict) -> str:
        """Calculate overall quality grade for the season"""
        success_rate = summary['success_rate']
        avg_accuracy = (summary['avg_batting_accuracy'] + summary['avg_pitching_accuracy']) / 2
        
        if success_rate >= 95 and avg_accuracy >= 98:
            return 'A+'
        elif success_rate >= 90 and avg_accuracy >= 95:
            return 'A'
        elif success_rate >= 80 and avg_accuracy >= 90:
            return 'B'
        elif success_rate >= 70 and avg_accuracy >= 85:
            return 'C'
        else:
            return 'D'
    
    def _generate_recommendations(self, summary: Dict, errors: List) -> List[str]:
        """Generate recommendations for improving data quality"""
        recommendations = []
        
        if summary['success_rate'] < 90:
            recommendations.append("Consider investigating failed games for common parsing issues")
        
        if summary['avg_batting_accuracy'] < 95:
            recommendations.append("Review batting stat validation logic for accuracy improvements")
        
        if summary['avg_pitching_accuracy'] < 95:
            recommendations.append("Review pitching stat validation logic for accuracy improvements")
        
        if len(errors) > summary['total_games'] * 0.1:
            recommendations.append("High error rate detected - consider URL validation and retry logic")
        
        return recommendations
    
    def _extract_game_id_from_url(self, url: str) -> str:
        """Extract game ID from URL for error reporting"""
        try:
            return extract_game_id(url)
        except:
            return url.split('/')[-1].replace('.shtml', '')
    
    def _init_database(self):
        """Initialize database with schema"""
        try:
            with sqlite3.connect(self.db_path, timeout=30.0) as conn:
                cursor = conn.cursor()
                self._create_schema(cursor)
                conn.commit()
        except Exception as e:
            self.logger.error(f"Database initialization failed: {e}")
            raise e
    
    def _create_schema(self, cursor):
        """Create all database tables"""
        
        # Players table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS players (
                player_id VARCHAR(100) PRIMARY KEY,
                full_name VARCHAR(200) NOT NULL,
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
                game_date DATE NOT NULL,
                game_time VARCHAR(20),
                home_team VARCHAR(10) NOT NULL,
                away_team VARCHAR(10) NOT NULL,
                runs_home_team INTEGER,
                runs_away_team INTEGER,
                winner VARCHAR(10),
                loser VARCHAR(10),
                venue VARCHAR(100),
                innings_played INTEGER DEFAULT 9,
                is_playoff BOOLEAN DEFAULT FALSE,
                playoff_round VARCHAR(50),
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
                batting_order INTEGER,
                positions_played VARCHAR(50),
                is_starter BOOLEAN DEFAULT FALSE,
                is_substitute BOOLEAN DEFAULT FALSE,
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
                is_starter BOOLEAN DEFAULT FALSE,
                pitching_order INTEGER,
                decisions VARCHAR(20),
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

        # At-bats table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS at_bats (
                event_id VARCHAR(50) PRIMARY KEY,
                game_id VARCHAR(50) NOT NULL,
                inning INTEGER NOT NULL,
                inning_half VARCHAR(10) NOT NULL,
                batter_id VARCHAR(100) NOT NULL,
                batter_name VARCHAR(100) NOT NULL,
                pitcher_id VARCHAR(100) NOT NULL, 
                pitcher_name VARCHAR(100) NOT NULL,
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

        # Validation reports table
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

        # Create indexes for performance
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_players_name ON players(full_name)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_games_date ON games(game_date)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_games_teams ON games(home_team, away_team)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_batting_game ON batting_appearances(game_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_batting_player ON batting_appearances(player_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_pitching_game ON pitching_appearances(game_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_pitching_player ON pitching_appearances(player_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_at_bats_game ON at_bats(game_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_at_bats_batter ON at_bats(batter_name)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_at_bats_pitcher ON at_bats(pitcher_name)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_validation_game ON validation_reports(game_id)")
    
    def _setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(__name__)


# Convenience functions for common use cases
def process_single_game(game_url: str, db_path: str = "mlb_games.db", 
                       validation_threshold: float = 95.0) -> Dict[str, Any]:
    """Process a single game with default settings"""
    processor = MLBGameProcessor(db_path, validation_threshold)
    return processor.process_single_game(game_url)

def process_game_list(game_urls: List[str], db_path: str = "mlb_games.db",
                     max_workers: int = 3, validation_threshold: float = 95.0) -> Dict[str, Any]:
    """Process a list of games with batch processing"""
    processor = MLBGameProcessor(db_path, validation_threshold, max_workers)
    return processor.process_multiple_games(game_urls)

def build_team_season(team_code: str, year: int, game_urls: List[str],
                     db_path: str = "mlb_games.db") -> Dict[str, Any]:
    """Build a complete team season"""
    processor = MLBGameProcessor(db_path)
    season_name = f"{team_code} {year} Season"
    return processor.build_season(game_urls, season_name)

def get_processing_summary(db_path: str = "mlb_games.db") -> Dict[str, Any]:
    """Get database processing summary"""
    processor = MLBGameProcessor(db_path)
    return processor.get_database_summary()


# Example usage and testing
def demo_clean_processor():
    """Demonstrate the clean processor capabilities"""
    
    print("MLB Game Data Processor - Clean Version")
    print("=" * 50)
    
    # Test URLs
    test_urls = [
        "https://www.baseball-reference.com/boxes/KCA/KCA202503290.shtml",
        "https://www.baseball-reference.com/boxes/NYA/NYA202505050.shtml",
        "https://www.baseball-reference.com/boxes/TEX/TEX202505180.shtml"
    ]
    
    # Initialize processor
    processor = MLBGameProcessor(
        db_path="database/demo_clean.db",
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
        for error in results['errors'][:3]:  # Show first 3
            print(f"  {error['game_id']}: {error['error']}")
    
    # Database summary
    db_summary = processor.get_database_summary()
    print(f"\nDatabase Summary:")
    for key, value in db_summary.items():
        print(f"  {key}: {value}")
    
    return results

def demo_season_build():
    """Demonstrate season building"""
    
    print("\nSeason Building Demo")
    print("=" * 30)
    
    # Sample URLs for a "mini-season"
    season_urls = get_games_by_team("NYY", 2025)
    
    # Build season
    season_results = build_team_season("NYY", 2025, season_urls, "debug_yankees_season.db")
    
    # Print season report
    report = season_results['season_report']
    print(f"Season: {report['season_name']}")
    print(f"Quality Grade: {report['quality_grade']}")
    print(f"Games: {report['successful_games']}/{report['total_games']} ({report['success_rate']:.1f}%)")
    print(f"Processing Time: {report['processing_time_hours']:.1f} hours")
    
    if report['recommendations']:
        print(f"\nRecommendations:")
        for rec in report['recommendations']:
            print(f"  • {rec}")
    
    return season_results


if __name__ == "__main__":
    # Run demos
    print("1. Single Game Processing Demo")
    single_result = process_single_game("https://www.baseball-reference.com/boxes/KCA/KCA202503290.shtml", "demo_single.db")
    if single_result['processing_status'] == 'success':
        print(f"   Success: {single_result['game_id']}")
        validation = single_result['validation_results']
        print(f"   Batting: {validation['batting'].accuracy_percentage:.1f}%")
        print(f"   Pitching: {validation['pitching'].accuracy_percentage:.1f}%")
    else:
        print(f"   Failed: {single_result.get('error_message', 'Unknown error')}")
    
    print("\n2. Batch Processing Demo")
    batch_results = demo_clean_processor()
    
    print("\n3. Season Building Demo")
    season_results = demo_season_build()
    
    print("\nClean MLB Game Data Processor ready for production use!")