"""
Unified Game Processor
======================
Single entry point for all game processing with flexible options.

Consolidates:
- pipeline/game_processor.py (parse + validate)
- database/clean_postgres_processor.py (parse + validate + store)
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
from datetime import datetime
from typing import Dict, Any, Optional
import logging

from parsing.parsing_utils import extract_game_id
from parsing.appearances_parser import (
    parse_batting_appearances, 
    parse_pitching_appearances,
    get_batting_stats_for_validation,
    get_pitching_stats_for_validation
)
from parsing.name_to_id_mapper import build_player_id_mapping, add_player_ids_to_events
from parsing.events_parser import parse_play_by_play_events
from parsing.game_metadata_parser import extract_game_metadata
from parsing.player_bio_parser import fetch_player_bio_if_needed
from validation.stat_validator import validate_batting_stats, validate_pitching_stats
from utils.url_cacher import SimpleFetcher

# Import database operations
from database.db_operations import store_game_data
from database.db_connection import check_game_exists

# Create logger at module level (prevents duplicate handlers)
module_logger = logging.getLogger(__name__)

fetcher = SimpleFetcher()

def process_game(
    game_url: str,
    validate: bool = True,
    store: bool = False,
    min_accuracy: float = 100.0,
    halt_on_validation_failure: bool = True,
    skip_if_exists: bool = True,
    logger: Optional[logging.Logger] = None
) -> Dict[str, Any]:
    """
    Unified game processing function with flexible options.
    
    Args:
        game_url: Baseball Reference game URL
        validate: Whether to validate stats against play-by-play (default: True)
        store: Whether to store results in database (default: False)
        min_accuracy: Minimum accuracy required for storage (default: 100.0)
        halt_on_validation_failure: Whether to skip storage if validation fails (default: True)
        skip_if_exists: Skip processing if game already in database (default: True)
        logger: Optional logger instance (creates default if None)
    
    Returns:
        Dict with processing results, validation, and storage status
        
    Examples:
        # Debug/test (parse + validate only):
        result = process_game(url, validate=True, store=False)
        
        # Production (parse + validate + store):
        result = process_game(url, validate=True, store=True, min_accuracy=100.0)
        
        # Quick parse (no validation):
        result = process_game(url, validate=False, store=False)
    """
    
    # Setup logging
    if logger is None:
        logger = module_logger
    
    game_id = extract_game_id(game_url)
    start_time = time.time()
    
    logger.info(f"Processing game: {game_url}")
    
    # =========================================================================
    # STEP 1: Check if game exists (if storing and skip_if_exists=True)
    # =========================================================================
    if store and skip_if_exists:
        if check_game_exists(game_url):
            logger.info(f"Skipping {game_id} - already in database")
            return {
                "game_url": game_url,
                "game_id": game_id,
                "processing_status": "skipped",
                "reason": "already_exists",
                "stored": False
            }
    
    try:
        # =====================================================================
        # STEP 2: Parse all data
        # =====================================================================
        parsing_results = _parse_all_data(game_url, game_id, logger)
        
        # =====================================================================
        # STEP 3: Validate (if requested)
        # =====================================================================
        validation_results = None
        if validate:
            validation_results = _validate_stats(parsing_results, logger)
            
            # Log validation results
            bat_acc = validation_results['batting']['accuracy']
            pit_acc = validation_results['pitching']['accuracy']
            bat_status = "✅" if bat_acc == 100.0 else "⚠️" if bat_acc >= 99.0 else "❌"
            pit_status = "✅" if pit_acc == 100.0 else "⚠️" if pit_acc >= 99.0 else "❌"
            
            logger.info(
                f"{game_id} | {bat_status} Bat: {bat_acc:.1f}% | {pit_status} Pit: {pit_acc:.1f}%"
            )
        
        # =====================================================================
        # STEP 4: Store to database (if requested and validation passes)
        # =====================================================================
        database_results = None
        should_store = False
        
        if store:
            # Determine if we should store based on validation
            if validate:
                bat_acc = validation_results['batting']['accuracy']
                pit_acc = validation_results['pitching']['accuracy']
                
                if halt_on_validation_failure:
                    # Only store if both accuracies meet minimum
                    should_store = bat_acc >= min_accuracy and pit_acc >= min_accuracy
                    if not should_store:
                        logger.warning(
                            f"Skipping storage for {game_id} - validation below threshold "
                            f"(Bat: {bat_acc:.1f}%, Pit: {pit_acc:.1f}%, Min: {min_accuracy:.1f}%)"
                        )
                else:
                    # Store regardless of validation
                    should_store = True
            else:
                # No validation performed, store anyway
                should_store = True
            
            if should_store:
                database_results = store_game_data(
                    parsing_results, 
                    validation_results,
                    fetcher
                )
                
                if database_results.get("status") == "success":
                    logger.info(f"Stored {game_id} to database")
                    storage_succeeded = True
                else:
                    logger.error(f"Storage failed for {game_id}: {database_results.get('error_message')}")
                    storage_succeeded = False
            else:
                database_results = {
                    "status": "skipped",
                    "reason": "validation_failed"
                }
                storage_succeeded = False
        
        # =====================================================================
        # STEP 5: Return comprehensive results
        # =====================================================================
        processing_time = time.time() - start_time
        
        return {
            "game_url": game_url,
            "game_id": game_id,
            "timestamp": datetime.now().isoformat(),
            "processing_time": processing_time,
            "processing_status": "success",
            "parsing_results": parsing_results,
            "validation_results": validation_results,
            "database_results": database_results,
            "stored": storage_succeeded if store else False
        }
        
    except Exception as e:
        processing_time = time.time() - start_time
        logger.error(f"Processing failed for {game_url}: {str(e)}")
        
        return {
            "game_url": game_url,
            "game_id": game_id,
            "timestamp": datetime.now().isoformat(),
            "processing_time": processing_time,
            "processing_status": "error",
            "error_message": str(e),
            "stored": False
        }


def _parse_all_data(game_url: str, game_id: str, logger: logging.Logger) -> Dict[str, Any]:
    """
    Parse all data types from a game page.
    
    Returns:
        Dict with game_id, game_metadata, batting_appearances, pitching_appearances, pbp_events
    """
    
    logger.debug(f"Parsing data for {game_id}")
    
    # Fetch page once
    soup = fetcher.fetch_page(game_url)
    
    # Parse game metadata
    game_metadata = extract_game_metadata(soup, game_url)
    
    # Parse appearances
    batting_appearances = parse_batting_appearances(soup, game_id)
    pitching_appearances = parse_pitching_appearances(soup, game_id)
    
    # Parse play-by-play events
    pbp_events = parse_play_by_play_events(soup, game_id)
    
    # Build name-to-ID mapping from appearances
    name_to_id_mapping = build_player_id_mapping(batting_appearances, pitching_appearances)
    
    # Enrich events with player IDs
    pbp_events = add_player_ids_to_events(pbp_events, name_to_id_mapping)
    
    # Extract unique player IDs (bios will be fetched during storage if needed)
    unique_player_ids = set()
    if not batting_appearances.empty:
        unique_player_ids.update(batting_appearances['player_id'].dropna().unique())
    if not pitching_appearances.empty:
        unique_player_ids.update(pitching_appearances['player_id'].dropna().unique())
    
    return {
        "game_id": game_id,
        "game_metadata": game_metadata,
        "batting_appearances": batting_appearances,
        "pitching_appearances": pitching_appearances,
        "pbp_events": pbp_events,
        "unique_player_ids": unique_player_ids
    }


def _validate_stats(parsing_results: Dict[str, Any], logger: logging.Logger) -> Dict[str, Any]:
    """
    Validate parsed stats against play-by-play events.
    
    Returns:
        Dict with batting and pitching validation results
    """
    
    game_id = parsing_results["game_id"]
    logger.debug(f"Validating stats for {game_id}")
    
    # Convert appearances to validation format
    official_batting = get_batting_stats_for_validation(parsing_results["batting_appearances"])
    official_pitching = get_pitching_stats_for_validation(parsing_results["pitching_appearances"])
    
    # Validate
    batting_validation = validate_batting_stats(official_batting, parsing_results["pbp_events"])
    pitching_validation = validate_pitching_stats(official_pitching, parsing_results["pbp_events"])
    
    return {
        "batting": batting_validation,
        "pitching": pitching_validation
    }


# TODO: Implement in Phase 3
# def _store_to_database(
#     parsing_results: Dict[str, Any],
#     validation_results: Dict[str, Any],
#     logger: logging.Logger
# ) -> Dict[str, Any]:
#     """
#     Store all game data to database.
#     
#     Returns:
#         Dict with storage status and any errors
#     """
#     pass


# =============================================================================
# Convenience wrappers for common use cases
# =============================================================================

def parse_and_validate_game(game_url: str) -> Dict[str, Any]:
    """
    Quick function for testing: parse and validate only (no storage).
    """
    return process_game(game_url, validate=True, store=False)


def process_and_store_game(game_url: str, min_accuracy: float = 100.0) -> Dict[str, Any]:
    """
    Production function: parse, validate, and store if validation passes.
    """
    return process_game(
        game_url, 
        validate=True, 
        store=True, 
        min_accuracy=min_accuracy,
        halt_on_validation_failure=True
    )


def quick_parse_game(game_url: str) -> Dict[str, Any]:
    """
    Fastest parsing: skip validation and storage.
    """
    return process_game(game_url, validate=False, store=False)


# =============================================================================
# Test/Debug
# =============================================================================

if __name__ == "__main__":
    # Test with the Robbie Ray game
    test_url = "https://www.baseball-reference.com/boxes/ARI/ARI201909240.shtml"
    
    print("Testing unified game processor...")
    print("=" * 80)
    
    # Test 1: Parse and validate only
    print("\n1️⃣ Parse + Validate (no storage):")
    result = parse_and_validate_game(test_url)
    print(f"   Status: {result['processing_status']}")
    print(f"   Time: {result['processing_time']:.2f}s")
    if result['validation_results']:
        print(f"   Batting: {result['validation_results']['batting']['accuracy']:.1f}%")
        print(f"   Pitching: {result['validation_results']['pitching']['accuracy']:.1f}%")
    
    # Test 2: Quick parse (no validation)
    print("\n2️⃣ Quick Parse (no validation or storage):")
    result = quick_parse_game(test_url)
    print(f"   Status: {result['processing_status']}")
    print(f"   Time: {result['processing_time']:.2f}s")
    print(f"   Validation performed: {result['validation_results'] is not None}")