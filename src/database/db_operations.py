"""
Database Operations
==================
All CRUD operations for storing MLB game data.
"""

import pandas as pd
import logging
from typing import List, Dict, Any

from parsing.player_bio_parser import parse_player_bio

logger = logging.getLogger(__name__)


def store_players(players: List[Dict], cursor, fetcher) -> int:
    """
    Store players, fetching bio data only for new players.
    
    Args:
        players: List of player dicts with player_id and full_name
        cursor: Database cursor
        fetcher: Page fetcher instance for getting bio data
        
    Returns:
        Number of players stored
    """
    stored_count = 0
    fetch_count = 0
    
    for player in players:
        player_id = player.get('player_id')
        player_name = player.get('full_name')
        
        try:
            # Check if player exists in database
            cursor.execute(
                "SELECT player_id FROM players WHERE player_id = %s", 
                (player_id,)
            )
            exists = cursor.fetchone()
            
            if not exists:
                # New player - fetch bio data
                logger.info(f"Fetching bio for new player: {player_name} ({player_id})")
                bio_data = parse_player_bio(player_id, fetcher)
                
                # Log the data we're about to insert
                logger.debug(f"Inserting player {player_id}:")
                logger.debug(f"  full_name: {bio_data.get('full_name') or player_name} (len={len(str(bio_data.get('full_name') or player_name))})")
                logger.debug(f"  bats: {bio_data.get('bats')}")
                logger.debug(f"  throws: {bio_data.get('throws')}")
                
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
        except Exception as e:
            logger.error(f"Failed to store player {player_id} ({player_name}): {e}")
            raise e
    
    if fetch_count > 0:
        logger.info(f"Fetched bio data for {fetch_count} new players")
    
    return stored_count


def store_game_metadata(game_metadata: Dict, cursor) -> int:
    """
    Store game metadata to games table using PostgreSQL UPSERT.
    
    Args:
        game_metadata: Dict with game information
        cursor: Database cursor
        
    Returns:
        Number of rows affected
    """
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
    
    return cursor.rowcount


def store_batting_appearances(game_id: str, batting_df: pd.DataFrame, cursor) -> int:
    """
    Store batting appearances to dedicated table.
    
    Args:
        game_id: Game identifier
        batting_df: DataFrame with batting appearance data
        cursor: Database cursor
        
    Returns:
        Number of rows stored
    """
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


def store_pitching_appearances(game_id: str, pitching_df: pd.DataFrame, cursor) -> int:
    """
    Store pitching appearances to dedicated table.
    
    Args:
        game_id: Game identifier
        pitching_df: DataFrame with pitching appearance data
        cursor: Database cursor
        
    Returns:
        Number of rows stored
    """
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


def store_play_by_play_events(game_id: str, events: pd.DataFrame, cursor) -> int:
    """
    Store play-by-play events to at_bats table.
    
    Args:
        game_id: Game identifier
        events: DataFrame with play-by-play event data
        cursor: Database cursor
        
    Returns:
        Number of events stored
    """
    if events.empty:
        return 0
    
    stored_count = 0
    for _, event in events.iterrows():
        # Convert pandas NaN to None for SQL NULL
        batter_id = event.get('batter_id')
        if pd.isna(batter_id):
            batter_id = None
            
        pitcher_id = event.get('pitcher_id')
        if pd.isna(pitcher_id):
            pitcher_id = None
        
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
            batter_id, event.get('batter_name'), 
            pitcher_id, event.get('pitcher_name'),
            event.get('description'), event.get('is_at_bat', False),
            event.get('is_hit', False), event.get('hit_type'),
            event.get('is_walk', False), event.get('is_strikeout', False),
            event.get('is_out', False), event.get('outs_recorded', 0),
            event.get('bases_reached', 0), event.get('event_order', 0)
        ))
        stored_count += cursor.rowcount
        
    return stored_count


def store_validation_report(game_id: str, validation_results: Dict, cursor):
    """
    Store validation results for quality tracking.
    
    Args:
        game_id: Game identifier
        validation_results: Dict with batting and pitching validation results
        cursor: Database cursor
    """
    for validation_type, report in validation_results.items():
        if validation_type == "overall":
            continue
        
        # Handle both ValidationReport objects and dicts
        if hasattr(report, 'status'):
            # It's a ValidationReport object
            status = report.status.value
            accuracy = report.accuracy_percentage
            total_official = report.total_official
            total_calculated = report.total_calculated
            discrepancies = len(report.discrepancies)
        elif isinstance(report, dict):
            # It's a dict from the new validation structure
            accuracy = report.get('accuracy', 0.0)
            players_compared = report.get('players_compared', 0)
            total_stats = report.get('total_stats', 0)
            total_differences = report.get('total_differences', 0)
            
            # Determine status based on accuracy
            if accuracy == 100.0:
                status = 'success'
            elif accuracy >= 95.0:
                status = 'warning'
            else:
                status = 'failure'
            
            # Map to expected columns
            total_official = players_compared
            total_calculated = players_compared
            discrepancies = total_differences
        else:
            logger.warning(f"Unknown validation report type: {type(report)}")
            continue
            
        cursor.execute("""
            INSERT INTO validation_reports
            (game_id, validation_type, status, accuracy_percentage, 
             total_official, total_calculated, discrepancies_count)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (game_id, validation_type) DO UPDATE SET
                status = EXCLUDED.status,
                accuracy_percentage = EXCLUDED.accuracy_percentage,
                total_official = EXCLUDED.total_official,
                total_calculated = EXCLUDED.total_calculated,
                discrepancies_count = EXCLUDED.discrepancies_count
        """, (
            game_id,
            validation_type,
            status,
            accuracy,
            total_official,
            total_calculated,
            discrepancies
        ))


def store_game_data(parsing_results: Dict, validation_results: Dict, fetcher) -> Dict[str, Any]:
    """
    Main function to store all game data to database.
    
    Args:
        parsing_results: Dict with all parsed game data
        validation_results: Dict with validation results
        fetcher: Page fetcher instance
        
    Returns:
        Dict with storage status and counts
    """
    from database.db_connection import get_connection
    
    game_id = parsing_results["game_id"]
    
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        # Extract unique players from appearances
        unique_players = []
        player_ids_seen = set()
        
        # From batting appearances
        batting_df = parsing_results["batting_appearances"]
        if not batting_df.empty:
            for _, row in batting_df.iterrows():
                player_id = row.get('player_id')
                if player_id and player_id not in player_ids_seen:
                    unique_players.append({
                        'player_id': player_id,
                        'full_name': row.get('player_name')
                    })
                    player_ids_seen.add(player_id)
        
        # From pitching appearances
        pitching_df = parsing_results["pitching_appearances"]
        if not pitching_df.empty:
            for _, row in pitching_df.iterrows():
                player_id = row.get('player_id')
                if player_id and player_id not in player_ids_seen:
                    unique_players.append({
                        'player_id': player_id,
                        'full_name': row.get('player_name')
                    })
                    player_ids_seen.add(player_id)
        
        # Store data in order (respecting foreign keys)
        try:
            players_stored = store_players(unique_players, cursor, fetcher)
            logger.info(f"Stored {players_stored} players")
        except Exception as e:
            logger.error(f"Failed to store players: {e}")
            raise
        
        try:
            # Fix game_id in metadata if it's a URL
            if parsing_results["game_metadata"].get("game_id", "").startswith("http"):
                parsing_results["game_metadata"]["game_id"] = game_id
            
            game_stored = store_game_metadata(parsing_results["game_metadata"], cursor)
            logger.info(f"Stored game metadata (rows affected: {game_stored})")
            logger.info(f"Game ID stored: '{parsing_results['game_metadata'].get('game_id')}'")
            
            # Verify it was actually inserted
            cursor.execute("SELECT game_id FROM games WHERE game_id = %s", (game_id,))
            check = cursor.fetchone()
            if check:
                logger.info(f"✅ Verified game exists in database: {check[0]}")
            else:
                logger.error(f"❌ Game NOT found in database after insert!")
        except Exception as e:
            logger.error(f"Failed to store game metadata: {e}")
            logger.error(f"Game metadata: {parsing_results['game_metadata']}")
            raise
        
        try:
            batting_stored = store_batting_appearances(game_id, batting_df, cursor)
            logger.info(f"Stored {batting_stored} batting appearances")
        except Exception as e:
            logger.error(f"Failed to store batting appearances: {e}")
            raise
        
        try:
            pitching_stored = store_pitching_appearances(game_id, pitching_df, cursor)
            logger.info(f"Stored {pitching_stored} pitching appearances")
        except Exception as e:
            logger.error(f"Failed to store pitching appearances: {e}")
            raise
        
        try:
            events_stored = store_play_by_play_events(game_id, parsing_results["pbp_events"], cursor)
            logger.info(f"Stored {events_stored} events")
        except Exception as e:
            logger.error(f"Failed to store events: {e}")
            raise
        
        try:
            store_validation_report(game_id, validation_results, cursor)
            logger.info(f"Stored validation report")
        except Exception as e:
            logger.error(f"Failed to store validation report: {e}")
            raise
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(
            f"Stored {game_id}: {players_stored} players, {batting_stored} batting, "
            f"{pitching_stored} pitching, {events_stored} events"
        )
        
        return {
            "status": "success",
            "game_id": game_id,
            "players_stored": players_stored,
            "batting_stored": batting_stored,
            "pitching_stored": pitching_stored,
            "events_stored": events_stored
        }
        
    except Exception as e:
        if 'conn' in locals():
            conn.rollback()
            conn.close()
        
        # Extract more detailed error info
        error_msg = str(e)
        logger.error(f"Failed to store game {game_id}: {error_msg}")
        
        # Try to extract which field is problematic from the error
        if "value too long" in error_msg:
            logger.error("⚠️  VARCHAR field too small - check your data:")
            logger.error(f"   Error details: {e}")
            
            # Try to identify the problematic data
            if 'batting_df' in locals() and not batting_df.empty:
                logger.error(f"   Sample batting data:")
                for _, row in batting_df.head(3).iterrows():
                    logger.error(f"     - {row['player_name']}: positions={row.get('positions_played')}, team={row.get('team')}")
            
            if 'game_metadata' in locals():
                logger.error(f"   Game metadata:")
                logger.error(f"     - home_team: {game_metadata.get('home_team')} (len={len(str(game_metadata.get('home_team', '')))})")
                logger.error(f"     - away_team: {game_metadata.get('away_team')} (len={len(str(game_metadata.get('away_team', '')))})")
                logger.error(f"     - venue: {game_metadata.get('venue')} (len={len(str(game_metadata.get('venue', '')))})")
        
        return {
            "status": "error",
            "game_id": game_id,
            "error_message": error_msg
        }
