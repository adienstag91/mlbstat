#!/usr/bin/env python3
"""
Player ID Resolution for Play-by-Play Events
============================================
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
from typing import Dict
from bs4 import BeautifulSoup
from parsing.appearances_parser import parse_batting_appearances, parse_pitching_appearances
from parsing.events_parser import parse_play_by_play_events
from utils.url_cacher import HighPerformancePageFetcher, SimpleFetcher
from parsing.parsing_utils import *

def build_player_id_mapping(batting_appearances: pd.DataFrame, 
                           pitching_appearances: pd.DataFrame) -> Dict[str, str]:
    """
    Build a mapping from player names to player IDs
    
    This should be called AFTER parsing appearances but BEFORE parsing events.
    
    Args:
        batting_appearances: DataFrame with player_name and player_id columns
        pitching_appearances: DataFrame with player_name and player_id columns
        
    Returns:
        Dictionary mapping player_name -> player_id
    """
    name_to_id = {}
    
    # Add from batting appearances
    if not batting_appearances.empty and 'player_name' in batting_appearances.columns:
        for _, row in batting_appearances.iterrows():
            if pd.notna(row.get('player_name')) and pd.notna(row.get('player_id')):
                name_to_id[row['player_name']] = row['player_id']
    
    # Add from pitching appearances (may overwrite, but should be same ID)
    if not pitching_appearances.empty and 'player_name' in pitching_appearances.columns:
        for _, row in pitching_appearances.iterrows():
            if pd.notna(row.get('player_name')) and pd.notna(row.get('player_id')):
                name_to_id[row['player_name']] = row['player_id']
    
    return name_to_id

def add_player_ids_to_events(events_df: pd.DataFrame, 
                             name_to_id_mapping: Dict[str, str]) -> pd.DataFrame:
    """
    Add player_id columns to events DataFrame based on player names
    
    Args:
        events_df: DataFrame with batter_name and pitcher_name columns
        name_to_id_mapping: Dictionary mapping names to IDs
        
    Returns:
        DataFrame with added batter_id and pitcher_id columns
    """
    if events_df.empty:
        return events_df
    
    # Add batter_id by looking up batter_name
    if 'batter_name' in events_df.columns:
        events_df['batter_id'] = events_df['batter_name'].map(name_to_id_mapping)
    
    # Add pitcher_id by looking up pitcher_name
    if 'pitcher_name' in events_df.columns:
        events_df['pitcher_id'] = events_df['pitcher_name'].map(name_to_id_mapping)
    
    # Log any unmapped names (for debugging)
    if 'batter_id' in events_df.columns:
        unmapped_batters = events_df[events_df['batter_id'].isna()]['batter_name'].unique()
        if len(unmapped_batters) > 0:
            print(f"Warning: {len(unmapped_batters)} batters couldn't be mapped to IDs:")
            for name in unmapped_batters[:5]:  # Show first 5
                print(f"  - {name}")
    
    if 'pitcher_id' in events_df.columns:
        unmapped_pitchers = events_df[events_df['pitcher_id'].isna()]['pitcher_name'].unique()
        if len(unmapped_pitchers) > 0:
            print(f"Warning: {len(unmapped_pitchers)} pitchers couldn't be mapped to IDs:")
            for name in unmapped_pitchers[:5]:  # Show first 5
                print(f"  - {name}")
    
    return events_df


# Update your game processor's _parse_all_data method like this:
def updated_parse_all_data_example(game_url: str):
    """
    Example showing how to integrate player ID mapping into your pipeline
    """
    # ... existing code to get soup and game_id ...

    # Fetch page
    simplefetch = SimpleFetcher()
    soup = simplefetch.fetch_page(game_url)
    game_id = extract_game_id(game_url)
    
    # 1. Parse appearances FIRST (these have both names and IDs)
    batting_appearances = parse_batting_appearances(soup, game_id)
    pitching_appearances = parse_pitching_appearances(soup, game_id)
    
    # 2. Build name-to-ID mapping from appearances
    name_to_id_mapping = build_player_id_mapping(
        batting_appearances, 
        pitching_appearances
    )
    print(name_to_id_mapping)
    
    # 3. Parse events (these only have names initially)
    play_by_play_events = parse_play_by_play_events(soup, game_id)
    
    # 4. Add player IDs to events using the mapping
    play_by_play_events = add_player_ids_to_events(
        play_by_play_events, 
        name_to_id_mapping
    )
    print(play_by_play_events)
    
    # Now play_by_play_events has both names AND IDs!
    # batter_name, batter_id, pitcher_name, pitcher_id
    
    return {
        "game_id": game_id,
        "batting_appearances": batting_appearances,
        "pitching_appearances": pitching_appearances,
        "play_by_play_events": play_by_play_events,
        "name_to_id_mapping": name_to_id_mapping  # Save for debugging
    }


# Update your database storage to handle both names and IDs:
def updated_store_play_by_play_events(events: pd.DataFrame, cursor) -> int:
    """
    Updated storage function that saves both names and IDs
    """
    if events.empty:
        return 0
    
    stored_count = 0
    for _, event in events.iterrows():
        cursor.execute("""
            INSERT OR REPLACE INTO at_bats
            (event_id, game_id, inning, inning_half, 
             batter_id, batter_name, pitcher_id, pitcher_name,
             description, is_at_bat, is_hit, hit_type, is_walk, is_strikeout,
             is_out, outs_recorded, bases_reached, event_order)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            event.get('event_id'), event.get('game_id'),
            event.get('inning'), event.get('inning_half'),
            event.get('batter_id'),      # ID for joins
            event.get('batter_name'),    # Name for display
            event.get('pitcher_id'),     # ID for joins
            event.get('pitcher_name'),   # Name for display
            event.get('description'), event.get('is_at_bat', False),
            event.get('is_hit', False), event.get('hit_type'),
            event.get('is_walk', False), event.get('is_strikeout', False),
            event.get('is_out', False), event.get('outs_recorded', 0),
            event.get('bases_reached', 0), event.get('event_order', 0)
        ))
        stored_count += cursor.rowcount
    return stored_count


# Update your database schema to include both name and ID columns:
def updated_at_bats_schema():
    """
    Updated schema with both names and IDs
    """
    schema = """
        CREATE TABLE IF NOT EXISTS at_bats (
            event_id VARCHAR(50) PRIMARY KEY,
            game_id VARCHAR(50) NOT NULL,
            inning INTEGER NOT NULL,
            inning_half VARCHAR(10) NOT NULL,
            
            -- Store BOTH ID and name for each player
            batter_id VARCHAR(100) NOT NULL,      -- For joins with players table
            batter_name VARCHAR(200),              -- For display
            pitcher_id VARCHAR(100) NOT NULL,     -- For joins with players table  
            pitcher_name VARCHAR(200),             -- For display
            
            description TEXT NOT NULL,
            is_at_bat BOOLEAN DEFAULT FALSE,
            is_hit BOOLEAN DEFAULT FALSE,
            hit_type VARCHAR(20),
            is_walk BOOLEAN DEFAULT FALSE,
            is_strikeout BOOLEAN DEFAULT FALSE,
            is_out BOOLEAN DEFAULT FALSE,
            outs_recorded INTEGER DEFAULT 0,
            bases_reached INTEGER DEFAULT 0,
            event_order INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            FOREIGN KEY (game_id) REFERENCES games(game_id),
            FOREIGN KEY (batter_id) REFERENCES players(player_id),
            FOREIGN KEY (pitcher_id) REFERENCES players(player_id)
        )
    """
    return schema


# Now you can do proper joins!
def example_query_with_proper_joins():
    """
    Example queries that now work properly
    """
    
    # Get events with full player information
    query1 = """
        SELECT 
            e.event_id,
            e.inning,
            e.description,
            pb.full_name as batter_full_name,  -- From players table
            e.batter_name,                      -- From events (for verification)
            pp.full_name as pitcher_full_name, -- From players table
            e.pitcher_name                      -- From events (for verification)
        FROM at_bats e
        LEFT JOIN players pb ON e.batter_id = pb.player_id
        LEFT JOIN players pp ON e.pitcher_id = pp.player_id
        WHERE e.game_id = 'KCA202503290'
    """
    
    # Get batting stats with events
    query2 = """
        SELECT 
            ba.player_name,
            ba.PA,
            ba.H,
            COUNT(e.event_id) as events_count,
            SUM(CASE WHEN e.is_hit THEN 1 ELSE 0 END) as hits_from_events
        FROM batting_appearances ba
        LEFT JOIN at_bats e ON ba.player_id = e.batter_id 
            AND ba.game_id = e.game_id
        GROUP BY ba.player_id, ba.player_name, ba.PA, ba.H
    """
    
    return query1, query2


if __name__ == "__main__":
    game_url = "https://www.baseball-reference.com/boxes/KCA/KCA202503290.shtml"
    updated_parse_all_data_example(game_url)
