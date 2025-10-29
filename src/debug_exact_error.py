#!/usr/bin/env python3
"""
Debug script to find the EXACT line causing the error
"""

import sys
import os
import traceback
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from parsing.appearances_parser import (
    parse_batting_appearances, parse_pitching_appearances,
    get_batting_stats_for_validation, get_pitching_stats_for_validation
)
from parsing.events_parser import parse_play_by_play_events
from parsing.parsing_utils import extract_game_id
from parsing.name_to_id_mapper import build_player_id_mapping, add_player_ids_to_events
from utils.url_cacher import HighPerformancePageFetcher
from validation.stat_validator import validate_batting_stats, validate_pitching_stats
import pandas as pd

game_url = "https://www.baseball-reference.com/boxes/NYN/NYN202410080.shtml"

print("=" * 70)
print("STEP-BY-STEP DEBUG TO FIND EXACT ERROR")
print("=" * 70)

try:
    # Step 1
    print("\n1️⃣ Fetching page...")
    fetcher = HighPerformancePageFetcher(cache_dir="cache")
    soup = fetcher.fetch_page(game_url)
    game_id = extract_game_id(game_url)
    print(f"   ✓ Game ID: {game_id}")
    
    # Step 2
    print("\n2️⃣ Parsing batting appearances...")
    batting_appearances = parse_batting_appearances(soup, game_id)
    print(f"   ✓ Parsed {len(batting_appearances)} batting appearances")
    
    # Step 3
    print("\n3️⃣ Parsing pitching appearances...")
    pitching_appearances = parse_pitching_appearances(soup, game_id)
    print(f"   ✓ Parsed {len(pitching_appearances)} pitching appearances")
    
    # Step 4
    print("\n4️⃣ Building player ID mapping...")
    name_to_id_mapping = build_player_id_mapping(batting_appearances, pitching_appearances)
    print(f"   ✓ Built mapping with {len(name_to_id_mapping)} players")
    
    # Step 5
    print("\n5️⃣ Parsing play-by-play events...")
    play_by_play_events = parse_play_by_play_events(soup, game_id)
    print(f"   ✓ Parsed {len(play_by_play_events)} events")
    
    # Step 6
    print("\n6️⃣ Adding player IDs to events...")
    play_by_play_events = add_player_ids_to_events(play_by_play_events, name_to_id_mapping)
    print(f"   ✓ Added IDs. Event columns: {list(play_by_play_events.columns)}")
    
    # Step 7
    print("\n7️⃣ Getting batting stats for validation...")
    batting_for_validation = get_batting_stats_for_validation(batting_appearances)
    print(f"   ✓ Got {len(batting_for_validation)} rows")
    print(f"   Columns: {list(batting_for_validation.columns)}")
    print(f"   Data types:")
    for col in ['PA', 'AB', 'H', 'BB', 'SO']:
        if col in batting_for_validation.columns:
            print(f"      {col}: {batting_for_validation[col].dtype}")
    
    # Step 8 - THIS IS WHERE IT LIKELY FAILS
    print("\n8️⃣ Calling validate_batting_stats...")
    print("   This is where the error likely occurs...")
    
    # Add try-catch inside the validation
    try:
        print("\n   8a. Checking for empty DataFrames...")
        if batting_for_validation.empty or play_by_play_events.empty:
            print("   ❌ One of the DataFrames is empty!")
        else:
            print(f"   ✓ Batting: {len(batting_for_validation)} rows, Events: {len(play_by_play_events)} rows")
        
        print("\n   8b. Converting numeric columns...")
        meaningful_columns = ['PA', 'AB', 'H', 'BB', 'SO', 'HR', '2B', '3B', 'SB', 'CS', 'HBP', 'GDP', 'SF', 'SH']
        for col in meaningful_columns:
            if col in batting_for_validation.columns:
                print(f"      Converting {col} (dtype: {batting_for_validation[col].dtype})...", end="")
                batting_for_validation[col] = pd.to_numeric(batting_for_validation[col], errors='coerce').fillna(0)
                print(f" → {batting_for_validation[col].dtype}")
        
        print("\n   8c. Filtering meaningful stats...")
        print(f"      Attempting: official[{meaningful_columns}].sum(axis=1) > 0")
        existing_cols = [c for c in meaningful_columns if c in batting_for_validation.columns]
        print(f"      Existing columns: {existing_cols}")
        
        # Test the sum operation
        print(f"      Testing sum operation...")
        sum_result = batting_for_validation[existing_cols].sum(axis=1)
        print(f"      ✓ Sum successful. Result dtype: {sum_result.dtype}")
        print(f"      First 3 sums: {sum_result.head(3).tolist()}")
        
        # Test the comparison
        print(f"      Testing comparison (sum > 0)...")
        comparison_result = sum_result > 0
        print(f"      ✓ Comparison successful!")
        print(f"      Meaningful players: {comparison_result.sum()}/{len(comparison_result)}")
        
        print("\n   8d. Filtering DataFrame...")
        official_filtered = batting_for_validation[comparison_result]
        print(f"      ✓ Filtered to {len(official_filtered)} players")
        
        print("\n   8e. Aggregating events by batter_name...")
        print(f"      Event columns: {list(play_by_play_events.columns)}")
        
        # Check what column we should group by
        if 'batter_name' in play_by_play_events.columns:
            group_col = 'batter_name'
        elif 'batter_id' in play_by_play_events.columns:
            group_col = 'batter_id'
        else:
            print(f"      ❌ ERROR: Neither 'batter_name' nor 'batter_id' found in events!")
            print(f"      Available columns: {list(play_by_play_events.columns)}")
            raise ValueError("Missing batter column in events")
        
        print(f"      Grouping by: {group_col}")
        
        # Check data types in events
        agg_cols = ['is_plate_appearance', 'is_at_bat', 'is_hit', 'is_walk', 'is_strikeout']
        print(f"      Checking event column types:")
        for col in agg_cols:
            if col in play_by_play_events.columns:
                dtype = play_by_play_events[col].dtype
                sample = play_by_play_events[col].head(3).tolist()
                print(f"         {col}: {dtype} - samples: {sample}")
            else:
                print(f"         {col}: MISSING")
        
        parsed = play_by_play_events.groupby(group_col).agg({
            'is_plate_appearance': 'sum',
            'is_at_bat': 'sum',
            'is_hit': 'sum',
            'is_walk': 'sum',
            'is_strikeout': 'sum'
        }).reset_index()
        print(f"      ✓ Aggregated to {len(parsed)} rows")
        
        print("\n   ✅ ALL VALIDATION STEPS PASSED!")
        
        # Now try the actual function
        print("\n   8f. Running actual validate_batting_stats()...")
        result = validate_batting_stats(batting_for_validation, play_by_play_events)
        print(f"   ✓ Validation complete! Accuracy: {result.get('accuracy', 0):.1f}%")
        
    except Exception as e:
        print(f"\n   ❌ ERROR in validation steps:")
        print(f"   Error type: {type(e).__name__}")
        print(f"   Error message: {str(e)}")
        print(f"\n   Full traceback:")
        traceback.print_exc()
        
except Exception as e:
    print(f"\n❌ ERROR at main level:")
    print(f"Error type: {type(e).__name__}")
    print(f"Error message: {str(e)}")
    print(f"\nFull traceback:")
    traceback.print_exc()

print("\n" + "=" * 70)
print("DEBUG COMPLETE")
print("=" * 70)
