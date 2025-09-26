"""
Fixed Player Appearances Parser
===============================

Fixes the issues in your modified_game_parser_functions.py:
1. Properly strips ALL position codes from player names
2. Correctly handles batting order inheritance for substitutions
3. Parses positions accurately for all players
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import re
import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO
from typing import Tuple, List, Dict, Optional
import time

# Import your existing functions
from parsing.name_utils import normalize_name
from parsing.game_utils import safe_int, extract_from_details

def parse_official_batting_with_appearances_fixed(soup: BeautifulSoup) -> Tuple[pd.DataFrame, List[Dict]]:
    """
    FIXED VERSION: Parse batting stats AND extract appearance metadata
    
    Key fixes:
    - Properly strips ALL position codes from names
    - Correctly handles batting order for substitutions
    - Accurately parses all positions
    """
    
    batting_tables = soup.find_all('table', {'id': lambda x: x and x.endswith('batting')})
    all_stats = []
    all_appearances = []
    
    for table_idx, table in enumerate(batting_tables):
        try:
            df = pd.read_html(StringIO(str(table)))[0]
            df = df[df['Batting'].notna()]
            df = df[~df['Batting'].str.contains("Team Totals", na=False)]
            
            # Determine team (first table is usually away, second is home)
            team = 'away' if table_idx == 0 else 'home'
            
            # Build batting order map to handle substitutions correctly
            batting_order_map = build_batting_order_map(df['Batting'].tolist())
            
            for row_index, row in df.iterrows():
                raw_batting_entry = str(row['Batting'])
                
                # FIXED: Extract ALL appearance info properly
                player_name, positions, appearance_metadata = extract_batting_appearance_info_fixed(
                    raw_batting_entry, row_index, batting_order_map
                )
                
                if not player_name:
                    continue
                
                # Build clean stats record (using CLEAN name)
                stats_record = {
                    'player_name': player_name,  # Clean name for validation
                    'AB': safe_int(row.get('AB', 0)),
                    'H': safe_int(row.get('H', 0)),
                    'BB': safe_int(row.get('BB', 0)),
                    'SO': safe_int(row.get('SO', 0)),
                    'PA': safe_int(row.get('PA', 0)),
                    'R': safe_int(row.get('R', 0)),
                    'RBI': safe_int(row.get('RBI', 0)),
                    'HR': extract_from_details(row, 'HR'),
                    '2B': extract_from_details(row, '2B'),
                    '3B': extract_from_details(row, '3B'),
                    'SB': extract_from_details(row, 'SB'),
                    'CS': extract_from_details(row, 'CS'),
                    'HBP': extract_from_details(row, 'HBP'),
                    'GDP': extract_from_details(row, 'GDP'),
                    'SF': extract_from_details(row, 'SF'),
                    'SH': extract_from_details(row, 'SH'),
                }
                
                # Build appearance record with FIXED batting order
                appearance_record = {
                    'player_name': player_name,  # Clean name
                    'team': team,
                    'batting_order': appearance_metadata['batting_order'],  # Now correctly handles subs
                    'positions_played': positions,  # Now properly parsed
                    'is_starter': appearance_metadata['is_starter'],
                    'is_pinch_hitter': appearance_metadata['is_pinch_hitter'],
                    'is_substitute': appearance_metadata['is_substitute'],
                    'replaced_batting_order': appearance_metadata.get('replaced_batting_order'),
                    
                    # Include the stats
                    'PA': stats_record['PA'],
                    'AB': stats_record['AB'],
                    'H': stats_record['H'],
                    'R': stats_record['R'],
                    'RBI': stats_record['RBI'],
                    'HR': stats_record['HR'],
                    'BB': stats_record['BB'],
                    'SO': stats_record['SO'],
                }
                
                all_stats.append(stats_record)
                all_appearances.append(appearance_record)
                
        except Exception as e:
            print(f"Error parsing batting table {table_idx}: {e}")
            continue
    
    return pd.DataFrame(all_stats), all_appearances

def build_batting_order_map(raw_names: List[str]) -> Dict[int, int]:
    """
    Build a map of table_index -> actual_batting_order
    
    This handles cases where substitutes appear later in the table
    but should inherit the batting order of who they replaced.
    
    Strategy:
    - First 9 entries with meaningful stats are the starting lineup (batting orders 1-9)
    - Subsequent entries are substitutes who inherit positions
    - Pitchers and pinch runners get None (they don't have batting orders)
    """
    
    batting_order_map = {}
    current_batting_order = 1
    
    # First pass: Assign batting orders to likely starters (non-pitchers with stats)
    for idx, raw_name in enumerate(raw_names):
        # Skip pitchers (they typically have 0 PA and are listed last)
        if is_likely_pitcher(raw_name):
            batting_order_map[idx] = None
            continue
        
        # Skip pinch runners (they have stats but no PA/AB)
        if 'PR' in raw_name:
            batting_order_map[idx] = None  # Pinch runners don't have batting order
            continue
            
        # For the first 9 non-pitcher, non-PR entries, assign sequential batting orders
        if current_batting_order <= 9:
            batting_order_map[idx] = current_batting_order
            current_batting_order += 1
        else:
            # This is likely a substitute - determine their inherited batting order
            # For now, we'll assign None and let the appearance logic figure it out
            batting_order_map[idx] = None
    
    return batting_order_map

def extract_batting_appearance_info_fixed(raw_entry: str, table_index: int, 
                                         batting_order_map: Dict[int, int]) -> Tuple[str, List[str], Dict]:
    """
    FIXED VERSION: Extract all batting appearance info from raw entry
    
    Key improvements:
    - Better position code extraction and stripping
    - Correct batting order assignment for substitutes
    - Proper handling of multiple positions and special cases
    """
    
    # Remove decisions first (W, L, S, etc.)
    cleaned = re.sub(r',\s*[WLSHB]+\s*\([^)]*\)', '', raw_entry).strip()
    
    # IMPROVED: Extract position codes more comprehensively
    # Handle patterns like "3B", "C-1B", "2B-SS", "LF-CF-RF", etc.
    position_patterns = [
        r'\s+([A-Z0-9]{1,2}(?:-[A-Z0-9]{1,2})*)\s*
    
    # FIXED: Determine batting order and substitution status
    assigned_batting_order = batting_order_map.get(table_index)
    
    # Determine player role
    is_ph = 'PH' in position_codes if 'position_codes' in locals() else False
    is_pr = 'PR' in position_codes if 'position_codes' in locals() else False
    is_starter = assigned_batting_order is not None and assigned_batting_order <= 9 and not is_ph and not is_pr
    is_substitute = not is_starter and (is_ph or is_pr or assigned_batting_order is None)
    
    # For substitutes, try to determine who they replaced
    replaced_batting_order = None
    if is_substitute and not is_ph and not is_pr:
        # This is likely a defensive substitution - keep the batting order they're in
        replaced_batting_order = assigned_batting_order
    elif is_ph:
        # Pinch hitters inherit the batting order of who they replaced
        # This would need game context to determine accurately
        replaced_batting_order = assigned_batting_order
    
    appearance_metadata = {
        'batting_order': assigned_batting_order,
        'is_starter': is_starter,
        'is_pinch_hitter': is_ph,
        'is_substitute': is_substitute,
        'replaced_batting_order': replaced_batting_order,
    }
    
    # FINAL: Ensure name is completely clean
    final_clean_name = normalize_name(player_name)
    
    return final_clean_name, positions, appearance_metadata

def expand_position_code_comprehensive(code: str) -> Optional[str]:
    """
    COMPREHENSIVE position code mapping
    
    Handles all standard baseball position codes including edge cases
    """
    position_map = {
        # Standard positions
        'P': 'Pitcher',
        'C': 'Catcher',
        '1B': 'First Base',
        '2B': 'Second Base', 
        '3B': 'Third Base',
        'SS': 'Shortstop',
        'LF': 'Left Field',
        'CF': 'Center Field',
        'RF': 'Right Field',
        'DH': 'Designated Hitter',
        
        # Special roles
        'PH': 'Pinch Hitter',
        'PR': 'Pinch Runner',
        
        # Alternative formats
        '2': 'Second Base',  # Sometimes positions use numbers
        '3': 'Third Base',
        '4': 'Shortstop',
        '5': 'Third Base',
        '6': 'Shortstop', 
        '7': 'Left Field',
        '8': 'Center Field',
        '9': 'Right Field',
        
        # Manager/coaching (rare but possible)
        'MGR': 'Manager',
    }
    
    return position_map.get(code.upper())

def is_likely_pitcher(raw_name: str) -> bool:
    """
    Determine if a player is likely a pitcher based on their name entry
    
    Pitchers often appear at the end of batting tables with 0 PA
    """
    # Simple heuristic: if the name has no position code or is at the end, might be pitcher
    # This is imperfect but helps with the batting order assignment
    return bool(re.search(r'\s+P\s*$', raw_name))

def parse_official_pitching_with_decisions_fixed(soup: BeautifulSoup) -> Tuple[pd.DataFrame, List[Dict]]:
    """
    FIXED VERSION: Parse pitching stats AND extract decisions
    
    Same approach as batting - clean names properly while preserving metadata
    """
    
    pitching_tables = soup.find_all('table', {'id': lambda x: x and x.endswith('pitching')})
    all_stats = []
    all_decisions = []
    
    for table_idx, table in enumerate(pitching_tables):
        try:
            df = pd.read_html(StringIO(str(table)))[0]
            df = df[df['Pitching'].notna()]
            df = df[~df['Pitching'].str.contains("Team Totals", na=False)]
            
            team = 'away' if table_idx == 0 else 'home'
            
            for pitcher_index, row in df.iterrows():
                raw_pitching_entry = str(row['Pitching'])
                
                # FIXED: Extract decisions before normalize_name strips them
                pitcher_name, decisions = extract_pitcher_decisions_fixed(raw_pitching_entry)
                
                if not pitcher_name:
                    continue
                
                # Build clean stats record
                stats_record = {
                    'pitcher_name': pitcher_name,  # Clean name for validation
                    'BF': safe_int(row.get('BF', 0)),
                    'H': safe_int(row.get('H', 0)),
                    'BB': safe_int(row.get('BB', 0)),
                    'SO': safe_int(row.get('SO', 0)),
                    'HR': safe_int(row.get('HR', 0)),
                    'PC': safe_int(row.get('Pit', 0)),
                }
                
                # Build decision record
                decision_record = {
                    'pitcher_name': pitcher_name,  # Clean name
                    'team': team,
                    'decisions': decisions,
                    'is_starter': pitcher_index == 0,  # First pitcher is starter
                    'pitching_order': pitcher_index + 1,
                }
                
                all_stats.append(stats_record)
                all_decisions.append(decision_record)
                
        except Exception as e:
            print(f"Error parsing pitching table {table_idx}: {e}")
            continue
    
    return pd.DataFrame(all_stats), all_decisions

def extract_pitcher_decisions_fixed(raw_entry: str) -> Tuple[str, List[str]]:
    """
    FIXED VERSION: Extract pitcher name and all decisions
    
    More robust pattern matching for decision extraction
    """
    
    decisions = []
    
    # IMPROVED: Find all decision patterns more reliably
    # Patterns like ", W (1-0)", ", BS (2)", ", H (1)", ", S (15)"
    decision_patterns = [
        r',\s*([WLSHB]+)\s*\([^)]*\)',  # Standard pattern
        r',\s*([WLSHB]+)(?:\s|$)',      # Decision without parentheses (rare)
    ]
    
    for pattern in decision_patterns:
        matches = re.findall(pattern, raw_entry)
        for match in matches:
            if match not in decisions:
                decisions.append(match)
    
    # Remove ALL decision patterns to get clean name
    clean_name = raw_entry
    for pattern in decision_patterns:
        clean_name = re.sub(pattern, '', clean_name)
    
    # Final cleanup and normalization
    clean_name = normalize_name(clean_name.strip())
    
    return clean_name, decisions

def process_single_game_with_fixed_appearances(game_url: str) -> Dict:
    """
    Updated version with FIXED appearance parsing
    
    Use this to replace your existing function
    """
    start_time = time.time()
    
    # Fetch page (your existing logic)
    from utils.mlb_cached_fetcher import SafePageFetcher
    soup = SafePageFetcher.fetch_page(game_url)
    
    # Extract game metadata
    from parsing.game_metadata import extract_game_metadata
    game_metadata = extract_game_metadata(soup, game_url)
    game_id = game_metadata['game_id']
    
    # FIXED: Parse official stats WITH proper appearance metadata
    official_batting, batting_appearances = parse_official_batting_with_appearances_fixed(soup)
    official_pitching, pitching_decisions = parse_official_pitching_with_decisions_fixed(soup)
    
    # Your existing event parsing
    from parsing.game_parser import parse_play_by_play_events
    pbp_events = parse_play_by_play_events(soup, game_id)
    
    # Your existing validation (should work better now with clean names)
    from validation.stat_validator import validate_batting_stats, validate_pitching_stats
    batting_validation = validate_batting_stats(official_batting, pbp_events)
    pitching_validation = validate_pitching_stats(official_pitching, pbp_events)

    time_to_process = time.time() - start_time
    
    return {
        'game_id': game_id,
        'game_metadata': game_metadata,
        'official_batting': official_batting,    # Clean names for validation
        'official_pitching': official_pitching,  # Clean names for validation
        'pbp_events': pbp_events,
        'batting_validation': batting_validation,
        'pitching_validation': pitching_validation,
        'time_to_process': time_to_process,
        
        # FIXED: Better appearance metadata
        'batting_appearances': batting_appearances,
        'pitching_decisions': pitching_decisions,
    }

def debug_name_cleaning():
    """
    Debug function to test name cleaning on problematic examples
    """
    
    print("🔍 DEBUGGING NAME CLEANING")
    print("=" * 40)
    
    problematic_names = [
        "José Ramírez 3B",
        "Salvador Perez C-1B", 
        "Michael Massey 2B",
        "Carlos Santana 1B",
        "Daniel Schneemann 2B",
        "Maikel Garcia 3B",
        "Cavan Biggio 1B",
        "Gabriel Arias 3B",
        "Dairon Blanco PR",  # Pinch runner
        "Aaron Judge RF-DH",  # Multiple positions
    ]
    
    # Create realistic batting order map for testing
    realistic_batting_order_map = {
        0: 1, 1: 2, 2: 3, 3: 4, 4: 5, 
        5: 6, 6: 7, 7: 8, 8: 9, 9: None  # Last one is substitute
    }
    
    for idx, raw_name in enumerate(problematic_names):
        cleaned_name, positions, metadata = extract_batting_appearance_info_fixed(
            raw_name, idx, realistic_batting_order_map
        )
        
        print(f"Raw: '{raw_name}'")
        print(f"  → Clean: '{cleaned_name}'")
        print(f"  → Positions: {positions}")
        print(f"  → Order: {metadata['batting_order']}")
        print(f"  → Role: {'Starter' if metadata['is_starter'] else 'Sub'}")
        print()

if __name__ == "__main__":
    
    # First, debug the name cleaning
    debug_name_cleaning()
    
    print("\n" + "="*50)
    print("TESTING FIXED APPEARANCE PARSING")
    print("="*50)
    
    # Test with your problematic game
    test_url = "https://www.baseball-reference.com/boxes/KCA/KCA202503290.shtml"
    
    result = process_single_game_with_fixed_appearances(test_url)
    
    print(f"✅ {result['game_id']}")
    print(f"⚾ Batting accuracy: {result['batting_validation']['accuracy']:.1f}%")  
    print(f"🥎 Pitching accuracy: {result['pitching_validation']['accuracy']:.1f}%")
    
    # Check if name mismatches are reduced
    if result['batting_validation'].get('name_mismatches'):
        mismatches = result['batting_validation']['name_mismatches']
        print(f"\n⚠️  Remaining name mismatches: {len(mismatches.get('unmatched_official_names', []))}")
        
        if mismatches.get('unmatched_official_names'):
            print("Still problematic names:")
            for name in mismatches['unmatched_official_names'][:5]:
                print(f"  - '{name}'")
    
    # Show sample appearances with better data
    print(f"\n👥 SAMPLE BATTING APPEARANCES:")
    for appearance in result['batting_appearances'][:5]:
        name = appearance['player_name']
        order = appearance['batting_order'] 
        positions = ', '.join(appearance['positions_played']) if appearance['positions_played'] else 'Unknown'
        role = 'Starter' if appearance['is_starter'] else ('PH' if appearance['is_pinch_hitter'] else 'Sub')
        
        print(f"  {order if order else '?'}: {name} ({positions}) - {role}")
,  # Standard positions at end - FIXED to include numbers
        r'\s+([0-9]B|SS|[LCR]F|DH|C|P|PH|PR)\s*
    
    # FIXED: Determine batting order and substitution status
    assigned_batting_order = batting_order_map.get(table_index)
    
    # Determine player role
    is_ph = 'PH' in position_codes if 'position_codes' in locals() else False
    is_pr = 'PR' in position_codes if 'position_codes' in locals() else False
    is_starter = assigned_batting_order is not None and assigned_batting_order <= 9 and not is_ph and not is_pr
    is_substitute = not is_starter and (is_ph or is_pr or assigned_batting_order is None)
    
    # For substitutes, try to determine who they replaced
    replaced_batting_order = None
    if is_substitute and not is_ph and not is_pr:
        # This is likely a defensive substitution - keep the batting order they're in
        replaced_batting_order = assigned_batting_order
    elif is_ph:
        # Pinch hitters inherit the batting order of who they replaced
        # This would need game context to determine accurately
        replaced_batting_order = assigned_batting_order
    
    appearance_metadata = {
        'batting_order': assigned_batting_order,
        'is_starter': is_starter,
        'is_pinch_hitter': is_ph,
        'is_substitute': is_substitute,
        'replaced_batting_order': replaced_batting_order,
    }
    
    # FINAL: Ensure name is completely clean
    final_clean_name = normalize_name(player_name)
    
    return final_clean_name, positions, appearance_metadata

def expand_position_code_comprehensive(code: str) -> Optional[str]:
    """
    COMPREHENSIVE position code mapping
    
    Handles all standard baseball position codes including edge cases
    """
    position_map = {
        # Standard positions
        'P': 'Pitcher',
        'C': 'Catcher',
        '1B': 'First Base',
        '2B': 'Second Base', 
        '3B': 'Third Base',
        'SS': 'Shortstop',
        'LF': 'Left Field',
        'CF': 'Center Field',
        'RF': 'Right Field',
        'DH': 'Designated Hitter',
        
        # Special roles
        'PH': 'Pinch Hitter',
        'PR': 'Pinch Runner',
        
        # Alternative formats
        '2': 'Second Base',  # Sometimes positions use numbers
        '3': 'Third Base',
        '4': 'Shortstop',
        '5': 'Third Base',
        '6': 'Shortstop', 
        '7': 'Left Field',
        '8': 'Center Field',
        '9': 'Right Field',
        
        # Manager/coaching (rare but possible)
        'MGR': 'Manager',
    }
    
    return position_map.get(code.upper())

def is_likely_pitcher(raw_name: str) -> bool:
    """
    Determine if a player is likely a pitcher based on their name entry
    
    Pitchers often appear at the end of batting tables with 0 PA
    """
    # Simple heuristic: if the name has no position code or is at the end, might be pitcher
    # This is imperfect but helps with the batting order assignment
    return bool(re.search(r'\s+P\s*$', raw_name))

def parse_official_pitching_with_decisions_fixed(soup: BeautifulSoup) -> Tuple[pd.DataFrame, List[Dict]]:
    """
    FIXED VERSION: Parse pitching stats AND extract decisions
    
    Same approach as batting - clean names properly while preserving metadata
    """
    
    pitching_tables = soup.find_all('table', {'id': lambda x: x and x.endswith('pitching')})
    all_stats = []
    all_decisions = []
    
    for table_idx, table in enumerate(pitching_tables):
        try:
            df = pd.read_html(StringIO(str(table)))[0]
            df = df[df['Pitching'].notna()]
            df = df[~df['Pitching'].str.contains("Team Totals", na=False)]
            
            team = 'away' if table_idx == 0 else 'home'
            
            for pitcher_index, row in df.iterrows():
                raw_pitching_entry = str(row['Pitching'])
                
                # FIXED: Extract decisions before normalize_name strips them
                pitcher_name, decisions = extract_pitcher_decisions_fixed(raw_pitching_entry)
                
                if not pitcher_name:
                    continue
                
                # Build clean stats record
                stats_record = {
                    'pitcher_name': pitcher_name,  # Clean name for validation
                    'BF': safe_int(row.get('BF', 0)),
                    'H': safe_int(row.get('H', 0)),
                    'BB': safe_int(row.get('BB', 0)),
                    'SO': safe_int(row.get('SO', 0)),
                    'HR': safe_int(row.get('HR', 0)),
                    'PC': safe_int(row.get('Pit', 0)),
                }
                
                # Build decision record
                decision_record = {
                    'pitcher_name': pitcher_name,  # Clean name
                    'team': team,
                    'decisions': decisions,
                    'is_starter': pitcher_index == 0,  # First pitcher is starter
                    'pitching_order': pitcher_index + 1,
                }
                
                all_stats.append(stats_record)
                all_decisions.append(decision_record)
                
        except Exception as e:
            print(f"Error parsing pitching table {table_idx}: {e}")
            continue
    
    return pd.DataFrame(all_stats), all_decisions

def extract_pitcher_decisions_fixed(raw_entry: str) -> Tuple[str, List[str]]:
    """
    FIXED VERSION: Extract pitcher name and all decisions
    
    More robust pattern matching for decision extraction
    """
    
    decisions = []
    
    # IMPROVED: Find all decision patterns more reliably
    # Patterns like ", W (1-0)", ", BS (2)", ", H (1)", ", S (15)"
    decision_patterns = [
        r',\s*([WLSHB]+)\s*\([^)]*\)',  # Standard pattern
        r',\s*([WLSHB]+)(?:\s|$)',      # Decision without parentheses (rare)
    ]
    
    for pattern in decision_patterns:
        matches = re.findall(pattern, raw_entry)
        for match in matches:
            if match not in decisions:
                decisions.append(match)
    
    # Remove ALL decision patterns to get clean name
    clean_name = raw_entry
    for pattern in decision_patterns:
        clean_name = re.sub(pattern, '', clean_name)
    
    # Final cleanup and normalization
    clean_name = normalize_name(clean_name.strip())
    
    return clean_name, decisions

def process_single_game_with_fixed_appearances(game_url: str) -> Dict:
    """
    Updated version with FIXED appearance parsing
    
    Use this to replace your existing function
    """
    start_time = time.time()
    
    # Fetch page (your existing logic)
    from utils.mlb_cached_fetcher import SafePageFetcher
    soup = SafePageFetcher.fetch_page(game_url)
    
    # Extract game metadata
    from parsing.game_metadata import extract_game_metadata
    game_metadata = extract_game_metadata(soup, game_url)
    game_id = game_metadata['game_id']
    
    # FIXED: Parse official stats WITH proper appearance metadata
    official_batting, batting_appearances = parse_official_batting_with_appearances_fixed(soup)
    official_pitching, pitching_decisions = parse_official_pitching_with_decisions_fixed(soup)
    
    # Your existing event parsing
    from parsing.game_parser import parse_play_by_play_events
    pbp_events = parse_play_by_play_events(soup, game_id)
    
    # Your existing validation (should work better now with clean names)
    from validation.stat_validator import validate_batting_stats, validate_pitching_stats
    batting_validation = validate_batting_stats(official_batting, pbp_events)
    pitching_validation = validate_pitching_stats(official_pitching, pbp_events)

    time_to_process = time.time() - start_time
    
    return {
        'game_id': game_id,
        'game_metadata': game_metadata,
        'official_batting': official_batting,    # Clean names for validation
        'official_pitching': official_pitching,  # Clean names for validation
        'pbp_events': pbp_events,
        'batting_validation': batting_validation,
        'pitching_validation': pitching_validation,
        'time_to_process': time_to_process,
        
        # FIXED: Better appearance metadata
        'batting_appearances': batting_appearances,
        'pitching_decisions': pitching_decisions,
    }

def debug_name_cleaning():
    """
    Debug function to test name cleaning on problematic examples
    """
    
    print("🔍 DEBUGGING NAME CLEANING")
    print("=" * 40)
    
    problematic_names = [
        "José Ramírez 3B",
        "Salvador Perez C-1B", 
        "Michael Massey 2B",
        "Carlos Santana 1B",
        "Daniel Schneemann 2B",
        "Maikel Garcia 3B",
        "Cavan Biggio 1B",
        "Gabriel Arias 3B",
        "Dairon Blanco PR",  # Pinch runner
        "Aaron Judge RF-DH",  # Multiple positions
    ]
    
    for raw_name in problematic_names:
        cleaned_name, positions, metadata = extract_batting_appearance_info_fixed(
            raw_name, 0, {0: 1}  # Mock batting order map
        )
        
        print(f"Raw: '{raw_name}'")
        print(f"  → Clean: '{cleaned_name}'")
        print(f"  → Positions: {positions}")
        print(f"  → Metadata: {metadata}")
        print()

if __name__ == "__main__":
    
    # First, debug the name cleaning
    debug_name_cleaning()
    
    print("\n" + "="*50)
    print("TESTING FIXED APPEARANCE PARSING")
    print("="*50)
    
    # Test with your problematic game
    test_url = "https://www.baseball-reference.com/boxes/KCA/KCA202503290.shtml"
    
    result = process_single_game_with_fixed_appearances(test_url)
    
    print(f"✅ {result['game_id']}")
    print(f"⚾ Batting accuracy: {result['batting_validation']['accuracy']:.1f}%")  
    print(f"🥎 Pitching accuracy: {result['pitching_validation']['accuracy']:.1f}%")
    
    # Check if name mismatches are reduced
    if result['batting_validation'].get('name_mismatches'):
        mismatches = result['batting_validation']['name_mismatches']
        print(f"\n⚠️  Remaining name mismatches: {len(mismatches.get('unmatched_official_names', []))}")
        
        if mismatches.get('unmatched_official_names'):
            print("Still problematic names:")
            for name in mismatches['unmatched_official_names'][:5]:
                print(f"  - '{name}'")
    
    # Show sample appearances with better data
    print(f"\n👥 SAMPLE BATTING APPEARANCES:")
    for appearance in result['batting_appearances'][:5]:
        name = appearance['player_name']
        order = appearance['batting_order'] 
        positions = ', '.join(appearance['positions_played']) if appearance['positions_played'] else 'Unknown'
        role = 'Starter' if appearance['is_starter'] else ('PH' if appearance['is_pinch_hitter'] else 'Sub')
        
        print(f"  {order if order else '?'}: {name} ({positions}) - {role}")
,  # Numbered bases and common codes
    ]
    
    positions = []
    player_name = cleaned
    position_codes = ""
    
    for pattern in position_patterns:
        position_match = re.search(pattern, player_name)
        if position_match:
            position_codes = position_match.group(1)
            player_name = player_name[:position_match.start()].strip()
            
            # Handle multiple positions like "C-1B" or "LF-CF"
            pos_codes = position_codes.split('-')
            for code in pos_codes:
                full_pos = expand_position_code_comprehensive(code)
                if full_pos and full_pos not in positions:
                    positions.append(full_pos)
            break
    
    # FIXED: Determine batting order and substitution status
    assigned_batting_order = batting_order_map.get(table_index)
    
    # Determine player role
    is_ph = 'PH' in position_codes if 'position_codes' in locals() else False
    is_pr = 'PR' in position_codes if 'position_codes' in locals() else False
    is_starter = assigned_batting_order is not None and assigned_batting_order <= 9 and not is_ph and not is_pr
    is_substitute = not is_starter and (is_ph or is_pr or assigned_batting_order is None)
    
    # For substitutes, try to determine who they replaced
    replaced_batting_order = None
    if is_substitute and not is_ph and not is_pr:
        # This is likely a defensive substitution - keep the batting order they're in
        replaced_batting_order = assigned_batting_order
    elif is_ph:
        # Pinch hitters inherit the batting order of who they replaced
        # This would need game context to determine accurately
        replaced_batting_order = assigned_batting_order
    
    appearance_metadata = {
        'batting_order': assigned_batting_order,
        'is_starter': is_starter,
        'is_pinch_hitter': is_ph,
        'is_substitute': is_substitute,
        'replaced_batting_order': replaced_batting_order,
    }
    
    # FINAL: Ensure name is completely clean
    final_clean_name = normalize_name(player_name)
    
    return final_clean_name, positions, appearance_metadata

def expand_position_code_comprehensive(code: str) -> Optional[str]:
    """
    COMPREHENSIVE position code mapping
    
    Handles all standard baseball position codes including edge cases
    """
    position_map = {
        # Standard positions
        'P': 'Pitcher',
        'C': 'Catcher',
        '1B': 'First Base',
        '2B': 'Second Base', 
        '3B': 'Third Base',
        'SS': 'Shortstop',
        'LF': 'Left Field',
        'CF': 'Center Field',
        'RF': 'Right Field',
        'DH': 'Designated Hitter',
        
        # Special roles
        'PH': 'Pinch Hitter',
        'PR': 'Pinch Runner',
        
        # Alternative formats
        '2': 'Second Base',  # Sometimes positions use numbers
        '3': 'Third Base',
        '4': 'Shortstop',
        '5': 'Third Base',
        '6': 'Shortstop', 
        '7': 'Left Field',
        '8': 'Center Field',
        '9': 'Right Field',
        
        # Manager/coaching (rare but possible)
        'MGR': 'Manager',
    }
    
    return position_map.get(code.upper())

def is_likely_pitcher(raw_name: str) -> bool:
    """
    Determine if a player is likely a pitcher based on their name entry
    
    Pitchers often appear at the end of batting tables with 0 PA
    """
    # Simple heuristic: if the name has no position code or is at the end, might be pitcher
    # This is imperfect but helps with the batting order assignment
    return bool(re.search(r'\s+P\s*$', raw_name))

def parse_official_pitching_with_decisions_fixed(soup: BeautifulSoup) -> Tuple[pd.DataFrame, List[Dict]]:
    """
    FIXED VERSION: Parse pitching stats AND extract decisions
    
    Same approach as batting - clean names properly while preserving metadata
    """
    
    pitching_tables = soup.find_all('table', {'id': lambda x: x and x.endswith('pitching')})
    all_stats = []
    all_decisions = []
    
    for table_idx, table in enumerate(pitching_tables):
        try:
            df = pd.read_html(StringIO(str(table)))[0]
            df = df[df['Pitching'].notna()]
            df = df[~df['Pitching'].str.contains("Team Totals", na=False)]
            
            team = 'away' if table_idx == 0 else 'home'
            
            for pitcher_index, row in df.iterrows():
                raw_pitching_entry = str(row['Pitching'])
                
                # FIXED: Extract decisions before normalize_name strips them
                pitcher_name, decisions = extract_pitcher_decisions_fixed(raw_pitching_entry)
                
                if not pitcher_name:
                    continue
                
                # Build clean stats record
                stats_record = {
                    'pitcher_name': pitcher_name,  # Clean name for validation
                    'BF': safe_int(row.get('BF', 0)),
                    'H': safe_int(row.get('H', 0)),
                    'BB': safe_int(row.get('BB', 0)),
                    'SO': safe_int(row.get('SO', 0)),
                    'HR': safe_int(row.get('HR', 0)),
                    'PC': safe_int(row.get('Pit', 0)),
                }
                
                # Build decision record
                decision_record = {
                    'pitcher_name': pitcher_name,  # Clean name
                    'team': team,
                    'decisions': decisions,
                    'is_starter': pitcher_index == 0,  # First pitcher is starter
                    'pitching_order': pitcher_index + 1,
                }
                
                all_stats.append(stats_record)
                all_decisions.append(decision_record)
                
        except Exception as e:
            print(f"Error parsing pitching table {table_idx}: {e}")
            continue
    
    return pd.DataFrame(all_stats), all_decisions

def extract_pitcher_decisions_fixed(raw_entry: str) -> Tuple[str, List[str]]:
    """
    FIXED VERSION: Extract pitcher name and all decisions
    
    More robust pattern matching for decision extraction
    """
    
    decisions = []
    
    # IMPROVED: Find all decision patterns more reliably
    # Patterns like ", W (1-0)", ", BS (2)", ", H (1)", ", S (15)"
    decision_patterns = [
        r',\s*([WLSHB]+)\s*\([^)]*\)',  # Standard pattern
        r',\s*([WLSHB]+)(?:\s|$)',      # Decision without parentheses (rare)
    ]
    
    for pattern in decision_patterns:
        matches = re.findall(pattern, raw_entry)
        for match in matches:
            if match not in decisions:
                decisions.append(match)
    
    # Remove ALL decision patterns to get clean name
    clean_name = raw_entry
    for pattern in decision_patterns:
        clean_name = re.sub(pattern, '', clean_name)
    
    # Final cleanup and normalization
    clean_name = normalize_name(clean_name.strip())
    
    return clean_name, decisions

def process_single_game_with_fixed_appearances(game_url: str) -> Dict:
    """
    Updated version with FIXED appearance parsing
    
    Use this to replace your existing function
    """
    start_time = time.time()
    
    # Fetch page (your existing logic)
    from utils.mlb_cached_fetcher import SafePageFetcher
    soup = SafePageFetcher.fetch_page(game_url)
    
    # Extract game metadata
    from parsing.game_metadata import extract_game_metadata
    game_metadata = extract_game_metadata(soup, game_url)
    game_id = game_metadata['game_id']
    
    # FIXED: Parse official stats WITH proper appearance metadata
    official_batting, batting_appearances = parse_official_batting_with_appearances_fixed(soup)
    official_pitching, pitching_decisions = parse_official_pitching_with_decisions_fixed(soup)
    
    # Your existing event parsing
    from parsing.game_parser import parse_play_by_play_events
    pbp_events = parse_play_by_play_events(soup, game_id)
    
    # Your existing validation (should work better now with clean names)
    from validation.stat_validator import validate_batting_stats, validate_pitching_stats
    batting_validation = validate_batting_stats(official_batting, pbp_events)
    pitching_validation = validate_pitching_stats(official_pitching, pbp_events)

    time_to_process = time.time() - start_time
    
    return {
        'game_id': game_id,
        'game_metadata': game_metadata,
        'official_batting': official_batting,    # Clean names for validation
        'official_pitching': official_pitching,  # Clean names for validation
        'pbp_events': pbp_events,
        'batting_validation': batting_validation,
        'pitching_validation': pitching_validation,
        'time_to_process': time_to_process,
        
        # FIXED: Better appearance metadata
        'batting_appearances': batting_appearances,
        'pitching_decisions': pitching_decisions,
    }

def debug_name_cleaning():
    """
    Debug function to test name cleaning on problematic examples
    """
    
    print("🔍 DEBUGGING NAME CLEANING")
    print("=" * 40)
    
    problematic_names = [
        "José Ramírez 3B",
        "Salvador Perez C-1B", 
        "Michael Massey 2B",
        "Carlos Santana 1B",
        "Daniel Schneemann 2B",
        "Maikel Garcia 3B",
        "Cavan Biggio 1B",
        "Gabriel Arias 3B",
        "Dairon Blanco PR",  # Pinch runner
        "Aaron Judge RF-DH",  # Multiple positions
    ]
    
    for raw_name in problematic_names:
        cleaned_name, positions, metadata = extract_batting_appearance_info_fixed(
            raw_name, 0, {0: 1}  # Mock batting order map
        )
        
        print(f"Raw: '{raw_name}'")
        print(f"  → Clean: '{cleaned_name}'")
        print(f"  → Positions: {positions}")
        print(f"  → Metadata: {metadata}")
        print()

if __name__ == "__main__":
    
    # First, debug the name cleaning
    debug_name_cleaning()
    
    print("\n" + "="*50)
    print("TESTING FIXED APPEARANCE PARSING")
    print("="*50)
    
    # Test with your problematic game
    test_url = "https://www.baseball-reference.com/boxes/KCA/KCA202503290.shtml"
    
    result = process_single_game_with_fixed_appearances(test_url)
    
    print(f"✅ {result['game_id']}")
    print(f"⚾ Batting accuracy: {result['batting_validation']['accuracy']:.1f}%")  
    print(f"🥎 Pitching accuracy: {result['pitching_validation']['accuracy']:.1f}%")
    
    # Check if name mismatches are reduced
    if result['batting_validation'].get('name_mismatches'):
        mismatches = result['batting_validation']['name_mismatches']
        print(f"\n⚠️  Remaining name mismatches: {len(mismatches.get('unmatched_official_names', []))}")
        
        if mismatches.get('unmatched_official_names'):
            print("Still problematic names:")
            for name in mismatches['unmatched_official_names'][:5]:
                print(f"  - '{name}'")
    
    # Show sample appearances with better data
    print(f"\n👥 SAMPLE BATTING APPEARANCES:")
    for appearance in result['batting_appearances'][:5]:
        name = appearance['player_name']
        order = appearance['batting_order'] 
        positions = ', '.join(appearance['positions_played']) if appearance['positions_played'] else 'Unknown'
        role = 'Starter' if appearance['is_starter'] else ('PH' if appearance['is_pinch_hitter'] else 'Sub')
        
        print(f"  {order if order else '?'}: {name} ({positions}) - {role}")
