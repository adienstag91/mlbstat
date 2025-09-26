"""
Modified Game Parser Functions
=============================

Updates to your existing parse_official_batting and parse_official_pitching 
to extract appearance metadata before normalize_name strips it away.
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

def parse_official_batting_with_appearances(soup: BeautifulSoup) -> Tuple[pd.DataFrame, List[Dict]]:
    """
    Parse batting stats AND extract appearance metadata
    Returns:
        Tuple of (clean_batting_stats_df, batting_appearances_list)
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
            
            for batting_order_index, row in df.iterrows():
                raw_batting_entry = str(row['Batting'])
                
                # EXTRACT APPEARANCE METADATA BEFORE normalize_name strips it
                player_name, positions, appearance_metadata = extract_batting_appearance_info(
                    raw_batting_entry, batting_order_index
                )
                
                if not player_name:
                    continue
                
                # Build clean stats record (your existing logic)
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
                
                # Build appearance record (NEW)
                appearance_record = {
                    'player_name': player_name,
                    'team': team,
                    'batting_order': appearance_metadata['batting_order'],
                    'positions_played': positions,
                    'is_starter': appearance_metadata['is_starter'],
                    'is_pinch_hitter': appearance_metadata['is_pinch_hitter'],
                    
                    # Include the stats in the appearance record too
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

def parse_official_pitching_with_decisions(soup: BeautifulSoup) -> Tuple[pd.DataFrame, List[Dict]]:
    """
    Parse pitching stats AND extract decisions
    
    Replaces your existing parse_official_pitching function.
    
    Returns:
        Tuple of (clean_pitching_stats_df, pitching_decisions_list)
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
                
                # EXTRACT DECISIONS BEFORE normalize_name strips them
                pitcher_name, decisions = extract_pitcher_decisions(raw_pitching_entry)
                
                if not pitcher_name:
                    continue
                
                # Build clean stats record (your existing logic)
                stats_record = {
                    'pitcher_name': pitcher_name,  # Clean name for validation
                    'BF': safe_int(row.get('BF', 0)),
                    'H': safe_int(row.get('H', 0)),
                    'BB': safe_int(row.get('BB', 0)),
                    'SO': safe_int(row.get('SO', 0)),
                    'HR': safe_int(row.get('HR', 0)),
                    'PC': safe_int(row.get('Pit', 0)),
                }
                
                # Build decision record (NEW)
                decision_record = {
                    'pitcher_name': pitcher_name,
                    'team': team,
                    'decisions': decisions,  # List like ['W'] or ['BS', 'L']
                    'is_starter': pitcher_index == 0,  # First pitcher is typically starter
                    'pitching_order': pitcher_index + 1,  # Order they entered game
                }
                
                all_stats.append(stats_record)
                all_decisions.append(decision_record)
                
        except Exception as e:
            print(f"Error parsing pitching table {table_idx}: {e}")
            continue
    
    return pd.DataFrame(all_stats), all_decisions

# ============================================================================
# HELPER FUNCTIONS FOR APPEARANCE METADATA EXTRACTION
# ============================================================================

def extract_batting_appearance_info(raw_entry: str, df_index: int) -> Tuple[str, List[str], Dict]:
    """
    Extract all batting appearance info from raw entry
    
    Args:
        raw_entry: "Aaron Judge RF" or "Juan Soto LF-DH" 
        df_index: Position in DataFrame (0-based)
        
    Returns:
        (clean_name, positions_list, appearance_metadata)
    """
    
    # Remove decisions first
    cleaned = re.sub(r',\s*[WLSHB]+\s*\([^)]*\)', '', raw_entry)
    
    # Extract positions from the end
    position_match = re.search(r'\s+([A-Z0-9]{1,3}(?:-[A-Z0-9]{1,3})*)\s*$', cleaned)
    
    if position_match:
        position_codes = position_match.group(1)
        player_name = cleaned[:position_match.start()].strip()
        
        # Handle multiple positions like "CF-RF"  
        pos_codes = position_codes.split('-')
        full_positions = []
        
        for code in pos_codes:
            full_pos = expand_position_code(code)
            if full_pos:
                full_positions.append(full_pos)
        
        # Determine appearance metadata
        is_ph = 'PH' in pos_codes
        is_starter = not is_ph and df_index < 9
        batting_order = (df_index + 1) if is_starter else None
        
        appearance_metadata = {
            'batting_order': batting_order,
            'is_starter': is_starter,
            'is_pinch_hitter': is_ph,
        }
        
        return player_name, full_positions, appearance_metadata
    
    # No position found - just return clean name
    return cleaned.strip(), [], {
        'batting_order': df_index + 1 if df_index < 9 else None,
        'is_starter': df_index < 9,
        'is_pinch_hitter': False,
    }

def extract_pitcher_decisions(raw_entry: str) -> Tuple[str, List[str]]:
    """
    Extract pitcher name and all decisions
    
    Examples:
    "Gerrit Cole, W (1-0)" → ("Gerrit Cole", ["W"])
    "Clay Holmes, BS (2), L (0-1)" → ("Clay Holmes", ["BS", "L"])
    "Jordan Lyles" → ("Jordan Lyles", [])
    """
    
    decisions = []
    
    # Find all decision patterns like ", W (1-0)" or ", BS (2)"
    decision_matches = re.findall(r',\s*([WLSHB]+)\s*\([^)]*\)', raw_entry)
    decisions.extend(decision_matches)
    
    # Remove all decision patterns to get clean name
    clean_name = re.sub(r',\s*[WLSHB]+\s*\([^)]*\)', '', raw_entry).strip()
    
    return clean_name, decisions

def expand_position_code(code: str) -> Optional[str]:
    """Expand position codes to full names"""
    position_map = {
        'P': 'Pitcher', 'C': 'Catcher',
        '1B': 'First Base', '2B': 'Second Base', '3B': 'Third Base', 'SS': 'Shortstop',
        'LF': 'Left Field', 'CF': 'Center Field', 'RF': 'Right Field',
        'DH': 'Designated Hitter', 'PH': 'Pinch Hitter', 'PR': 'Pinch Runner'
    }
    return position_map.get(code.upper())

# ============================================================================
# UPDATE YOUR EXISTING PIPELINE
# ============================================================================

def process_single_game_with_appearances(game_url: str) -> Dict:
    """
    Updated version of your process_single_game that includes appearance metadata
    
    Replace your existing function with this enhanced version
    """
    start_time = time.time()
    
    # Fetch page (your existing logic)
    from utils.mlb_cached_fetcher import SafePageFetcher
    soup = SafePageFetcher.fetch_page(game_url)
    
    # Extract game metadata (from previous step)
    from parsing.game_metadata import extract_game_metadata
    game_metadata = extract_game_metadata(soup, game_url)
    
    game_id = game_metadata['game_id']
    
    # Parse official stats WITH appearance metadata (ENHANCED)
    official_batting, batting_appearances = parse_official_batting_with_appearances(soup)
    official_pitching, pitching_decisions = parse_official_pitching_with_decisions(soup)
    
    # Your existing event parsing
    from parsing.game_parser import parse_play_by_play_events
    pbp_events = parse_play_by_play_events(soup, game_id)
    
    # Your existing validation
    from validation.stat_validator import validate_batting_stats, validate_pitching_stats
    batting_validation = validate_batting_stats(official_batting, pbp_events)
    pitching_validation = validate_pitching_stats(official_pitching, pbp_events)

    time_to_process = time.time() - start_time
    
    return {
        'game_id': game_id,
        'game_metadata': game_metadata,          # → games table
        'official_batting': official_batting,    # → validation (clean stats)
        'official_pitching': official_pitching,  # → validation (clean stats)
        'pbp_events': pbp_events,        # → events table
        'batting_validation': batting_validation,
        'pitching_validation': pitching_validation,
        'time_to_process': time_to_process,
        
        # NEW: Appearance metadata for database
        'batting_appearances': batting_appearances,  # → appearances table
        'pitching_decisions': pitching_decisions,    # → appearances table
    }

# ============================================================================
# EXAMPLE OUTPUT STRUCTURE
# ============================================================================

def show_example_output():
    """Show what the enhanced parsing produces"""
    
    print("📊 ENHANCED PARSING OUTPUT")
    print("=" * 35)
    
    print("✅ BATTING APPEARANCES:")
    batting_appearances_example = [
        {
            'player_name': 'Aaron Judge',
            'team': 'home',
            'batting_order': 3,
            'positions_played': ['Right Field'],
            'is_starter': True,
            'is_pinch_hitter': False,
            'PA': 4, 'AB': 3, 'H': 1, 'HR': 1, 'RBI': 2
        },
        {
            'player_name': 'Juan Soto', 
            'team': 'home',
            'batting_order': 2,
            'positions_played': ['Left Field', 'Designated Hitter'],  # Multiple positions
            'is_starter': True,
            'is_pinch_hitter': False,
            'PA': 4, 'AB': 4, 'H': 2, 'HR': 0, 'RBI': 1
        },
        {
            'player_name': 'Oswaldo Cabrera',
            'team': 'home', 
            'batting_order': None,  # Pinch hitter
            'positions_played': ['Pinch Hitter'],
            'is_starter': False,
            'is_pinch_hitter': True,
            'PA': 1, 'AB': 1, 'H': 0, 'HR': 0, 'RBI': 0
        }
    ]
    
    for appearance in batting_appearances_example:
        name = appearance['player_name']
        order = appearance['batting_order']
        positions = ', '.join(appearance['positions_played'])
        stats = f"{appearance['H']}-{appearance['AB']}"
        
        if appearance['is_pinch_hitter']:
            print(f"   PH: {name} ({positions}) - {stats}")
        else:
            print(f"   {order}: {name} ({positions}) - {stats}")
    
    print("\n✅ PITCHING DECISIONS:")
    pitching_decisions_example = [
        {
            'pitcher_name': 'Gerrit Cole',
            'team': 'home',
            'decisions': ['W'],  # Win
            'is_starter': True,
            'pitching_order': 1
        },
        {
            'pitcher_name': 'Clay Holmes',
            'team': 'home', 
            'decisions': ['BS', 'L'],  # Blown save AND loss
            'is_starter': False,
            'pitching_order': 3
        }
    ]
    
    for decision in pitching_decisions_example:
        name = decision['pitcher_name']
        decisions_str = ', '.join(decision['decisions']) if decision['decisions'] else 'None'
        role = 'Starter' if decision['is_starter'] else 'Reliever'
        
        print(f"   {name} ({role}): {decisions_str}")
    
    print("\n🎯 DATABASE READY:")
    print("   📊 Clean stats → validation system (unchanged)")
    print("   👥 Appearances → database appearances table")
    print("   🏆 Decisions → database appearances table") 
    print("   ⚾ Single parse → dual output")

if __name__ == "__main__":
    test_urls = [
        "https://www.baseball-reference.com/boxes/KCA/KCA202503290.shtml",
        "https://www.baseball-reference.com//boxes/NYA/NYA202505050.shtml",
        "https://www.baseball-reference.com//boxes/TEX/TEX202505180.shtml",
        "https://www.baseball-reference.com//boxes/CLE/CLE202505300.shtml",
        "https://www.baseball-reference.com//boxes/ANA/ANA202506230.shtml",
        "https://www.baseball-reference.com//boxes/ATL/ATL202506270.shtml",
        ]
    results = process_single_game_with_appearances(test_urls[0])

    print(f"✅ {results['game_id']}: Batting {results['batting_validation']['accuracy']:.1f}%, Pitching: {results['pitching_validation']['accuracy']:.1f}%")
    print(f"⏱️ Processing Time: {results['time_to_process']:.2f}s\n")
    print("Batting Box Score:")
    print(f"{results['official_batting']}\n")
    print("Pitching Box Score:")
    print(f"{results['official_pitching']}\n")
    print("Play By Play Events Table:")
    print(f"{results['pbp_events']}\n")
    if results['batting_validation']['differences']:
        print("Batting Differences:")
        print(results['batting_validation']['differences'])
    if results['pitching_validation']['differences']:
        print("Pitching Differences:")
        print(results['pitching_validation']['differences'])
    if results['batting_validation']['name_mismatches']['unmatched_official_names']:
        print("Name Mismatches:")
        print(results['batting_validation']['name_mismatches'])
    print("Batting Appearances:")
    print(f"{results['batting_appearances']}\n")
    print("Pitching Decisions:")
    print(f"{results['pitching_decisions']}\n")


        