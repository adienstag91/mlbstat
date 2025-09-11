"""
Clean Fixed Player Appearances Parser
====================================

Fresh, clean version that fixes the position parsing and batting order issues.
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
    Parse batting stats AND extract appearance metadata with proper fixes
    """
    
    batting_tables = soup.find_all('table', {'id': lambda x: x and x.endswith('batting')})
    all_stats = []
    all_appearances = []
    
    for table_idx, table in enumerate(batting_tables):
        try:
            df = pd.read_html(StringIO(str(table)))[0]
            df = df[df['Batting'].notna()]
            df = df[~df['Batting'].str.contains("Team Totals", na=False)]
            
            team = 'away' if table_idx == 0 else 'home'
            
            # Build batting order map
            batting_order_map = build_batting_order_map(df['Batting'].tolist())
            
            for row_index, row in df.iterrows():
                raw_batting_entry = str(row['Batting'])
                
                # Extract appearance info
                player_name, positions, appearance_metadata = extract_batting_appearance_info(
                    raw_batting_entry, row_index, batting_order_map
                )
                
                if not player_name:
                    continue
                
                # Build clean stats record
                stats_record = {
                    'player_name': player_name,
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
                
                # Build appearance record
                appearance_record = {
                    'player_name': player_name,
                    'team': team,
                    'batting_order': appearance_metadata['batting_order'],
                    'positions_played': positions,
                    'is_starter': appearance_metadata['is_starter'],
                    'is_pinch_hitter': appearance_metadata['is_pinch_hitter'],
                    'is_substitute': appearance_metadata['is_substitute'],
                    
                    # Include stats
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
    """Build a map of table_index -> actual_batting_order"""
    
    batting_order_map = {}
    current_batting_order = 1
    
    for idx, raw_name in enumerate(raw_names):
        # Skip pitchers
        if is_likely_pitcher(raw_name):
            batting_order_map[idx] = None
            continue
        
        # Skip pinch runners
        if 'PR' in raw_name:
            batting_order_map[idx] = None
            continue
            
        # Assign batting orders 1-9 to first 9 non-pitcher, non-PR entries
        if current_batting_order <= 9:
            batting_order_map[idx] = current_batting_order
            current_batting_order += 1
        else:
            batting_order_map[idx] = None
    
    return batting_order_map

def extract_batting_appearance_info(raw_entry: str, table_index: int, 
                                   batting_order_map: Dict[int, int]) -> Tuple[str, List[str], Dict]:
    """Extract all batting appearance info from raw entry"""
    
    # Remove decisions first
    cleaned = re.sub(r',\s*[WLSHB]+\s*\([^)]*\)', '', raw_entry).strip()
    
    # Extract position codes - fixed patterns
    position_patterns = [
        r'\s+([A-Z0-9]{1,2}(?:-[A-Z0-9]{1,2})*)\s*$',  # C-1B, 3B, etc.
        r'\s+([0-9]B|SS|[LCR]F|DH|C|P|PH|PR)\s*$',     # Common codes
    ]
    
    positions = []
    player_name = cleaned
    position_codes = ""
    
    for pattern in position_patterns:
        position_match = re.search(pattern, player_name)
        if position_match:
            position_codes = position_match.group(1)
            player_name = player_name[:position_match.start()].strip()
            
            # Handle multiple positions like "C-1B"
            pos_codes = position_codes.split('-')
            for code in pos_codes:
                full_pos = expand_position_code(code)
                if full_pos and full_pos not in positions:
                    positions.append(full_pos)
            break
    
    # Determine batting order and role
    assigned_batting_order = batting_order_map.get(table_index)
    
    is_ph = 'PH' in position_codes
    is_pr = 'PR' in position_codes
    is_starter = assigned_batting_order is not None and assigned_batting_order <= 9 and not is_ph and not is_pr
    is_substitute = not is_starter and (is_ph or is_pr or assigned_batting_order is None)
    
    appearance_metadata = {
        'batting_order': assigned_batting_order,
        'is_starter': is_starter,
        'is_pinch_hitter': is_ph,
        'is_substitute': is_substitute,
    }
    
    # Final name cleaning
    final_clean_name = normalize_name(player_name)
    
    return final_clean_name, positions, appearance_metadata

def expand_position_code(code: str) -> Optional[str]:
    """Expand position codes to full names"""
    position_map = {
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
        'PH': 'Pinch Hitter',
        'PR': 'Pinch Runner',
    }
    return position_map.get(code.upper())

def is_likely_pitcher(raw_name: str) -> bool:
    """Determine if a player is likely a pitcher"""
    return bool(re.search(r'\s+P\s*$', raw_name))

def parse_official_pitching_with_decisions_fixed(soup: BeautifulSoup) -> Tuple[pd.DataFrame, List[Dict]]:
    """Parse pitching stats AND extract decisions"""
    
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
                
                # Extract decisions
                pitcher_name, decisions = extract_pitcher_decisions(raw_pitching_entry)
                
                if not pitcher_name:
                    continue
                
                # Build clean stats record
                stats_record = {
                    'pitcher_name': pitcher_name,
                    'BF': safe_int(row.get('BF', 0)),
                    'H': safe_int(row.get('H', 0)),
                    'BB': safe_int(row.get('BB', 0)),
                    'SO': safe_int(row.get('SO', 0)),
                    'HR': safe_int(row.get('HR', 0)),
                    'PC': safe_int(row.get('Pit', 0)),
                }
                
                # Build decision record
                decision_record = {
                    'pitcher_name': pitcher_name,
                    'team': team,
                    'decisions': decisions,
                    'is_starter': pitcher_index == 0,
                    'pitching_order': pitcher_index + 1,
                }
                
                all_stats.append(stats_record)
                all_decisions.append(decision_record)
                
        except Exception as e:
            print(f"Error parsing pitching table {table_idx}: {e}")
            continue
    
    return pd.DataFrame(all_stats), all_decisions

def extract_pitcher_decisions(raw_entry: str) -> Tuple[str, List[str]]:
    """Extract pitcher name and all decisions"""
    
    decisions = []
    
    # Find all decision patterns
    decision_matches = re.findall(r',\s*([WLSHB]+)\s*\([^)]*\)', raw_entry)
    decisions.extend(decision_matches)
    
    # Remove all decision patterns to get clean name
    clean_name = re.sub(r',\s*[WLSHB]+\s*\([^)]*\)', '', raw_entry).strip()
    clean_name = normalize_name(clean_name)
    
    return clean_name, decisions

def process_single_game_with_fixed_appearances(game_url: str) -> Dict:
    """Process single game with fixed appearance parsing"""
    start_time = time.time()
    
    # Fetch page
    from utils.mlb_cached_fetcher import SafePageFetcher
    soup = SafePageFetcher.fetch_page(game_url)
    
    # Extract game metadata
    from parsing.game_metadata import extract_game_metadata
    game_metadata = extract_game_metadata(soup, game_url)
    game_id = game_metadata['game_id']
    
    # Parse with fixed functions
    official_batting, batting_appearances = parse_official_batting_with_appearances_fixed(soup)
    official_pitching, pitching_decisions = parse_official_pitching_with_decisions_fixed(soup)
    
    # Parse events
    from parsing.game_parser import parse_play_by_play_events
    pbp_events = parse_play_by_play_events(soup, game_id)
    
    # Validation
    from validation.stat_validator import validate_batting_stats, validate_pitching_stats
    batting_validation = validate_batting_stats(official_batting, pbp_events)
    pitching_validation = validate_pitching_stats(official_pitching, pbp_events)

    time_to_process = time.time() - start_time
    
    return {
        'game_id': game_id,
        'game_metadata': game_metadata,
        'official_batting': official_batting,
        'official_pitching': official_pitching,
        'pbp_events': pbp_events,
        'batting_validation': batting_validation,
        'pitching_validation': pitching_validation,
        'time_to_process': time_to_process,
        'batting_appearances': batting_appearances,
        'pitching_decisions': pitching_decisions,
    }

def debug_name_cleaning():
    """Debug function to test name cleaning"""
    
    print("🔍 DEBUGGING NAME CLEANING")
    print("=" * 40)
    
    test_names = [
        "José Ramírez 3B",
        "Salvador Perez C-1B", 
        "Michael Massey 2B",
        "Carlos Santana 1B",
        "Daniel Schneemann 2B",
        "Maikel Garcia 3B",
        "Cavan Biggio 1B",
        "Gabriel Arias 3B",
        "Dairon Blanco PR",
        "Aaron Judge RF-DH",
    ]
    
    # Create realistic batting order map
    batting_order_map = {
        0: 1, 1: 2, 2: 3, 3: 4, 4: 5, 
        5: 6, 6: 7, 7: 8, 8: 9, 9: None  # PR gets None
    }
    
    for idx, raw_name in enumerate(test_names):
        cleaned_name, positions, metadata = extract_batting_appearance_info(
            raw_name, idx, batting_order_map
        )
        
        print(f"Raw: '{raw_name}'")
        print(f"  → Clean: '{cleaned_name}'")
        print(f"  → Positions: {positions}")
        print(f"  → Order: {metadata['batting_order']}")
        print(f"  → Role: {'Starter' if metadata['is_starter'] else 'Sub'}")
        print()

if __name__ == "__main__":
    
    # Debug name cleaning
    debug_name_cleaning()
    
    print("\n" + "="*50)
    print("TESTING FIXED APPEARANCE PARSING")
    print("="*50)
    
    # Test with your game
    test_url = "https://www.baseball-reference.com/boxes/KCA/KCA202503290.shtml"
    
    result = process_single_game_with_fixed_appearances(test_url)
    
    print(f"✅ {result['game_id']}")
    print(f"⚾ Batting accuracy: {result['batting_validation']['accuracy']:.1f}%")  
    print(f"🥎 Pitching accuracy: {result['pitching_validation']['accuracy']:.1f}%")
    
    # Check name mismatches
    if result['batting_validation'].get('name_mismatches'):
        mismatches = result['batting_validation']['name_mismatches']
        print(f"\n⚠️  Remaining name mismatches: {len(mismatches.get('unmatched_official_names', []))}")
        
        if mismatches.get('unmatched_official_names'):
            print("Still problematic names:")
            for name in mismatches['unmatched_official_names'][:5]:
                print(f"  - '{name}'")
    
    # Show sample appearances
    print(f"\n👥 SAMPLE BATTING APPEARANCES:")
    for appearance in result['batting_appearances']:
        name = appearance['player_name']
        order = appearance['batting_order'] 
        positions = ', '.join(appearance['positions_played']) if appearance['positions_played'] else 'Unknown'
        role = 'Starter' if appearance['is_starter'] else ('PH' if appearance['is_pinch_hitter'] else 'Sub')
        
        order_str = str(order) if order else 'PR/P'
        print(f"  {order_str:>2}: {name} ({positions}) - {role}")
