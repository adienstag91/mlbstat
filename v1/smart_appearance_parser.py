"""
Smart Appearance Parser with Proper Batting Order Logic
======================================================

Based on the HTML analysis, this creates a much smarter parser that:
1. Excludes pitchers from batting appearances entirely
2. Uses PA/AB stats to determine actual batting participation
3. Identifies substitutions based on position overlap and stats
4. Correctly assigns batting orders based on game flow
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

def parse_official_batting_with_smart_appearances(soup: BeautifulSoup) -> Tuple[pd.DataFrame, List[Dict]]:
    """
    Smart parsing that properly handles batting order and substitutions
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
            
            # Smart analysis of the team's batting lineup
            lineup_analysis = analyze_team_lineup(df['Batting'].tolist(), df, table)
            
            for row_index, row in df.iterrows():
                raw_batting_entry = str(row['Batting'])
                
                # Get analysis for this player
                player_analysis = lineup_analysis.get(row_index, {})
                
                # Extract clean name and positions
                player_name, positions = extract_clean_name_and_positions(raw_batting_entry)
                
                if not player_name:
                    continue
                
                # Skip pitchers entirely from batting appearances
                if player_analysis.get('is_pitcher', False):
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
                
                # Build smart appearance record
                appearance_record = {
                    'player_name': player_name,
                    'team': team,
                    'batting_order': player_analysis.get('batting_order'),
                    'positions_played': positions,
                    'is_starter': player_analysis.get('is_starter', False),
                    'is_pinch_hitter': player_analysis.get('is_pinch_hitter', False),
                    'is_substitute': player_analysis.get('is_substitute', False),
                    'substitution_type': player_analysis.get('substitution_type'),
                    'replaced_player': player_analysis.get('replaced_player'),
                    
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

def analyze_team_lineup(raw_names: List[str], df: pd.DataFrame, table: BeautifulSoup) -> Dict[int, Dict]:
    """
    Analyze the entire team lineup to identify starters, substitutes, and batting order
    """
    
    lineup_analysis = {}
    batting_order = 1
    position_assignments = {}  # Track which positions are taken by starters
    
    # Get CSK values from HTML to understand Baseball Reference's sorting
    rows = table.find_all('tr')
    data_rows = [row for row in rows if row.find('td')]
    csk_values = []
    
    for row in data_rows:
        cells = row.find_all(['td', 'th'])
        if cells:
            player_cell = cells[0]
            csk = player_cell.get('csk', '')
            csk_values.append(csk)
    
    # Analyze each player
    for idx, raw_name in enumerate(raw_names):
        try:
            row_data = df.iloc[idx]
            pa = safe_int(row_data.get('PA', 0))
            ab = safe_int(row_data.get('AB', 0))
            
            # Extract clean info
            clean_name, positions = extract_clean_name_and_positions(raw_name)
            primary_position = positions[0] if positions else None
            
            analysis = {
                'raw_name': raw_name,
                'clean_name': clean_name,
                'positions': positions,
                'primary_position': primary_position,
                'pa': pa,
                'ab': ab,
            }
            
            # Determine player type
            is_pitcher = bool(re.search(r'\\s+P\\s*$', raw_name))
            is_pinch_runner = 'PR' in raw_name
            has_batting_stats = pa > 0 or ab > 0
            
            # Get CSK value for sorting insight
            csk = csk_values[idx] if idx < len(csk_values) else ''
            is_pitcher_by_csk = csk.startswith('10')  # Pitchers have csk 101, 103, etc.
            
            if is_pitcher or is_pitcher_by_csk:
                analysis.update({
                    'is_pitcher': True,
                    'is_starter': False,
                    'is_substitute': False,
                    'batting_order': None,
                })
                
            elif is_pinch_runner:
                analysis.update({
                    'is_pitcher': False,
                    'is_pinch_runner': True,
                    'is_starter': False,
                    'is_substitute': True,
                    'substitution_type': 'pinch_runner',
                    'batting_order': None,
                })
                
            elif batting_order <= 9 and has_batting_stats:
                # This is likely a starter
                analysis.update({
                    'is_pitcher': False,
                    'is_starter': True,
                    'is_substitute': False,
                    'batting_order': batting_order,
                })
                
                # Track position assignment
                if primary_position:
                    position_assignments[primary_position] = {
                        'starter': clean_name,
                        'batting_order': batting_order
                    }
                
                batting_order += 1
                
            elif has_batting_stats:
                # This player has batting stats but comes after the first 9
                # They're likely a substitute who replaced someone
                replaced_info = find_replaced_player(primary_position, position_assignments)
                
                analysis.update({
                    'is_pitcher': False,
                    'is_starter': False,
                    'is_substitute': True,
                    'substitution_type': 'batting_substitute',
                    'batting_order': replaced_info.get('batting_order') if replaced_info else None,
                    'replaced_player': replaced_info.get('starter') if replaced_info else None,
                })
                
            else:
                # Player with no batting stats - defensive substitute
                analysis.update({
                    'is_pitcher': False,
                    'is_starter': False,
                    'is_substitute': True,
                    'substitution_type': 'defensive_substitute',
                    'batting_order': None,
                })
            
            lineup_analysis[idx] = analysis
            
        except Exception as e:
            print(f"Error analyzing player {idx}: {e}")
            lineup_analysis[idx] = {'is_pitcher': True}  # Default to exclude
    
    return lineup_analysis

def find_replaced_player(substitute_position: str, position_assignments: Dict) -> Optional[Dict]:
    """
    Find which starter this substitute likely replaced based on position
    """
    if not substitute_position or substitute_position not in position_assignments:
        return None
    
    return position_assignments[substitute_position]

def extract_clean_name_and_positions(raw_entry: str) -> Tuple[str, List[str]]:
    """
    Extract clean player name and positions from raw batting entry
    """
    
    # Remove decisions first
    cleaned = re.sub(r',\\s*[WLSHB]+\\s*\\([^)]*\\)', '', raw_entry).strip()
    
    # Extract position codes
    position_patterns = [
        r'\\s+([A-Z0-9]{1,2}(?:-[A-Z0-9]{1,2})*)\\s*$',  # C-1B, 3B, etc.
        r'\\s+([0-9]B|SS|[LCR]F|DH|C|P|PH|PR)\\s*$',     # Common codes
    ]
    
    positions = []
    player_name = cleaned
    
    for pattern in position_patterns:
        position_match = re.search(pattern, player_name)
        if position_match:
            position_codes = position_match.group(1)
            player_name = player_name[:position_match.start()].strip()
            
            # Handle multiple positions
            pos_codes = position_codes.split('-')
            for code in pos_codes:
                full_pos = expand_position_code(code)
                if full_pos and full_pos not in positions:
                    positions.append(full_pos)
            break
    
    clean_name = normalize_name(player_name)
    return clean_name, positions

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

def process_single_game_with_smart_appearances(game_url: str) -> Dict:
    """Process single game with smart appearance parsing"""
    start_time = time.time()
    
    # Fetch page
    from utils.mlb_cached_fetcher import SafePageFetcher
    soup = SafePageFetcher.fetch_page(game_url)
    
    # Extract game metadata
    from parsing.game_metadata import extract_game_metadata
    game_metadata = extract_game_metadata(soup, game_url)
    game_id = game_metadata['game_id']
    
    # Smart parsing
    official_batting, batting_appearances = parse_official_batting_with_smart_appearances(soup)
    
    # Pitching (unchanged)
    from parsing.game_parser import parse_official_pitching
    official_pitching = parse_official_pitching(soup)
    
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
    }

def debug_smart_parsing():
    """Debug the smart parsing logic"""
    
    print("🧠 SMART PARSING DEBUG")
    print("=" * 40)
    
    test_url = "https://www.baseball-reference.com/boxes/KCA/KCA202503290.shtml"
    result = process_single_game_with_smart_appearances(test_url)
    
    print(f"✅ {result['game_id']}")
    print(f"⚾ Batting accuracy: {result['batting_validation']['accuracy']:.1f}%")
    
    # Show batting appearances (should exclude pitchers now)
    print(f"\\n👥 SMART BATTING APPEARANCES:")
    
    for appearance in result['batting_appearances']:
        name = appearance['player_name']
        order = appearance['batting_order']
        positions = ', '.join(appearance['positions_played']) if appearance['positions_played'] else 'Unknown'
        
        if appearance['is_starter']:
            role = f"Starter #{order}"
        elif appearance['is_substitute']:
            sub_type = appearance.get('substitution_type', 'substitute')
            replaced = appearance.get('replaced_player', '')
            if replaced:
                role = f"{sub_type.title()} (replaced {replaced})"
            else:
                role = sub_type.title()
        else:
            role = "Unknown"
        
        pa = appearance['PA']
        ab = appearance['AB']
        
        order_str = str(order) if order else '--'
        print(f"  {order_str:>2}: {name:20s} ({positions:15s}) {role:25s} PA={pa} AB={ab}")

if __name__ == "__main__":
    debug_smart_parsing()
