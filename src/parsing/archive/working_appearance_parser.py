"""
Working Appearance Parser - Clean Version
========================================
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

from parsing.name_utils import normalize_name
from parsing.game_utils import safe_int, extract_from_details

def extract_name_and_positions(raw_entry: str) -> Tuple[str, List[str]]:
    """Extract clean player name and positions"""
    
    # Remove decisions first
    cleaned = re.sub(r',\s*[WLSHB]+\s*\([^)]*\)', '', raw_entry).strip()
    
    positions = []
    player_name = cleaned
    
    # Look for position codes at the end
    position_match = re.search(r'\s+([A-Z0-9]{1,2}(?:-[A-Z0-9]{1,2})*)\s*$', cleaned)
    
    if position_match:
        position_codes = position_match.group(1)
        player_name = cleaned[:position_match.start()].strip()
        
        # Split and expand positions
        for code in position_codes.split('-'):
            expanded = expand_position_code(code.strip())
            if expanded and expanded not in positions:
                positions.append(expanded)
    
    clean_name = normalize_name(player_name)
    return clean_name, positions

def expand_position_code(code: str) -> Optional[str]:
    """Expand position codes to full names"""
    position_map = {
        'P': 'Pitcher', 'C': 'Catcher', '1B': 'First Base', '2B': 'Second Base',
        '3B': 'Third Base', 'SS': 'Shortstop', 'LF': 'Left Field', 'CF': 'Center Field',
        'RF': 'Right Field', 'DH': 'Designated Hitter', 'PH': 'Pinch Hitter', 'PR': 'Pinch Runner',
    }
    return position_map.get(code.upper())

def test_position_extraction():
    """Test position extraction"""
    
    print("Testing position extraction:")
    print("=" * 30)
    
    test_names = [
        "Steven Kwan LF",
        "José Ramírez 3B", 
        "Salvador Perez C-1B",
        "Dairon Blanco PR",
        "Gavin Williams P",
    ]
    
    for raw_name in test_names:
        clean_name, positions = extract_name_and_positions(raw_name)
        position_match = re.search(r'\s+([A-Z0-9]{1,2}(?:-[A-Z0-9]{1,2})*)\s*$', raw_name)
        
        print(f"Raw: '{raw_name}'")
        print(f"  Clean: '{clean_name}'")
        print(f"  Positions: {positions}")
        if position_match:
            print(f"  Regex: Found '{position_match.group(1)}'")
        else:
            print(f"  Regex: No match")
        print()

def analyze_batting_order(df: pd.DataFrame, table: BeautifulSoup) -> Dict[int, Dict]:
    """Analyze batting order and substitutions"""
    
    analysis = {}
    batting_order = 1
    starters_by_position = {}
    
    # Get HTML rows
    rows = table.find_all('tr')
    data_rows = [row for row in rows if row.find('td')]
    
    for idx, row in df.iterrows():
        raw_name = str(row['Batting'])
        pa = safe_int(row.get('PA', 0))
        ab = safe_int(row.get('AB', 0))
        
        # Extract info
        clean_name, positions = extract_name_and_positions(raw_name)
        primary_position = positions[0] if positions else None
        
        # Get CSK from HTML
        csk = ''
        if idx < len(data_rows):
            html_row = data_rows[idx]
            cells = html_row.find_all(['td', 'th'])
            if cells:
                csk = cells[0].get('csk', '')
        
        # Check if pitcher
        pitcher_pattern = r'\s+P\s*$'
        is_pitcher_name = bool(re.search(pitcher_pattern, raw_name))
        is_pitcher_csk = csk.startswith('10')
        is_pitcher_pos = 'Pitcher' in positions
        is_pitcher = is_pitcher_name or is_pitcher_csk or is_pitcher_pos
        
        is_pinch_runner = 'PR' in raw_name or 'Pinch Runner' in positions
        has_batting_stats = pa > 0 or ab > 0
        
        player_analysis = {
            'clean_name': clean_name,
            'positions': positions,
            'primary_position': primary_position,
            'pa': pa,
            'ab': ab,
            'is_pitcher': is_pitcher,
        }
        
        if is_pitcher:
            player_analysis.update({
                'batting_order': None,
                'is_starter': False,
                'is_substitute': False,
            })
            
        elif is_pinch_runner:
            player_analysis.update({
                'batting_order': None,
                'is_starter': False,
                'is_substitute': True,
                'substitution_type': 'pinch_runner',
                'is_pinch_hitter': False,
            })
            
        elif batting_order <= 9 and has_batting_stats:
            # This is a starter
            player_analysis.update({
                'batting_order': batting_order,
                'is_starter': True,
                'is_substitute': False,
                'is_pinch_hitter': False,
            })
            
            # Track position for substitutions
            if primary_position:
                starters_by_position[primary_position] = {
                    'name': clean_name,
                    'batting_order': batting_order
                }
            
            batting_order += 1
            
        elif has_batting_stats:
            # This is a substitute
            replaced_info = None
            if primary_position and primary_position in starters_by_position:
                replaced_info = starters_by_position[primary_position]
            
            player_analysis.update({
                'batting_order': replaced_info['batting_order'] if replaced_info else None,
                'is_starter': False,
                'is_substitute': True,
                'is_pinch_hitter': 'PH' in raw_name,
                'substitution_type': 'pinch_hitter' if 'PH' in raw_name else 'substitute',
                'replaced_player': replaced_info['name'] if replaced_info else None,
            })
            
        else:
            # No batting stats
            player_analysis.update({
                'batting_order': None,
                'is_starter': False,
                'is_substitute': True,
                'is_pinch_hitter': False,
                'substitution_type': 'defensive_substitute',
            })
        
        analysis[idx] = player_analysis
    
    return analysis

def parse_batting_appearances(soup: BeautifulSoup) -> Tuple[pd.DataFrame, List[Dict]]:
    """Parse batting with appearance metadata"""
    
    batting_tables = soup.find_all('table', {'id': lambda x: x and x.endswith('batting')})
    all_stats = []
    all_appearances = []
    
    for table_idx, table in enumerate(batting_tables):
        try:
            df = pd.read_html(StringIO(str(table)))[0]
            df = df[df['Batting'].notna()]
            df = df[~df['Batting'].str.contains("Team Totals", na=False)]
            
            team = 'away' if table_idx == 0 else 'home'
            
            # Analyze lineup
            lineup_analysis = analyze_batting_order(df, table)
            
            for row_index, row in df.iterrows():
                analysis = lineup_analysis.get(row_index, {})
                
                # Skip pitchers
                if analysis.get('is_pitcher', False):
                    continue
                
                player_name = analysis.get('clean_name', '')
                positions = analysis.get('positions', [])
                
                if not player_name:
                    continue
                
                # Stats record
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
                
                # Appearance record
                appearance_record = {
                    'player_name': player_name,
                    'team': team,
                    'batting_order': analysis.get('batting_order'),
                    'positions_played': positions,
                    'is_starter': analysis.get('is_starter', False),
                    'is_pinch_hitter': analysis.get('is_pinch_hitter', False),
                    'is_substitute': analysis.get('is_substitute', False),
                    'substitution_type': analysis.get('substitution_type'),
                    'replaced_player': analysis.get('replaced_player'),
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

def process_game_final(game_url: str) -> Dict:
    """Process game with final appearance logic"""
    start_time = time.time()
    
    from utils.mlb_cached_fetcher import SafePageFetcher
    soup = SafePageFetcher.fetch_page(game_url)
    
    from parsing.game_metadata import extract_game_metadata
    game_metadata = extract_game_metadata(soup, game_url)
    game_id = game_metadata['game_id']
    
    # Parse with final logic
    official_batting, batting_appearances = parse_batting_appearances(soup)
    
    from parsing.game_parser import parse_official_pitching, parse_play_by_play_events
    official_pitching = parse_official_pitching(soup)
    pbp_events = parse_play_by_play_events(soup, game_id)
    
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

def show_results():
    """Show final results"""
    
    test_url = "https://www.baseball-reference.com/boxes/KCA/KCA202503290.shtml"
    result = process_game_final(test_url)
    
    print(f"Results for {result['game_id']}")
    print("=" * 40)
    print(f"Batting accuracy: {result['batting_validation']['accuracy']:.1f}%")
    
    # Show appearances by team
    away_apps = [app for app in result['batting_appearances'] if app['team'] == 'away']
    home_apps = [app for app in result['batting_appearances'] if app['team'] == 'home']
    
    print(f"\nAWAY TEAM:")
    for app in away_apps:
        name = app['player_name']
        order = app['batting_order']
        positions = ', '.join(app['positions_played']) if app['positions_played'] else 'Unknown'
        pa = app['PA']
        
        if app['is_starter']:
            role = f"Starter #{order}"
        elif app['is_substitute']:
            sub_type = app.get('substitution_type', 'sub')
            role = sub_type
        else:
            role = "Other"
        
        order_str = str(order) if order else '--'
        print(f"  {order_str:>2}: {name:18s} ({positions:15s}) {role:15s} PA={pa}")
    
    print(f"\nHOME TEAM:")
    for app in home_apps:
        name = app['player_name']
        order = app['batting_order']
        positions = ', '.join(app['positions_played']) if app['positions_played'] else 'Unknown'
        pa = app['PA']
        
        if app['is_starter']:
            role = f"Starter #{order}"
        elif app['is_substitute']:
            sub_type = app.get('substitution_type', 'sub')
            role = sub_type
        else:
            role = "Other"
        
        order_str = str(order) if order else '--'
        print(f"  {order_str:>2}: {name:18s} ({positions:15s}) {role:15s} PA={pa}")

if __name__ == "__main__":
    # Test position extraction first
    test_position_extraction()
    
    print("\n" + "="*50)
    
    # Show results
    show_results()
