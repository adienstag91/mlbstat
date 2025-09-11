"""
Corrected Appearance Parser
==========================

Fixes the position extraction and batting order logic based on the HTML analysis.
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

def parse_official_batting_with_corrected_appearances(soup: BeautifulSoup) -> Tuple[pd.DataFrame, List[Dict]]:
    """
    Corrected parsing that properly handles batting order and substitutions
    """
    
    batting_tables = soup.find_all('table', {'id': lambda x: x and x.endswith('batting')})
    all_stats = []
    all_appearances = []
    
    for table_idx, table in enumerate(batting_tables):
        try:
            # Parse table data
            df = pd.read_html(StringIO(str(table)))[0]
            df = df[df['Batting'].notna()]
            df = df[~df['Batting'].str.contains("Team Totals", na=False)]
            
            team = 'away' if table_idx == 0 else 'home'
            
            # Get HTML rows for CSK analysis
            rows = table.find_all('tr')
            data_rows = [row for row in rows if row.find('td')]
            
            # Analyze lineup intelligently
            lineup_data = analyze_lineup_correctly(df, data_rows, team)
            
            for row_index, row in df.iterrows():
                raw_batting_entry = str(row['Batting'])
                analysis = lineup_data.get(row_index, {})
                
                # Skip pitchers entirely
                if analysis.get('is_pitcher', False):
                    continue
                
                player_name = analysis.get('clean_name', '')
                positions = analysis.get('positions', [])
                
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
                    'batting_order': analysis.get('batting_order'),
                    'positions_played': positions,
                    'is_starter': analysis.get('is_starter', False),
                    'is_pinch_hitter': analysis.get('is_pinch_hitter', False),
                    'is_substitute': analysis.get('is_substitute', False),
                    'substitution_type': analysis.get('substitution_type'),
                    'replaced_player': analysis.get('replaced_player'),
                    
                    # Stats
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

def analyze_lineup_correctly(df: pd.DataFrame, html_rows: List, team: str) -> Dict[int, Dict]:
    """
    Correctly analyze lineup based on stats and position logic
    """
    
    lineup_analysis = {}
    starters_by_position = {}
    batting_order = 1
    
    # First pass: Identify starters and pitchers
    for idx, row in df.iterrows():
        raw_name = str(row['Batting'])
        pa = safe_int(row.get('PA', 0))
        ab = safe_int(row.get('AB', 0))
        
        # Extract clean name and positions
        clean_name, positions = extract_name_and_positions_fixed(raw_name)
        primary_position = positions[0] if positions else None
        
        # Get CSK from HTML if available
        csk = ''
        if idx < len(html_rows):
            html_row = html_rows[idx]
            cells = html_row.find_all(['td', 'th'])
            if cells:
                csk = cells[0].get('csk', '')
        
        # Determine player type
        is_pitcher = (
            bool(re.search(r'\s+P\s*
        
        is_pinch_runner = 'PR' in raw_name or 'Pinch Runner' in positions
        has_batting_stats = pa > 0 or ab > 0
        
        analysis = {
            'raw_name': raw_name,
            'clean_name': clean_name,
            'positions': positions,
            'primary_position': primary_position,
            'pa': pa,
            'ab': ab,
            'csk': csk,
            'is_pitcher': is_pitcher,
        }
        
        if is_pitcher:
            analysis.update({
                'batting_order': None,
                'is_starter': False,
                'is_substitute': False,
            })
            
        elif is_pinch_runner:
            analysis.update({
                'batting_order': None,
                'is_starter': False,
                'is_substitute': True,
                'substitution_type': 'pinch_runner',
                'is_pinch_hitter': False,
            })
            
        elif batting_order <= 9 and has_batting_stats:
            # This is a starter
            analysis.update({
                'batting_order': batting_order,
                'is_starter': True,
                'is_substitute': False,
                'is_pinch_hitter': False,
            })
            
            # Track position for substitution detection
            if primary_position:
                starters_by_position[primary_position] = {
                    'name': clean_name,
                    'batting_order': batting_order
                }
            
            batting_order += 1
            
        elif has_batting_stats:
            # This is a substitute who batted
            replaced_info = None
            if primary_position and primary_position in starters_by_position:
                replaced_info = starters_by_position[primary_position]
            
            analysis.update({
                'batting_order': replaced_info['batting_order'] if replaced_info else None,
                'is_starter': False,
                'is_substitute': True,
                'is_pinch_hitter': 'PH' in raw_name,
                'substitution_type': 'pinch_hitter' if 'PH' in raw_name else 'substitute',
                'replaced_player': replaced_info['name'] if replaced_info else None,
            })
            
        else:
            # Defensive substitute or player with no batting stats
            analysis.update({
                'batting_order': None,
                'is_starter': False,
                'is_substitute': True,
                'is_pinch_hitter': False,
                'substitution_type': 'defensive_substitute',
            })
        
        lineup_analysis[idx] = analysis
    
    return lineup_analysis

def extract_name_and_positions_fixed(raw_entry: str) -> Tuple[str, List[str]]:
    """
    FIXED: Extract clean player name and positions
    """
    
    # Remove decisions first
    cleaned = re.sub(r',\s*[WLSHB]+\s*\([^)]*\)', '', raw_entry).strip()
    
    # More comprehensive position extraction
    positions = []
    player_name = cleaned
    
    # Look for position codes at the end
    # Pattern matches: "3B", "C-1B", "LF-CF", "P", "PR", "PH", etc.
    position_match = re.search(r'\s+([A-Z0-9]{1,2}(?:-[A-Z0-9]{1,2})*)\s*

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

def process_single_game_with_corrected_appearances(game_url: str) -> Dict:
    """Process single game with corrected appearance parsing"""
    start_time = time.time()
    
    # Fetch page
    from utils.mlb_cached_fetcher import SafePageFetcher
    soup = SafePageFetcher.fetch_page(game_url)
    
    # Extract game metadata
    from parsing.game_metadata import extract_game_metadata
    game_metadata = extract_game_metadata(soup, game_url)
    game_id = game_metadata['game_id']
    
    # Corrected parsing
    official_batting, batting_appearances = parse_official_batting_with_corrected_appearances(soup)
    
    # Pitching (unchanged for now)
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

def debug_position_extraction():
    """Debug why positions are showing as Unknown"""
    
    print("🔍 DEBUGGING POSITION EXTRACTION")
    print("=" * 40)
    
    test_names = [
        "Steven Kwan LF",
        "José Ramírez 3B", 
        "Salvador Perez C-1B",
        "Dairon Blanco PR",
        "Gavin Williams P",
    ]
    
    for raw_name in test_names:
        clean_name, positions = extract_name_and_positions_fixed(raw_name)
        
        print(f"Raw: '{raw_name}'")
        print(f"  → Clean: '{clean_name}'")
        print(f"  → Positions: {positions}")
        
        # Debug the regex matching
        position_match = re.search(r'\s+([A-Z0-9]{1,2}(?:-[A-Z0-9]{1,2})*)\s*

def show_corrected_batting_order():
    """Show how the corrected batting order should look"""
    
    test_url = "https://www.baseball-reference.com/boxes/KCA/KCA202503290.shtml"
    result = process_single_game_with_corrected_appearances(test_url)
    
    print("📊 CORRECTED BATTING APPEARANCES")
    print("=" * 40)
    print(f"Game: {result['game_id']}")
    print(f"Batting accuracy: {result['batting_validation']['accuracy']:.1f}%")
    
    # Group by team
    away_apps = [app for app in result['batting_appearances'] if app['team'] == 'away']
    home_apps = [app for app in result['batting_appearances'] if app['team'] == 'home']
    
    print(f"\\nAWAY TEAM:")
    for app in away_apps:
        name = app['player_name']
        order = app['batting_order']
        positions = ', '.join(app['positions_played']) if app['positions_played'] else 'No position'
        pa = app['PA']
        
        if app['is_starter']:
            role = f"Starter #{order}"
        elif app['is_substitute']:
            sub_type = app.get('substitution_type', 'substitute')
            replaced = app.get('replaced_player', '')
            if replaced:
                role = f"{sub_type} (→ {replaced})"
            else:
                role = sub_type
        else:
            role = "Unknown"
        
        order_str = str(order) if order else '--'
        print(f"  {order_str:>2}: {name:18s} ({positions:12s}) {role:20s} PA={pa}")
    
    print(f"\\nHOME TEAM:")
    for app in home_apps:
        name = app['player_name']
        order = app['batting_order']
        positions = ', '.join(app['positions_played']) if app['positions_played'] else 'No position'
        pa = app['PA']
        
        if app['is_starter']:
            role = f"Starter #{order}"
        elif app['is_substitute']:
            sub_type = app.get('substitution_type', 'substitute')
            replaced = app.get('replaced_player', '')
            if replaced:
                role = f"{sub_type} (→ {replaced})"
            else:
                role = sub_type
        else:
            role = "Unknown"
        
        order_str = str(order) if order else '--'
        print(f"  {order_str:>2}: {name:18s} ({positions:12s}) {role:20s} PA={pa}")

if __name__ == "__main__":
    # First debug the position extraction
    debug_position_extraction()
    
    print("\\n" + "="*50)
    
    # Then show the corrected results
    show_corrected_batting_order()
, cleaned)
    
    if position_match:
        position_codes = position_match.group(1)
        player_name = cleaned[:position_match.start()].strip()
        
        # Split multiple positions and expand each
        for code in position_codes.split('-'):
            expanded = expand_position_code(code.strip())
            if expanded and expanded not in positions:
                positions.append(expanded)
    
    # Final name cleaning
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

def process_single_game_with_corrected_appearances(game_url: str) -> Dict:
    """Process single game with corrected appearance parsing"""
    start_time = time.time()
    
    # Fetch page
    from utils.mlb_cached_fetcher import SafePageFetcher
    soup = SafePageFetcher.fetch_page(game_url)
    
    # Extract game metadata
    from parsing.game_metadata import extract_game_metadata
    game_metadata = extract_game_metadata(soup, game_url)
    game_id = game_metadata['game_id']
    
    # Corrected parsing
    official_batting, batting_appearances = parse_official_batting_with_corrected_appearances(soup)
    
    # Pitching (unchanged for now)
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

def debug_position_extraction():
    """Debug why positions are showing as Unknown"""
    
    print("🔍 DEBUGGING POSITION EXTRACTION")
    print("=" * 40)
    
    test_names = [
        "Steven Kwan LF",
        "José Ramírez 3B", 
        "Salvador Perez C-1B",
        "Dairon Blanco PR",
        "Gavin Williams P",
    ]
    
    for raw_name in test_names:
        clean_name, positions = extract_name_and_positions_fixed(raw_name)
        
        print(f"Raw: '{raw_name}'")
        print(f"  → Clean: '{clean_name}'")
        print(f"  → Positions: {positions}")
        
        # Debug the regex matching
        position_match = re.search(r'\\s+([A-Z0-9]{1,2}(?:-[A-Z0-9]{1,2})*)\\s*$', raw_name)
        if position_match:
            print(f"  → Regex match: '{position_match.group(1)}'")
        else:
            print(f"  → No regex match!")
        print()

def show_corrected_batting_order():
    """Show how the corrected batting order should look"""
    
    test_url = "https://www.baseball-reference.com/boxes/KCA/KCA202503290.shtml"
    result = process_single_game_with_corrected_appearances(test_url)
    
    print("📊 CORRECTED BATTING APPEARANCES")
    print("=" * 40)
    print(f"Game: {result['game_id']}")
    print(f"Batting accuracy: {result['batting_validation']['accuracy']:.1f}%")
    
    # Group by team
    away_apps = [app for app in result['batting_appearances'] if app['team'] == 'away']
    home_apps = [app for app in result['batting_appearances'] if app['team'] == 'home']
    
    print(f"\\nAWAY TEAM:")
    for app in away_apps:
        name = app['player_name']
        order = app['batting_order']
        positions = ', '.join(app['positions_played']) if app['positions_played'] else 'No position'
        pa = app['PA']
        
        if app['is_starter']:
            role = f"Starter #{order}"
        elif app['is_substitute']:
            sub_type = app.get('substitution_type', 'substitute')
            replaced = app.get('replaced_player', '')
            if replaced:
                role = f"{sub_type} (→ {replaced})"
            else:
                role = sub_type
        else:
            role = "Unknown"
        
        order_str = str(order) if order else '--'
        print(f"  {order_str:>2}: {name:18s} ({positions:12s}) {role:20s} PA={pa}")
    
    print(f"\\nHOME TEAM:")
    for app in home_apps:
        name = app['player_name']
        order = app['batting_order']
        positions = ', '.join(app['positions_played']) if app['positions_played'] else 'No position'
        pa = app['PA']
        
        if app['is_starter']:
            role = f"Starter #{order}"
        elif app['is_substitute']:
            sub_type = app.get('substitution_type', 'substitute')
            replaced = app.get('replaced_player', '')
            if replaced:
                role = f"{sub_type} (→ {replaced})"
            else:
                role = sub_type
        else:
            role = "Unknown"
        
        order_str = str(order) if order else '--'
        print(f"  {order_str:>2}: {name:18s} ({positions:12s}) {role:20s} PA={pa}")

if __name__ == "__main__":
    # First debug the position extraction
    debug_position_extraction()
    
    print("\\n" + "="*50)
    
    # Then show the corrected results
    show_corrected_batting_order()
, raw_name)
        if position_match:
            print(f"  → Regex match: '{position_match.group(1)}'")
        else:
            print(f"  → No regex match!")
        print()

def show_corrected_batting_order():
    """Show how the corrected batting order should look"""
    
    test_url = "https://www.baseball-reference.com/boxes/KCA/KCA202503290.shtml"
    result = process_single_game_with_corrected_appearances(test_url)
    
    print("📊 CORRECTED BATTING APPEARANCES")
    print("=" * 40)
    print(f"Game: {result['game_id']}")
    print(f"Batting accuracy: {result['batting_validation']['accuracy']:.1f}%")
    
    # Group by team
    away_apps = [app for app in result['batting_appearances'] if app['team'] == 'away']
    home_apps = [app for app in result['batting_appearances'] if app['team'] == 'home']
    
    print(f"\\nAWAY TEAM:")
    for app in away_apps:
        name = app['player_name']
        order = app['batting_order']
        positions = ', '.join(app['positions_played']) if app['positions_played'] else 'No position'
        pa = app['PA']
        
        if app['is_starter']:
            role = f"Starter #{order}"
        elif app['is_substitute']:
            sub_type = app.get('substitution_type', 'substitute')
            replaced = app.get('replaced_player', '')
            if replaced:
                role = f"{sub_type} (→ {replaced})"
            else:
                role = sub_type
        else:
            role = "Unknown"
        
        order_str = str(order) if order else '--'
        print(f"  {order_str:>2}: {name:18s} ({positions:12s}) {role:20s} PA={pa}")
    
    print(f"\\nHOME TEAM:")
    for app in home_apps:
        name = app['player_name']
        order = app['batting_order']
        positions = ', '.join(app['positions_played']) if app['positions_played'] else 'No position'
        pa = app['PA']
        
        if app['is_starter']:
            role = f"Starter #{order}"
        elif app['is_substitute']:
            sub_type = app.get('substitution_type', 'substitute')
            replaced = app.get('replaced_player', '')
            if replaced:
                role = f"{sub_type} (→ {replaced})"
            else:
                role = sub_type
        else:
            role = "Unknown"
        
        order_str = str(order) if order else '--'
        print(f"  {order_str:>2}: {name:18s} ({positions:12s}) {role:20s} PA={pa}")

if __name__ == "__main__":
    # First debug the position extraction
    debug_position_extraction()
    
    print("\\n" + "="*50)
    
    # Then show the corrected results
    show_corrected_batting_order()
, cleaned)
    
    if position_match:
        position_codes = position_match.group(1)
        player_name = cleaned[:position_match.start()].strip()
        
        # Split multiple positions and expand each
        for code in position_codes.split('-'):
            expanded = expand_position_code(code.strip())
            if expanded and expanded not in positions:
                positions.append(expanded)
    
    # Final name cleaning
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

def process_single_game_with_corrected_appearances(game_url: str) -> Dict:
    """Process single game with corrected appearance parsing"""
    start_time = time.time()
    
    # Fetch page
    from utils.mlb_cached_fetcher import SafePageFetcher
    soup = SafePageFetcher.fetch_page(game_url)
    
    # Extract game metadata
    from parsing.game_metadata import extract_game_metadata
    game_metadata = extract_game_metadata(soup, game_url)
    game_id = game_metadata['game_id']
    
    # Corrected parsing
    official_batting, batting_appearances = parse_official_batting_with_corrected_appearances(soup)
    
    # Pitching (unchanged for now)
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

def debug_position_extraction():
    """Debug why positions are showing as Unknown"""
    
    print("🔍 DEBUGGING POSITION EXTRACTION")
    print("=" * 40)
    
    test_names = [
        "Steven Kwan LF",
        "José Ramírez 3B", 
        "Salvador Perez C-1B",
        "Dairon Blanco PR",
        "Gavin Williams P",
    ]
    
    for raw_name in test_names:
        clean_name, positions = extract_name_and_positions_fixed(raw_name)
        
        print(f"Raw: '{raw_name}'")
        print(f"  → Clean: '{clean_name}'")
        print(f"  → Positions: {positions}")
        
        # Debug the regex matching
        position_match = re.search(r'\\s+([A-Z0-9]{1,2}(?:-[A-Z0-9]{1,2})*)\\s*$', raw_name)
        if position_match:
            print(f"  → Regex match: '{position_match.group(1)}'")
        else:
            print(f"  → No regex match!")
        print()

def show_corrected_batting_order():
    """Show how the corrected batting order should look"""
    
    test_url = "https://www.baseball-reference.com/boxes/KCA/KCA202503290.shtml"
    result = process_single_game_with_corrected_appearances(test_url)
    
    print("📊 CORRECTED BATTING APPEARANCES")
    print("=" * 40)
    print(f"Game: {result['game_id']}")
    print(f"Batting accuracy: {result['batting_validation']['accuracy']:.1f}%")
    
    # Group by team
    away_apps = [app for app in result['batting_appearances'] if app['team'] == 'away']
    home_apps = [app for app in result['batting_appearances'] if app['team'] == 'home']
    
    print(f"\\nAWAY TEAM:")
    for app in away_apps:
        name = app['player_name']
        order = app['batting_order']
        positions = ', '.join(app['positions_played']) if app['positions_played'] else 'No position'
        pa = app['PA']
        
        if app['is_starter']:
            role = f"Starter #{order}"
        elif app['is_substitute']:
            sub_type = app.get('substitution_type', 'substitute')
            replaced = app.get('replaced_player', '')
            if replaced:
                role = f"{sub_type} (→ {replaced})"
            else:
                role = sub_type
        else:
            role = "Unknown"
        
        order_str = str(order) if order else '--'
        print(f"  {order_str:>2}: {name:18s} ({positions:12s}) {role:20s} PA={pa}")
    
    print(f"\\nHOME TEAM:")
    for app in home_apps:
        name = app['player_name']
        order = app['batting_order']
        positions = ', '.join(app['positions_played']) if app['positions_played'] else 'No position'
        pa = app['PA']
        
        if app['is_starter']:
            role = f"Starter #{order}"
        elif app['is_substitute']:
            sub_type = app.get('substitution_type', 'substitute')
            replaced = app.get('replaced_player', '')
            if replaced:
                role = f"{sub_type} (→ {replaced})"
            else:
                role = sub_type
        else:
            role = "Unknown"
        
        order_str = str(order) if order else '--'
        print(f"  {order_str:>2}: {name:18s} ({positions:12s}) {role:20s} PA={pa}")

if __name__ == "__main__":
    # First debug the position extraction
    debug_position_extraction()
    
    print("\\n" + "="*50)
    
    # Then show the corrected results
    show_corrected_batting_order()
, raw_name)) or 
            csk.startswith('10') or
            'Pitcher' in positions
        )
        
        is_pinch_runner = 'PR' in raw_name or 'Pinch Runner' in positions
        has_batting_stats = pa > 0 or ab > 0
        
        analysis = {
            'raw_name': raw_name,
            'clean_name': clean_name,
            'positions': positions,
            'primary_position': primary_position,
            'pa': pa,
            'ab': ab,
            'csk': csk,
            'is_pitcher': is_pitcher,
        }
        
        if is_pitcher:
            analysis.update({
                'batting_order': None,
                'is_starter': False,
                'is_substitute': False,
            })
            
        elif is_pinch_runner:
            analysis.update({
                'batting_order': None,
                'is_starter': False,
                'is_substitute': True,
                'substitution_type': 'pinch_runner',
                'is_pinch_hitter': False,
            })
            
        elif batting_order <= 9 and has_batting_stats:
            # This is a starter
            analysis.update({
                'batting_order': batting_order,
                'is_starter': True,
                'is_substitute': False,
                'is_pinch_hitter': False,
            })
            
            # Track position for substitution detection
            if primary_position:
                starters_by_position[primary_position] = {
                    'name': clean_name,
                    'batting_order': batting_order
                }
            
            batting_order += 1
            
        elif has_batting_stats:
            # This is a substitute who batted
            replaced_info = None
            if primary_position and primary_position in starters_by_position:
                replaced_info = starters_by_position[primary_position]
            
            analysis.update({
                'batting_order': replaced_info['batting_order'] if replaced_info else None,
                'is_starter': False,
                'is_substitute': True,
                'is_pinch_hitter': 'PH' in raw_name,
                'substitution_type': 'pinch_hitter' if 'PH' in raw_name else 'substitute',
                'replaced_player': replaced_info['name'] if replaced_info else None,
            })
            
        else:
            # Defensive substitute or player with no batting stats
            analysis.update({
                'batting_order': None,
                'is_starter': False,
                'is_substitute': True,
                'is_pinch_hitter': False,
                'substitution_type': 'defensive_substitute',
            })
        
        lineup_analysis[idx] = analysis
    
    return lineup_analysis

def extract_name_and_positions_fixed(raw_entry: str) -> Tuple[str, List[str]]:
    """
    FIXED: Extract clean player name and positions
    """
    
    # Remove decisions first
    cleaned = re.sub(r',\s*[WLSHB]+\s*\([^)]*\)', '', raw_entry).strip()
    
    # More comprehensive position extraction
    positions = []
    player_name = cleaned
    
    # Look for position codes at the end
    # Pattern matches: "3B", "C-1B", "LF-CF", "P", "PR", "PH", etc.
    position_match = re.search(r'\s+([A-Z0-9]{1,2}(?:-[A-Z0-9]{1,2})*)\s*

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

def process_single_game_with_corrected_appearances(game_url: str) -> Dict:
    """Process single game with corrected appearance parsing"""
    start_time = time.time()
    
    # Fetch page
    from utils.mlb_cached_fetcher import SafePageFetcher
    soup = SafePageFetcher.fetch_page(game_url)
    
    # Extract game metadata
    from parsing.game_metadata import extract_game_metadata
    game_metadata = extract_game_metadata(soup, game_url)
    game_id = game_metadata['game_id']
    
    # Corrected parsing
    official_batting, batting_appearances = parse_official_batting_with_corrected_appearances(soup)
    
    # Pitching (unchanged for now)
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

def debug_position_extraction():
    """Debug why positions are showing as Unknown"""
    
    print("🔍 DEBUGGING POSITION EXTRACTION")
    print("=" * 40)
    
    test_names = [
        "Steven Kwan LF",
        "José Ramírez 3B", 
        "Salvador Perez C-1B",
        "Dairon Blanco PR",
        "Gavin Williams P",
    ]
    
    for raw_name in test_names:
        clean_name, positions = extract_name_and_positions_fixed(raw_name)
        
        print(f"Raw: '{raw_name}'")
        print(f"  → Clean: '{clean_name}'")
        print(f"  → Positions: {positions}")
        
        # Debug the regex matching
        position_match = re.search(r'\s+([A-Z0-9]{1,2}(?:-[A-Z0-9]{1,2})*)\s*

def show_corrected_batting_order():
    """Show how the corrected batting order should look"""
    
    test_url = "https://www.baseball-reference.com/boxes/KCA/KCA202503290.shtml"
    result = process_single_game_with_corrected_appearances(test_url)
    
    print("📊 CORRECTED BATTING APPEARANCES")
    print("=" * 40)
    print(f"Game: {result['game_id']}")
    print(f"Batting accuracy: {result['batting_validation']['accuracy']:.1f}%")
    
    # Group by team
    away_apps = [app for app in result['batting_appearances'] if app['team'] == 'away']
    home_apps = [app for app in result['batting_appearances'] if app['team'] == 'home']
    
    print(f"\\nAWAY TEAM:")
    for app in away_apps:
        name = app['player_name']
        order = app['batting_order']
        positions = ', '.join(app['positions_played']) if app['positions_played'] else 'No position'
        pa = app['PA']
        
        if app['is_starter']:
            role = f"Starter #{order}"
        elif app['is_substitute']:
            sub_type = app.get('substitution_type', 'substitute')
            replaced = app.get('replaced_player', '')
            if replaced:
                role = f"{sub_type} (→ {replaced})"
            else:
                role = sub_type
        else:
            role = "Unknown"
        
        order_str = str(order) if order else '--'
        print(f"  {order_str:>2}: {name:18s} ({positions:12s}) {role:20s} PA={pa}")
    
    print(f"\\nHOME TEAM:")
    for app in home_apps:
        name = app['player_name']
        order = app['batting_order']
        positions = ', '.join(app['positions_played']) if app['positions_played'] else 'No position'
        pa = app['PA']
        
        if app['is_starter']:
            role = f"Starter #{order}"
        elif app['is_substitute']:
            sub_type = app.get('substitution_type', 'substitute')
            replaced = app.get('replaced_player', '')
            if replaced:
                role = f"{sub_type} (→ {replaced})"
            else:
                role = sub_type
        else:
            role = "Unknown"
        
        order_str = str(order) if order else '--'
        print(f"  {order_str:>2}: {name:18s} ({positions:12s}) {role:20s} PA={pa}")

if __name__ == "__main__":
    # First debug the position extraction
    debug_position_extraction()
    
    print("\\n" + "="*50)
    
    # Then show the corrected results
    show_corrected_batting_order()
, cleaned)
    
    if position_match:
        position_codes = position_match.group(1)
        player_name = cleaned[:position_match.start()].strip()
        
        # Split multiple positions and expand each
        for code in position_codes.split('-'):
            expanded = expand_position_code(code.strip())
            if expanded and expanded not in positions:
                positions.append(expanded)
    
    # Final name cleaning
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

def process_single_game_with_corrected_appearances(game_url: str) -> Dict:
    """Process single game with corrected appearance parsing"""
    start_time = time.time()
    
    # Fetch page
    from utils.mlb_cached_fetcher import SafePageFetcher
    soup = SafePageFetcher.fetch_page(game_url)
    
    # Extract game metadata
    from parsing.game_metadata import extract_game_metadata
    game_metadata = extract_game_metadata(soup, game_url)
    game_id = game_metadata['game_id']
    
    # Corrected parsing
    official_batting, batting_appearances = parse_official_batting_with_corrected_appearances(soup)
    
    # Pitching (unchanged for now)
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

def debug_position_extraction():
    """Debug why positions are showing as Unknown"""
    
    print("🔍 DEBUGGING POSITION EXTRACTION")
    print("=" * 40)
    
    test_names = [
        "Steven Kwan LF",
        "José Ramírez 3B", 
        "Salvador Perez C-1B",
        "Dairon Blanco PR",
        "Gavin Williams P",
    ]
    
    for raw_name in test_names:
        clean_name, positions = extract_name_and_positions_fixed(raw_name)
        
        print(f"Raw: '{raw_name}'")
        print(f"  → Clean: '{clean_name}'")
        print(f"  → Positions: {positions}")
        
        # Debug the regex matching
        position_match = re.search(r'\\s+([A-Z0-9]{1,2}(?:-[A-Z0-9]{1,2})*)\\s*$', raw_name)
        if position_match:
            print(f"  → Regex match: '{position_match.group(1)}'")
        else:
            print(f"  → No regex match!")
        print()

def show_corrected_batting_order():
    """Show how the corrected batting order should look"""
    
    test_url = "https://www.baseball-reference.com/boxes/KCA/KCA202503290.shtml"
    result = process_single_game_with_corrected_appearances(test_url)
    
    print("📊 CORRECTED BATTING APPEARANCES")
    print("=" * 40)
    print(f"Game: {result['game_id']}")
    print(f"Batting accuracy: {result['batting_validation']['accuracy']:.1f}%")
    
    # Group by team
    away_apps = [app for app in result['batting_appearances'] if app['team'] == 'away']
    home_apps = [app for app in result['batting_appearances'] if app['team'] == 'home']
    
    print(f"\\nAWAY TEAM:")
    for app in away_apps:
        name = app['player_name']
        order = app['batting_order']
        positions = ', '.join(app['positions_played']) if app['positions_played'] else 'No position'
        pa = app['PA']
        
        if app['is_starter']:
            role = f"Starter #{order}"
        elif app['is_substitute']:
            sub_type = app.get('substitution_type', 'substitute')
            replaced = app.get('replaced_player', '')
            if replaced:
                role = f"{sub_type} (→ {replaced})"
            else:
                role = sub_type
        else:
            role = "Unknown"
        
        order_str = str(order) if order else '--'
        print(f"  {order_str:>2}: {name:18s} ({positions:12s}) {role:20s} PA={pa}")
    
    print(f"\\nHOME TEAM:")
    for app in home_apps:
        name = app['player_name']
        order = app['batting_order']
        positions = ', '.join(app['positions_played']) if app['positions_played'] else 'No position'
        pa = app['PA']
        
        if app['is_starter']:
            role = f"Starter #{order}"
        elif app['is_substitute']:
            sub_type = app.get('substitution_type', 'substitute')
            replaced = app.get('replaced_player', '')
            if replaced:
                role = f"{sub_type} (→ {replaced})"
            else:
                role = sub_type
        else:
            role = "Unknown"
        
        order_str = str(order) if order else '--'
        print(f"  {order_str:>2}: {name:18s} ({positions:12s}) {role:20s} PA={pa}")

if __name__ == "__main__":
    # First debug the position extraction
    debug_position_extraction()
    
    print("\\n" + "="*50)
    
    # Then show the corrected results
    show_corrected_batting_order()
, raw_name)
        if position_match:
            print(f"  → Regex match: '{position_match.group(1)}'")
        else:
            print(f"  → No regex match!")
        print()

def show_corrected_batting_order():
    """Show how the corrected batting order should look"""
    
    test_url = "https://www.baseball-reference.com/boxes/KCA/KCA202503290.shtml"
    result = process_single_game_with_corrected_appearances(test_url)
    
    print("📊 CORRECTED BATTING APPEARANCES")
    print("=" * 40)
    print(f"Game: {result['game_id']}")
    print(f"Batting accuracy: {result['batting_validation']['accuracy']:.1f}%")
    
    # Group by team
    away_apps = [app for app in result['batting_appearances'] if app['team'] == 'away']
    home_apps = [app for app in result['batting_appearances'] if app['team'] == 'home']
    
    print(f"\\nAWAY TEAM:")
    for app in away_apps:
        name = app['player_name']
        order = app['batting_order']
        positions = ', '.join(app['positions_played']) if app['positions_played'] else 'No position'
        pa = app['PA']
        
        if app['is_starter']:
            role = f"Starter #{order}"
        elif app['is_substitute']:
            sub_type = app.get('substitution_type', 'substitute')
            replaced = app.get('replaced_player', '')
            if replaced:
                role = f"{sub_type} (→ {replaced})"
            else:
                role = sub_type
        else:
            role = "Unknown"
        
        order_str = str(order) if order else '--'
        print(f"  {order_str:>2}: {name:18s} ({positions:12s}) {role:20s} PA={pa}")
    
    print(f"\\nHOME TEAM:")
    for app in home_apps:
        name = app['player_name']
        order = app['batting_order']
        positions = ', '.join(app['positions_played']) if app['positions_played'] else 'No position'
        pa = app['PA']
        
        if app['is_starter']:
            role = f"Starter #{order}"
        elif app['is_substitute']:
            sub_type = app.get('substitution_type', 'substitute')
            replaced = app.get('replaced_player', '')
            if replaced:
                role = f"{sub_type} (→ {replaced})"
            else:
                role = sub_type
        else:
            role = "Unknown"
        
        order_str = str(order) if order else '--'
        print(f"  {order_str:>2}: {name:18s} ({positions:12s}) {role:20s} PA={pa}")

if __name__ == "__main__":
    # First debug the position extraction
    debug_position_extraction()
    
    print("\\n" + "="*50)
    
    # Then show the corrected results
    show_corrected_batting_order()
, cleaned)
    
    if position_match:
        position_codes = position_match.group(1)
        player_name = cleaned[:position_match.start()].strip()
        
        # Split multiple positions and expand each
        for code in position_codes.split('-'):
            expanded = expand_position_code(code.strip())
            if expanded and expanded not in positions:
                positions.append(expanded)
    
    # Final name cleaning
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

def process_single_game_with_corrected_appearances(game_url: str) -> Dict:
    """Process single game with corrected appearance parsing"""
    start_time = time.time()
    
    # Fetch page
    from utils.mlb_cached_fetcher import SafePageFetcher
    soup = SafePageFetcher.fetch_page(game_url)
    
    # Extract game metadata
    from parsing.game_metadata import extract_game_metadata
    game_metadata = extract_game_metadata(soup, game_url)
    game_id = game_metadata['game_id']
    
    # Corrected parsing
    official_batting, batting_appearances = parse_official_batting_with_corrected_appearances(soup)
    
    # Pitching (unchanged for now)
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

def debug_position_extraction():
    """Debug why positions are showing as Unknown"""
    
    print("🔍 DEBUGGING POSITION EXTRACTION")
    print("=" * 40)
    
    test_names = [
        "Steven Kwan LF",
        "José Ramírez 3B", 
        "Salvador Perez C-1B",
        "Dairon Blanco PR",
        "Gavin Williams P",
    ]
    
    for raw_name in test_names:
        clean_name, positions = extract_name_and_positions_fixed(raw_name)
        
        print(f"Raw: '{raw_name}'")
        print(f"  → Clean: '{clean_name}'")
        print(f"  → Positions: {positions}")
        
        # Debug the regex matching
        position_match = re.search(r'\\s+([A-Z0-9]{1,2}(?:-[A-Z0-9]{1,2})*)\\s*$', raw_name)
        if position_match:
            print(f"  → Regex match: '{position_match.group(1)}'")
        else:
            print(f"  → No regex match!")
        print()

def show_corrected_batting_order():
    """Show how the corrected batting order should look"""
    
    test_url = "https://www.baseball-reference.com/boxes/KCA/KCA202503290.shtml"
    result = process_single_game_with_corrected_appearances(test_url)
    
    print("📊 CORRECTED BATTING APPEARANCES")
    print("=" * 40)
    print(f"Game: {result['game_id']}")
    print(f"Batting accuracy: {result['batting_validation']['accuracy']:.1f}%")
    
    # Group by team
    away_apps = [app for app in result['batting_appearances'] if app['team'] == 'away']
    home_apps = [app for app in result['batting_appearances'] if app['team'] == 'home']
    
    print(f"\\nAWAY TEAM:")
    for app in away_apps:
        name = app['player_name']
        order = app['batting_order']
        positions = ', '.join(app['positions_played']) if app['positions_played'] else 'No position'
        pa = app['PA']
        
        if app['is_starter']:
            role = f"Starter #{order}"
        elif app['is_substitute']:
            sub_type = app.get('substitution_type', 'substitute')
            replaced = app.get('replaced_player', '')
            if replaced:
                role = f"{sub_type} (→ {replaced})"
            else:
                role = sub_type
        else:
            role = "Unknown"
        
        order_str = str(order) if order else '--'
        print(f"  {order_str:>2}: {name:18s} ({positions:12s}) {role:20s} PA={pa}")
    
    print(f"\\nHOME TEAM:")
    for app in home_apps:
        name = app['player_name']
        order = app['batting_order']
        positions = ', '.join(app['positions_played']) if app['positions_played'] else 'No position'
        pa = app['PA']
        
        if app['is_starter']:
            role = f"Starter #{order}"
        elif app['is_substitute']:
            sub_type = app.get('substitution_type', 'substitute')
            replaced = app.get('replaced_player', '')
            if replaced:
                role = f"{sub_type} (→ {replaced})"
            else:
                role = sub_type
        else:
            role = "Unknown"
        
        order_str = str(order) if order else '--'
        print(f"  {order_str:>2}: {name:18s} ({positions:12s}) {role:20s} PA={pa}")

if __name__ == "__main__":
    # First debug the position extraction
    debug_position_extraction()
    
    print("\\n" + "="*50)
    
    # Then show the corrected results
    show_corrected_batting_order()
