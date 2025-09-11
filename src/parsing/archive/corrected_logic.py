"""
Corrected Appearance Parser with Substitution Logic
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import re
import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO
from typing import Tuple, List, Dict, Optional

from parsing.name_utils import normalize_name
from parsing.game_utils import safe_int, extract_from_details

def extract_name_and_positions(raw_entry: str) -> Tuple[str, List[str]]:
    """Extract clean player name and positions"""
    cleaned = re.sub(r',\s*[WLSHB]+\s*\([^)]*\)', '', raw_entry).strip()
    
    positions = []
    player_name = cleaned
    
    position_match = re.search(r'\s+([A-Z0-9]{1,2}(?:-[A-Z0-9]{1,2})*)\s*$', cleaned)
    
    if position_match:
        position_codes = position_match.group(1)
        player_name = cleaned[:position_match.start()].strip()
        
        for code in position_codes.split('-'):
            expanded = expand_position_code(code.strip())
            if expanded and expanded not in positions:
                positions.append(expanded)
    
    clean_name = normalize_name(player_name)
    return clean_name, positions

def expand_position_code(code: str) -> Optional[str]:
    position_map = {
        'P': 'Pitcher', 'C': 'Catcher', '1B': 'First Base', '2B': 'Second Base',
        '3B': 'Third Base', 'SS': 'Shortstop', 'LF': 'Left Field', 'CF': 'Center Field',
        'RF': 'Right Field', 'DH': 'Designated Hitter', 'PH': 'Pinch Hitter', 'PR': 'Pinch Runner',
    }
    return position_map.get(code.upper())

def parse_team_with_substitutions(df: pd.DataFrame, team: str) -> List[Dict]:
    """Parse team batting with proper substitution logic"""
    
    appearances = []
    batting_order = 1
    starters_by_position = {}
    
    for idx, row in df.iterrows():
        raw_name = str(row['Batting'])
        pa = safe_int(row.get('PA', 0))
        ab = safe_int(row.get('AB', 0))
        
        clean_name, positions = extract_name_and_positions(raw_name)
        primary_position = positions[0] if positions else None
        
        # Skip pitchers entirely
        is_pitcher = 'Pitcher' in positions or bool(re.search(r'\s+P\s*$', raw_name))
        if is_pitcher:
            continue
        
        # Handle pinch runners (no batting order)
        is_pinch_runner = 'Pinch Runner' in positions
        if is_pinch_runner:
            appearances.append({
                'player_name': clean_name,
                'team': team,
                'batting_order': None,
                'positions_played': positions,
                'is_starter': False,
                'is_substitute': True,
                'substitution_type': 'pinch_runner',
                'PA': pa, 'AB': ab
            })
            continue
        
        # Check if this player replaces someone at the same position
        replaced_info = None
        if primary_position and primary_position in starters_by_position:
            replaced_info = starters_by_position[primary_position]
        
        if replaced_info and pa > 0:
            # This is a positional substitute with batting stats
            appearances.append({
                'player_name': clean_name,
                'team': team,
                'batting_order': replaced_info['batting_order'],
                'positions_played': positions,
                'is_starter': False,
                'is_substitute': True,
                'substitution_type': 'positional_substitute',
                'replaced_player': replaced_info['name'],
                'PA': pa, 'AB': ab
            })
        elif batting_order <= 9 and pa > 0:
            # This is a starter
            appearances.append({
                'player_name': clean_name,
                'team': team,
                'batting_order': batting_order,
                'positions_played': positions,
                'is_starter': True,
                'is_substitute': False,
                'PA': pa, 'AB': ab
            })
            
            # Track this position
            if primary_position:
                starters_by_position[primary_position] = {
                    'name': clean_name,
                    'batting_order': batting_order
                }
            
            batting_order += 1
        elif pa > 0:
            # Batter beyond position 9 - likely a late substitute
            appearances.append({
                'player_name': clean_name,
                'team': team,
                'batting_order': None,  # Will need game context to determine
                'positions_played': positions,
                'is_starter': False,
                'is_substitute': True,
                'substitution_type': 'late_substitute',
                'PA': pa, 'AB': ab
            })
        else:
            # Defensive substitute with no batting stats
            appearances.append({
                'player_name': clean_name,
                'team': team,
                'batting_order': None,
                'positions_played': positions,
                'is_starter': False,
                'is_substitute': True,
                'substitution_type': 'defensive_substitute',
                'PA': pa, 'AB': ab
            })
    
    return appearances

def test_corrected_logic():
    """Test the corrected substitution logic"""
    test_url = "https://www.baseball-reference.com/boxes/BAL/BAL202509050.shtml"
    
    from utils.mlb_cached_fetcher import SafePageFetcher
    soup = SafePageFetcher.fetch_page(test_url)
    
    batting_tables = soup.find_all('table', {'id': lambda x: x and x.endswith('batting')})
    
    for table_idx, table in enumerate(batting_tables):
        team_name = 'away' if table_idx == 0 else 'home'
        print(f"\n{team_name.upper()} TEAM:")
        
        df = pd.read_html(StringIO(str(table)))[0]
        df = df[df['Batting'].notna()]
        df = df[~df['Batting'].str.contains("Team Totals", na=False)]
        
        appearances = parse_team_with_substitutions(df, team_name)
        
        for app in appearances:
            name = app['player_name']
            order = app['batting_order']
            positions = ', '.join(app['positions_played']) if app['positions_played'] else 'Unknown'
            pa = app['PA']
            
            if app['is_starter']:
                role = f"Starter #{order}"
            elif app['is_substitute']:
                sub_type = app.get('substitution_type', 'substitute')
                replaced = app.get('replaced_player', '')
                if replaced:
                    role = f"{sub_type} (→{replaced}, order {order})"
                else:
                    role = sub_type
            else:
                role = "Other"
            
            order_str = str(order) if order else '--'
            print(f"  {order_str:>2}: {name:18s} ({positions:15s}) {role:30s} PA={pa}")

if __name__ == "__main__":
    test_corrected_logic()