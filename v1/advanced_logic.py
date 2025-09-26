"""
Advanced Substitution Logic - Batting Order Inheritance
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

def parse_batting_order_inheritance(df: pd.DataFrame, team: str) -> List[Dict]:
    """
    Smart batting order assignment using inheritance logic
    
    Key insight: Pinch hitters inherit the batting order of whoever they replace,
    regardless of position changes after the substitution.
    """
    
    appearances = []
    
    # Step 1: Identify the starting 9 (first 9 players with PA > 0, excluding pitchers)
    starters = []
    batting_order = 1
    
    for idx, row in df.iterrows():
        raw_name = str(row['Batting'])
        pa = safe_int(row.get('PA', 0))
        clean_name, positions = extract_name_and_positions(raw_name)
        
        # Skip pitchers
        is_pitcher = 'Pitcher' in positions or bool(re.search(r'\s+P\s*$', raw_name))
        if is_pitcher:
            continue
            
        # Skip pinch runners (they don't bat)
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
                'PA': pa
            })
            continue
        
        if len(starters) < 9 and pa > 0:
            # This is a starter
            starter_info = {
                'player_name': clean_name,
                'team': team,
                'batting_order': batting_order,
                'positions_played': positions,
                'is_starter': True,
                'is_substitute': False,
                'PA': pa,
                'df_index': idx
            }
            starters.append(starter_info)
            appearances.append(starter_info)
            batting_order += 1
            
        elif pa > 0:
            # This player has batting stats but comes after the starting 9
            # They must have replaced someone in the batting order
            
            # Look for pinch hitter designation
            is_pinch_hitter = 'Pinch Hitter' in positions
            
            if is_pinch_hitter:
                # Find which batting order spot this PH took over
                # Strategy: Look at PA distribution and guess based on game flow
                inherited_order = guess_inherited_batting_order(clean_name, pa, starters)
                
                appearances.append({
                    'player_name': clean_name,
                    'team': team,
                    'batting_order': inherited_order,
                    'positions_played': positions,
                    'is_starter': False,
                    'is_substitute': True,
                    'substitution_type': 'pinch_hitter',
                    'PA': pa
                })
            else:
                # Regular substitute - try to figure out who they replaced
                appearances.append({
                    'player_name': clean_name,
                    'team': team,
                    'batting_order': None,  # Would need play-by-play to determine
                    'positions_played': positions,
                    'is_starter': False,
                    'is_substitute': True,
                    'substitution_type': 'substitute',
                    'PA': pa
                })
        else:
            # No batting stats - defensive substitute
            appearances.append({
                'player_name': clean_name,
                'team': team,
                'batting_order': None,
                'positions_played': positions,
                'is_starter': False,
                'is_substitute': True,
                'substitution_type': 'defensive_substitute',
                'PA': pa
            })
    
    return appearances

def guess_inherited_batting_order(player_name: str, pa: int, starters: List[Dict]) -> Optional[int]:
    """
    Guess which batting order a substitute inherited based on PA counts
    
    This is imperfect without play-by-play data, but better than position matching
    """
    
    # Look for starters with unusually low PA counts (they were likely replaced)
    avg_pa = sum(s['PA'] for s in starters) / len(starters) if starters else 0
    
    candidates = []
    for starter in starters:
        if starter['PA'] < avg_pa * 0.7:  # Much lower than average
            candidates.append(starter['batting_order'])
    
    # Return the first candidate, or None if unclear
    return candidates[0] if candidates else None

def test_advanced_logic():
    """Test with the problematic Baltimore game"""
    test_url = "https://www.baseball-reference.com/boxes/BAL/BAL202509050.shtml"
    
    from utils.mlb_cached_fetcher import SafePageFetcher
    soup = SafePageFetcher.fetch_page(test_url)
    
    batting_tables = soup.find_all('table', {'id': lambda x: x and x.endswith('batting')})
    
    for table_idx, table in enumerate(batting_tables):
        team_name = 'away' if table_idx == 0 else 'home'
        print(f"\n{team_name.upper()} TEAM - ADVANCED LOGIC:")
        
        df = pd.read_html(StringIO(str(table)))[0]
        df = df[df['Batting'].notna()]
        df = df[~df['Batting'].str.contains("Team Totals", na=False)]
        
        appearances = parse_batting_order_inheritance(df, team_name)
        
        # Sort by batting order for display
        starters = [app for app in appearances if app['is_starter']]
        substitutes = [app for app in appearances if app['is_substitute']]
        
        print("STARTERS:")
        for app in sorted(starters, key=lambda x: x['batting_order']):
            name = app['player_name']
            order = app['batting_order']
            positions = ', '.join(app['positions_played'])
            pa = app['PA']
            print(f"  #{order}: {name:18s} ({positions:15s}) PA={pa}")
        
        print("\nSUBSTITUTES:")
        for app in substitutes:
            name = app['player_name']
            order = app['batting_order']
            positions = ', '.join(app['positions_played'])
            pa = app['PA']
            sub_type = app.get('substitution_type', 'sub')
            
            order_str = f"#{order}" if order else "No order"
            print(f"  {order_str:>8}: {name:18s} ({positions:15s}) {sub_type:15s} PA={pa}")

if __name__ == "__main__":
    test_advanced_logic()