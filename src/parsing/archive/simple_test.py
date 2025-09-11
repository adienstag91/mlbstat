"""
Simple Working Appearance Parser
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
    """Expand position codes"""
    position_map = {
        'P': 'Pitcher', 'C': 'Catcher', '1B': 'First Base', '2B': 'Second Base',
        '3B': 'Third Base', 'SS': 'Shortstop', 'LF': 'Left Field', 'CF': 'Center Field',
        'RF': 'Right Field', 'DH': 'Designated Hitter', 'PH': 'Pinch Hitter', 'PR': 'Pinch Runner',
    }
    return position_map.get(code.upper())

def test_simple():
    """Simple test"""
    test_url = "https://www.baseball-reference.com/boxes/KCA/KCA202503290.shtml"
    
    from utils.mlb_cached_fetcher import SafePageFetcher
    soup = SafePageFetcher.fetch_page(test_url)
    
    batting_tables = soup.find_all('table', {'id': lambda x: x and x.endswith('batting')})
    
    for table_idx, table in enumerate(batting_tables):
        team = 'AWAY' if table_idx == 0 else 'HOME'
        print(f"\n{team} TEAM:")
        
        df = pd.read_html(StringIO(str(table)))[0]
        df = df[df['Batting'].notna()]
        df = df[~df['Batting'].str.contains("Team Totals", na=False)]
        
        batting_order = 1
        for idx, row in df.iterrows():
            raw_name = str(row['Batting'])
            pa = safe_int(row.get('PA', 0))
            
            clean_name, positions = extract_name_and_positions(raw_name)
            pos_str = ', '.join(positions) if positions else 'Unknown'
            
            # Simple pitcher detection
            is_pitcher = 'Pitcher' in positions or bool(re.search(r'\s+P\s*$', raw_name))
            
            if is_pitcher:
                print(f"  PITCHER: {clean_name} ({pos_str}) PA={pa}")
            elif pa > 0:
                print(f"  #{batting_order}: {clean_name} ({pos_str}) PA={pa}")
                batting_order += 1
            else:
                print(f"  SUB: {clean_name} ({pos_str}) PA={pa}")

if __name__ == "__main__":
    # Test position extraction
    print("Testing positions:")
    test_names = ["Steven Kwan LF", "José Ramírez 3B", "Salvador Perez C-1B"]
    for name in test_names:
        clean, pos = extract_name_and_positions(name)
        print(f"  {name} -> {clean}, {pos}")
    
    # Test simple parsing
    test_simple()