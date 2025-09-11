"""
HTML Indentation-Based Substitution Detection
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

def check_html_indentation(html_row) -> bool:
    """Check if a table row has indentation indicating a substitute"""
    
    if not html_row:
        return False
    
    cells = html_row.find_all(['td', 'th'])
    if not cells:
        return False
    
    # Check the player name cell (usually first cell)
    player_cell = cells[0]
    
    # Method 1: Check for leading non-breaking spaces in text
    cell_text = player_cell.get_text()
    has_leading_spaces = bool(re.match(r'^[\s\xa0\u00a0]+', cell_text))
    
    # Method 2: Check HTML content for &nbsp; entities
    cell_html = str(player_cell)
    has_nbsp_entities = '&nbsp;' in cell_html or '\xa0' in cell_html
    
    # Method 3: Check for special CSS classes that might indicate indentation
    cell_classes = player_cell.get('class', [])
    has_indent_class = any('indent' in str(cls).lower() for cls in cell_classes)
    
    # Method 4: Check for inline styles with padding/margin
    cell_style = player_cell.get('style', '')
    has_indent_style = any(prop in cell_style.lower() for prop in ['padding-left', 'margin-left', 'text-indent'])
    
    return has_leading_spaces or has_nbsp_entities or has_indent_class or has_indent_style

def parse_with_html_indentation(df: pd.DataFrame, html_table, team: str) -> List[Dict]:
    """Parse batting using HTML indentation to detect substitutes"""
    
    appearances = []
    batting_order = 1
    
    # Get HTML rows
    html_rows = html_table.find_all('tr')
    data_rows = [row for row in html_rows if row.find('td')]  # Skip header rows
    
    print(f"\nDEBUG - {team.upper()} TEAM HTML ANALYSIS:")
    
    for idx, row in df.iterrows():
        raw_name = str(row['Batting'])
        pa = safe_int(row.get('PA', 0))
        clean_name, positions = extract_name_and_positions(raw_name)
        
        # Skip pitchers
        is_pitcher = 'Pitcher' in positions or bool(re.search(r'\s+P\s*$', raw_name))
        if is_pitcher:
            continue
        
        # Get corresponding HTML row for indentation check
        html_row = data_rows[idx] if idx < len(data_rows) else None
        is_indented = check_html_indentation(html_row)
        
        # Debug output
        indent_indicators = []
        if html_row:
            cell = html_row.find_all(['td', 'th'])[0]
            cell_text = cell.get_text()
            cell_html = str(cell)[:100]
            
            if re.match(r'^[\s\xa0\u00a0]+', cell_text):
                indent_indicators.append("leading_spaces")
            if '&nbsp;' in cell_html or '\xa0' in cell_html:
                indent_indicators.append("nbsp_entities")
            
        print(f"  {idx:2d}: {raw_name:30s} -> Indented: {is_indented} {indent_indicators} PA={pa}")
        
        # Handle pinch runners separately
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
        
        if is_indented and pa > 0:
            # This is a substitute - don't increment batting order
            appearances.append({
                'player_name': clean_name,
                'team': team,
                'batting_order': batting_order - 1 if batting_order > 1 else None,  # Inherit previous order
                'positions_played': positions,
                'is_starter': False,
                'is_substitute': True,
                'substitution_type': 'pinch_hitter' if 'Pinch Hitter' in positions else 'substitute',
                'PA': pa
            })
        elif pa > 0 and batting_order <= 9:
            # This is a starter
            appearances.append({
                'player_name': clean_name,
                'team': team,
                'batting_order': batting_order,
                'positions_played': positions,
                'is_starter': True,
                'is_substitute': False,
                'PA': pa
            })
            batting_order += 1
        elif pa > 0:
            # Has batting stats but unclear position
            appearances.append({
                'player_name': clean_name,
                'team': team,
                'batting_order': None,
                'positions_played': positions,
                'is_starter': False,
                'is_substitute': True,
                'substitution_type': 'unclear_substitute',
                'PA': pa
            })
        else:
            # Defensive substitute
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

def test_html_indentation():
    """Test HTML indentation detection"""
    test_url = "https://www.baseball-reference.com/boxes/BAL/BAL202509050.shtml"
    
    from utils.mlb_cached_fetcher import SafePageFetcher
    soup = SafePageFetcher.fetch_page(test_url)
    
    batting_tables = soup.find_all('table', {'id': lambda x: x and x.endswith('batting')})
    
    for table_idx, table in enumerate(batting_tables):
        team_name = 'away' if table_idx == 0 else 'home'
        
        df = pd.read_html(StringIO(str(table)))[0]
        df = df[df['Batting'].notna()]
        df = df[~df['Batting'].str.contains("Team Totals", na=False)]
        
        appearances = parse_with_html_indentation(df, table, team_name)
        
        print(f"\n{team_name.upper()} TEAM RESULTS:")
        print("STARTERS:")
        starters = [app for app in appearances if app['is_starter']]
        for app in sorted(starters, key=lambda x: x['batting_order']):
            name = app['player_name']
            order = app['batting_order']
            positions = ', '.join(app['positions_played'])
            pa = app['PA']
            print(f"  #{order}: {name:18s} ({positions:15s}) PA={pa}")
        
        print("\nSUBSTITUTES:")
        substitutes = [app for app in appearances if app['is_substitute']]
        for app in substitutes:
            name = app['player_name']
            order = app['batting_order']
            positions = ', '.join(app['positions_played'])
            pa = app['PA']
            sub_type = app.get('substitution_type', 'sub')
            
            order_str = f"#{order}" if order else "No order"
            print(f"  {order_str:>8}: {name:18s} ({positions:15s}) {sub_type:15s} PA={pa}")

if __name__ == "__main__":
    test_html_indentation()