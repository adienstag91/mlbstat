"""
Game, appearance, and event utilities
========================
"""

import re
import uuid
import pandas as pd
import unicodedata
from typing import Tuple, Dict, Optional, List

def extract_game_id(url: str) -> str:
    """Extract game ID from URL"""
    match = re.search(r'/boxes/[A-Z]{3}/([A-Z]{3}\d{8,9})', url)
    return match.group(1) if match else 'unknown'

def parse_inning(inn_str: str) -> int:
    """Parse inning number"""
    match = re.search(r'(\d+)', str(inn_str))
    return int(match.group(1)) if match else 0

def parse_inning_half(inn_str: str) -> str:
    """Parse inning half"""
    inn_lower = str(inn_str).lower()
    if inn_lower.startswith('t'):
        return 'top'
    elif inn_lower.startswith('b'):
        return 'bottom'
    return ''

def parse_pitch_count(count_str: str) -> int:
    """Parse pitch count"""
    match = re.match(r'^(\d+)', str(count_str))
    return int(match.group(1)) if match else 0

def safe_int(value, default=0):
    """Safely convert to int"""
    try:
        return int(float(value)) if pd.notna(value) else default
    except (ValueError, TypeError):
        return default

def generate_event_id() -> str:
    """Generate unique event ID"""
    return str(uuid.uuid4())

def extract_from_details(row: pd.Series, stat: str) -> int:
    """Extract stat from Details column"""
    if 'Details' not in row.index or pd.isna(row['Details']):
        return 0
    
    details = str(row['Details'])
    match = re.search(rf"(\d+)·{stat}|(?:^|,)\s*{stat}(?:,|$)", details)
    return int(match.group(1)) if match and match.group(1) else (1 if match else 0)

def fix_pitch_count_duplicates(events: pd.DataFrame) -> pd.DataFrame:
    """Fix pitch count double-counting in non-PA events"""
    if events.empty:
         return events
    
    events = events.sort_values(['inning', 'inning_half']).reset_index(drop=True)
    
    for (inning, half), group in events.groupby(['inning', 'inning_half']):
        indices = group.index.tolist()
        
        for i, idx in enumerate(indices):
            event = events.loc[idx]
            
            if event['is_plate_appearance'] or event['pitch_count'] == 0:
                continue
            
            is_last_event = (i == len(indices) - 1)
            has_followup_pa = False
            
            if not is_last_event:
                next_event = events.loc[indices[i + 1]]
                if (next_event['is_plate_appearance'] and 
                    next_event['batter_name'] == event['batter_name'] and
                    next_event['pitcher_name'] == event['pitcher_name']):
                    has_followup_pa = True
            
            if has_followup_pa:
                events.loc[idx, 'pitch_count'] = 0
    
    return events

def extract_player_id(html_cell) -> Optional[str]:
    """Extract Baseball Reference player ID from HTML cell"""
    if not html_cell:
        return None
    
    # Look for player links like /players/o/ohtansh01.shtml
    link = html_cell.find('a', href=True)
    if link and link.get('href'):
        href = link.get('href')
        # Extract player ID from URL
        match = re.search(r'/players/[a-z]/([a-z\.\d]+)\.shtml', href)
        if match:
            return match.group(1)
    
    return None

def extract_name_and_positions(raw_entry: str) -> tuple[str, List[str]]:
    """Extract clean player name and positions from raw batting entry"""
    # Remove decisions first (W, L, S, etc.)
    cleaned = re.sub(r',\s*[WLSHB]+\s*\([^)]*\)', '', raw_entry).strip()
    
    positions = []
    player_name = cleaned
    
    # Extract position codes at the end: "3B", "C-1B", "LF-CF", etc.
    position_match = re.search(r'\s+([A-Z0-9]{1,2}(?:-[A-Z0-9]{1,2})*)\s*$', cleaned)
    
    if position_match:
        position_codes = position_match.group(1)
        player_name = cleaned[:position_match.start()].strip()
        
        # Handle multiple positions like "C-1B"
        for code in position_codes.split('-'):
            #expanded = expand_position_code(code.strip())
            #if expanded and expanded not in positions:
            positions.append(code)
    
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

def extract_pitcher_decisions(raw_entry: str) -> Tuple[str, List[str]]:
    """Extract pitcher name and all decisions (W, L, S, H, BS)"""
    decisions = []
    
    # Find all decision patterns like ", W (1-0)", ", BS (2)", etc.
    decision_matches = re.findall(r',\s*([WLSHB]+)\s*\([^)]*\)', raw_entry)
    decisions.extend(decision_matches)
    
    # Remove all decision patterns to get clean name
    clean_name = re.sub(r',\s*[WLSHB]+\s*\([^)]*\)', '', raw_entry).strip()
    clean_name = normalize_name(clean_name)
    
    return clean_name, decisions

def check_html_indentation(html_row) -> bool:
    """Check if HTML row has indentation indicating a substitute"""
    if not html_row:
        return False
    
    cells = html_row.find_all(['td', 'th'])
    if not cells:
        return False
    
    # Check the player name cell for indentation markers
    player_cell = cells[0]
    
    # Check for leading non-breaking spaces or regular spaces
    cell_text = player_cell.get_text()
    has_leading_spaces = bool(re.match(r'^[\s\xa0\u00a0]+', cell_text))
    
    # Check HTML content for &nbsp; entities
    cell_html = str(player_cell)
    has_nbsp_entities = '&nbsp;' in cell_html or '\xa0' in cell_html
    
    # Check for CSS indentation styles
    cell_style = player_cell.get('style', '')
    has_indent_style = any(prop in cell_style.lower() for prop in ['padding-left', 'margin-left', 'text-indent'])
    
    return has_leading_spaces or has_nbsp_entities or has_indent_style

def normalize_name(name: str) -> str:
    """Normalize name for consistent matching"""
    if pd.isna(name) or not name:
        return ""
    
    # Unicode normalization and clean whitespace
    cleaned = unicodedata.normalize('NFKD', str(name))
    cleaned = re.sub(r'[\s\xa0]+', ' ', cleaned).strip()
    
    # Remove ALL trailing result codes (multiple W,L,S,B,H patterns)
    cleaned = re.sub(r',\s*[WLSHB]+\s*\([^)]*\)(?:\s*,\s*[WLSHB]+\s*\([^)]*\))*$', '', cleaned)
    
    # Handle name suffixes BEFORE removing position codes
    suffix_match = re.search(r'\s+(II|III|IV|Jr\.?|Sr\.?)\s*([A-Z]{1,3})*$', cleaned)
    
    preserved_suffix = ""
    if suffix_match:
        preserved_suffix = suffix_match.group(1)
        cleaned = cleaned[:suffix_match.start()] + ' ' + (suffix_match.group(2) or '')
        cleaned = cleaned.strip()
    
    # Remove position codes
    cleaned = re.sub(r"((?:[A-Z0-9]{1,3})(?:-[A-Z0-9]{1,3})*)$", "", cleaned).strip()
    
    # Add back the preserved suffix
    if preserved_suffix:
        cleaned = f"{cleaned} {preserved_suffix}"
    
    return cleaned
