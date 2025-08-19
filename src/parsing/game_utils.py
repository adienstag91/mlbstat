"""
Game and event utilities
========================
"""

import re
import uuid
import pandas as pd

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
                    next_event['batter_id'] == event['batter_id'] and
                    next_event['pitcher_id'] == event['pitcher_id']):
                    has_followup_pa = True
            
            if has_followup_pa:
                events.loc[idx, 'pitch_count'] = 0
    
    return events
