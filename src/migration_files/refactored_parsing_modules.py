# ============================================================================
# parsing/name_utils.py - Name resolution and normalization
# ============================================================================

import re
import unicodedata
import pandas as pd
from typing import Dict, Set
from bs4 import BeautifulSoup

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
        # Remove suffix temporarily for position code removal
        cleaned = cleaned[:suffix_match.start()] + ' ' + (suffix_match.group(2) or '')
        cleaned = cleaned.strip()
    
    # Remove position codes (now suffix is safe)
    cleaned = re.sub(r"((?:[A-Z0-9]{1,3})(?:-[A-Z0-9]{1,3})*)$", "", cleaned).strip()
    
    # Add back the preserved suffix
    if preserved_suffix:
        cleaned = f"{cleaned} {preserved_suffix}"
    
    return cleaned

def extract_canonical_names(soup: BeautifulSoup) -> Set[str]:
    """Extract canonical names from box score tables"""
    names = set()
    for table_type in ['batting', 'pitching']:
        tables = soup.find_all('table', {'id': lambda x: x and table_type in x.lower()})
        for table in tables:
            for row in table.find_all('tr'):
                name_cell = row.find('th', {'data-stat': 'player'})
                if name_cell:
                    name = normalize_name(name_cell.get_text(strip=True))
                    if name and name not in ['Player', 'Batting', 'Pitching']:
                        names.add(name)
    return names

def build_name_resolver(canonical_names: Set[str]) -> Dict[str, str]:
    """Build name resolution mapping"""
    mappings = {}
    for name in canonical_names:
        mappings[name] = name
        # Add abbreviated versions
        if ' ' in name:
            parts = name.split(' ')
            if len(parts) >= 2:
                abbrev = f"{parts[0][0]}. {' '.join(parts[1:])}"
                mappings[abbrev] = name
    return mappings

# ============================================================================
# parsing/game_utils.py - Game and event utilities
# ============================================================================

import uuid

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

# ============================================================================
# parsing/outcome_analyzer.py - Event outcome analysis
# ============================================================================

from typing import Optional

def analyze_outcome(description: str) -> Optional[Dict]:
    """Analyze play outcome - handles all types of baseball events"""
    desc = description.lower().strip()
    outcome = {
        'is_plate_appearance': True, 'is_at_bat': False, 'is_hit': False, 'hit_type': None,
        'is_walk': False, 'is_strikeout': False, 'is_sacrifice_fly': False, 'is_sacrifice_hit': False,
        'is_out': False, 'outs_recorded': 0, 'bases_reached': 0,
    }
    
    # Check for pure baserunning plays FIRST (before compound play logic)
    pure_baserunning_patterns = [
        r'caught stealing.*interference by runner',
        r'interference by runner.*caught stealing', 
        r'double play.*caught stealing.*interference',
        r'caught stealing.*double play.*interference',
        r'^interference by runner',
        r'^runner interference'
    ]
    
    for pattern in pure_baserunning_patterns:
        if re.search(pattern, desc):
            outcome.update({'is_plate_appearance': False})
            return outcome
    
    # Handle compound plays - prioritize BATTER outcome over baserunning
    has_batter_action = any(pattern in desc for pattern in [
        'strikeout', 'struck out', 'single', 'double', 'triple', 'home run',
        'walk', 'grounded out', 'flied out', 'lined out', 'popped out',
        'hit by pitch', 'sacrifice'
    ])
    
    has_baserunning = any(pattern in desc for pattern in [
        'caught stealing', 'pickoff', 'picked off', 'wild pitch', 'passed ball'
    ])
    
    # If compound play, focus on BATTER'S outcome
    if has_batter_action and has_baserunning:
        desc = desc.split(',')[0].strip()  # Take first part before comma
    
    # Sacrifice flies (not at-bats)
    if re.search(r'sacrifice fly|sac fly|flyball.*sacrifice fly', desc):
        outcome.update({'is_sacrifice_fly': True, 'is_out': True, 'outs_recorded': 1})
        return outcome
    
    # Sacrifice hits (not at-bats, e.g. sac bunts)
    if re.search(r'sacrifice bunt|sac bunt|bunt.*sacrifice', desc):
        outcome.update({'is_sacrifice_hit': True, 'is_out': True, 'outs_recorded': 1})
        return outcome
    
    # Walks (not at-bats)
    if re.search(r'^walk\b|^intentional walk', desc):
        outcome.update({'is_walk': True})
        return outcome
    
    # Hit by pitch (not at-bats)
    if re.search(r'^hit by pitch|^hbp\b', desc):
        return outcome

    # SPECIAL CASE: Strikeout with wild pitch/passed ball
    if re.search(r'strikeout.*wild pitch|strikeout.*passed ball|wild pitch.*strikeout|passed ball.*strikeout', desc):
        outcome.update({
            'is_at_bat': True,
            'is_strikeout': True,
            'is_out': False,
            'outs_recorded': 0})
        return outcome
        
    # SPECIAL CASE: Double play with strikeout
    elif re.search(r'double play.*strikeout|strikeout.*double play', desc):
        outcome.update({'is_at_bat': True, 'is_strikeout': True, 'is_out': True, 'outs_recorded': 2})
        return outcome
    
    # Pure baserunning (no batter action)
    elif re.search(r'caught stealing|pickoff|picked off|wild pitch|passed ball|balk', desc) and not has_batter_action:
        outcome.update({'is_plate_appearance': False})
        return outcome
    
    # At-bat outcomes
    outcome['is_at_bat'] = True

    # Reached on error
    if re.search(r'reached.*error|reached.*e\d+', desc):
        outcome.update({'is_out': False})
        return outcome

    # Reached on catcher's interference
    if re.search(r'reached.*interference', desc):
        outcome.update({'is_out': False, 'is_at_bat': False})
        return outcome
    
    # Strikeouts
    if re.search(r'^strikeout\b|^struck out|strikeout looking|strikeout swinging', desc):
        outcome.update({'is_strikeout': True, 'is_out': True})
        return outcome

    # Double plays
    if re.search(r'grounded into double play|gdp\b|double play', desc):
        outcome.update({'is_out': True, 'outs_recorded': 2})
        return outcome

    # Batter's interference
    if re.search(r'interference by batter', desc):
        outcome.update({'is_out': True, 'outs_recorded': 1})
        return outcome
    
    # Other outs
    out_patterns = [
        r'grounded out\b', r'flied out\b', r'lined out\b', r'popped out\b',
        r'groundout\b', r'flyout\b', r'lineout\b', r'popout\b', r'popfly\b', r'flyball\b', r"fielder's choice\b"
    ]
    
    for pattern in out_patterns:
        if re.search(pattern, desc):
            outcome.update({'is_out': True, 'outs_recorded': 1})
            return outcome

    # Home runs
    if re.search(r'home run\b|^hr\b', desc):
        outcome.update({'is_hit': True, 'hit_type': 'home_run', 'bases_reached': 4})
        return outcome
    
    # Other hits
    hit_patterns = [
        (r'^single\b.*(?:to|up|through)', 'single', 1),
        (r'^double\b.*(?:to|down)|ground-rule double', 'double', 2),
        (r'^triple\b.*(?:to|down)', 'triple', 3)
    ]
    
    for pattern, hit_type, bases in hit_patterns:
        if re.search(pattern, desc):
            outcome.update({'is_hit': True, 'hit_type': hit_type, 'bases_reached': bases})
            return outcome
            
    return None

# ============================================================================
# parsing/stats_parser.py - Official stats parsing
# ============================================================================

from io import StringIO
from typing import List

def extract_from_details(row: pd.Series, stat: str) -> int:
    """Extract stat from Details column"""
    if 'Details' not in row.index or pd.isna(row['Details']):
        return 0
    
    details = str(row['Details'])
    match = re.search(rf"(\d+)·{stat}|(?:^|,)\s*{stat}(?:,|$)", details)
    return int(match.group(1)) if match and match.group(1) else (1 if match else 0)

def parse_official_batting(soup: BeautifulSoup) -> pd.DataFrame:
    """Parse official batting stats"""
    batting_tables = soup.find_all('table', {'id': lambda x: x and x.endswith('batting')})
    all_stats = []
    
    for table in batting_tables:
        try:
            df = pd.read_html(StringIO(str(table)))[0]
            df = df[df['Batting'].notna()]
            df = df[~df['Batting'].str.contains("Team Totals", na=False)]
            
            for _, row in df.iterrows():
                player_name = normalize_name(row['Batting'])
                if player_name:
                    all_stats.append({
                        'player_name': player_name,
                        'AB': safe_int(row.get('AB', 0)),
                        'H': safe_int(row.get('H', 0)),
                        'BB': safe_int(row.get('BB', 0)),
                        'SO': safe_int(row.get('SO', 0)),
                        'PA': safe_int(row.get('PA', 0)),
                        'HR': extract_from_details(row, 'HR'),
                        '2B': extract_from_details(row, '2B'),
                        '3B': extract_from_details(row, '3B'),
                        'SB': extract_from_details(row, 'SB'),
                        'CS': extract_from_details(row, 'CS'),
                        'HBP': extract_from_details(row, 'HBP'),
                        'GDP': extract_from_details(row, 'GDP'),
                        'SF': extract_from_details(row, 'SF'),
                        'SH': extract_from_details(row, 'SH'),
                    })
        except Exception:
            continue
    
    return pd.DataFrame(all_stats)

def parse_official_pitching(soup: BeautifulSoup) -> pd.DataFrame:
    """Parse official pitching stats"""
    pitching_tables = soup.find_all('table', {'id': lambda x: x and x.endswith('pitching')})
    all_stats = []
    
    for table in pitching_tables:
        try:
            df = pd.read_html(StringIO(str(table)))[0]
            df = df[df['Pitching'].notna()]
            df = df[~df['Pitching'].str.contains("Team Totals", na=False)]
            
            for _, row in df.iterrows():
                pitcher_name = normalize_name(row['Pitching'])
                if pitcher_name:
                    all_stats.append({
                        'pitcher_name': pitcher_name,
                        'BF': safe_int(row.get('BF', 0)),
                        'H': safe_int(row.get('H', 0)),
                        'BB': safe_int(row.get('BB', 0)),
                        'SO': safe_int(row.get('SO', 0)),
                        'HR': safe_int(row.get('HR', 0)),
                        'PC': safe_int(row.get('Pit', 0)),
                    })
        except Exception:
            continue
    
    return pd.DataFrame(all_stats)

# ============================================================================
# parsing/events_parser.py - Event parsing and processing
# ============================================================================

from typing import Optional

def parse_single_event(row: pd.Series, game_id: str, name_resolver: Dict[str, str]) -> Optional[Dict]:
    """Parse a single play-by-play row into structured event"""
    # Clean and resolve names
    batter_name = normalize_name(row['Batter'])
    pitcher_name = normalize_name(row['Pitcher'])
    description = str(row['Play Description']).strip()
    
    resolved_batter = name_resolver.get(batter_name, batter_name)
    resolved_pitcher = name_resolver.get(pitcher_name, pitcher_name)
    
    # Analyze outcome
    outcome = analyze_outcome(description)
    if not outcome:
        return None
    
    return {
        'event_id': generate_event_id(),
        'game_id': game_id,
        'inning': parse_inning(row.get('Inn', '')),
        'inning_half': parse_inning_half(row.get('Inn', '')),
        'batter_id': resolved_batter,
        'pitcher_id': resolved_pitcher,
        'description': description,
        'is_plate_appearance': outcome['is_plate_appearance'],
        'is_at_bat': outcome['is_at_bat'],
        'is_hit': outcome['is_hit'],
        'hit_type': outcome.get('hit_type'),
        'is_walk': outcome['is_walk'],
        'is_strikeout': outcome['is_strikeout'],
        'is_sacrifice_fly': outcome['is_sacrifice_fly'],
        'is_sacrifice_hit': outcome['is_sacrifice_hit'],
        'is_out': outcome['is_out'],
        'outs_recorded': outcome['outs_recorded'],
        'bases_reached': outcome['bases_reached'],
        'pitch_count': parse_pitch_count(row.get('Pit(cnt)', '')),
    }

def parse_play_by_play_events(soup: BeautifulSoup, game_id: str, name_resolver: Dict[str, str]) -> pd.DataFrame:
    """Parse all play-by-play events from game"""
    pbp_table = soup.find("table", id="play_by_play")
    if not pbp_table:
        return pd.DataFrame()
    
    try:
        df = pd.read_html(StringIO(str(pbp_table)))[0]
    except Exception:
        return pd.DataFrame()
    
    # Clean data - keep ALL events
    df = df[df['Inn'].notna() & df['Play Description'].notna() & df['Pitcher'].notna()]
    df = df[~df['Batter'].str.contains("Top of the|Bottom of the", case=False, na=False)]
    
    events = []
    for _, row in df.iterrows():
        event = parse_single_event(row, game_id, name_resolver)
        if event:
            events.append(event)

    # Convert to DataFrame and fix pitch count duplicates
    events_df = pd.DataFrame(events)
    if not events_df.empty:
        events_df = fix_pitch_count_duplicates(events_df)

    return events_df

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
            
            # Check if next event is a PA with same batter/pitcher
            is_last_event = (i == len(indices) - 1)
            has_followup_pa = False
            
            if not is_last_event:
                next_event = events.loc[indices[i + 1]]
                if (next_event['is_plate_appearance'] and 
                    next_event['batter_id'] == event['batter_id'] and
                    next_event['pitcher_id'] == event['pitcher_id']):
                    has_followup_pa = True
            
            # Zero out pitch count if there's a follow-up PA
            if has_followup_pa:
                events.loc[idx, 'pitch_count'] = 0
    
    return events
