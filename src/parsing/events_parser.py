"""
Event outcome analysis
=====================
"""


import re
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from typing import Optional, Dict
from io import StringIO
import pandas as pd
from bs4 import BeautifulSoup
from utils.url_cacher import HighPerformancePageFetcher
fetcher = HighPerformancePageFetcher(max_cache_size_mb=500)
from parsing.parsing_utils import *

def analyze_event_outcome(description: str) -> Optional[Dict]:
    """Analyze play outcome - handles all types of baseball events"""
    desc = description.lower().strip()
    outcome = {
        'is_plate_appearance': True, 'is_at_bat': False, 'is_hit': False, 'hit_type': None,
        'is_walk': False, 'is_strikeout': False, 'is_sacrifice_fly': False, 'is_sacrifice_hit': False,
        'is_out': False, 'outs_recorded': 0, 'bases_reached': 0,
    }
    
    # Check for pure baserunning plays FIRST
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
    
    # Handle compound plays
    has_batter_action = any(pattern in desc for pattern in [
        'strikeout', 'struck out', 'single', 'double', 'triple', 'home run',
        'walk', 'grounded out', 'flied out', 'lined out', 'popped out',
        'hit by pitch', 'sacrifice'
    ])
    
    has_baserunning = any(pattern in desc for pattern in [
        'caught stealing', 'pickoff', 'picked off', 'wild pitch', 'passed ball'
    ])
    
    if has_batter_action and has_baserunning:
        desc = desc.split(',')[0].strip()
    
    # Sacrifice flies
    if re.search(r'sacrifice fly|sac fly|flyball.*sacrifice fly', desc):
        outcome.update({'is_sacrifice_fly': True, 'is_out': True, 'outs_recorded': 1})
        return outcome
    
    # Sacrifice hits
    if re.search(r'sacrifice bunt|sac bunt|bunt.*sacrifice', desc):
        outcome.update({'is_sacrifice_hit': True, 'is_out': True, 'outs_recorded': 1})
        return outcome
    
    # Walks
    if re.search(r'^walk\b|^intentional walk', desc):
        outcome.update({'is_walk': True})
        return outcome
    
    # Hit by pitch
    if re.search(r'^hit by pitch|^hbp\b', desc):
        return outcome

    # Strikeout with wild pitch/passed ball
    if re.search(r'strikeout.*wild pitch|strikeout.*passed ball|wild pitch.*strikeout|passed ball.*strikeout', desc):
        outcome.update({
            'is_at_bat': True,
            'is_strikeout': True,
            'is_out': False,
            'outs_recorded': 0})
        return outcome
        
    # Double play with strikeout
    elif re.search(r'double play.*strikeout|strikeout.*double play', desc):
        outcome.update({'is_at_bat': True, 'is_strikeout': True, 'is_out': True, 'outs_recorded': 2})
        return outcome
    
    # Pure baserunning
    elif re.search(r'caught stealing|pickoff|picked off|wild pitch|passed ball|balk', desc) and not has_batter_action:
        outcome.update({'is_plate_appearance': False})
        return outcome
    
    # At-bat outcomes
    outcome['is_at_bat'] = True

    # Reached on error
    if re.search(r'reached.*error|reached.*e\d+', desc):
        outcome.update({'is_out': False})
        return outcome

    # Reached on interference
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

    # Batter interference
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

def parse_single_event(row: pd.Series, game_id: str, event_order: int = 0) -> Optional[Dict]:
    """Parse a single play-by-play row into structured event"""
    batter_name = normalize_name(row['Batter'])
    pitcher_name = normalize_name(row['Pitcher'])
    description = str(row['Play Description']).strip()
    
    outcome = analyze_event_outcome(description)
    if not outcome:
        return None
    
    return {
        'event_id': generate_event_id(),
        'game_id': game_id,
        'inning': parse_inning(row.get('Inn', '')),
        'inning_half': parse_inning_half(row.get('Inn', '')),
        'batter_id': batter_name,
        'pitcher_id': pitcher_name,
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
        'event_order': event_order,
    }

def parse_play_by_play_events(soup: BeautifulSoup, game_id: str) -> pd.DataFrame:
    """Parse all play-by-play events from game"""
    pbp_table = soup.find("table", id="play_by_play")
    if not pbp_table:
        return pd.DataFrame()
    
    try:
        df = pd.read_html(StringIO(str(pbp_table)))[0]
    except Exception:
        return pd.DataFrame()
    
    df = df[df['Inn'].notna() & df['Play Description'].notna() & df['Pitcher'].notna()]
    df = df[~df['Batter'].str.contains("Top of the|Bottom of the", case=False, na=False)]
    
    events = []
    for event_order, (_, row) in enumerate(df.iterrows(), start=1):
        event = parse_single_event(row, game_id, event_order)
        if event:
            events.append(event)

    events_df = pd.DataFrame(events)
    if not events_df.empty:
        # Fix pitch counts first
        events_df = fix_pitch_count_duplicates(events_df)
        
        # THEN reassign proper sequential order
        events_df = events_df.reset_index(drop=True)
        events_df['event_order'] = range(1, len(events_df) + 1)

    return events_df

def test_events_parser(game_url):
    """Test the play by play events parser"""

    # Fetch page
    soup = fetcher.fetch_page(game_url)
    game_id = extract_game_id(game_url)
    
    print(f"Testing play-by-play events parser: {game_url}")
    print("=" * 60)
    
    # Process game
    results = parse_play_by_play_events(soup, game_id)
    
    print(f"\nPLAY-BY-PLAY EVENTS:")
    print (results)
    return results


class SimpleFetcher:
    """Temporary fetcher for testing without cache database conflicts"""
    
    def fetch_page(self, url: str) -> BeautifulSoup:
        from playwright.sync_api import sync_playwright
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            )
            page = context.new_page()
            page.set_default_timeout(30000)
            page.goto(url, wait_until='domcontentloaded')
            page.wait_for_timeout(2000)
            html_content = page.content()
            browser.close()
        
        return BeautifulSoup(html_content, "html.parser")

if __name__ == "__main__":
    game_url = "https://www.baseball-reference.com/boxes/KCA/KCA202503290.shtml"
    test_events_parser(game_url)
