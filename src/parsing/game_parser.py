"""
Main game parsing pipeline
============================
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.mlb_cached_fetcher import SafePageFetcher
from parsing.name_utils import normalize_name
from parsing.game_utils import *
from parsing.outcome_analyzer import analyze_outcome
from bs4 import BeautifulSoup
from io import StringIO
import pandas as pd
from typing import Dict, List, Optional
import re

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

def parse_single_event(row: pd.Series, game_id: str) -> Optional[Dict]:
    """Parse a single play-by-play row into structured event"""
    batter_name = normalize_name(row['Batter'])
    pitcher_name = normalize_name(row['Pitcher'])
    description = str(row['Play Description']).strip()
    
    outcome = analyze_outcome(description)
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
    for _, row in df.iterrows():
        event = parse_single_event(row, game_id)
        if event:
            events.append(event)

    events_df = pd.DataFrame(events)
    if not events_df.empty:
        events_df = fix_pitch_count_duplicates(events_df)

    return events_df