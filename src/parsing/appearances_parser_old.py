"""
Complete Player Appearances Parser
=================================

Comprehensive parser for batting and pitching appearances that extracts:
- Official batting/pitching statistics
- Player batting order and positions
- Starter vs substitute determination using HTML indentation
- Player IDs/URLs from Baseball Reference links
- Team assignments and appearance metadata

This replaces the old UnifiedEventsParser appearance functionality.
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

from utils.url_cacher import HighPerformancePageFetcher
fetcher = HighPerformancePageFetcher(max_cache_size_mb=500)

from parsing.parsing_utils import extract_game_id, safe_int, extract_from_details, normalize_name, extract_name_and_positions, check_html_indentation, extract_player_id, extract_pitcher_decisions

def parse_batting_appearances(soup: BeautifulSoup) -> Tuple[pd.DataFrame, List[Dict]]:
    """
    Parse batting appearances with official stats and metadata
    
    Returns:
        Tuple of (official_batting_stats_df, batting_appearances_list)
    """
    
    batting_tables = soup.find_all('table', {'id': lambda x: x and x.endswith('batting')})
    all_batting_stats = []
    all_batting_appearances = []
    
    for table_idx, table in enumerate(batting_tables):
        try:
            # Parse table with pandas
            df = pd.read_html(StringIO(str(table)))[0]
            df = df[df['Batting'].notna()]
            df = df[~df['Batting'].str.contains("Team Totals", na=False)]
            
            team = 'away' if table_idx == 0 else 'home'
            
            # Get HTML rows for indentation and player ID extraction
            html_rows = table.find_all('tr')
            data_rows = [row for row in html_rows if row.find('td')]
            
            batting_order = 1
            
            for row_idx, row in df.iterrows():
                raw_batting_entry = str(row['Batting'])
                
                # Extract player info
                clean_name, positions = extract_name_and_positions(raw_batting_entry)
                
                # Get corresponding HTML row
                html_row = data_rows[row_idx] if row_idx < len(data_rows) else None
                player_id = extract_player_id(html_row.find_all(['td', 'th'])[0] if html_row else None)
                is_indented = check_html_indentation(html_row)
                
                # Skip pitchers entirely from batting appearances
                is_pitcher = 'Pitcher' in positions or bool(re.search(r'\s+P\s*$', raw_batting_entry))
                if is_pitcher:
                    continue
                
                # Get stats
                pa = safe_int(row.get('PA', 0))
                ab = safe_int(row.get('AB', 0))
                
                # Build official stats record (for validation)
                batting_stats = {
                    'player_id' : player_id,
                    'player_name': clean_name,
                    'team': team,
                    'AB': ab,
                    'H': safe_int(row.get('H', 0)),
                    'BB': safe_int(row.get('BB', 0)),
                    'SO': safe_int(row.get('SO', 0)),
                    'PA': pa,
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
                
                # Determine appearance metadata
                is_pinch_runner = 'PR' in positions
                is_pinch_hitter = 'PH' in positions
                
                if (is_pinch_runner or is_pinch_hitter or is_indented) and pa > 0:
                    appearance_metadata = {
                        'batting_order': batting_order - 1 if batting_order > 1 else None,  # Inherit previous
                        'is_starter': False,
                        'is_substitute': True
                    }
                elif (is_pinch_runner or is_pinch_hitter or is_indented) and pa == 0:
                    appearance_metadata = {
                        'batting_order': None,
                        'is_starter': False,
                        'is_substitute': True,
                    }
                else:
                    appearance_metadata = {
                        'batting_order': batting_order,
                        'is_starter': True,
                        'is_substitute': False,
                    }
                    batting_order += 1
                
                # Build appearance record
                batting_appearance = {
                    'player_name': clean_name,
                    'player_id': player_id,
                    'team': team,
                    'batting_order': appearance_metadata['batting_order'],
                    'positions_played': positions,
                    'is_starter': appearance_metadata['is_starter'],
                    'is_substitute': appearance_metadata['is_substitute'],
                    
                    # Include key batting stats in appearance record
                    'PA': pa,
                    'AB': ab,
                    'H': batting_stats['H'],
                    'R': batting_stats['R'],
                    'RBI': batting_stats['RBI'],
                    'HR': batting_stats['HR'],
                }
                
                all_batting_stats.append(batting_stats)
                all_batting_appearances.append(batting_appearance)
                
        except Exception as e:
            print(f"Error parsing batting table {table_idx}: {e}")
            continue
    
    return pd.DataFrame(all_batting_stats), pd.DataFrame(all_batting_appearances)

def parse_pitching_appearances(soup: BeautifulSoup) -> Tuple[pd.DataFrame, List[Dict]]:
    """
    Parse pitching appearances with decisions and metadata
    
    Returns:
        Tuple of (official_pitching_stats_df, pitching_appearances_list)
    """
    
    pitching_tables = soup.find_all('table', {'id': lambda x: x and x.endswith('pitching')})
    all_pitching_stats = []
    all_pitching_appearances = []
    
    for table_idx, table in enumerate(pitching_tables):
        try:
            df = pd.read_html(StringIO(str(table)))[0]
            df = df[df['Pitching'].notna()]
            df = df[~df['Pitching'].str.contains("Team Totals", na=False)]
            
            team = 'away' if table_idx == 0 else 'home'
            
            # Get HTML rows for player ID extraction
            html_rows = table.find_all('tr')
            data_rows = [row for row in html_rows if row.find('td')]
            
            for row_idx, row in df.iterrows():
                raw_pitching_entry = str(row['Pitching'])
                
                # Extract pitcher name and decisions
                pitcher_name, decisions = extract_pitcher_decisions(raw_pitching_entry)
                
                # Get player ID from HTML
                html_row = data_rows[row_idx] if row_idx < len(data_rows) else None
                player_id = extract_player_id(html_row.find_all(['td', 'th'])[0] if html_row else None)
                
                # Build official stats record
                pitching_stats = {
                    'player_id': player_id,
                    'pitcher_name': pitcher_name,
                    'team': team,
                    'BF': safe_int(row.get('BF', 0)),
                    'H': safe_int(row.get('H', 0)),
                    'BB': safe_int(row.get('BB', 0)),
                    'SO': safe_int(row.get('SO', 0)),
                    'HR': safe_int(row.get('HR', 0)),
                    'PC': safe_int(row.get('Pit', 0)),  # Pitch count
                }
                
                # Build appearance record
                pitching_appearance = {
                    'player_name': pitcher_name,
                    'player_id': player_id,
                    'team': team,
                    'decisions': decisions,
                    'is_starter': row_idx == 0,  # First pitcher is starter
                    'pitching_order': row_idx + 1,
                    
                    # Include key pitching stats
                    'BF': pitching_stats['BF'],
                    'H': pitching_stats['H'],
                    'BB': pitching_stats['BB'],
                    'SO': pitching_stats['SO'],
                    'HR': pitching_stats['HR'],
                    'PC': pitching_stats['PC'],
                }
                
                all_pitching_stats.append(pitching_stats)
                all_pitching_appearances.append(pitching_appearance)
                
        except Exception as e:
            print(f"Error parsing pitching table {table_idx}: {e}")
            continue
    
    return pd.DataFrame(all_pitching_stats), all_pitching_appearances


def process_game_appearances(game_url: str) -> Dict:
    """
    Process a single game to extract all appearance metadata
    
    Returns comprehensive appearance data for database insertion
    """
    start_time = time.time()
    
    # Fetch page
    soup = fetcher.fetch_page(game_url)
    
    game_id = extract_game_id(game_url)
    
    # Parse batting and pitching appearances
    official_batting, batting_appearances = parse_batting_appearances(soup)
    official_pitching, pitching_appearances = parse_pitching_appearances(soup)
    
    processing_time = time.time() - start_time
    
    return {
        'game_id': game_id,
        
        # For validation against play-by-play
        'official_batting': official_batting,
        'official_pitching': official_pitching,
        
        # For database appearances table
        'batting_appearances': batting_appearances,
        'pitching_appearances': pitching_appearances,
        
        'processing_time': processing_time,
    }

def test_complete_appearances():
    """Test the complete appearances parser"""
    
    test_games = [
        "https://www.baseball-reference.com/boxes/KCA/KCA202503290.shtml",  # Original test game
        "https://www.baseball-reference.com/boxes/BAL/BAL202509050.shtml",  # Complex substitutions
    ]
    
    for game_url in test_games:
        print(f"\nTesting: {game_url}")
        print("=" * 60)
        
        result = process_game_appearances(game_url)
        
        print(f"Game: {result['game_id']}")
        print(f"Processing time: {result['processing_time']:.2f}s")
        
        # Separate batting appearances by team
        batting_apps = result['batting_appearances']
        away_batters = [app for app in batting_apps if app['team'] == 'away']
        home_batters = [app for app in batting_apps if app['team'] == 'home']
        
        # Show Away Team
        print(f"\nAWAY TEAM BATTING ({len(away_batters)} players):")
        away_starters = [app for app in away_batters if app['is_starter']]
        away_subs = [app for app in away_batters if app['is_substitute']]
        
        print(f"  Starters ({len(away_starters)}):")
        for app in sorted(away_starters, key=lambda x: x['batting_order'] or 0):
            name = app['player_name']
            order = app['batting_order']
            positions = ', '.join(app['positions_played'])
            pa = app['PA']
            player_id = app['player_id'] or 'No ID'
            print(f"    #{order}: {name:20s} ({positions:15s}) PA={pa} ID={player_id}")
        
        print(f"  Substitutes ({len(away_subs)}):")
        for app in away_subs:
            name = app['player_name']
            order = app['batting_order']
            positions = ', '.join(app['positions_played'])
            pa = app['PA']
            player_id = app['player_id'] or 'No ID'
            order_str = f"#{order}" if order else "No order"
            print(f"    {order_str:>8}: {name:20s} ({positions:6s}) PA={pa} ID={player_id}")
        
        # Show Home Team
        print(f"\nHOME TEAM BATTING ({len(home_batters)} players):")
        home_starters = [app for app in home_batters if app['is_starter']]
        home_subs = [app for app in home_batters if app['is_substitute']]
        
        print(f"  Starters ({len(home_starters)}):")
        for app in sorted(home_starters, key=lambda x: x['batting_order'] or 0):
            name = app['player_name']
            order = app['batting_order']
            positions = ', '.join(app['positions_played'])
            pa = app['PA']
            player_id = app['player_id'] or 'No ID'
            print(f"    #{order}: {name:20s} ({positions:6s}) PA={pa} ID={player_id}")
        
        print(f"  Substitutes ({len(home_subs)}):")
        for app in home_subs:
            name = app['player_name']
            order = app['batting_order']
            positions = ', '.join(app['positions_played'])
            pa = app['PA']
            player_id = app['player_id'] or 'No ID'
            order_str = f"#{order}" if order else "No order"
            print(f"    {order_str:>8}: {name:20s} ({positions:15s}) PA={pa} ID={player_id}")
        
        # Show pitching appearances by team
        pitching_apps = result['pitching_appearances']
        away_pitchers = [app for app in pitching_apps if app['team'] == 'away']
        home_pitchers = [app for app in pitching_apps if app['team'] == 'home']
        
        print(f"\nAWAY TEAM PITCHING ({len(away_pitchers)} pitchers):")
        for app in away_pitchers:
            name = app['player_name']
            decisions = ', '.join(app['decisions']) if app['decisions'] else 'None'
            role = 'Starter' if app['is_starter'] else f"Reliever #{app['pitching_order']}"
            bf = app['BF']
            player_id = app['player_id'] or 'No ID'
            print(f"    {role:12s}: {name:20s} BF={bf:2d} Decisions={decisions:8s} ID={player_id}")
        
        print(f"\nHOME TEAM PITCHING ({len(home_pitchers)} pitchers):")
        for app in home_pitchers:
            name = app['player_name']
            decisions = ', '.join(app['decisions']) if app['decisions'] else 'None'
            role = 'Starter' if app['is_starter'] else f"Reliever #{app['pitching_order']}"
            bf = app['BF']
            player_id = app['player_id'] or 'No ID'
            print(f"    {role:12s}: {name:20s} BF={bf:2d} Decisions={decisions:8s} ID={player_id}")

        # Show official batting stats by team
        official_batting = result['official_batting']
        official_pitching = result['official_pitching']

        print(f"\nOFFICIAL BATTING STATS:")
        print("AWAY TEAM:")
        away_batting = official_batting[official_batting['team'] == 'away']
        if not away_batting.empty:
            print(away_batting.to_string(index=False))
        else:
            print("  No away team batting data")

        print("\nHOME TEAM:")
        home_batting = official_batting[official_batting['team'] == 'home']
        if not home_batting.empty:
            print(home_batting.to_string(index=False))
        else:
            print("  No home team batting data")

        print(f"\nOFFICIAL PITCHING STATS:")
        print("AWAY TEAM:")
        away_pitching = official_pitching[official_pitching['team'] == 'away']
        if not away_pitching.empty:
            print(away_pitching.to_string(index=False))
        else:
            print("  No away team pitching data")

        print("\nHOME TEAM:")
        home_pitching = official_pitching[official_pitching['team'] == 'home']
        if not home_pitching.empty:
            print(home_pitching.to_string(index=False))
        else:
            print("  No home team pitching data")

if __name__ == "__main__":
    #test_complete_appearances()
    test_results = process_game_appearances("https://www.baseball-reference.com/boxes/KCA/KCA202503290.shtml")
    print(test_results['official_batting'])
    print(f"\n{test_results['batting_appearances']}")
