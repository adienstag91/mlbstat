"""
Refactored Player Appearances Parser - Separate Tables Approach
=============================================================

Returns separate DataFrames for batting and pitching appearances.
Handles Ohtani-style two-way players without data loss or overwrites.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import re
import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO
from typing import Tuple, Dict
import time

from utils.url_cacher import HighPerformancePageFetcher
fetcher = HighPerformancePageFetcher(max_cache_size_mb=500)

from parsing.parsing_utils import (
    extract_game_id, safe_int, extract_from_details, normalize_name, 
    extract_name_and_positions, check_html_indentation, extract_player_id, 
    extract_pitcher_decisions
)

def determine_batting_order_and_starter_status(positions: list, is_indented: bool, 
                                             pa: int, current_batting_order: int) -> dict:
    """
    Determine batting order and starter status for a player
    
    Args:
        positions: List of position codes ['3B', 'SS']
        is_indented: Whether HTML row is indented (indicates substitute)
        pa: Plate appearances
        current_batting_order: Current batting order counter
        
    Returns:
        Dict with batting_order, is_starter, is_substitute
    """
    is_pinch_runner = 'PR' in positions
    is_pinch_hitter = 'PH' in positions
    
    # Substitute with plate appearances - inherit batting order
    if (is_pinch_runner or is_pinch_hitter or is_indented) and pa > 0:
        return {
            'batting_order': max(1, current_batting_order - 1),  # Inherit previous
            'is_starter': False,
            'is_substitute': True
        }
    
    # Substitute without plate appearances - no batting order
    elif (is_pinch_runner or is_pinch_hitter or is_indented) and pa == 0:
        return {
            'batting_order': None,
            'is_starter': False,
            'is_substitute': True,
        }
    
    # Regular starter
    else:
        return {
            'batting_order': current_batting_order,
            'is_starter': True,
            'is_substitute': False,
        }

def parse_batting_appearances(soup: BeautifulSoup, game_id: str) -> pd.DataFrame:
    """
    Parse batting appearances - clean DataFrame with only batting data
    
    Args:
        soup: BeautifulSoup object of game page
        game_id: Game identifier
        
    Returns:
        DataFrame with batting appearances only (no pitching nulls)
    """
    
    batting_tables = soup.find_all('table', {'id': lambda x: x and x.endswith('batting')})
    all_batting_data = []
    
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
            
            current_batting_order = 1
            
            for row_idx, row in df.iterrows():
                raw_batting_entry = str(row['Batting'])
                
                # Extract player info using modular functions
                clean_name, positions = extract_name_and_positions(raw_batting_entry)
                
                # Get corresponding HTML row
                html_row = data_rows[row_idx] if row_idx < len(data_rows) else None
                player_id = extract_player_id(html_row.find_all(['td', 'th'])[0] if html_row else None)
                is_indented = check_html_indentation(html_row)
                
                # Skip pitchers entirely from batting appearances
                is_pitcher = 'P' in positions or bool(re.search(r'\s+P\s*$', raw_batting_entry))
                if is_pitcher:
                    continue
                
                # Get stats
                pa = safe_int(row.get('PA', 0))
                ab = safe_int(row.get('AB', 0))
                
                # Determine starter status using modular function
                appearance_info = determine_batting_order_and_starter_status(
                    positions, is_indented, pa, current_batting_order
                )
                
                # Update batting order counter for next player
                if appearance_info['is_starter']:
                    current_batting_order += 1
                
                # Build batting record (ONLY batting fields)
                batting_record = {
                    # Identifiers
                    'game_id': game_id,
                    'player_id': player_id,
                    'player_name': clean_name,
                    'team': team,
                    
                    # Batting appearance metadata
                    'batting_order': appearance_info['batting_order'],
                    'positions_played': ','.join(positions) if positions else '',
                    'is_starter': appearance_info['is_starter'],
                    'is_substitute': appearance_info['is_substitute'],
                    
                    # Batting statistics only
                    'PA': pa,
                    'AB': ab,
                    'H': safe_int(row.get('H', 0)),
                    'R': safe_int(row.get('R', 0)),
                    'RBI': safe_int(row.get('RBI', 0)),
                    'BB': safe_int(row.get('BB', 0)),
                    'SO': safe_int(row.get('SO', 0)),
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
                
                all_batting_data.append(batting_record)
                
        except Exception as e:
            print(f"Error parsing batting table {table_idx}: {e}")
            continue
    
    return pd.DataFrame(all_batting_data)

def determine_pitching_role_and_decisions(decisions: list, pitching_order: int) -> dict:
    """
    Determine pitching role and format decisions
    
    Args:
        decisions: List of decisions like ['W', 'S']
        pitching_order: Order pitcher appeared (1 = starter)
        
    Returns:
        Dict with role information
    """
    return {
        'is_starter': pitching_order == 1,
        'pitching_order': pitching_order,
        'decisions': ','.join(decisions) if decisions else '',
    }

def parse_pitching_appearances(soup: BeautifulSoup, game_id: str) -> pd.DataFrame:
    """
    Parse pitching appearances - clean DataFrame with only pitching data
    
    Args:
        soup: BeautifulSoup object of game page
        game_id: Game identifier
        
    Returns:
        DataFrame with pitching appearances only (no batting nulls)
    """
    
    pitching_tables = soup.find_all('table', {'id': lambda x: x and x.endswith('pitching')})
    all_pitching_data = []
    
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
                
                # Extract pitcher name and decisions using modular function
                pitcher_name, decisions = extract_pitcher_decisions(raw_pitching_entry)
                
                # Get player ID from HTML
                html_row = data_rows[row_idx] if row_idx < len(data_rows) else None
                player_id = extract_player_id(html_row.find_all(['td', 'th'])[0] if html_row else None)
                
                # Determine role using modular function
                role_info = determine_pitching_role_and_decisions(decisions, row_idx + 1)
                
                # Build pitching record (ONLY pitching fields)
                pitching_record = {
                    # Identifiers
                    'game_id': game_id,
                    'player_id': player_id,
                    'player_name': pitcher_name,
                    'team': team,
                    
                    # Pitching appearance metadata
                    'is_starter': role_info['is_starter'],
                    'pitching_order': role_info['pitching_order'],
                    'decisions': role_info['decisions'],
                    
                    # Pitching statistics only
                    'BF': safe_int(row.get('BF', 0)),
                    'H_allowed': safe_int(row.get('H', 0)),
                    'R_allowed': safe_int(row.get('R', 0)),
                    'ER': safe_int(row.get('ER', 0)),
                    'BB_allowed': safe_int(row.get('BB', 0)),
                    'SO_pitched': safe_int(row.get('SO', 0)),
                    'HR_allowed': safe_int(row.get('HR', 0)),
                    'IP': float(row.get('IP', 0.0)) if row.get('IP') else 0.0,
                    'pitches_thrown': safe_int(row.get('Pit', 0)),
                }
                
                all_pitching_data.append(pitching_record)
                
        except Exception as e:
            print(f"Error parsing pitching table {table_idx}: {e}")
            continue
    
    return pd.DataFrame(all_pitching_data)

def process_game_appearances(game_url: str) -> Dict:
    """
    Process a single game to extract all appearance data
    
    Returns:
        Dict with separate batting and pitching DataFrames
    """
    start_time = time.time()
    
    # Fetch page
    soup = fetcher.fetch_page(game_url)
    game_id = extract_game_id(game_url)
    
    # Parse batting and pitching separately
    batting_appearances = parse_batting_appearances(soup, game_id)
    pitching_appearances = parse_pitching_appearances(soup, game_id)
    
    processing_time = time.time() - start_time
    
    return {
        'game_id': game_id,
        'batting_appearances': batting_appearances,
        'pitching_appearances': pitching_appearances,
        'processing_time': processing_time,
    }

def get_batting_stats_for_validation(batting_df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract batting stats formatted for validation against play-by-play
    
    Args:
        batting_df: DataFrame from parse_batting_appearances()
        
    Returns:
        DataFrame formatted for validation functions
    """
    if batting_df.empty:
        return pd.DataFrame()
    
    # Rename columns to match validation expectations
    validation_df = batting_df.copy()
    validation_df = validation_df.rename(columns={
        'player_name': 'player_name'  # Keep as is
    })
    
    # Select only columns needed for validation
    validation_columns = [
        'player_id', 'player_name', 'team', 'AB', 'H', 'BB', 'SO', 
        'PA', 'R', 'RBI', 'HR', '2B', '3B', 'SB', 'CS', 'HBP', 'GDP', 'SF', 'SH'
    ]
    
    available_columns = [col for col in validation_columns if col in validation_df.columns]
    return validation_df[available_columns]

def get_pitching_stats_for_validation(pitching_df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract pitching stats formatted for validation against play-by-play
    
    Args:
        pitching_df: DataFrame from parse_pitching_appearances()
        
    Returns:
        DataFrame formatted for validation functions
    """
    if pitching_df.empty:
        return pd.DataFrame()
    
    # Rename columns to match validation expectations
    validation_df = pitching_df.copy()
    validation_df = validation_df.rename(columns={
        'player_name': 'pitcher_name',
        'H_allowed': 'H',
        'BB_allowed': 'BB',
        'SO_pitched': 'SO',
        'HR_allowed': 'HR',
        'pitches_thrown': 'PC'
    })
    
    # Select only columns needed for validation
    validation_columns = [
        'player_id', 'pitcher_name', 'team', 'BF', 'H', 'BB', 'SO', 'HR', 'PC'
    ]
    
    available_columns = [col for col in validation_columns if col in validation_df.columns]
    return validation_df[available_columns]

def test_ohtani_scenario():
    """Test handling of two-way players like Ohtani"""
    
    print("Testing Ohtani-style two-way player scenario...")
    print("=" * 60)
    
    # This would be a game where Ohtani both pitches and hits
    test_url = "https://www.baseball-reference.com/boxes/LAN/LAN202509160.shtml"
    
    result = process_game_appearances(test_url)
    
    batting_df = result['batting_appearances']
    pitching_df = result['pitching_appearances']
    
    print(f"Game: {result['game_id']}")
    print(f"Batting appearances: {len(batting_df)}")
    print(f"Pitching appearances: {len(pitching_df)}")
    
    # Check for any players who appear in both
    if not batting_df.empty and not pitching_df.empty:
        batting_players = set(batting_df['player_id'].dropna())
        pitching_players = set(pitching_df['player_id'].dropna())
        two_way_players = batting_players.intersection(pitching_players)
        
        print(f"Two-way players in this game: {len(two_way_players)}")
        for player_id in two_way_players:
            batting_row = batting_df[batting_df['player_id'] == player_id].iloc[0]
            pitching_row = pitching_df[pitching_df['player_id'] == player_id].iloc[0]
            
            print(f"\n{player_id} ({batting_row['player_name']}):")
            print(f"  Batting: {batting_row['PA']} PA, {batting_row['AB']} AB, {batting_row['H']} H")
            print(f"  Pitching: {pitching_row['BF']} BF, {pitching_row['H_allowed']} H allowed, {pitching_row['SO_pitched']} SO")
    
    # Show validation DataFrames
    batting_validation = get_batting_stats_for_validation(batting_df)
    pitching_validation = get_pitching_stats_for_validation(pitching_df)
    
    print(f"\nValidation DataFrames ready:")
    print(f"  Batting validation: {len(batting_validation)} records")
    print(f"  Pitching validation: {len(pitching_validation)} records")
    
    return result

def test_refactored_appearances(test_url):
    """Test the refactored separate tables approach"""
    
    print(f"Testing refactored parser: {test_url}")
    print("=" * 60)
    
    # Process game
    result = process_game_appearances(test_url)
    
    batting_df = result['batting_appearances']
    pitching_df = result['pitching_appearances']
    
    print(f"Game: {result['game_id']}")
    print(f"Processing time: {result['processing_time']:.2f}s")
    
    # Show batting appearances
    if not batting_df.empty:
        print(f"\nBATTING APPEARANCES ({len(batting_df)}):")
        print(f"Columns: {list(batting_df.columns)}")
        print(batting_df[['player_name', 'team', 'batting_order', 'positions_played', 
                         'is_starter', 'PA', 'AB', 'H', 'HR']])
    
    # Show pitching appearances  
    if not pitching_df.empty:
        print(f"\nPITCHING APPEARANCES ({len(pitching_df)}):")
        print(f"Columns: {list(pitching_df.columns)}")
        print(pitching_df[['player_name', 'team', 'is_starter', 'decisions', 
                          'BF', 'H_allowed', 'BB_allowed', 'SO_pitched']])
    
    return result

if __name__ == "__main__":
    # Test both scenarios
    test_url = "https://www.baseball-reference.com/boxes/LAN/LAN202509160.shtml"
    test_refactored_appearances(test_url)
    print("\n" + "="*60 + "\n")
    test_ohtani_scenario()