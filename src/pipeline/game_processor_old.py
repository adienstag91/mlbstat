"""
Main game processing pipeline
============================
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.url_cacher import HighPerformancePageFetcher, SimpleFetcher
fetcher = SimpleFetcher()
from parsing.events_parser import parse_play_by_play_events
from parsing.appearances_parser import (
    parse_batting_appearances, parse_pitching_appearances,
    get_batting_stats_for_validation, get_pitching_stats_for_validation
)
from validation.stat_validator import validate_batting_stats, validate_pitching_stats
from parsing.parsing_utils import *
from bs4 import BeautifulSoup
from io import StringIO
import pandas as pd
from typing import Dict, List, Optional
import time
import sys

def process_single_game(game_url: str, display_results: bool = True) -> Dict:
    """Process a complete game into events from the play-by-play and official stats from the box score"""
    start_time = time.time()

    soup = fetcher.fetch_page(game_url)
    
    game_id = extract_game_id(game_url)
    
    # Parse appearances
    batting_appearances = parse_batting_appearances(soup, game_id)
    pitching_appearances = parse_pitching_appearances(soup, game_id)

    # Convert to validation format
    official_batting = get_batting_stats_for_validation(batting_appearances)
    official_pitching = get_pitching_stats_for_validation(pitching_appearances)
    
    # Parse events
    pbp_events = parse_play_by_play_events(soup, game_id)
        
    batting_validation = validate_batting_stats(official_batting, pbp_events)
    pitching_validation = validate_pitching_stats(official_pitching, pbp_events)

    time_to_process = time.time() - start_time

    if display_results:
        print(f"✅ {game_id}: Batting {batting_validation['accuracy']:.1f}%, Pitching: {pitching_validation['accuracy']:.1f}%")
        print(f"⏱️ Processing Time: {time_to_process:.2f}s\n")
        print("Batting Box Score:")
        print(f"{official_batting}\n")
        print("Pitching Box Score:")
        print(f"{official_pitching}\n")
        print("Play By Play Events Table:")
        print(f"{pbp_events}\n")
        if batting_validation['differences']:
            print("Batting Differences:")
            print(batting_validation['differences'])
        if pitching_validation['differences']:
            print("Pitching Differences:")
            print(pitching_validation['differences'])
        if batting_validation['name_mismatches']['unmatched_official_names']:
            print("Name Mismatches:")
            print(batting_validation['name_mismatches'])

    
    return {
        'game_id': game_id,
        'time_to_process': time_to_process,
        'official_batting': official_batting,
        'official_pitching': official_pitching,
        'pbp_events': pbp_events,
        'batting_validation': batting_validation,
        'pitching_validation': pitching_validation
    }

def process_multiple_games(game_urls: List[str]) -> List[Dict]:
    """Process multiple games with error handling"""
    results = []
    
    for i, url in enumerate(game_urls):
        try:
            print(f"Processing game {i+1}/{len(game_urls)}: {url}")
            result = process_single_game(url)
            results.append(result)
        except Exception as e:
            print(f"❌ Failed to process {url}: {e}")
    
    return results

if __name__ == "__main__":
    test_urls = [
        "https://www.baseball-reference.com/boxes/KCA/KCA202503290.shtml",
        "https://www.baseball-reference.com//boxes/NYA/NYA202505050.shtml",
        "https://www.baseball-reference.com//boxes/TEX/TEX202505180.shtml",
        "https://www.baseball-reference.com//boxes/CLE/CLE202505300.shtml",
        "https://www.baseball-reference.com//boxes/ANA/ANA202506230.shtml",
        "https://www.baseball-reference.com//boxes/ATL/ATL202506270.shtml",
        # Add more test URLs here
    ]
    if len(sys.argv) > 1 and sys.argv[1] == "multi":
        process_multiple_games(test_urls)
    else:
        process_single_game(test_urls[0])