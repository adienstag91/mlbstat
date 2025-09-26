"""
Game Metadata Extraction
================================
"""

import re
import pandas as pd
from bs4 import BeautifulSoup
from typing import Dict, Optional, List
from datetime import datetime
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.url_cacher import HighPerformancePageFetcher
fetcher = HighPerformancePageFetcher(max_cache_size_mb=500)
from parsing.parsing_utils import extract_game_id


def extract_game_metadata(soup: BeautifulSoup, game_url: str) -> Dict:
    """Extract metadata using HTML structure, not regex patterns"""
    
    scorebox = soup.find('div', class_='scorebox')
    if not scorebox:
        return {
        'game_id': extract_game_id(game_url),
        'game_url': game_url,
        'home_team': None, 'away_team': None,
        'runs_home_team': None, 'runs_away_team': None, 'winner': None,
        'date': None, 'game_time': None, 'venue': None,
        'is_playoff': False, 'playoff_round': None,
        'player_positions': {}, 'player_profile_urls': {}
    }
    
    metadata = {
        'game_id': extract_game_id(game_url),
        'game_url': game_url,
        
        # From scorebox structure
        'home_team': get_team_from_scorebox(scorebox, position='home'),
        'away_team': get_team_from_scorebox(scorebox, position='away'),
        'runs_home_team': get_score_from_scorebox(scorebox, position='home'),
        'runs_away_team': get_score_from_scorebox(scorebox, position='away'),
        
        # From structured meta elements
        'venue': get_venue_from_structure(scorebox),
        'date': get_date_from_structure(scorebox),
        'game_time': get_time_from_structure(scorebox),
        
        # Simple game type detection
        'is_playoff': is_playoff_game(soup),
        'playoff_round': get_playoff_round(soup),

        # Game length info
        'innings_played': get_innings_played(soup),
    }
    
    # Calculate winner
    if metadata['runs_home_team'] is not None and metadata['runs_away_team'] is not None:
        if metadata['runs_home_team'] > metadata['runs_away_team']:
            metadata['winner'] = metadata['home_team']
        elif metadata['runs_away_team'] > metadata['runs_home_team']:
            metadata['winner'] = metadata['away_team']
        else:
            metadata['winner'] = 'Tie'
    else:
        metadata['winner'] = None
    
    return metadata

def get_team_from_scorebox(scorebox: BeautifulSoup, position: str) -> Optional[str]:
    """Get team from scorebox links"""
    team_links = scorebox.find_all('a', href=re.compile(r'/teams/[A-Z]{3}/'))
    
    if len(team_links) >= 2:
        if position == 'away':
            link = team_links[0]
        else:  # home
            link = team_links[1]
        
        href = link.get('href', '')
        match = re.search(r'/teams/([A-Z]{3})/', href)
        return match.group(1) if match else None
    
    return None

def get_score_from_scorebox(scorebox: BeautifulSoup, position: str) -> Optional[int]:
    """Get score from scorebox structure"""
    score_divs = scorebox.find_all('div', class_='score')
    
    if len(score_divs) >= 2:
        try:
            if position == 'away':
                return int(score_divs[0].get_text().strip())
            else:  # home
                return int(score_divs[1].get_text().strip())
        except (ValueError, IndexError):
            pass
    
    return None

def get_venue_from_structure(scorebox: BeautifulSoup) -> Optional[str]:
    """
    Get venue using HTML structure:
    <strong>Venue</strong>": Yankee Stadium III"
    """
    
    # Find the <strong>Venue</strong> element
    venue_strong = scorebox.find('strong', string='Venue')
    
    if venue_strong:
        # Get the text that follows the <strong> tag
        # It should be something like ": Yankee Stadium III"
        next_text = venue_strong.next_sibling
        
        if next_text and isinstance(next_text, str):
            # Remove the leading ": " and return clean venue name
            venue = next_text.strip().lstrip(':').strip()
            return venue if venue else None
    
    return None

def get_date_from_structure(scorebox: BeautifulSoup) -> Optional[str]:
    """
    Get date from scorebox_meta structure
    HTML: <div>Wednesday, October 30, 2024</div>
    """
    
    scorebox_meta = scorebox.find('div', class_='scorebox_meta')
    if not scorebox_meta:
        return None
    
    # Find the div that contains a date pattern (day, month date, year)
    divs = scorebox_meta.find_all('div')
    
    for div in divs:
        div_text = div.get_text().strip()
        
        # Look for date pattern like "Wednesday, October 30, 2024"
        # Try to parse as date
        try:
            # Remove day of week if present
            date_part = div_text
            if ',' in date_part:
                # Split by comma and try the second part (after day of week)
                parts = date_part.split(',', 1)
                if len(parts) > 1:
                    date_part = parts[1].strip()
                
                # Try to parse the date
                date_obj = datetime.strptime(date_part, '%B %d, %Y')
                return date_obj.strftime('%Y-%m-%d')
            else:
                # Try direct parsing
                date_obj = datetime.strptime(date_part, '%B %d, %Y')
                return date_obj.strftime('%Y-%m-%d')
                
        except ValueError:
            # Not a date, continue to next div
            continue
    
    return None

def get_time_from_structure(scorebox: BeautifulSoup) -> Optional[str]:
    """
    Get time from scorebox_meta structure  
    HTML: <div>Start Time: 8:08 p.m. Local</div>
    """
    
    scorebox_meta = scorebox.find('div', class_='scorebox_meta')
    if not scorebox_meta:
        return None
    
    # Find the div that contains "Start Time:"
    divs = scorebox_meta.find_all('div')
    
    for div in divs:
        div_text = div.get_text().strip()
        
        # Look for "Start Time:" pattern
        if 'Start Time:' in div_text:
            # Extract time part after "Start Time:"
            time_part = div_text.replace('Start Time:', '').strip()
            
            # Clean up the time format
            # "8:08 p.m. Local" -> "8:08 PM"
            time_part = time_part.replace(' Local', '').replace('.', '').upper()
            time_part = time_part.replace('P.M', 'PM').replace('A.M', 'AM')
            
            return time_part if time_part else None
    
    return None

def get_innings_played(soup: BeautifulSoup) -> Optional[int]:
    """
    Get total innings played from line score table
    Handles: 9 innings (normal), extra innings (10+), shortened games (5-8)
    """
    
    # Find the line score table (shows inning-by-inning scoring)
    line_score = soup.find('table', {'class': 'linescore'}) or soup.find('table', {'id': 'line_score'})
    
    if line_score:
        try:
            # Get the header row to count innings
            header_row = line_score.find('thead')
            if header_row:
                # Count inning columns (skip team name, R, H, E columns)
                th_elements = header_row.find_all('th')
                
                inning_count = 0
                for th in th_elements:
                    th_text = th.get_text().strip()
                    
                    # Inning columns are typically numbered: "1", "2", "3", etc.
                    if th_text.isdigit():
                        inning_num = int(th_text)
                        inning_count = max(inning_count, inning_num)
                
                return inning_count if inning_count > 0 else None
            
            # Fallback: count columns in the first data row
            first_row = line_score.find('tbody')
            if first_row:
                tr = first_row.find('tr')
                if tr:
                    td_elements = tr.find_all('td')
                    
                    # Count numeric columns (excluding team, R, H, E)
                    inning_count = 0
                    for td in td_elements:
                        td_text = td.get_text().strip()
                        
                        # Skip non-numeric columns
                        if td_text.isdigit() or td_text == 'X' or td_text == '-':
                            # This might be an inning score
                            continue
                        elif td.get('class') and 'team' in str(td.get('class')):
                            # Skip team name column
                            continue
                    
                    # Alternative approach: count all td elements and subtract known columns
                    # Line score typically has: Team | 1 | 2 | 3 | ... | R | H | E
                    total_columns = len(td_elements)
                    
                    if total_columns >= 4:  # At least team + 1 inning + R + H + E
                        # Subtract team name (1) + R, H, E (3) = 4
                        inning_count = total_columns - 4
                        return inning_count if inning_count > 0 else None
        
        except Exception:
            pass
    
    # Fallback: look for extra inning indicators in page text
    page_text = soup.get_text().lower()
    
    # Look for explicit mentions of innings
    for innings in range(15, 9, -1):  # Check 15 down to 10 innings
        if f'{innings} inning' in page_text or f'{innings}th inning' in page_text:
            return innings
    
    # Default to 9 innings if we can't determine otherwise
    return 9

def is_shortened_game(soup: BeautifulSoup, innings_played: int) -> bool:
    """
    Determine if this was a shortened game (weather, doubleheader rule, etc.)
    """
    
    if innings_played and innings_played < 9:
        return True
    
    # Look for weather/rain delay indicators
    page_text = soup.get_text().lower()
    weather_indicators = ['rain', 'weather', 'suspended', 'called', 'postponed']
    
    if any(indicator in page_text for indicator in weather_indicators):
        return True
    
    return False

def is_extra_innings_game(innings_played: int) -> bool:
    """Simple check for extra innings"""
    return innings_played and innings_played > 9

def is_playoff_game(soup: BeautifulSoup) -> bool:
    """Simple playoff detection from page title"""
    
    title = soup.find('title')
    if title:
        title_text = title.get_text().lower()
        playoff_keywords = ['playoff', 'wild card', 'division series', 'championship series', 'world series']
        return any(keyword in title_text for keyword in playoff_keywords)
    
    return False

def get_playoff_round(soup: BeautifulSoup) -> Optional[str]:
    """Get playoff round from page title"""
    
    if not is_playoff_game(soup):
        return None
    
    title = soup.find('title')
    if title:
        title_text = title.get_text().lower()
        
        if 'wild card' in title_text:
            return 'Wild Card'
        elif 'division series' in title_text or 'alds' in title_text or 'nlds' in title_text:
            return 'Division Series'
        elif 'championship series' in title_text or 'alcs' in title_text or 'nlcs' in title_text:
            return 'Championship Series'
        elif 'world series' in title_text:
            return 'World Series'
    
    return 'Playoff'

if __name__ == "__main__":
    game_url = "https://www.baseball-reference.com/boxes/LAN/LAN202410250.shtml"
    soup = fetcher.fetch_page(game_url)
    test_meta_data = extract_game_metadata(soup,game_url)
    print(f"Game ID: {test_meta_data['game_id']}")
    print(f"Game Date: {test_meta_data['date']}")
    print(f"Game Time: {test_meta_data['game_time']}")
    print(f"Venue: {test_meta_data['venue']}")
    print(f"Home Team: {test_meta_data['home_team']}")
    print(f"Away Team: {test_meta_data['away_team']}")
    print(f"Innings Played: {test_meta_data['innings_played']}")
    print(f"Is Playoff? {test_meta_data['is_playoff']}")
    print(f"Playoff Round: {test_meta_data['playoff_round']}")
    print(f"{test_meta_data['home_team']} Runs: {test_meta_data['runs_home_team']}")
    print(f"{test_meta_data['away_team']} Runs: {test_meta_data['runs_away_team']}")
    print(f"Winner: {test_meta_data['winner']}")


