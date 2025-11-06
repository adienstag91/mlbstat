#!/usr/bin/env python3
"""
Player Biographical Data Parser
===============================

Fetches stable player data from Baseball Reference player pages.
Only called for new players not in database.
"""

import re
from bs4 import BeautifulSoup
from typing import Dict, Optional
from datetime import datetime
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import sqlite3
from utils.url_cacher import SimpleFetcher

def parse_player_bio(player_id: str, fetcher) -> Dict[str, any]:
    """Parse biographical data from player page"""

    player_url = f"https://www.baseball-reference.com/players/{player_id[0]}/{player_id}.shtml"

    try:
        soup = fetcher.fetch_page(player_url)

        return {
            'player_id': player_id,
            'full_name': get_player_name(soup),
            'bats': get_bats(soup),
            'throws': get_throws(soup),
            'birth_date': get_birth_date(soup),
            'debut_date': get_debut_date(soup),
            'height_inches': get_height_inches(soup),
            'weight_lbs': get_weight_lbs(soup)
        }
    except Exception as e:
        return {
            'player_id': player_id,
            'error': str(e)
        }

def get_player_name(soup: BeautifulSoup) -> Optional[str]:
    """Extract player's full name from page"""
    # Name is in <h1><span>Player Name</span></h1>
    h1 = soup.find('h1')
    if h1:
        span = h1.find('span')
        if span:
            return span.get_text().strip()
    return None

def get_bats(soup: BeautifulSoup) -> Optional[str]:
    """
    Extract batting handedness (L/R/S for switch)
    HTML: <strong>Bats:</strong> Right
    """
    bats_strong = soup.find('strong', string=re.compile(r'Bats?:'))
    if bats_strong:
        # Get next text after the <strong> tag
        bats_text = bats_strong.next_sibling
        if bats_text and isinstance(bats_text, str):
            bats_text = bats_text.strip()
            if 'Right' in bats_text:
                return 'R'
            elif 'Left' in bats_text:
                return 'L'
            elif 'Switch' or 'Both' in bats_text:
                return 'S'
    return None

def get_throws(soup: BeautifulSoup) -> Optional[str]:
    """
    Extract throwing handedness (L/R)
    HTML: <strong>Throws:</strong> Right
    """
    throws_strong = soup.find('strong', string=re.compile(r'Throws?:'))
    if throws_strong:
        throws_text = throws_strong.next_sibling
        if throws_text and isinstance(throws_text, str):
            throws_text = throws_text.strip()
            if 'Right' in throws_text:
                return 'R'
            elif 'Left' in throws_text:
                return 'L'
    return None

def get_birth_date(soup: BeautifulSoup) -> Optional[str]:
    """
    Extract birth date
    HTML: <span id="necro-birth" data-birth="1992-04-26">April 26, 1992</span>
    """
    # First try the data attribute (most reliable)
    necro_birth = soup.find('span', id='necro-birth')
    if necro_birth and necro_birth.get('data-birth'):
        return necro_birth.get('data-birth')
    
    # Fallback: look for "Born:" pattern
    born_strong = soup.find('strong', string='Born:')
    if born_strong:
        # Try to find nearby date
        parent = born_strong.parent
        if parent:
            text = parent.get_text()
            # Look for date pattern like "April 26, 1992"
            date_match = re.search(r'([A-Z][a-z]+\s+\d{1,2},\s+\d{4})', text)
            if date_match:
                try:
                    date_obj = datetime.strptime(date_match.group(1), '%B %d, %Y')
                    return date_obj.strftime('%Y-%m-%d')
                except ValueError:
                    pass
    
    return None

def get_debut_date(soup: BeautifulSoup) -> Optional[str]:
    """
    Extract MLB debut date
    HTML: <strong>Debut:</strong> April 1, 2018
    """
    debut_strong = soup.find('strong', string=re.compile(r'(MLB )?Debut:'))
    if debut_strong:
        parent = debut_strong.parent
        if parent:
            text = parent.get_text()
            # Look for date pattern
            date_match = re.search(r'([A-Z][a-z]+\s+\d{1,2},\s+\d{4})', text)
            if date_match:
                try:
                    date_obj = datetime.strptime(date_match.group(1), '%B %d, %Y')
                    return date_obj.strftime('%Y-%m-%d')
                except ValueError:
                    pass
    
    return None

def get_height_inches(soup: BeautifulSoup) -> Optional[int]:
    """
    Extract height in inches
    HTML: <span>6-7</span> (6 feet 7 inches)
    """
    # Look for height pattern near "Height:" or in meta description
    meta = soup.find('div', id='meta')
    if meta:
        text = meta.get_text()
        # Pattern: 6-7 or 6' 7" or similar
        height_match = re.search(r'(\d+)[\'\\-]\s*(\d+)', text)
        if height_match:
            feet = int(height_match.group(1))
            inches = int(height_match.group(2))
            return feet * 12 + inches
    
    return None

def get_weight_lbs(soup: BeautifulSoup) -> Optional[int]:
    """
    Extract weight in pounds
    HTML: <span>282lb</span>
    """
    meta = soup.find('div', id='meta')
    if meta:
        text = meta.get_text()
        # Pattern: 282lb or 282 lb
        weight_match = re.search(r'(\d{2,3})\s*lb', text)
        if weight_match:
            return int(weight_match.group(1))
    
    return None

# Database integration functions
def fetch_player_bio_if_needed(player_id: str, player_name: str, db_path: str, 
                               fetcher) -> Dict[str, any]:
    """
    Check if player exists in database. If not, fetch bio data.
    
    Args:
        player_id: Baseball Reference player ID
        player_name: Player's full name
        db_path: Path to SQLite database
        fetcher: Page fetcher object (HighPerformancePageFetcher)
        
    Returns:
        Dictionary with player bio data (from DB or freshly parsed)
    """
    # Check if player exists in database
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT player_id, full_name, bats, throws, birth_date, 
                   debut_date, height_inches, weight_lbs
            FROM players 
            WHERE player_id = ?
        """, (player_id,))
        
        existing = cursor.fetchone()
    
    if existing:
        # Player exists - return existing data
        return {
            'player_id': existing[0],
            'full_name': existing[1],
            'bats': existing[2],
            'throws': existing[3],
            'birth_date': existing[4],
            'debut_date': existing[5],
            'height_inches': existing[6],
            'weight_lbs': existing[7],
            'source': 'database'
        }
    else:
        # New player - fetch from web
        player_url = f"https://www.baseball-reference.com/players/{player_id[0]}/{player_id}.shtml"
        
        try:
            soup = fetcher.fetch_page(player_url)
            bio_data = parse_player_bio(soup, player_id)
            bio_data['source'] = 'web_fetch'
            return bio_data
        except Exception as e:
            # Return minimal data if fetch fails
            return {
                'player_id': player_id,
                'full_name': player_name,
                'bats': None,
                'throws': None,
                'birth_date': None,
                'debut_date': None,
                'height_inches': None,
                'weight_lbs': None,
                'source': 'error',
                'error': str(e)
            }

# Test function
if __name__ == "__main__":
    
    fetcher = SimpleFetcher()
    
    bio = parse_player_bio("sabatc.01", fetcher)
    
    print("Player Bio Data:")
    print(f"  Name: {bio['full_name']}")
    print(f"  Bats: {bio['bats']}")
    print(f"  Throws: {bio['throws']}")
    print(f"  Birth Date: {bio['birth_date']}")
    print(f"  Debut: {bio['debut_date']}")
    print(f"  Height: {bio['height_inches']} inches")
    print(f"  Weight: {bio['weight_lbs']} lbs")
