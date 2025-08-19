"""
Name resolution and normalization functions
==========================================
"""

import re
import unicodedata
import pandas as pd
from typing import Dict, Set
from bs4 import BeautifulSoup
from utils.mlb_cached_fetcher import SafePageFetcher

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

def extract_canonical_names(soup: BeautifulSoup) -> Set[str]:
    """Extract canonical names from box score tables"""
    names = set()
    for table_type in ['batting', 'pitching']:
        tables = soup.find_all('table', {'id': lambda x: x and table_type in x.lower()})
        for table in tables:
            for row in table.find_all('tr'):
                name_cell = row.find('th', {'data-stat': 'player'})
                if name_cell:
                    name_og = name_cell.get_text(strip=True)
                    print(name_og)
                    name = normalize_name(name_cell.get_text(strip=True))
                    print(name)
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

if __name__ == "__main__":
    game_url = "https://www.baseball-reference.com/boxes/KCA/KCA202503290.shtml"
    soup = SafePageFetcher.fetch_page(game_url)
    canonical_names = extract_canonical_names(soup)
    print(f"canonical_names: {canonical_names}")
    print("resolver")
    resolver = build_name_resolver(canonical_names)
    for row in resolver:
        player = resolver.get(row, row)
        print(player)
