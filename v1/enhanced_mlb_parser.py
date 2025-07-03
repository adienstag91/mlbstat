"""
Enhanced MLB Play-by-Play Parser with IMPROVED Runner Attribution
================================================================
Fixes for better baserunning stats and network reliability
"""

import pandas as pd
import numpy as np
import re
import time
import random
from bs4 import BeautifulSoup
from io import StringIO
from playwright.sync_api import sync_playwright
from dataclasses import dataclass
from typing import Dict, List, Set, Optional, Tuple
from collections import defaultdict

@dataclass
class PlayerStats:
    """Track individual player statistics"""
    name: str
    AB: int = 0  # At-bats
    H: int = 0   # Hits
    HR: int = 0  # Home runs
    RBI: int = 0 # Runs batted in
    R: int = 0   # Runs scored
    BB: int = 0  # Walks
    SO: int = 0  # Strikeouts
    SB: int = 0  # Stolen bases
    CS: int = 0  # Caught stealing
    doubles: int = 0  # Doubles (2B)
    triples: int = 0  # Triples (3B)
    HBP: int = 0     # Hit by pitch
    SF: int = 0      # Sacrifice flies

    def to_dict(self):
        return {
            'batter': self.name,
            'AB': self.AB,
            'H': self.H,
            'HR': self.HR,
            'RBI': self.RBI,
            'R': self.R,
            'BB': self.BB,
            'SO': self.SO,
            'SB': self.SB,
            'CS': self.CS,
            '2B': self.doubles,
            '3B': self.triples,
            'SF': self.SF
        }

class EnhancedGameState:
    """Enhanced game state tracking with better runner management"""
    
    def __init__(self):
        self.inning = 1
        self.half_inning = 'top'  # 'top' or 'bottom'
        self.outs = 0
        self.bases = {1: None, 2: None, 3: None}  # Base occupancy
        self.batting_team = 'away'  # 'home' or 'away'
        self.current_batter = None
        self.runners_on_base = {}  # Track which at-bat each runner came from
    
    def clear_bases(self):
        """Clear all bases"""
        self.bases = {1: None, 2: None, 3: None}
        self.runners_on_base = {}
    
    def advance_inning(self):
        """Move to next half-inning"""
        if self.half_inning == 'top':
            self.half_inning = 'bottom'
            self.batting_team = 'home'
        else:
            self.half_inning = 'top'
            self.batting_team = 'away'
            self.inning += 1
        self.outs = 0
        self.clear_bases()
    
    def add_runner_to_base(self, player, base, at_bat_context=None):
        """Add a runner to specified base with context"""
        if 1 <= base <= 3:
            self.bases[base] = player
            if at_bat_context:
                self.runners_on_base[player] = at_bat_context

class SimpleWorkingNameResolver:
    """Simple name resolver focused on the actual problem"""
    
    def __init__(self, canonical_names):
        self.canonical_names = list(canonical_names)
        self.name_mappings = {}
        self.cache = {}
        
        self._build_simple_mappings()
        print(f"🔧 Built {len(self.name_mappings)} name mappings")
        self._debug_mappings()
    
    def _build_simple_mappings(self):
        """Build simple initial-based mappings"""
        
        # Map each canonical name to itself
        for name in self.canonical_names:
            self.name_mappings[name] = name
        
        # For each canonical name, create abbreviated versions
        for canonical_name in self.canonical_names:
            if ' ' in canonical_name:
                # Handle regular "First Last" names
                parts = canonical_name.split(' ')
                if len(parts) >= 2:
                    first = parts[0]
                    last = ' '.join(parts[1:])  # Handle "Jr.", "III", etc.
                    
                    # Create abbreviated version "F. Last"
                    abbreviated = f"{first[0]}. {last}"
                    self.name_mappings[abbreviated] = canonical_name
                    
                    # Also handle without the period
                    abbreviated_no_period = f"{first[0]} {last}"
                    self.name_mappings[abbreviated_no_period] = canonical_name
        
        # Handle special cases manually if needed
        special_cases = {
            # Add any specific mappings that don't follow the pattern
        }
        self.name_mappings.update(special_cases)
    
    def _debug_mappings(self):
        """Show the created mappings"""
        print(f"\n🔍 NAME MAPPINGS:")
        
        # Show abbreviated -> full mappings
        abbrev_mappings = {k: v for k, v in self.name_mappings.items() 
                          if k != v and ('.' in k or len(k.split()) == 2)}
        
        for abbrev, full in sorted(abbrev_mappings.items())[:10]:
            print(f"   '{abbrev}' -> '{full}'")
        
        if len(abbrev_mappings) > 10:
            print(f"   ... and {len(abbrev_mappings) - 10} more mappings")
    
    def resolve_name(self, name):
        """Resolve any name to canonical form"""
        
        if not name or pd.isna(name):
            return name
        
        name_str = str(name).strip()
        
        # Check cache first
        if name_str in self.cache:
            return self.cache[name_str]
        
        # Direct mapping
        if name_str in self.name_mappings:
            result = self.name_mappings[name_str]
            self.cache[name_str] = result
            return result
        
        # Case-insensitive mapping
        for mapped_name, canonical in self.name_mappings.items():
            if name_str.lower() == mapped_name.lower():
                self.cache[name_str] = canonical
                return canonical
        
        # Clean and try again (remove extra whitespace, etc.)
        cleaned = re.sub(r'\s+', ' ', name_str).strip()
        if cleaned != name_str and cleaned in self.name_mappings:
            result = self.name_mappings[cleaned]
            self.cache[name_str] = result
            return result
        
        # If no mapping found, return original
        self.cache[name_str] = name_str
        return name_str

def test_simple_resolver():
    """Test the simple resolver on known problematic cases"""
    
    # Sample canonical names from your box scores
    canonical_names = [
        "Ben Rice", "Aaron Judge", "Jazz Chisholm Jr.", 
        "Joey Ortiz", "Oswaldo Cabrera", "Jake Bauers", "Trent Grisham"
    ]
    
    resolver = SimpleWorkingNameResolver(canonical_names)
    
    # Test cases that were causing issues
    test_cases = [
        "B. Rice",      # Should -> "Ben Rice"
        "Ben Rice",     # Should -> "Ben Rice" 
        "A. Judge",     # Should -> "Aaron Judge"
        "Aaron Judge",  # Should -> "Aaron Judge"
        "J. Chisholm",  # Should -> "Jazz Chisholm Jr."
        "Jazz Chisholm Jr.",  # Should -> "Jazz Chisholm Jr."
        "J. Ortiz",     # Should -> "Joey Ortiz"
        "O. Cabrera",   # Should -> "Oswaldo Cabrera"
        "J. Bauers",    # Should -> "Jake Bauers"
        "T. Grisham"    # Should -> "Trent Grisham"
    ]
    
    print(f"\n🧪 TESTING SIMPLE RESOLVER:")
    for test_name in test_cases:
        resolved = resolver.resolve_name(test_name)
        status = "✅" if resolved != test_name else "⚠️"
        print(f"   {status} '{test_name}' -> '{resolved}'")
    
    return resolver

def safe_fetch_page(url, max_retries=3, base_delay=5):
    """Safely fetch a page with exponential backoff"""
    
    for attempt in range(max_retries):
        try:
            delay = base_delay * (2 ** attempt) + random.uniform(1, 3)
            
            if attempt > 0:
                print(f"   🔄 Retry {attempt + 1}/{max_retries} after {delay:.1f}s delay...")
                time.sleep(delay)
            
            with sync_playwright() as p:
                browser = p.chromium.launch(
                    headless=True,
                    args=['--no-sandbox', '--disable-dev-shm-usage']
                )
                
                context = browser.new_context(
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                )
                
                page = context.new_page()
                
                # Set timeouts
                page.set_default_timeout(30000)  # 30 second timeout
                
                # Navigate and wait for content
                page.goto(url, wait_until='domcontentloaded')
                page.wait_for_timeout(3000)  # Wait for dynamic content
                
                content = page.content()
                browser.close()
                
                return BeautifulSoup(content, "html.parser")
                
        except Exception as e:
            print(f"   ❌ Attempt {attempt + 1} failed: {str(e)[:100]}...")
            if attempt == max_retries - 1:
                print(f"   💥 All {max_retries} attempts failed for {url}")
                raise e
    
    return None

def enhanced_parse_play_by_play(description, batter, game_state, name_resolver):
    """Enhanced play parser with better runner tracking"""
    
    if not description or pd.isna(description):
        return _create_empty_play_result()
    
    desc = str(description).strip()
    
    # Initialize result
    result = {
        "is_plate_appearance": True,
        "is_hit": False,
        "hit_type": None,
        "is_walk": False,
        "is_strikeout": False,
        "is_home_run": False,
        "is_hbp": False,
        "is_sacrifice": False,
        "out_type": None,
        "outs_on_play": 0,
        "rbi_count": 0,
        "bases_earned": 0,
        "baserunning_events": [],
        "scoring_events": []
    }
    
    # Check for pure baserunning plays
    baserunning_only_patterns = [
        r'^Stolen Base',
        r'^Caught Stealing',
        r'^Wild Pitch',
        r'^Passed Ball',
        r'^Balk',
        r'^Pickoff',
        r'Defensive Indifference',
        r'^[A-Z]\.\s*[A-Z][a-z]+\s+(?:Caught Stealing|steals)',
        r'^[A-Z][a-z]+\s+[A-Z][a-z]+\s+(?:Caught Stealing|steals)'
    ]
    
    for pattern in baserunning_only_patterns:
        if re.search(pattern, desc, re.IGNORECASE):
            result["is_plate_appearance"] = False
            result["baserunning_events"] = enhanced_parse_baserunning_events(desc, name_resolver)
            return result
    
    # Enhanced RBI counting with better context awareness
    def extract_enhanced_rbi_count(text):
        """More sophisticated RBI extraction"""
        
        # Explicit RBI mentions
        explicit_patterns = [
            r'(\d+)\s*RBI',
            r'(\d+)\s*runs?\s+(?:batted\s+in)',
        ]
        
        for pattern in explicit_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return int(match.group(1))
        
        # Count scoring events more carefully
        rbi_count = 0
        
        # Look for explicit scoring mentions
        scoring_patterns = [
            r'([A-Z][a-z]+ [A-Z][a-z]+)\s+(?:scores|Scores)',
            r'([A-Z]\.\s*[A-Z][a-z]+)\s+(?:scores|Scores)',
        ]
        
        for pattern in scoring_patterns:
            matches = re.findall(pattern, text)
            rbi_count += len(matches)
        
        # Handle home runs
        if re.search(r'(?:Home\s+Run|HR(?:\s|$))', text, re.IGNORECASE):
            if rbi_count == 0:
                # Infer from context
                if re.search(r'(?:grand\s+slam)', text, re.IGNORECASE):
                    return 4
                elif re.search(r'(?:3-run|three-run)', text, re.IGNORECASE):
                    return 3
                elif re.search(r'(?:2-run|two-run)', text, re.IGNORECASE):
                    return 2
                else:
                    return 1  # Solo home run
            else:
                return rbi_count + 1  # Add batter's run
        
        return rbi_count
    
    # Process play outcomes
    
    # 1. Home runs
    if re.search(r'(?:Home\s+Run|HR(?:\s|$))', desc, re.IGNORECASE):
        result.update({
            "is_hit": True,
            "is_home_run": True,
            "hit_type": "home_run",
            "bases_earned": 4
        })
        result["rbi_count"] = extract_enhanced_rbi_count(desc)
        
        # Batter scores on home run
        result["scoring_events"].append({
            "type": "run_scored",
            "player": batter,
            "context": "home_run_batter"
        })
    
    # 2. Other hits
    elif (re.search(r'(?:Single|Double|Triple)(?!\s+Play)', desc, re.IGNORECASE) and
          not re.search(r'(?:Double\s+Play|Triple\s+Play)', desc, re.IGNORECASE)):
        
        result["is_hit"] = True
        
        if re.search(r'Triple(?!\s+Play)', desc, re.IGNORECASE):
            result.update({"hit_type": "triple", "bases_earned": 3})
        elif re.search(r'Double(?!\s+Play)', desc, re.IGNORECASE):
            result.update({"hit_type": "double", "bases_earned": 2})
        elif re.search(r'Single', desc, re.IGNORECASE):
            result.update({"hit_type": "single", "bases_earned": 1})
        
        result["rbi_count"] = extract_enhanced_rbi_count(desc)
    
    # 3. Walks
    elif re.search(r'(?:Walk|BB|Ball\s+4|Base\s+on\s+Balls)', desc, re.IGNORECASE):
        result.update({
            "is_walk": True,
            "bases_earned": 1
        })
        result["rbi_count"] = extract_enhanced_rbi_count(desc)
    
    # 4. Strikeouts
    elif re.search(r'(?:Strikeout|Strike.*Out|SO)', desc, re.IGNORECASE):
        result.update({
            "is_strikeout": True,
            "outs_on_play": 1
        })
    
    # 5. Hit by pitch
    elif re.search(r'(?:Hit.*Pitch|HBP)', desc, re.IGNORECASE):
        result.update({
            "is_hbp": True,
            "bases_earned": 1
        })
        result["rbi_count"] = extract_enhanced_rbi_count(desc)
    
    # 6. Sacrifice flies (very restrictive)
    elif (re.search(r'(?:Sacrifice.*Fly|Sac.*Fly|SF)', desc, re.IGNORECASE) or
          re.search(r'/Sacrifice\s+Fly', desc, re.IGNORECASE)):
        result.update({
            "is_sacrifice": True,
            "out_type": "sacrifice_fly",
            "outs_on_play": 1
        })
        
        rbi_count = extract_enhanced_rbi_count(desc)
        result["rbi_count"] = rbi_count if rbi_count > 0 else 1
    
    # 7. Sacrifice bunts
    elif re.search(r'(?:^Sacrifice.*(?:Bunt|Hit)|^Sac.*(?:Bunt|Hit))', desc, re.IGNORECASE):
        result.update({
            "is_sacrifice": True,
            "out_type": "sacrifice_hit",
            "outs_on_play": 1
        })
        result["rbi_count"] = extract_enhanced_rbi_count(desc)
    
    # 8. Errors and fielder's choices
    elif re.search(r'(?:Error|Fielder.*Choice)', desc, re.IGNORECASE):
        result["bases_earned"] = 1
        result["rbi_count"] = extract_enhanced_rbi_count(desc)
        
        if re.search(r'(?:Double.*Play|DP)', desc, re.IGNORECASE):
            result["outs_on_play"] = 2
        elif re.search(r'(?:out)', desc, re.IGNORECASE):
            result["outs_on_play"] = 1
    
    # 9. All other outs
    elif re.search(r'(?:Groundout|Flyout|Flyball|Lineout|Popfly|Popout|Out|Grounded|Flied|Lined|Popped|Pop)', desc, re.IGNORECASE):
        if re.search(r'(?:Double.*Play|DP)', desc, re.IGNORECASE):
            result["outs_on_play"] = 2
        else:
            result["outs_on_play"] = 1
        
        result["rbi_count"] = extract_enhanced_rbi_count(desc)
    
    # Parse enhanced baserunning events
    result["baserunning_events"] = enhanced_parse_baserunning_events(desc, name_resolver)
    
    # Extract scoring events from baserunning
    for event in result["baserunning_events"]:
        if event["type"] == "run_scored":
            result["scoring_events"].append(event)
    
    return result

def enhanced_parse_baserunning_events(description, name_resolver):
    """Enhanced baserunning parser with better name extraction"""
    events = []
    
    if not description:
        return events
    
    # More comprehensive stolen base patterns
    sb_patterns = [
        r'Stolen Base.*?([A-Z][a-z]+ [A-Z][a-z]+|[A-Z]\.\s*[A-Z][a-z]+)',
        r'([A-Z][a-z]+ [A-Z][a-z]+|[A-Z]\.\s*[A-Z][a-z]+)\s+(?:steals|Steals)',
        r'SB:\s*([A-Z][a-z]+ [A-Z][a-z]+|[A-Z]\.\s*[A-Z][a-z]+)',
    ]
    
    for pattern in sb_patterns:
        for match in re.finditer(pattern, description):
            player_name = name_resolver.resolve_name(match.group(1).strip())
            events.append({
                "type": "stolen_base",
                "player": player_name,
                "context": description
            })
    
    # Enhanced caught stealing patterns
    cs_patterns = [
        r'Caught Stealing.*?([A-Z][a-z]+ [A-Z][a-z]+|[A-Z]\.\s*[A-Z][a-z]+)',
        r'([A-Z][a-z]+ [A-Z][a-z]+|[A-Z]\.\s*[A-Z][a-z]+)\s+(?:Caught Stealing|caught stealing)',
        r'CS:\s*([A-Z][a-z]+ [A-Z][a-z]+|[A-Z]\.\s*[A-Z][a-z]+)',
    ]
    
    for pattern in cs_patterns:
        for match in re.finditer(pattern, description):
            player_name = name_resolver.resolve_name(match.group(1).strip())
            events.append({
                "type": "caught_stealing",
                "player": player_name,
                "context": description
            })
    
    # Enhanced scoring patterns
    score_patterns = [
        r'([A-Z][a-z]+ [A-Z][a-z]+)\s+(?:scores|Scores)',
        r'([A-Z]\.\s*[A-Z][a-z]+)\s+(?:scores|Scores)',
        r'([A-Z][a-z]+ [A-Z][a-z]+)\s+(?:runs home|crosses the plate)',
    ]
    
    for pattern in score_patterns:
        for match in re.finditer(pattern, description):
            player_name = name_resolver.resolve_name(match.group(1).strip())
            events.append({
                "type": "run_scored",
                "player": player_name,
                "context": description
            })
    
    return events

def _create_empty_play_result():
    """Create an empty play result for error cases"""
    return {
        "is_plate_appearance": False,
        "is_hit": False,
        "hit_type": None,
        "is_walk": False,
        "is_strikeout": False,
        "is_home_run": False,
        "is_hbp": False,
        "is_sacrifice": False,
        "out_type": None,
        "outs_on_play": 0,
        "rbi_count": 0,
        "bases_earned": 0,
        "baserunning_events": [],
        "scoring_events": []
    }

def extract_enhanced_stats(url):
    """Enhanced stats extraction with improved reliability"""
    
    print(f"🔍 Fetching game data from {url}")
    
    # Safely fetch the page
    try:
        soup = safe_fetch_page(url)
    except Exception as e:
        print(f"❌ Failed to fetch page: {e}")
        return pd.DataFrame()

    # Extract canonical names
    print("📋 Extracting canonical names from box score...")
    canonical_names = extract_canonical_names(soup)
    print(f"✅ Found {len(canonical_names)} canonical players")

    # Find play-by-play table
    table = soup.find("table", id="play_by_play")
    if table is None:
        print("❌ No play-by-play table found")
        return pd.DataFrame()

    # Parse the table
    try:
        df = pd.read_html(StringIO(str(table)))[0]
    except Exception as e:
        print(f"❌ Error parsing play-by-play table: {e}")
        return pd.DataFrame()
    
    # Clean the data
    df = df[df['Inn'].notna() & df['Play Description'].notna() & df['Batter'].notna()]
    df = df[~df['Batter'].str.contains("Top of the|Bottom of the|inning", case=False, na=False)]
    df = df[~df['Batter'].str.contains("Team Totals", case=False, na=False)]
    
    print(f"📊 Processing {len(df)} plays")
    
    # Initialize components
    game_state = EnhancedGameState()
    name_resolver = SimpleWorkingNameResolver(canonical_names)
    player_stats = {}
    
    def get_or_create_player_stats(player_name):
        resolved_name = name_resolver.resolve_name(player_name)
        if resolved_name not in player_stats:
            player_stats[resolved_name] = PlayerStats(resolved_name)
        return player_stats[resolved_name]
    
    # Process each play
    for idx, row in df.iterrows():
        raw_batter_name = row['Batter']
        current_batter = name_resolver.resolve_name(raw_batter_name)
        game_state.current_batter = current_batter
        
        # Parse the play
        parsed_play = enhanced_parse_play_by_play(
            row['Play Description'], 
            current_batter, 
            game_state, 
            name_resolver
        )
        
        # Handle pure baserunning events
        if not parsed_play["is_plate_appearance"]:
            for event in parsed_play["baserunning_events"]:
                player = get_or_create_player_stats(event["player"])
                if event["type"] == "stolen_base":
                    player.SB += 1
                elif event["type"] == "caught_stealing":
                    player.CS += 1
                elif event["type"] == "run_scored":
                    player.R += 1
            continue
        
        # Update batter stats
        batter_stats = get_or_create_player_stats(current_batter)
        
        # At-bat counting
        is_ab = not (
            parsed_play["is_walk"] or 
            parsed_play["is_hbp"] or 
            (parsed_play["is_sacrifice"] and parsed_play["out_type"] in ["sacrifice_fly", "sacrifice_hit"])
        )
        
        if is_ab:
            batter_stats.AB += 1
        
        # Count hits and types
        if parsed_play["is_hit"]:
            batter_stats.H += 1
            if parsed_play["hit_type"] == "home_run":
                batter_stats.HR += 1
            elif parsed_play["hit_type"] == "double":
                batter_stats.doubles += 1
            elif parsed_play["hit_type"] == "triple":
                batter_stats.triples += 1
        
        # Count other outcomes
        if parsed_play["is_walk"]:
            batter_stats.BB += 1
        if parsed_play["is_strikeout"]:
            batter_stats.SO += 1
        if parsed_play["is_hbp"]:
            batter_stats.HBP += 1
        if parsed_play["is_sacrifice"] and parsed_play["out_type"] == "sacrifice_fly":
            batter_stats.SF += 1
        
        # Count RBIs
        batter_stats.RBI += parsed_play["rbi_count"]
        
        # Handle scoring events
        # FIXED: Handle runs more carefully to prevent double-counting

        # 1. If it's a home run, batter scores (and only the batter)
        if parsed_play["is_home_run"]:
            batter_stats.R += 1

        # 2. Handle OTHER players who scored (from scoring_events)
        # This covers runners who scored due to this batter's action
        for event in parsed_play["scoring_events"]:
            player_name = event["player"]
            # FIXED: Don't double-count the batter's run on home runs
            if not (parsed_play["is_home_run"] and player_name == current_batter):
                player = get_or_create_player_stats(player_name)
                player.R += 1

        # 3. Handle non-scoring baserunning events (SB, CS only)
        for event in parsed_play["baserunning_events"]:
            player_name = event["player"]
            if not player_name.startswith("runner_"):
                player = get_or_create_player_stats(player_name)
                if event["type"] == "stolen_base":
                    player.SB += 1
                elif event["type"] == "caught_stealing":
                    player.CS += 1
                # NOTE: Removed run_scored handling here to prevent double-counting
        
        # Update game state
        game_state.outs += parsed_play["outs_on_play"]
        if game_state.outs >= 3:
            game_state.advance_inning()
    
    # Convert to DataFrame
    if not player_stats:
        print("❌ No player stats found")
        return pd.DataFrame()
    
    stats_dicts = [stats.to_dict() for stats in player_stats.values()]
    result_df = pd.DataFrame(stats_dicts)
    
    print(f"✅ Extracted stats for {len(result_df)} players")
    return result_df

def extract_canonical_names(soup):
    """Extract canonical player names from box score"""
    canonical_names = set()
    
    # Get names from batting tables
    batting_tables = soup.find_all('table', {'id': lambda x: x and ('batting' in x.lower())})
    for table in batting_tables:
        rows = table.find_all('tr')
        for row in rows:
            name_cell = row.find('th', {'data-stat': 'player'})
            if name_cell and name_cell.get_text(strip=True):
                name = name_cell.get_text(strip=True)
                if name and name not in ['Player', '', 'Batting', 'Team Totals']:
                    # Clean the name
                    cleaned = re.sub(r"((?:[A-Z0-9]{1,3})(?:-[A-Z0-9]{1,3})*)$", "", name.strip())
                    cleaned = cleaned.replace("\xa0", " ").strip()
                    if cleaned:
                        canonical_names.add(cleaned)
    
    return canonical_names

def get_official_stats(url):
    """Extract official stats from box score with improved reliability"""
    
    print("📊 Fetching official stats from box score...")
    
    try:
        soup = safe_fetch_page(url)
    except Exception as e:
        print(f"❌ Failed to fetch official stats: {e}")
        return pd.DataFrame()

    all_dfs = []
    
    for tbl in soup.find_all("table"):
        table_id = tbl.get("id", "")
        if not table_id.endswith("batting"):
            continue

        try:
            df = pd.read_html(StringIO(str(tbl)))[0]
        except Exception as e:
            print(f"⚠️  Error parsing batting table {table_id}: {e}")
            continue
            
        df = df[df['Batting'].notna()]
        df = df[~df['Batting'].str.contains("Team Totals", na=False)]
        
        # Clean batter names
        df['batter'] = df['Batting'].apply(
            lambda x: re.sub(r"((?:[A-Z0-9]{1,3})(?:-[A-Z0-9]{1,3})*)$", "", str(x).strip()).replace("\xa0", " ").strip()
        )

        # Parse details column for advanced stats
        if 'Details' in df.columns:
            def parse_details_column(details_str):
                stats = {"HR": 0, "2B": 0, "3B": 0, "SB": 0, "CS": 0, "SF": 0}
                if pd.isna(details_str):
                    return stats

                parts = [p.strip() for p in str(details_str).split(",")]
                for part in parts:
                    match = re.match(r"(\d+)·(HR|2B|3B|SB|CS|SF|GDP)", part)
                    if match:
                        count, stat = match.groups()
                        if stat in stats:
                            stats[stat] += int(count)
                    elif part in stats:
                        stats[part] += 1
                return stats
            
            parsed_stats = df['Details'].apply(parse_details_column).apply(pd.Series)
            for stat in ['SB', 'CS', '2B', '3B', 'SF', 'HR']:
                df[stat] = parsed_stats.get(stat, 0)
        else:
            for stat in ['SB', 'CS', '2B', '3B', 'SF', 'HR']:
                df[stat] = 0

        all_dfs.append(df)

    if not all_dfs:
        print("❌ No batting tables found")
        return pd.DataFrame()

    combined = pd.concat(all_dfs, ignore_index=True)
    
    # Ensure all required columns exist
    required_cols = ['batter', 'AB', 'H', 'RBI', 'BB', 'SO', 'HR', 'SB', 'CS', '2B', '3B', 'SF', 'R']
    for col in required_cols:
        if col not in combined.columns:
            combined[col] = 0

    combined = combined[required_cols].copy()
    
    # Add official suffix
    combined.columns = ["batter"] + [f"{col}_official" for col in combined.columns if col != "batter"]
    
    print(f"✅ Extracted official stats for {len(combined)} players")
    return combined.reset_index(drop=True)

def validate_stats(url):
    """Enhanced validation with better error handling"""
    print(f"\n🔍 Analyzing game: {url}")
    
    try:
        parsed_df = extract_enhanced_stats(url)
        official_df = get_official_stats(url)
        
        if parsed_df.empty or official_df.empty:
            print("❌ Error: Could not extract data")
            return pd.DataFrame()
        
        print(f"📊 Parsed {len(parsed_df)} players, Official {len(official_df)} players")
        
        # Merge data
        merged = pd.merge(parsed_df, official_df, how="outer", on="batter", indicator=True)
        
        # Fill NaN values with 0 for numeric columns
        numeric_cols = ['AB', 'H', 'HR', 'RBI', 'R', 'BB', 'SO', 'SB', 'CS', '2B', '3B', 'SF']
        for col in numeric_cols:
            merged[col] = merged[col].fillna(0)
            merged[f"{col}_official"] = merged[f"{col}_official"].fillna(0)
            merged[f"{col}_diff"] = merged[col] - merged[f"{col}_official"]
        
        # Calculate accuracy metrics
        total_stats = 0
        matching_stats = 0
        
        for col in numeric_cols:
            total_stats += len(merged)
            matching_stats += (merged[f"{col}_diff"] == 0).sum()
        
        accuracy = (matching_stats / total_stats) * 100 if total_stats > 0 else 0
        print(f"📈 Overall stat accuracy: {accuracy:.1f}%")
        
        # Show players with differences
        has_diff = merged[[f"{col}_diff" for col in numeric_cols]].abs().sum(axis=1) > 0
        if has_diff.any():
            print(f"⚠️  {has_diff.sum()} players with stat differences")
            
            # Show specific differences for debugging
            diff_players = merged[has_diff]
            for _, player in diff_players.iterrows():
                diffs = []
                for col in numeric_cols:
                    if player[f"{col}_diff"] != 0:
                        diffs.append(f"{col}: {player[col]:.0f} vs {player[f'{col}_official']:.0f} (diff: {player[f'{col}_diff']:.0f})")
                if diffs:
                    print(f"   {player['batter']}: {', '.join(diffs)}")
        else:
            print("✅ All stats match perfectly!")
        
        # Reorder columns for better readability
        result_cols = ["batter", "_merge"]
        for col in numeric_cols:
            result_cols.extend([col, f"{col}_official", f"{col}_diff"])
        
        return merged[result_cols]
        
    except Exception as e:
        print(f"❌ Error during validation: {e}")
        import traceback
        print(f"   Full traceback: {traceback.format_exc()}")
        return pd.DataFrame()

# Test the enhanced version
if __name__ == "__main__":
    # Test with a specific game
    game_url = "https://www.baseball-reference.com/boxes/NYA/NYA202503270.shtml"
    
    print("🧪 Testing enhanced MLB parser...")
    validation_df = validate_stats(game_url)
    
    if not validation_df.empty:
        pd.set_option("display.max_columns", None)
        pd.set_option("display.width", None)
        pd.set_option("display.max_rows", None)
        
        print("\n🧾 VALIDATION REPORT:")
        print(validation_df)
        
        # Show only players with differences
        numeric_cols = ['AB', 'H', 'HR', 'RBI', 'R', 'BB', 'SO', 'SB', 'CS', '2B', '3B', 'SF']
        has_diff = validation_df[[f"{col}_diff" for col in numeric_cols]].abs().sum(axis=1) > 0
        
        if has_diff.any():
            print("\n❌ PLAYERS WITH DIFFERENCES:")
            print(validation_df[has_diff])
        else:
            print("\n✅ NO STAT DIFFERENCES FOUND!")
    else:
        print("\n❌ Validation failed - no data returned")