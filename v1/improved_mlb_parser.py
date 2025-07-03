"""
Enhanced MLB Play-by-Play Parser with FIXED Logic
================================================
Complete working version with all bugs fixed
"""

import pandas as pd
import numpy as np
import re
import time
from bs4 import BeautifulSoup
from io import StringIO
from playwright.sync_api import sync_playwright
from dataclasses import dataclass
from typing import Dict, List, Set, Optional

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

class GameState:
    """Track the state of the game during parsing"""
    
    def __init__(self):
        self.inning = 1
        self.half_inning = 'top'  # 'top' or 'bottom'
        self.outs = 0
        self.bases = {1: None, 2: None, 3: None}  # Base occupancy
        self.batting_team = 'away'  # 'home' or 'away'
        self.current_batter = None
    
    def clear_bases(self):
        """Clear all bases"""
        self.bases = {1: None, 2: None, 3: None}
    
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
    
    def add_runner_to_base(self, player, base):
        """Add a runner to specified base"""
        if 1 <= base <= 3:
            self.bases[base] = player

def parse_play_by_play(description, batter, game_state, name_resolver):
    """FIXED VERSION - Enhanced play outcome parser with improved accuracy"""
    
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
        "baserunning_events": []
    }
    
    # FIXED: Check for pure baserunning plays (no plate appearance)
    if re.search(r'Defensive Indifference', desc, re.IGNORECASE):
        result["is_plate_appearance"] = False
        result["baserunning_events"] = parse_baserunning_events(desc, name_resolver)
        return result
    
    baserunning_only_patterns = [
        r'^Stolen Base',
        r'^Caught Stealing',
        r'^Wild Pitch',
        r'^Passed Ball',
        r'^Balk',
        r'^Pickoff',
        r'Caught Stealing.*\(PO\)',  # Player name + Caught Stealing with pickoff
        r'^[A-Z]\.\s*[A-Z][a-z]+\s+Caught Stealing',  # "D. Hamilton Caught Stealing"
        r'^[A-Z][a-z]+\s+[A-Z][a-z]+\s+Caught Stealing'  # "David Hamilton Caught Stealing"
    ]
    
    for pattern in baserunning_only_patterns:
        if re.search(pattern, desc, re.IGNORECASE):
            result["is_plate_appearance"] = False
            result["baserunning_events"] = parse_baserunning_events(desc, name_resolver)
            return result
    
    # FIXED RBI counting function
    def extract_rbi_count(text):
        """Fixed RBI extraction that's more accurate"""
        
        # 1. Explicit RBI mentions
        explicit_patterns = [
            r'(\d+)\s*RBI',  # "2 RBI", "3 RBI"
            r'(\d+)\s*runs?\s+(?:batted\s+in)',  # "2 runs batted in"
        ]
        
        for pattern in explicit_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return int(match.group(1))
        
        # 2. Count explicit scoring mentions
        rbi_count = 0
        scoring_patterns = [
            r'([A-Z][a-z]+ [A-Z][a-z]+)\s+(?:scores|Scores)',
            r'([A-Z]\.\s*[A-Z][a-z]+)\s+(?:scores|Scores)',
        ]
        
        for pattern in scoring_patterns:
            matches = re.findall(pattern, text)
            rbi_count += len(matches)
        
        # 3. FIXED: For home runs, always add 1 for the batter
        if re.search(r'(?:Home\s+Run|HR(?:\s|$))', text, re.IGNORECASE):
            if rbi_count == 0:
                # Look for contextual clues
                if re.search(r'(?:grand\s+slam)', text, re.IGNORECASE):
                    return 4
                elif re.search(r'(?:3-run|three-run)', text, re.IGNORECASE):
                    return 3
                elif re.search(r'(?:2-run|two-run)', text, re.IGNORECASE):
                    return 2
                else:
                    return 1  # Solo home run
            else:
                # FIXED: Add 1 for the batter + the runners who scored
                return rbi_count + 1
        
        return rbi_count
    
    # Process in strict order to avoid conflicts
    
    # 1. Home runs - first priority
    if re.search(r'(?:Home\s+Run|HR(?:\s|$))', desc, re.IGNORECASE):
        result.update({
            "is_hit": True,
            "is_home_run": True,
            "hit_type": "home_run",
            "bases_earned": 4
        })
        result["rbi_count"] = extract_rbi_count(desc)
    
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
        
        result["rbi_count"] = extract_rbi_count(desc)
    
    # 3. Walks
    elif re.search(r'(?:Walk|BB|Ball\s+4|Base\s+on\s+Balls)', desc, re.IGNORECASE):
        result.update({
            "is_walk": True,
            "bases_earned": 1
        })
        result["rbi_count"] = extract_rbi_count(desc)
    
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
        result["rbi_count"] = extract_rbi_count(desc)
    
    # 6. EXPLICIT sacrifice flies ONLY - FIXED to be much more restrictive
    elif (re.search(r'(?:Sacrifice.*Fly|Sac.*Fly|SF)', desc, re.IGNORECASE) or
          re.search(r'/Sacrifice\s+Fly', desc, re.IGNORECASE)):
        result.update({
            "is_sacrifice": True,
            "out_type": "sacrifice_fly",
            "outs_on_play": 1
        })
        
        rbi_count = extract_rbi_count(desc)
        result["rbi_count"] = rbi_count if rbi_count > 0 else 1
    
    # 7. Sacrifice bunts/hits - FIXED to be extremely specific
    elif re.search(r'(?:^Sacrifice.*(?:Bunt|Hit)|^Sac.*(?:Bunt|Hit))', desc, re.IGNORECASE):
        result.update({
            "is_sacrifice": True,
            "out_type": "sacrifice_hit",
            "outs_on_play": 1
        })
        result["rbi_count"] = extract_rbi_count(desc)
    
    # 8. Errors and fielder's choices
    elif re.search(r'(?:Error|Fielder.*Choice)', desc, re.IGNORECASE):
        result["bases_earned"] = 1
        result["rbi_count"] = extract_rbi_count(desc)
        
        if re.search(r'(?:Double.*Play|DP)', desc, re.IGNORECASE):
            result["outs_on_play"] = 2
        elif re.search(r'(?:out)', desc, re.IGNORECASE):
            result["outs_on_play"] = 1
    
    # 9. FIXED: All other outs - NEVER mark as sacrifice flies unless explicitly stated above
    elif re.search(r'(?:Groundout|Flyout|Flyball|Lineout|Popfly|Popout|Out|Grounded|Flied|Lined|Popped|Pop)', desc, re.IGNORECASE):
        # Regular outs - count outs and RBIs but NOT sacrifice flies
        if re.search(r'(?:Double.*Play|DP)', desc, re.IGNORECASE):
            result["outs_on_play"] = 2
        else:
            result["outs_on_play"] = 1
        
        result["rbi_count"] = extract_rbi_count(desc)
        # NOTE: We deliberately do NOT mark these as sacrifice flies
        # Only the explicit section above can mark sacrifice flies
    
    # Parse baserunning events
    result["baserunning_events"] = parse_baserunning_events(desc, name_resolver)
    
    return result

def parse_baserunning_events(description, name_resolver):
    """Parse baserunning events with improved name matching"""
    events = []
    
    if not description:
        return events
    
    # Stolen bases
    sb_patterns = [
        r'Stolen Base.*?([A-Z][a-z]+ [A-Z][a-z]+|[A-Z]\.\s*[A-Z][a-z]+)',
        r'([A-Z][a-z]+ [A-Z][a-z]+|[A-Z]\.\s*[A-Z][a-z]+)\s+(?:steals|Steals)',
    ]
    
    for pattern in sb_patterns:
        for match in re.finditer(pattern, description):
            player_name = name_resolver.resolve_name(match.group(1).strip())
            events.append({
                "type": "stolen_base",
                "player": player_name
            })
    
    # Caught stealing
    cs_patterns = [
        r'Caught Stealing.*?([A-Z][a-z]+ [A-Z][a-z]+|[A-Z]\.\s*[A-Z][a-z]+)',
        r'([A-Z][a-z]+ [A-Z][a-z]+|[A-Z]\.\s*[A-Z][a-z]+)\s+(?:Caught Stealing|caught stealing)',
    ]
    
    for pattern in cs_patterns:
        for match in re.finditer(pattern, description):
            player_name = name_resolver.resolve_name(match.group(1).strip())
            events.append({
                "type": "caught_stealing",
                "player": player_name
            })
    
    # FIXED Runs scored - More precise patterns
    score_patterns = [
        r'([A-Z][a-z]+ [A-Z][a-z]+)\s+(?:scores|Scores)',
        r'([A-Z]\.\s*[A-Z][a-z]+)\s+(?:scores|Scores)',
    ]
    
    # Only count explicit "scores" mentions
    for pattern in score_patterns:
        for match in re.finditer(pattern, description):
            player_name = name_resolver.resolve_name(match.group(1).strip())
            events.append({
                "type": "run_scored",
                "player": player_name
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
        "baserunning_events": []
    }

def extract_enhanced_stats(url):
    """FIXED VERSION - Extract stats with better at-bat and RBI counting"""
    
    # Fetch the page
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        page.goto(url)
        soup = BeautifulSoup(page.content(), "html.parser")
        browser.close()

    # Extract canonical names
    print("📋 EXTRACTING CANONICAL NAMES FROM BOX SCORE...")
    canonical_names = extract_canonical_names(soup)
    print(f"✅ Found {len(canonical_names)} canonical players")

    table = soup.find("table", id="play_by_play")
    if table is None:
        return pd.DataFrame()

    df = pd.read_html(StringIO(str(table)))[0]
    df = df[df['Inn'].notna() & df['Play Description'].notna() & df['Batter'].notna()]
    df = df[~df['Batter'].str.contains("Top of the|Bottom of the|inning", case=False, na=False)]
    df = df[~df['Batter'].str.contains("Team Totals", case=False, na=False)]
    
    # Initialize components
    game_state = GameState()
    name_resolver = SimpleNameResolver(canonical_names)
    player_stats = {}
    
    def get_or_create_player_stats(player_name):
        resolved_name = name_resolver.resolve_name(player_name)
        if resolved_name not in player_stats:
            player_stats[resolved_name] = PlayerStats(resolved_name)
        return player_stats[resolved_name]
    
    # Process each play with enhanced debugging
    for idx, row in df.iterrows():
        raw_batter_name = row['Batter']
        current_batter = name_resolver.resolve_name(raw_batter_name)
        game_state.current_batter = current_batter
        
        # Parse the play using FIXED function
        parsed_play = parse_play_by_play(row['Play Description'], current_batter, game_state, name_resolver)
        
        # Debug specific players we know are having issues
        #debug_players = ['Aaron Judge', 'Carlos Narváez', 'Anthony Volpe', 'Cody Bellinger', 'Jarren Duran']
        #if current_batter in debug_players:
            #print(f"🔍 DEBUG {current_batter}: '{row['Play Description']}'")
            #is_ab = not (parsed_play['is_walk'] or parsed_play['is_hbp'] or (parsed_play['is_sacrifice'] and parsed_play['out_type'] in ['sacrifice_fly', 'sacrifice_hit']))
            #print(f"   → Sacrifice: {parsed_play['is_sacrifice']}, RBI: {parsed_play['rbi_count']}, AB: {is_ab}")
            
            # Extra debugging for problematic cases
        if parsed_play['is_sacrifice'] and parsed_play['rbi_count'] == 0:
            print(f"   ⚠️  WARNING: Regular flyout marked as sacrifice! Out type: {parsed_play['out_type']}")
        
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
        
        # FIXED at-bat counting - more precise logic
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
        
        # For home runs, batter scores
        if parsed_play["is_home_run"]:
            batter_stats.R += 1
        
        # FIXED baserunning events
        for event in parsed_play["baserunning_events"]:
            player_name = event["player"]
            if not player_name.startswith("runner_"):
                player = get_or_create_player_stats(player_name)
                if event["type"] == "run_scored":
                    player.R += 1
                elif event["type"] == "stolen_base":
                    player.SB += 1
                elif event["type"] == "caught_stealing":
                    player.CS += 1
        
        # Update game state
        game_state.outs += parsed_play["outs_on_play"]
        if game_state.outs >= 3:
            game_state.advance_inning()
    
    # Convert to DataFrame
    if not player_stats:
        return pd.DataFrame()
    
    stats_dicts = [stats.to_dict() for stats in player_stats.values()]
    return pd.DataFrame(stats_dicts)

def extract_canonical_names(soup):
    """Extract the definitive list of player names from box score"""
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

class SimpleNameResolver:
    """Name resolver for matching play-by-play names to box score names"""
    
    def __init__(self, canonical_names):
        self.canonical_names = canonical_names
        self.cache = {}
        
        # Build mapping dictionaries
        self.initial_to_full = {}
        self.last_to_full = {}
        self._build_mappings()
    
    def _build_mappings(self):
        """Build mappings from canonical names"""
        
        # Separate full names from initial names
        full_names = [name for name in self.canonical_names if ' ' in name and not re.match(r'^[A-Z]\.', name)]
        initial_names = [name for name in self.canonical_names if re.match(r'^[A-Z]\.\s*[A-Z][a-z]+', name)]
        
        # Map initials to full names
        for initial_name in initial_names:
            try:
                initial, last = initial_name.split('. ', 1)
                for full_name in full_names:
                    if ' ' in full_name:
                        first, full_last = full_name.split(' ', 1)
                        if (full_last.lower() == last.lower() and 
                            first.upper().startswith(initial.upper())):
                            self.initial_to_full[initial_name] = full_name
                            break
            except ValueError:
                continue
        
        # Map last names to full names (only if unambiguous)
        last_name_counts = {}
        for full_name in full_names:
            if ' ' in full_name:
                last = full_name.split(' ')[-1]
                if last not in last_name_counts:
                    last_name_counts[last] = []
                last_name_counts[last].append(full_name)
        
        for last, matches in last_name_counts.items():
            if len(matches) == 1:
                self.last_to_full[last] = matches[0]
    
    def resolve_name(self, name):
        """Resolve any name variant to canonical box score name"""
        if not name:
            return name
            
        # Clean the input
        cleaned = re.sub(r"((?:[A-Z0-9]{1,3})(?:-[A-Z0-9]{1,3})*)$", "", name.strip())
        cleaned = cleaned.replace("\xa0", " ").strip()
        
        # Check cache first
        if cleaned in self.cache:
            return self.cache[cleaned]
        
        # Exact match in canonical names
        if cleaned in self.canonical_names:
            self.cache[cleaned] = cleaned
            return cleaned
        
        # Check mappings
        if cleaned in self.initial_to_full:
            result = self.initial_to_full[cleaned]
            self.cache[cleaned] = result
            return result
        
        if cleaned in self.last_to_full:
            result = self.last_to_full[cleaned]
            self.cache[cleaned] = result
            return result
        
        # Try case-insensitive matching
        for canonical in self.canonical_names:
            if cleaned.lower() == canonical.lower():
                self.cache[cleaned] = canonical
                return canonical
        
        # Handle initial format matching
        if re.match(r'^[A-Z]\.\s*[A-Z][a-z]+', cleaned):
            try:
                initial, last = cleaned.split('. ', 1)
                for canonical in self.canonical_names:
                    if ' ' in canonical:
                        first, canon_last = canonical.split(' ', 1)
                        if (canon_last.lower() == last.lower() and 
                            first.upper().startswith(initial.upper())):
                            self.cache[cleaned] = canonical
                            return canonical
            except ValueError:
                pass
        
        # No match found
        self.cache[cleaned] = cleaned
        return cleaned

def get_official_stats(url):
    """Extract official stats from box score"""
    max_retries = 3
    retry_delay = 5
    
    for attempt in range(max_retries):
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch()
                page = browser.new_page()
                page.goto(url, timeout=60000, wait_until='domcontentloaded')
                page.wait_for_timeout(2000)
                soup = BeautifulSoup(page.content(), "html.parser")
                browser.close()
                break
        except Exception as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                print("All attempts failed, returning empty DataFrame")
                return pd.DataFrame()

    all_dfs = []
    
    for tbl in soup.find_all("table"):
        table_id = tbl.get("id", "")
        if not table_id.endswith("batting"):
            continue

        df = pd.read_html(StringIO(str(tbl)))[0]
        df = df[df['Batting'].notna()]
        df = df[~df['Batting'].str.contains("Team Totals", na=False)]
        
        # Clean batter names
        df['batter'] = df['Batting'].apply(lambda x: re.sub(r"((?:[A-Z0-9]{1,3})(?:-[A-Z0-9]{1,3})*)$", "", str(x).strip()).replace("\xa0", " ").strip())

        # Parse details column
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
    return combined.reset_index(drop=True)

def validate_stats(url):
    """FIXED VERSION - Enhanced validation with improved accuracy"""
    print(f"\n🔍 Analyzing game: {url}")
    
    parsed_df = extract_enhanced_stats(url)  # Now uses the fixed version
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

# Test the FIXED version
if __name__ == "__main__":
    # Test with the specific game
    game_url = "https://www.baseball-reference.com/boxes/NYA/NYA202503270.shtml"
    validation_df = validate_stats(game_url)
    
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", None)
    pd.set_option("display.max_rows", None)
    
    print("\n🧾 VALIDATION REPORT:")
    print(validation_df)
    
    # Show only players with differences
    if not validation_df.empty:
        numeric_cols = ['AB', 'H', 'HR', 'RBI', 'R', 'BB', 'SO', 'SB', 'CS', '2B', '3B', 'SF']
        has_diff = validation_df[[f"{col}_diff" for col in numeric_cols]].abs().sum(axis=1) > 0
        
        if has_diff.any():
            print("\n❌ PLAYERS WITH DIFFERENCES:")
            print(validation_df[has_diff])
        else:
            print("\n✅ NO STAT DIFFERENCES FOUND!")