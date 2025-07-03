"""
Single Game Validator - 100% Accuracy Focus
===========================================

Step 1: Parse one game perfectly before building infrastructure
"""

import pandas as pd
import numpy as np
import re
from bs4 import BeautifulSoup
from io import StringIO
from playwright.sync_api import sync_playwright
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
import time
import unicodedata

@dataclass
class GameStats:
    """Container for parsed game statistics"""
    game_id: str
    game_date: str
    home_team: str
    away_team: str
    official_batting_stats: pd.DataFrame
    official_pitching_stats: pd.DataFrame
    parsed_events: pd.DataFrame

class SafePageFetcher:
    """Robust page fetching with retries"""
    
    @staticmethod
    def fetch_page(url: str, max_retries: int = 3) -> BeautifulSoup:
        """Safely fetch page with retries"""
        
        for attempt in range(max_retries):
            try:
                with sync_playwright() as p:
                    browser = p.chromium.launch(headless=True)
                    context = browser.new_context(
                        user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                    )
                    page = context.new_page()
                    page.set_default_timeout(30000)
                    
                    page.goto(url, wait_until='domcontentloaded')
                    page.wait_for_timeout(2000)
                    
                    content = page.content()
                    browser.close()
                    
                    return BeautifulSoup(content, "html.parser")
                    
            except Exception as e:
                print(f"❌ Attempt {attempt + 1} failed: {str(e)[:100]}...")
                if attempt < max_retries - 1:
                    time.sleep(3 * (attempt + 1))
                else:
                    raise Exception(f"Failed to fetch {url} after {max_retries} attempts")

class OfficialStatsParser:
    """Parse official stats from box score - TARGET: 99%+ accuracy"""
    
    def __init__(self):
        self.canonical_names = set()
        self.name_resolver = None
    
    def parse_official_batting_stats(self, soup: BeautifulSoup) -> pd.DataFrame:
        """Parse official batting statistics from box score tables"""
        
        print("📊 Parsing official batting stats...")
        
        # Extract canonical names first
        self.canonical_names = self._extract_canonical_names(soup)
        self.name_resolver = self._build_name_resolver()
        
        all_batting_stats = []
        
        # Find all batting tables
        batting_tables = soup.find_all('table', {'id': lambda x: x and x.endswith('batting')})
        
        if not batting_tables:
            print("❌ No batting tables found")
            return pd.DataFrame()
        
        print(f"✅ Found {len(batting_tables)} batting tables")
        
        for table in batting_tables:
            team_stats = self._parse_batting_table(table)
            all_batting_stats.extend(team_stats)
        
        if not all_batting_stats:
            return pd.DataFrame()
        
        df = pd.DataFrame(all_batting_stats)
        print(f"✅ Parsed official stats for {len(df)} players")
        
        return df
    
    def _extract_canonical_names(self, soup: BeautifulSoup) -> set:
        """Extract canonical player names from BOTH batting and pitching box scores"""
        
        canonical_names = set()
        
        # Extract from batting tables
        batting_tables = soup.find_all('table', {'id': lambda x: x and 'batting' in x.lower()})
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
        # Extract from pitching tables
        pitching_tables = soup.find_all('table', {'id': lambda x: x and 'pitching' in x.lower()})
        for table in pitching_tables:
            rows = table.find_all('tr')
            for row in rows:
                name_cell = row.find('th', {'data-stat': 'player'})
                if name_cell and name_cell.get_text(strip=True):
                    name = name_cell.get_text(strip=True)
                    if name and name not in ['Player', '', 'Pitching', 'Team Totals']:
                        # Clean the name (same logic as batting)
                        cleaned = re.sub(r"((?:[A-Z0-9]{1,3})(?:-[A-Z0-9]{1,3})*)$", "", name.strip())
                        cleaned = cleaned.replace("\xa0", " ").strip()
                        if cleaned:
                            canonical_names.add(cleaned)
        
        print(f"📋 Found {len(canonical_names)} canonical names (batting + pitching)")
        return canonical_names
    
    def _build_name_resolver(self) -> Dict[str, str]:
        """Build name resolution mapping"""
        
        name_mappings = {}
        
        # Map canonical names to themselves
        for name in self.canonical_names:
            name_mappings[name] = name
        
        # Create abbreviated mappings
        for canonical_name in self.canonical_names:
            if ' ' in canonical_name:
                parts = canonical_name.split(' ')
                if len(parts) >= 2:
                    first = parts[0]
                    last = ' '.join(parts[1:])
                    
                    # Standard abbreviation
                    abbreviated = f"{first[0]}. {last}"
                    name_mappings[abbreviated] = canonical_name
                    
                    # Handle Jazz Chisholm specifically
                    if first == "Jazz":
                        name_mappings[f"J. {last}"] = canonical_name
        
        # DEBUG: Show some key mappings
        print(f"🔧 Built {len(name_mappings)} name mappings")
        print(f"📋 Sample mappings:")
        sample_mappings = list(name_mappings.items())[:10]
        for original, resolved in sample_mappings:
            if original != resolved:  # Only show actual mappings
                print(f"   '{original}' -> '{resolved}'")
        
        return name_mappings
    
    def _parse_batting_table(self, table) -> List[Dict]:
        """Parse a single batting table"""
        
        try:
            df = pd.read_html(StringIO(str(table)))[0]
        except Exception as e:
            print(f"❌ Error parsing batting table: {e}")
            return []
        
        # Clean the data
        df = df[df['Batting'].notna()]
        df = df[~df['Batting'].str.contains("Team Totals", na=False)]
        
        batting_stats = []
        
        for _, row in df.iterrows():
            player_name = self._clean_player_name(row['Batting'])
            
            if not player_name:
                continue
            
            # Helper function to safely convert to int
            def safe_int(value, default=0):
                if pd.isna(value):
                    return default
                try:
                    return int(float(value))
                except (ValueError, TypeError):
                    return default
            
            # Extract stats (these are OFFICIAL - our source of truth)
            stats = {
                'player_name': player_name,
                'AB': safe_int(row.get('AB', 0)),
                'R': safe_int(row.get('R', 0)),           # Official runs
                'H': safe_int(row.get('H', 0)),           # Official hits
                'RBI': safe_int(row.get('RBI', 0)),       # Official RBIs  
                'BB': safe_int(row.get('BB', 0)),
                'SO': safe_int(row.get('SO', 0)),
                'HR': self._extract_from_details(row, 'HR'),
                '2B': self._extract_from_details(row, '2B'),
                '3B': self._extract_from_details(row, '3B'),
                'SB': self._extract_from_details(row, 'SB'),
                'CS': self._extract_from_details(row, 'CS'),
                'SF': self._extract_from_details(row, 'SF'),
            }
            
            batting_stats.append(stats)
        
        return batting_stats
    
    def _clean_player_name(self, name: str) -> str:
        """Clean player name from box score"""
        if pd.isna(name):
            return ""
        
        cleaned = re.sub(r"((?:[A-Z0-9]{1,3})(?:-[A-Z0-9]{1,3})*)$", "", str(name).strip())
        return cleaned.replace("\xa0", " ").strip()
    
    def _extract_from_details(self, row: pd.Series, stat: str) -> int:
        """Extract stat from Details column"""
        
        if 'Details' not in row.index or pd.isna(row['Details']):
            return 0
        
        details = str(row['Details'])
        
        # Look for pattern like "2·HR" or just "HR"
        pattern = rf"(\d+)·{stat}|(?:^|,)\s*{stat}(?:,|$)"
        match = re.search(pattern, details)
        
        if match:
            if match.group(1):
                return int(match.group(1))
            else:
                return 1
        
        return 0

class EnhancedSimpleEventParser:
    """Parse more at-bat events while maintaining 99%+ accuracy"""
    
    def __init__(self, name_resolver: Dict[str, str]):
        self.name_resolver = name_resolver
    
    def parse_simple_events(self, soup) -> pd.DataFrame:
        """Parse events with expanded at-bat detection"""
        
        print("⚾ Parsing events with enhanced at-bat detection...")
        
        # Get play-by-play table
        pbp_table = soup.find("table", id="play_by_play")
        if not pbp_table:
            print("❌ No play-by-play table found")
            return pd.DataFrame()
        
        try:
            from io import StringIO
            df = pd.read_html(StringIO(str(pbp_table)))[0]
        except Exception as e:
            print(f"❌ Error parsing play-by-play table: {e}")
            return pd.DataFrame()
        
        # Clean the data
        df = df[df['Inn'].notna() & df['Play Description'].notna() & df['Batter'].notna()]
        df = df[~df['Batter'].str.contains("Top of the|Bottom of the|inning", case=False, na=False)]
        df = df[~df['Batter'].str.contains("Team Totals", case=False, na=False)]
        
        events = []
        
        for _, row in df.iterrows():
            event = self._parse_enhanced_event(row)
            if event:
                events.append(event)
        
        if not events:
            return pd.DataFrame()
        
        result_df = pd.DataFrame(events)
        print(f"✅ Parsed {len(result_df)} events")
        
        # Show breakdown
        at_bat_count = result_df['is_at_bat'].sum()
        hit_count = result_df['is_hit'].sum()
        walk_count = result_df['is_walk'].sum()
        strikeout_count = result_df['is_strikeout'].sum()
        
        print(f"   At-bats: {at_bat_count}")
        print(f"   Hits: {hit_count}")
        print(f"   Walks: {walk_count}")
        print(f"   Strikeouts: {strikeout_count}")
        
        return result_df
    
    def _parse_enhanced_event(self, row: pd.Series) -> Optional[Dict]:
        """Parse event with enhanced at-bat detection"""
        
        batter_name = str(row['Batter']).strip()
        description = str(row['Play Description']).strip()
        
        # Resolve batter name (fixes the \xa0 issue)
        resolved_batter = self.name_resolver.get(batter_name, batter_name)
        
        # Parse the outcome with enhanced logic
        outcome = self._analyze_enhanced_outcome(description)
        
        if not outcome:
            return None  # Skip truly ambiguous plays
        
        return {
            'batter_name': resolved_batter,
            'description': description,
            'is_at_bat': outcome['is_at_bat'],
            'is_hit': outcome['is_hit'],
            'hit_type': outcome.get('hit_type'),
            'is_walk': outcome['is_walk'],
            'is_strikeout': outcome['is_strikeout'],
            'bases_reached': outcome['bases_reached'],
        }
    
    def _analyze_enhanced_outcome(self, description: str) -> Optional[Dict]:
        """Enhanced outcome analysis - capture more at-bats"""
        
        desc = description.lower().strip()
        
        # CRITICAL: Check sacrifice flies FIRST before any other patterns!
        # This MUST come before flyball patterns to prevent conflicts
        
        # SACRIFICE FLIES (not at-bats in official scoring) - ABSOLUTE FIRST PRIORITY!
        if re.search(r'sacrifice fly|sac fly|flyball.*sacrifice fly', desc):
            return {
                'is_at_bat': False,
                'is_hit': False,
                'is_walk': False,
                'is_strikeout': False,
                'bases_reached': 0
            }
        
        # SACRIFICE BUNTS (not at-bats in official scoring)
        if re.search(r'sacrifice bunt|sac bunt', desc):
            return {
                'is_at_bat': False,
                'is_hit': False,
                'is_walk': False,
                'is_strikeout': False,
                'bases_reached': 0
            }
        
        # WALKS (not at-bats)
        if re.search(r'^walk\b|^intentional walk', desc):
            return {
                'is_at_bat': False,
                'is_hit': False,
                'is_walk': True,
                'is_strikeout': False,
                'bases_reached': 1
            }
        
        # HIT BY PITCH (not at-bats)
        if re.search(r'^hit by pitch|^hbp\b', desc):
            return {
                'is_at_bat': False,
                'is_hit': False,
                'is_walk': False,
                'is_strikeout': False,
                'bases_reached': 1
            }
        
        # STRIKEOUTS (at-bats)
        if re.search(r'^strikeout\b|^struck out', desc):
            return {
                'is_at_bat': True,
                'is_hit': False,
                'is_walk': False,
                'is_strikeout': True,
                'bases_reached': 0
            }
        
        # HOME RUNS (at-bats, hits)
        if re.search(r'^home run\b|^hr\b', desc):
            return {
                'is_at_bat': True,
                'is_hit': True,
                'hit_type': 'home_run',
                'is_walk': False,
                'is_strikeout': False,
                'bases_reached': 4
            }
        
        # HITS (at-bats, hits)
        if re.search(r'^single\b.*(?:to|up|through)', desc):
            return {
                'is_at_bat': True,
                'is_hit': True,
                'hit_type': 'single',
                'is_walk': False,
                'is_strikeout': False,
                'bases_reached': 1
            }
        
        if re.search(r'^double\b.*(?:to|down)', desc):
            return {
                'is_at_bat': True,
                'is_hit': True,
                'hit_type': 'double',
                'is_walk': False,
                'is_strikeout': False,
                'bases_reached': 2
            }
        
        if re.search(r'^triple\b.*(?:to|down)', desc):
            return {
                'is_at_bat': True,
                'is_hit': True,
                'hit_type': 'triple',
                'is_walk': False,
                'is_strikeout': False,
                'bases_reached': 3
            }
        
        # OBVIOUS OUTS (at-bats, not hits) - AFTER sacrifice checks!
        out_patterns = [
            r'^grounded out\b',           # "grounded out to shortstop"
            r'^ground out\b',             # "ground out to first"
            r'^flied out\b',              # "flied out to center field"
            r'^lined out\b',              # "lined out to third base"
            r'^popped out\b',             # "popped out to catcher"
            r'^fouled out\b',             # "fouled out to first base"
            r'^groundout\b',              # Alternative spelling
            r'^flyout\b',                 # Alternative spelling
            r'^lineout\b',                # Alternative spelling
            r'^popout\b',                 # Alternative spelling
            r'popfly\b',                  # Your addition for "popfly" cases
            r'flyball:',                  # Your addition for "Flyball: CF" cases - AFTER sacrifice checks!
        ]
        
        for pattern in out_patterns:
            if re.search(pattern, desc):
                return {
                    'is_at_bat': True,
                    'is_hit': False,
                    'is_walk': False,
                    'is_strikeout': False,
                    'bases_reached': 0
                }
        
        # DOUBLE PLAYS and FIELDER'S CHOICES (at-bats, not hits)
        if re.search(r'grounded into double play|gdp\b|double play', desc):
            return {
                'is_at_bat': True,
                'is_hit': False,
                'is_walk': False,
                'is_strikeout': False,
                'bases_reached': 0
            }
        
        # SKIP truly ambiguous cases:
        # - "reached on error" (could be scored various ways)
        # - Complex plays with multiple actions
        # - Catcher interference
        
        return None  # Skip remaining ambiguous cases

@dataclass
class PitchingStats:
    """Container for parsed pitching statistics"""
    pitcher_name: str
    batters_faced: int = 0
    hits_allowed: int = 0
    walks_allowed: int = 0
    strikeouts: int = 0
    home_runs_allowed: int = 0
    total_pitches: int = 0
    outs_recorded: int = 0

class OfficialPitchingStatsParser:
    """Parse official pitching stats from box score - TARGET: 99%+ accuracy"""
    
    def __init__(self, name_resolver: Dict[str, str]):
        self.name_resolver = name_resolver
    
    def parse_official_pitching_stats(self, soup: BeautifulSoup) -> pd.DataFrame:
        """Parse official pitching statistics from box score tables"""
        
        print("📊 Parsing official pitching stats...")
        
        all_pitching_stats = []
        
        # Find all pitching tables
        pitching_tables = soup.find_all('table', {'id': lambda x: x and x.endswith('pitching')})
        
        if not pitching_tables:
            print("❌ No pitching tables found")
            return pd.DataFrame()
        
        print(f"✅ Found {len(pitching_tables)} pitching tables")
        
        for table in pitching_tables:
            team_stats = self._parse_pitching_table(table)
            all_pitching_stats.extend(team_stats)
        
        if not all_pitching_stats:
            return pd.DataFrame()
        
        df = pd.DataFrame(all_pitching_stats)
        print(f"✅ Parsed official pitching stats for {len(df)} pitchers")
        
        return df
    
    def _parse_pitching_table(self, table) -> List[Dict]:
        """Parse a single pitching table"""
        
        try:
            df = pd.read_html(StringIO(str(table)))[0]
        except Exception as e:
            print(f"❌ Error parsing pitching table: {e}")
            return []
        
        # Clean the data
        df = df[df['Pitching'].notna()]
        df = df[~df['Pitching'].str.contains("Team Totals", na=False)]
        
        pitching_stats = []
        
        for _, row in df.iterrows():
            pitcher_name = self._clean_pitcher_name(row['Pitching'])
            
            if not pitcher_name:
                continue
            
            # Helper function to safely convert to int/float
            def safe_int(value, default=0):
                if pd.isna(value):
                    return default
                try:
                    return int(float(value))
                except (ValueError, TypeError):
                    return default
            
            def safe_float(value, default=0.0):
                if pd.isna(value):
                    return default
                try:
                    return float(value)
                except (ValueError, TypeError):
                    return default
            
            # Extract core stats (these are OFFICIAL - our source of truth)
            stats = {
                'pitcher_name': pitcher_name,
                'IP': safe_float(row.get('IP', 0.0)),      # Innings pitched
                'H': safe_int(row.get('H', 0)),            # Hits allowed
                'R': safe_int(row.get('R', 0)),            # Runs allowed
                'ER': safe_int(row.get('ER', 0)),          # Earned runs
                'BB': safe_int(row.get('BB', 0)),          # Walks
                'SO': safe_int(row.get('SO', 0)),          # Strikeouts
                'HR': safe_int(row.get('HR', 0)),          # Home runs allowed
                'BF': safe_int(row.get('BF', 0)),          # Batters faced
                'Pit': safe_int(row.get('Pit', 0)),        # Pitches thrown
                # Results (will be strings or NaN)
                'Dec': str(row.get('Dec', '')).strip() if pd.notna(row.get('Dec', '')) else '',
            }
            
            pitching_stats.append(stats)
        
        return pitching_stats
    
    def _clean_pitcher_name(self, name: str) -> str:
        """Clean pitcher name from box score"""
        if pd.isna(name):
            return ""
        
        # Remove any trailing codes (like batting stats parsing)
        cleaned = re.sub(r"((?:[A-Z0-9]{1,3})(?:-[A-Z0-9]{1,3})*)$", "", str(name).strip())
        return cleaned.replace("\xa0", " ").strip()

class SimplePitchingEventParser:
    """Parse ONLY crystal-clear pitching events - TARGET: 99%+ accuracy"""
    
    def __init__(self, name_resolver: Dict[str, str]):
        self.name_resolver = name_resolver
    
    def parse_pitching_events(self, soup: BeautifulSoup) -> pd.DataFrame:
        """Parse pitching events from play-by-play"""
        
        print("⚾ Parsing pitching events (high-confidence only)...")
        
        # Get play-by-play table
        pbp_table = soup.find("table", id="play_by_play")
        if not pbp_table:
            print("❌ No play-by-play table found")
            return pd.DataFrame()
        
        try:
            df = pd.read_html(StringIO(str(pbp_table)))[0]
        except Exception as e:
            print(f"❌ Error parsing play-by-play table: {e}")
            return pd.DataFrame()
        
        # Clean the data
        df = df[df['Inn'].notna() & df['Play Description'].notna() & df['Pitcher'].notna()]
        df = df[~df['Pitcher'].str.contains("Top of the|Bottom of the|inning", case=False, na=False)]
        df = df[~df['Pitcher'].str.contains("Team Totals", case=False, na=False)]
        
        events = []
        
        for _, row in df.iterrows():
            event = self._parse_single_pitching_event(row)
            if event:
                events.append(event)
        
        if not events:
            return pd.DataFrame()
        
        result_df = pd.DataFrame(events)
        print(f"✅ Parsed {len(result_df)} pitching events")
        
        return result_df
    
    def _parse_single_pitching_event(self, row: pd.Series) -> Optional[Dict]:
        """Parse a single pitching event - ONLY if we're 99%+ confident"""
        
        pitcher_name = str(row['Pitcher']).strip()
        description = str(row['Play Description']).strip()
        
        # Handle pitch count (format like "5(3-1)")
        pitch_count = self._parse_pitch_count(row.get('Cnt', ''))
        
        # Resolve pitcher name (same normalization as batting)
        resolved_pitcher = self.name_resolver.get(pitcher_name, pitcher_name)
        
        # Parse the pitching outcome
        outcome = self._analyze_pitching_outcome(description)
        
        if not outcome:
            return None  # Skip ambiguous plays
        
        return {
            'pitcher_name': resolved_pitcher,
            'description': description,
            'pitch_count': pitch_count,
            'batter_faced': 1,  # Each event = 1 batter faced
            'hit_allowed': outcome['hit_allowed'],
            'walk_allowed': outcome['walk_allowed'],
            'strikeout': outcome['strikeout'],
            'home_run_allowed': outcome['home_run_allowed'],
            'out_recorded': outcome['out_recorded'],
        }
    
    def _parse_pitch_count(self, count_str: str) -> int:
        """Parse pitch count from format like '5(3-1)' -> 5 pitches"""
        
        if pd.isna(count_str) or not count_str:
            return 0
        
        count_str = str(count_str).strip()
        
        # Look for number before parentheses: "5(3-1)" -> 5
        match = re.match(r'^(\d+)', count_str)
        if match:
            try:
                return int(match.group(1))
            except ValueError:
                return 0
        
        return 0
    
    def _analyze_pitching_outcome(self, description: str) -> Optional[Dict]:
        """Analyze pitching outcome - ONLY obvious cases"""
        
        desc = description.lower().strip()
        
        # Initialize outcome
        outcome = {
            'hit_allowed': False,
            'walk_allowed': False,
            'strikeout': False,
            'home_run_allowed': False,
            'out_recorded': False,
        }
        
        # SACRIFICE FLIES - not hits, but outs recorded
        if re.search(r'sacrifice fly|sac fly|flyball.*sacrifice fly', desc):
            outcome['out_recorded'] = True  # Pitcher gets out, but not charged with at-bat
            return outcome
        
        # WALKS - clear pitcher responsibility
        if re.search(r'^walk\b|^intentional walk', desc):
            outcome['walk_allowed'] = True
            return outcome
        
        # STRIKEOUTS - clear pitcher credit
        if re.search(r'^strikeout\b|^struck out', desc):
            outcome['strikeout'] = True
            outcome['out_recorded'] = True
            return outcome
        
        # HOME RUNS - clear pitcher responsibility
        if re.search(r'^home run\b|^hr\b', desc):
            outcome['hit_allowed'] = True
            outcome['home_run_allowed'] = True
            return outcome
        
        # OTHER HITS - clear pitcher responsibility
        if re.search(r'^single\b.*(?:to|up|through)', desc):
            outcome['hit_allowed'] = True
            return outcome
        
        if re.search(r'^double\b.*(?:to|down)', desc):
            outcome['hit_allowed'] = True
            return outcome
        
        if re.search(r'^triple\b.*(?:to|down)', desc):
            outcome['hit_allowed'] = True
            return outcome
        
        # OBVIOUS OUTS - pitcher gets credit for out
        out_patterns = [
            r'^grounded out\b', r'^ground out\b', r'^flied out\b', r'^lined out\b',
            r'^popped out\b', r'^fouled out\b', r'^groundout\b', r'^flyout\b',
            r'^lineout\b', r'^popout\b', r'popfly\b', r'flyball:',
        ]
        
        for pattern in out_patterns:
            if re.search(pattern, desc):
                outcome['out_recorded'] = True
                return outcome
        
        # DOUBLE PLAYS - pitcher gets credit for outs
        if re.search(r'grounded into double play|gdp\b|double play', desc):
            outcome['out_recorded'] = True  # Note: might be 2 outs, but start simple
            return outcome
        
        # HIT BY PITCH - pitcher responsibility
        if re.search(r'^hit by pitch|^hbp\b', desc):
            # Not a walk, not a hit, but pitcher's responsibility
            return outcome  # All False - special case
        
        # Skip ambiguous cases
        return None

class PitchingStatsValidator:
    """Validate pitching parsing accuracy"""
    
    def __init__(self, name_resolver: Dict[str, str]):
        self.name_resolver = name_resolver

    def validate_pitching_stats(self, soup: BeautifulSoup) -> Dict:
        """Validate pitching stats parsing"""
        
        print(f"\n🎯 VALIDATING PITCHING STATS:")
        print("=" * 40)
        
        # Parse official pitching stats
        official_parser = OfficialPitchingStatsParser(self.name_resolver)
        official_pitching = official_parser.parse_official_pitching_stats(soup)
        
        # Parse pitching events
        event_parser = SimplePitchingEventParser(self.name_resolver)
        pitching_events = event_parser.parse_pitching_events(soup)
        
        # Validate accuracy
        validation_results = self._validate_pitching_accuracy(official_pitching, pitching_events)
        
        return {
            'official_pitching_stats': official_pitching,
            'parsed_pitching_events': pitching_events,
            'validation_results': validation_results
        }
        
    def _validate_pitching_accuracy(self, official_stats: pd.DataFrame, parsed_events: pd.DataFrame) -> Dict:
        """Compare parsed pitching events vs official stats"""
        
        print(f"\n🔍 VALIDATING PITCHING ACCURACY:")
        print("-" * 30)
        
        if official_stats.empty or parsed_events.empty:
            print("❌ No pitching data to validate")
            return {'accuracy': 0, 'status': 'failed'}
        
        print(f"📊 Official pitching stats: {len(official_stats)} pitchers")
        print(f"📊 Parsed pitching events: {len(parsed_events['pitcher_name'].unique())} pitchers")
        
        # ADD NAME DEBUGGING FOR PITCHERS (same as batting)
        name_resolver = self._debug_pitcher_name_matching(official_stats, parsed_events)
        
        # Apply name resolution if we found mappings
        if name_resolver:
            print(f"\n🔧 APPLYING PITCHER NAME RESOLUTION:")
            print(f"   Found {len(name_resolver)} pitcher name mappings")
            
            # Apply resolution to parsed pitching events
            parsed_events = parsed_events.copy()
            parsed_events['resolved_pitcher_name'] = parsed_events['pitcher_name'].map(
                lambda x: name_resolver.get(x, x)
            )
            
            # Use resolved names for aggregation
            parsed_events_for_agg = parsed_events.copy()
            parsed_events_for_agg['pitcher_name'] = parsed_events_for_agg['resolved_pitcher_name']
        
        # Aggregate parsed events by pitcher
        parsed_stats = self._aggregate_pitching_events(parsed_events_for_agg if name_resolver else parsed_events)
        
        # Compare stats
        comparison_details = self._compare_pitching_stats(official_stats, parsed_stats)
        
        return comparison_details

    # ADD THIS NEW METHOD TO YOUR PitchingStatsValidator CLASS:

    def _debug_pitcher_name_matching(self, official_stats: pd.DataFrame, parsed_events: pd.DataFrame) -> Dict[str, str]:
        """Debug and fix pitcher name matching issues (same logic as batting)"""
        
        print(f"\n🔍 DEBUGGING PITCHER NAME MATCHING:")
        print("-" * 40)
        
        if official_stats.empty or parsed_events.empty:
            print("❌ Empty dataframes - cannot debug")
            return {}
        
        official_names = set(official_stats['pitcher_name'].tolist())
        parsed_names = set(parsed_events['pitcher_name'].unique())
        
        print(f"📊 Before normalization:")
        print(f"   Official pitchers: {len(official_names)}")
        print(f"   Parsed pitchers: {len(parsed_names)}")
        print(f"   Exact matches: {len(official_names & parsed_names)}")
        
        # Show character-level differences for first few names
        print(f"\n🔍 PITCHER CHARACTER ANALYSIS (first 3 names):")
        
        official_sample = list(official_names)[:3]
        parsed_sample = list(parsed_names)[:3]
        
        for i, name in enumerate(official_sample):
            print(f"   Official[{i}]: '{name}' | len={len(name)} | repr={repr(name)}")
            print(f"   Chars: {[ord(c) for c in name]}")
        
        for i, name in enumerate(parsed_sample):
            print(f"   Parsed[{i}]: '{name}' | len={len(name)} | repr={repr(name)}")
            print(f"   Chars: {[ord(c) for c in name]}")
        
        # Try different normalization strategies (same as batting)
        name_resolver = self._find_best_pitcher_normalization(official_names, parsed_names)
        
        return name_resolver

    # ADD THIS NEW METHOD TO YOUR PitchingStatsValidator CLASS:

    def _find_best_pitcher_normalization(self, official_names: set, parsed_names: set) -> Dict[str, str]:
        """Try different normalization strategies to find pitcher matches"""
        
        print(f"\n🔧 TESTING PITCHER NORMALIZATION STRATEGIES:")
        
        import unicodedata
        
        strategies = [
            ('unicode_nfkd', lambda x: unicodedata.normalize('NFKD', x)),
            ('strip_spaces', lambda x: re.sub(r'\s+', ' ', x.strip())),
            ('remove_nbsp', lambda x: x.replace('\xa0', ' ').strip()),
            ('clean_whitespace', lambda x: re.sub(r'[\s\xa0]+', ' ', x).strip()),
            ('full_normalize', lambda x: unicodedata.normalize('NFKD', re.sub(r'[\s\xa0]+', ' ', x).strip())),
        ]
        
        best_resolver = {}
        best_matches = 0
        
        for strategy_name, normalize_func in strategies:
            print(f"\n   Testing: {strategy_name}")
            
            # Normalize both sets
            official_normalized = {}
            parsed_normalized = {}
            
            for name in official_names:
                normalized = normalize_func(name)
                official_normalized[normalized] = name
            
            for name in parsed_names:
                normalized = normalize_func(name)
                parsed_normalized[normalized] = name
            
            # Find matches
            matches = set(official_normalized.keys()) & set(parsed_normalized.keys())
            
            print(f"   Matches: {len(matches)}")
            
            if len(matches) > best_matches:
                best_matches = len(matches)
                # Create resolver mapping parsed -> official
                best_resolver = {}
                for normalized_name in matches:
                    parsed_original = parsed_normalized[normalized_name]
                    official_original = official_normalized[normalized_name]
                    best_resolver[parsed_original] = official_original
                
                print(f"   ✅ NEW BEST! {len(matches)} matches")
                
                # Show some examples
                for normalized_name in list(matches)[:3]:
                    parsed_orig = parsed_normalized[normalized_name]
                    official_orig = official_normalized[normalized_name]
                    print(f"      '{parsed_orig}' -> '{official_orig}'")
        
        print(f"\n🎯 BEST PITCHER STRATEGY FOUND: {best_matches} matches")
        return best_resolver
    
    def _aggregate_pitching_events(self, events_df: pd.DataFrame) -> pd.DataFrame:
        """Aggregate pitching events into pitcher stats"""
        
        if events_df.empty:
            return pd.DataFrame()
        
        # Group by pitcher and aggregate
        aggregated = events_df.groupby('pitcher_name').agg({
            'batter_faced': 'sum',
            'hit_allowed': 'sum',
            'walk_allowed': 'sum',
            'strikeout': 'sum',
            'home_run_allowed': 'sum',
            'out_recorded': 'sum',
            'pitch_count': 'sum'
        }).reset_index()
        
        # Rename columns to match official stats
        aggregated.columns = [
            'pitcher_name', 'parsed_BF', 'parsed_H', 'parsed_BB', 
            'parsed_SO', 'parsed_HR', 'parsed_outs', 'parsed_pitches'
        ]
        
        print(f"\n📊 Aggregated pitching stats sample:")
        for _, row in aggregated.head(5).iterrows():
            print(f"   {row['pitcher_name']}: BF={row['parsed_BF']}, H={row['parsed_H']}, BB={row['parsed_BB']}, SO={row['parsed_SO']}")
        
        return aggregated
    
    def _compare_pitching_stats(self, official: pd.DataFrame, parsed: pd.DataFrame) -> Dict:
        """Compare official vs parsed pitching stats"""
        
        print(f"\n🔍 PITCHING MERGE DEBUG:")
        print(f"   Official shape: {official.shape}")
        print(f"   Parsed shape: {parsed.shape}")
        
        # Merge the dataframes
        comparison = pd.merge(official, parsed, on='pitcher_name', how='inner')
        
        print(f"   After inner merge: {comparison.shape}")
        
        if comparison.empty:
            print("❌ No matching pitchers found!")
            return {'accuracy': 0, 'status': 'failed', 'total_differences': 0, 'total_stats': 0}
        
        # Fill any remaining NaN values
        comparison = comparison.fillna(0)
        
        # Calculate differences for stats we can compare
        comparison['BF_diff'] = comparison['parsed_BF'] - comparison['BF']
        comparison['H_diff'] = comparison['parsed_H'] - comparison['H'] 
        comparison['BB_diff'] = comparison['parsed_BB'] - comparison['BB']
        comparison['SO_diff'] = comparison['parsed_SO'] - comparison['SO']
        comparison['HR_diff'] = comparison['parsed_HR'] - comparison['HR']
        comparison['Pit_diff'] = comparison['parsed_pitches'] - comparison['Pit']
        
        # Calculate accuracy
        total_diffs = (comparison['BF_diff'].abs().sum() + 
                      comparison['H_diff'].abs().sum() + 
                      comparison['BB_diff'].abs().sum() + 
                      comparison['SO_diff'].abs().sum() + 
                      comparison['HR_diff'].abs().sum())
        
        total_stats = (comparison['BF'].sum() + comparison['H'].sum() + 
                      comparison['BB'].sum() + comparison['SO'].sum() + comparison['HR'].sum())
        
        accuracy = ((total_stats - total_diffs) / total_stats * 100) if total_stats > 0 else 0
        
        # Show results
        print(f"📊 PITCHING VALIDATION RESULTS:")
        print(f"   Pitchers compared: {len(comparison)}")
        print(f"   Total differences: {total_diffs}")
        print(f"   Total stats compared: {total_stats}")
        print(f"   Accuracy: {accuracy:.1f}%")
        
        # Show any differences
        has_diffs = (comparison['BF_diff'].abs() + comparison['H_diff'].abs() + 
                    comparison['BB_diff'].abs() + comparison['SO_diff'].abs() + 
                    comparison['HR_diff'].abs()) > 0
        
        if has_diffs.any():
            print(f"\n⚠️  PITCHERS WITH DIFFERENCES:")
            diff_pitchers = comparison[has_diffs]
            for _, pitcher in diff_pitchers.iterrows():
                diffs = []
                if pitcher['BF_diff'] != 0:
                    diffs.append(f"BF: {pitcher['BF_diff']:+.0f}")
                if pitcher['H_diff'] != 0:
                    diffs.append(f"H: {pitcher['H_diff']:+.0f}")
                if pitcher['BB_diff'] != 0:
                    diffs.append(f"BB: {pitcher['BB_diff']:+.0f}")
                if pitcher['SO_diff'] != 0:
                    diffs.append(f"SO: {pitcher['SO_diff']:+.0f}")
                if pitcher['HR_diff'] != 0:
                    diffs.append(f"HR: {pitcher['HR_diff']:+.0f}")
                
                if diffs:
                    print(f"   {pitcher['pitcher_name']}: {', '.join(diffs)}")
        else:
            print(f"\n✅ PERFECT PITCHING ACCURACY - NO DIFFERENCES!")
        
        return {
            'accuracy': accuracy,
            'total_differences': total_diffs,
            'total_stats': total_stats,
            'status': 'perfect' if accuracy == 100 else 'good' if accuracy >= 99 else 'needs_work',
            'comparison_df': comparison
        }

class SingleGameValidator:
    """Validate parsing accuracy for a single game"""
    
    def __init__(self):
        self.official_parser = OfficialStatsParser()
        
    def validate_single_game(self, game_url: str) -> Dict:
        """Process and validate a single game"""
        
        print(f"🎯 VALIDATING SINGLE GAME:")
        print(f"   URL: {game_url}")
        print("=" * 60)
        
        # Fetch the page
        soup = SafePageFetcher.fetch_page(game_url)
        
        # Parse official stats
        official_batting = self.official_parser.parse_official_batting_stats(soup)
        
        # Parse simple events
        event_parser = EnhancedSimpleEventParser(self.official_parser.name_resolver)
        parsed_events = event_parser.parse_simple_events(soup)

        pitching_validator = PitchingStatsValidator(self.official_parser.name_resolver)
        pitching_results = pitching_validator.validate_pitching_stats(soup)
        
        # Validate accuracy
        validation_results = self._validate_accuracy_with_debug(official_batting, parsed_events)
        
        return {
           'game_url': game_url,
           'official_batting_stats': official_batting,
           'parsed_events': parsed_events,
           'validation_results': validation_results,
           # ADD THESE:
           'official_pitching_stats': pitching_results['official_pitching_stats'],
           'parsed_pitching_events': pitching_results['parsed_pitching_events'],
           'pitching_validation_results': pitching_results['validation_results']
       }
    
    def _validate_accuracy(self, official_stats: pd.DataFrame, parsed_events: pd.DataFrame) -> Dict:
        """Compare parsed events vs official stats"""
        
        print("\n🔍 VALIDATING ACCURACY:")
        print("-" * 30)
        
        if official_stats.empty or parsed_events.empty:
            print("❌ No data to validate")
            return {'accuracy': 0, 'status': 'failed'}
        
        # DEBUG: Show what we have
        print(f"📊 Official stats players: {len(official_stats)}")
        print(f"📊 Parsed events players: {len(parsed_events['batter_name'].unique())}")
        
        # Show some examples of each
        print(f"\n📋 Sample official players:")
        for name in official_stats['player_name'].head(5):
            print(f"   '{name}'")
        
        print(f"\n📋 Sample parsed players:")
        for name in parsed_events['batter_name'].unique()[:5]:
            print(f"   '{name}'")
        
        # Aggregate parsed events by player
        parsed_stats = self._aggregate_parsed_events(parsed_events)
        
        print(f"\n📊 After aggregation:")
        print(f"   Official: {len(official_stats)} players")
        print(f"   Parsed: {len(parsed_stats)} players")
        
        # Compare stats we can measure
        validation_details = self._compare_parseable_stats(official_stats, parsed_stats)
        
        return validation_details
    
    def _aggregate_parsed_events(self, events_df: pd.DataFrame) -> pd.DataFrame:
        """Aggregate events into player stats"""
        
        if events_df.empty:
            return pd.DataFrame()
        
        # DEBUG: Check for duplicate player names
        player_counts = events_df['batter_name'].value_counts()
        print(f"\n📊 Player event counts:")
        for player, count in player_counts.head(10).items():
            print(f"   {player}: {count} events")
        
        # Group by player and aggregate
        aggregated = events_df.groupby('batter_name').agg({
            'is_at_bat': 'sum',
            'is_hit': 'sum', 
            'is_walk': 'sum',
            'is_strikeout': 'sum'
        }).reset_index()
        
        # Rename columns to match official stats
        aggregated.columns = ['player_name', 'parsed_AB', 'parsed_H', 'parsed_BB', 'parsed_SO']
        
        print(f"\n📊 Aggregated stats sample:")
        for _, row in aggregated.head(5).iterrows():
            print(f"   {row['player_name']}: AB={row['parsed_AB']}, H={row['parsed_H']}, BB={row['parsed_BB']}, SO={row['parsed_SO']}")
        
        return aggregated
    
    def _compare_parseable_stats(self, official: pd.DataFrame, parsed: pd.DataFrame) -> Dict:
        """Compare only the stats we can reliably parse"""
        
        print(f"\n🔍 MERGE DEBUG:")
        print(f"   Official shape: {official.shape}")
        print(f"   Parsed shape: {parsed.shape}")
        
        # DEBUG: Check for exact name matches
        official_players = set(official['player_name'].tolist())
        parsed_players = set(parsed['player_name'].tolist())
        
        print(f"   Common players: {len(official_players & parsed_players)}")
        print(f"   Only in official: {len(official_players - parsed_players)}")
        print(f"   Only in parsed: {len(parsed_players - official_players)}")
        
        # DETAILED DEBUG: Show exact name differences
        print(f"\n🔍 DETAILED NAME COMPARISON:")
        
        # Find players that should match but don't
        for parsed_name in list(parsed_players)[:5]:
            print(f"\n   Parsed: '{parsed_name}' (len={len(parsed_name)})")
            # Look for similar names in official
            similar_official = [name for name in official_players 
                              if parsed_name.lower() in name.lower() or name.lower() in parsed_name.lower()]
            if similar_official:
                for official_name in similar_official[:2]:
                    print(f"   Official: '{official_name}' (len={len(official_name)})")
                    print(f"   Match: {parsed_name == official_name}")
                    # Show character-by-character differences
                    if parsed_name != official_name:
                        print(f"   Char diff: {[c for c in parsed_name]} vs {[c for c in official_name]}")
            else:
                print(f"   No similar names found in official")
        
        # Show some examples of mismatches
        if parsed_players - official_players:
            print(f"\n   📋 First 5 only in parsed:")
            for name in list(parsed_players - official_players)[:5]:
                print(f"      '{name}' (repr: {repr(name)})")
        
        if official_players - parsed_players:
            print(f"\n   📋 First 5 only in official:")
            for name in list(official_players - parsed_players)[:5]:
                print(f"      '{name}' (repr: {repr(name)})")
        
        # Merge the dataframes - ONLY include players we have data for both
        comparison = pd.merge(official, parsed, on='player_name', how='inner')  # Changed to inner!
        
        print(f"   After inner merge: {comparison.shape}")
        
        if comparison.empty:
            print("❌ No matching players found!")
            return {'accuracy': 0, 'status': 'failed', 'total_differences': 0, 'total_stats': 0}
        
        # Fill any remaining NaN values
        comparison = comparison.fillna(0)
        
        # Calculate differences for stats we actually parsed
        comparison['AB_diff'] = comparison['parsed_AB'] - comparison['AB']
        comparison['H_diff'] = comparison['parsed_H'] - comparison['H']
        comparison['BB_diff'] = comparison['parsed_BB'] - comparison['BB']
        comparison['SO_diff'] = comparison['parsed_SO'] - comparison['SO']
        
        # Calculate accuracy
        total_diffs = (comparison['AB_diff'].abs().sum() + 
                      comparison['H_diff'].abs().sum() + 
                      comparison['BB_diff'].abs().sum() + 
                      comparison['SO_diff'].abs().sum())
        
        total_stats = (comparison['AB'].sum() + comparison['H'].sum() + 
                      comparison['BB'].sum() + comparison['SO'].sum())
        
        accuracy = ((total_stats - total_diffs) / total_stats * 100) if total_stats > 0 else 0
        
        # Show results
        print(f"📊 VALIDATION RESULTS:")
        print(f"   Players compared: {len(comparison)}")
        print(f"   Total differences: {total_diffs}")
        print(f"   Total stats compared: {total_stats}")
        print(f"   Accuracy: {accuracy:.1f}%")
        
        # Show any differences
        has_diffs = (comparison['AB_diff'].abs() + comparison['H_diff'].abs() + 
                    comparison['BB_diff'].abs() + comparison['SO_diff'].abs()) > 0
        
        if has_diffs.any():
            print(f"\n⚠️  PLAYERS WITH DIFFERENCES:")
            diff_players = comparison[has_diffs]
            for _, player in diff_players.iterrows():
                diffs = []
                if player['AB_diff'] != 0:
                    diffs.append(f"AB: {player['AB_diff']:+.0f}")
                if player['H_diff'] != 0:
                    diffs.append(f"H: {player['H_diff']:+.0f}")
                if player['BB_diff'] != 0:
                    diffs.append(f"BB: {player['BB_diff']:+.0f}")
                if player['SO_diff'] != 0:
                    diffs.append(f"SO: {player['SO_diff']:+.0f}")
                
                if diffs:
                    print(f"   {player['player_name']}: {', '.join(diffs)}")
        else:
            print(f"\n✅ PERFECT ACCURACY - NO DIFFERENCES!")
        
        return {
            'accuracy': accuracy,
            'total_differences': total_diffs,
            'total_stats': total_stats,
            'status': 'perfect' if accuracy == 100 else 'good' if accuracy >= 99 else 'needs_work',
            'comparison_df': comparison
        }

    def debug_name_matching(self, official_stats: pd.DataFrame, parsed_events: pd.DataFrame) -> Dict[str, str]:
        """Debug and fix name matching issues"""
        
        print(f"\n🔍 DEBUGGING NAME MATCHING:")
        print("-" * 40)
        
        if official_stats.empty or parsed_events.empty:
            print("❌ Empty dataframes - cannot debug")
            return {}
        
        official_names = set(official_stats['player_name'].tolist())
        parsed_names = set(parsed_events['batter_name'].unique())
        
        print(f"📊 Before normalization:")
        print(f"   Official players: {len(official_names)}")
        print(f"   Parsed players: {len(parsed_names)}")
        print(f"   Exact matches: {len(official_names & parsed_names)}")
        
        # Show character-level differences for first few names
        print(f"\n🔍 CHARACTER ANALYSIS (first 3 names):")
        
        official_sample = list(official_names)[:3]
        parsed_sample = list(parsed_names)[:3]
        
        for i, name in enumerate(official_sample):
            print(f"   Official[{i}]: '{name}' | len={len(name)} | repr={repr(name)}")
            print(f"   Chars: {[ord(c) for c in name]}")
        
        for i, name in enumerate(parsed_sample):
            print(f"   Parsed[{i}]: '{name}' | len={len(name)} | repr={repr(name)}")
            print(f"   Chars: {[ord(c) for c in name]}")
        
        # Try different normalization strategies
        name_resolver = self._find_best_normalization(official_names, parsed_names)
        
        return name_resolver

    def _find_best_normalization(self, official_names: set[str], parsed_names: set[str]) -> Dict[str, str]:
        """Try different normalization strategies to find matches"""
        
        print(f"\n🔧 TESTING NORMALIZATION STRATEGIES:")
        
        strategies = [
            ('unicode_nfkd', lambda x: unicodedata.normalize('NFKD', x)),
            ('strip_spaces', lambda x: re.sub(r'\s+', ' ', x.strip())),
            ('remove_nbsp', lambda x: x.replace('\xa0', ' ').strip()),
            ('clean_whitespace', lambda x: re.sub(r'[\s\xa0]+', ' ', x).strip()),
            ('full_normalize', lambda x: unicodedata.normalize('NFKD', re.sub(r'[\s\xa0]+', ' ', x).strip())),
        ]
        
        best_resolver = {}
        best_matches = 0
        
        for strategy_name, normalize_func in strategies:
            print(f"\n   Testing: {strategy_name}")
            
            # Normalize both sets
            official_normalized = {}
            parsed_normalized = {}
            
            for name in official_names:
                normalized = normalize_func(name)
                official_normalized[normalized] = name
            
            for name in parsed_names:
                normalized = normalize_func(name)
                parsed_normalized[normalized] = name
            
            # Find matches
            matches = set(official_normalized.keys()) & set(parsed_normalized.keys())
            
            print(f"   Matches: {len(matches)}")
            
            if len(matches) > best_matches:
                best_matches = len(matches)
                # Create resolver mapping parsed -> official
                best_resolver = {}
                for normalized_name in matches:
                    parsed_original = parsed_normalized[normalized_name]
                    official_original = official_normalized[normalized_name]
                    best_resolver[parsed_original] = official_original
                
                print(f"   ✅ NEW BEST! {len(matches)} matches")
                
                # Show some examples
                for normalized_name in list(matches)[:3]:
                    parsed_orig = parsed_normalized[normalized_name]
                    official_orig = official_normalized[normalized_name]
                    print(f"      '{parsed_orig}' -> '{official_orig}'")
        
        print(f"\n🎯 BEST STRATEGY FOUND: {best_matches} matches")
        return best_resolver

    def _validate_accuracy_with_debug(self, official_stats: pd.DataFrame, parsed_events: pd.DataFrame) -> Dict:
        """Enhanced validation with name debugging"""
        
        print("\n🔍 VALIDATING ACCURACY WITH DEBUG:")
        print("-" * 30)
        
        if official_stats.empty or parsed_events.empty:
            print("❌ No data to validate")
            return {'accuracy': 0, 'status': 'failed'}
        
        # DEBUG name matching first
        name_resolver = self.debug_name_matching(official_stats, parsed_events)
        
        # Apply name resolution if we found mappings
        if name_resolver:
            print(f"\n🔧 APPLYING NAME RESOLUTION:")
            print(f"   Found {len(name_resolver)} name mappings")
            
            # Apply resolution to parsed events
            parsed_events = parsed_events.copy()
            parsed_events['resolved_batter_name'] = parsed_events['batter_name'].map(
                lambda x: name_resolver.get(x, x)
            )
            
            # Use resolved names for aggregation
            parsed_events_for_agg = parsed_events.copy()
            parsed_events_for_agg['batter_name'] = parsed_events_for_agg['resolved_batter_name']
        
        # Continue with original validation logic
        parsed_stats = self._aggregate_parsed_events(parsed_events_for_agg if name_resolver else parsed_events)
        
        print(f"\n📊 After name resolution:")
        print(f"   Official: {len(official_stats)} players")
        print(f"   Parsed: {len(parsed_stats)} players")
        
        # Compare stats
        validation_details = self._compare_parseable_stats(official_stats, parsed_stats)
        
        # Add debug info
        validation_details['name_resolver'] = name_resolver
        validation_details['resolution_applied'] = bool(name_resolver)
        
        return validation_details

# TEST FUNCTION
def test_single_game():
    """Test the validator on a single game"""
    
    # Use one of your test games
    test_url = "https://www.baseball-reference.com/boxes/NYA/NYA202503270.shtml"
    
    validator = SingleGameValidator()
    results = validator.validate_single_game(test_url)
    
    print(f"\n🎯 FINAL RESULTS:")
    print(f"   Status: {results['validation_results']['status']}")
    print(f"   Accuracy: {results['validation_results']['accuracy']:.1f}%")
    
    if results['validation_results']['accuracy'] == 100:
        print(f"🎉 PERFECT! Ready for multiple games.")
    else:
        print(f"🔧 Need to refine parsing logic.")
    
    return results

if __name__ == "__main__":
    test_single_game()