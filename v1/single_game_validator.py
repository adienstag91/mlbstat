"""
Clean Single Game Validator - Production Ready
==============================================

Streamlined, production-ready validator with minimal debug output.
Maintains 100% functionality with much cleaner code.
"""

import pandas as pd
import numpy as np
import re
import unicodedata
from bs4 import BeautifulSoup
from io import StringIO
from playwright.sync_api import sync_playwright
from dataclasses import dataclass
from typing import Dict, List, Optional
import time

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
                if attempt < max_retries - 1:
                    time.sleep(3 * (attempt + 1))
                else:
                    raise Exception(f"Failed to fetch {url} after {max_retries} attempts")

class OfficialStatsParser:
    """Parse official stats from box score"""
    
    def __init__(self):
        self.canonical_names = set()
        self.name_resolver = None
    
    def parse_official_batting_stats(self, soup: BeautifulSoup) -> pd.DataFrame:
        """Parse official batting statistics from box score tables"""
        # Extract canonical names (batting + pitching)
        self.canonical_names = self._extract_canonical_names(soup)
        self.name_resolver = self._build_name_resolver()
        
        batting_tables = soup.find_all('table', {'id': lambda x: x and x.endswith('batting')})
        if not batting_tables:
            return pd.DataFrame()
        
        all_batting_stats = []
        for table in batting_tables:
            team_stats = self._parse_batting_table(table)
            all_batting_stats.extend(team_stats)
        
        return pd.DataFrame(all_batting_stats) if all_batting_stats else pd.DataFrame()
    
    def _extract_canonical_names(self, soup: BeautifulSoup) -> set:
        """Extract canonical player names from batting and pitching box scores"""
        canonical_names = set()
        
        # Extract from both batting and pitching tables
        for table_type in ['batting', 'pitching']:
            tables = soup.find_all('table', {'id': lambda x: x and table_type in x.lower()})
            for table in tables:
                rows = table.find_all('tr')
                for row in rows:
                    name_cell = row.find('th', {'data-stat': 'player'})
                    if name_cell and name_cell.get_text(strip=True):
                        name = name_cell.get_text(strip=True)
                        if name and name not in ['Player', '', 'Batting', 'Pitching', 'Team Totals']:
                            cleaned = re.sub(r"((?:[A-Z0-9]{1,3})(?:-[A-Z0-9]{1,3})*)$", "", name.strip())
                            cleaned = cleaned.replace("\xa0", " ").strip()
                            if cleaned:
                                canonical_names.add(cleaned)
        
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
                    abbreviated = f"{first[0]}. {last}"
                    name_mappings[abbreviated] = canonical_name
        
        return name_mappings
    
    def _parse_batting_table(self, table) -> List[Dict]:
        """Parse a single batting table"""
        try:
            df = pd.read_html(StringIO(str(table)))[0]
        except Exception:
            return []
        
        df = df[df['Batting'].notna()]
        df = df[~df['Batting'].str.contains("Team Totals", na=False)]
        
        batting_stats = []
        for _, row in df.iterrows():
            player_name = self._clean_player_name(row['Batting'])
            if not player_name:
                continue
            
            def safe_int(value, default=0):
                if pd.isna(value):
                    return default
                try:
                    return int(float(value))
                except (ValueError, TypeError):
                    return default
            
            stats = {
                'player_name': player_name,
                'AB': safe_int(row.get('AB', 0)),
                'R': safe_int(row.get('R', 0)),
                'H': safe_int(row.get('H', 0)),
                'RBI': safe_int(row.get('RBI', 0)),
                'BB': safe_int(row.get('BB', 0)),
                'SO': safe_int(row.get('SO', 0)),
                'PA': safe_int(row.get('PA', 0)),
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
        pattern = rf"(\d+)·{stat}|(?:^|,)\s*{stat}(?:,|$)"
        match = re.search(pattern, details)
        
        if match:
            return int(match.group(1)) if match.group(1) else 1
        return 0

class EventParser:
    """Parse batting events from play-by-play"""
    
    def __init__(self, name_resolver: Dict[str, str]):
        self.name_resolver = name_resolver
    
    def parse_events(self, soup) -> pd.DataFrame:
        """Parse events with enhanced at-bat detection"""
        pbp_table = soup.find("table", id="play_by_play")
        if not pbp_table:
            return pd.DataFrame()
        
        try:
            df = pd.read_html(StringIO(str(pbp_table)))[0]
        except Exception:
            return pd.DataFrame()
        
        # Clean the data
        df = df[df['Inn'].notna() & df['Play Description'].notna() & df['Batter'].notna()]
        df = df[~df['Batter'].str.contains("Top of the|Bottom of the|inning", case=False, na=False)]
        df = df[~df['Batter'].str.contains("Team Totals", case=False, na=False)]
        
        events = []
        for _, row in df.iterrows():
            event = self._parse_event(row)
            if event:
                events.append(event)
        
        return pd.DataFrame(events) if events else pd.DataFrame()
    
    def _parse_event(self, row: pd.Series) -> Optional[Dict]:
        """Parse a single event"""
        batter_name = str(row['Batter']).strip()
        description = str(row['Play Description']).strip()
        resolved_batter = self.name_resolver.get(batter_name, batter_name)
        
        outcome = self._analyze_outcome(description)
        if not outcome:
            return None
        
        return {
            'batter_name': resolved_batter,
            'description': description,
            'is_plate_appearance': outcome['is_plate_appearance'],
            'is_at_bat': outcome['is_at_bat'],
            'is_hit': outcome['is_hit'],
            'hit_type': outcome.get('hit_type'),
            'is_walk': outcome['is_walk'],
            'is_strikeout': outcome['is_strikeout'],
            'bases_reached': outcome['bases_reached'],
        }
    
    def _analyze_outcome(self, description: str) -> Optional[Dict]:
        """Analyze play outcome"""
        desc = description.lower().strip()
        
        # Sacrifice flies (not at-bats) - check first!
        if re.search(r'sacrifice fly|sac fly|flyball.*sacrifice fly', desc):
            return {'is_plate_appearance': True,'is_at_bat': False, 'is_hit': False, 'is_walk': False, 'is_strikeout': False, 'bases_reached': 0}
        
        # Sacrifice bunts (not at-bats)
        if re.search(r'sacrifice bunt|sac bunt', desc):
            return {'is_plate_appearance': True,'is_at_bat': False, 'is_hit': False, 'is_walk': False, 'is_strikeout': False, 'bases_reached': 0}
        
        # Walks (not at-bats)
        if re.search(r'^walk\b|^intentional walk', desc):
            return {'is_plate_appearance': True,'is_at_bat': False, 'is_hit': False, 'is_walk': True, 'is_strikeout': False, 'bases_reached': 1}
        
        # Hit by pitch (not at-bats)
        if re.search(r'^hit by pitch|^hbp\b', desc):
            return {'is_plate_appearance': True,'is_at_bat': False, 'is_hit': False, 'is_walk': False, 'is_strikeout': False, 'bases_reached': 1}
        
        # Strikeouts (at-bats)
        if re.search(r'^strikeout\b|^struck out', desc):
            return {'is_plate_appearance': True,'is_at_bat': True, 'is_hit': False, 'is_walk': False, 'is_strikeout': True, 'bases_reached': 0}
        
        # Home runs (at-bats, hits)
        if re.search(r'^home run\b|^hr\b', desc):
            return {'is_plate_appearance': True,'is_at_bat': True, 'is_hit': True, 'hit_type': 'home_run', 'is_walk': False, 'is_strikeout': False, 'bases_reached': 4}
        
        # Other hits (at-bats, hits)
        hit_patterns = [
            (r'^single\b.*(?:to|up|through)', 'single', 1),
            (r'^double\b.*(?:to|down)', 'double', 2),
            (r'^triple\b.*(?:to|down)', 'triple', 3)
        ]
        
        for pattern, hit_type, bases in hit_patterns:
            if re.search(pattern, desc):
                return {'is_plate_appearance': True,'is_at_bat': True, 'is_hit': True, 'hit_type': hit_type, 'is_walk': False, 'is_strikeout': False, 'bases_reached': bases}
        
        # Outs (at-bats, not hits)
        out_patterns = [
            r'^grounded out\b', r'^ground out\b', r'^flied out\b', r'^lined out\b',
            r'^popped out\b', r'^fouled out\b', r'^groundout\b', r'^flyout\b',
            r'^lineout\b', r'^popout\b', r'popfly\b', r'flyball:',
            r'grounded into double play|gdp\b|double play'
        ]
        
        for pattern in out_patterns:
            if re.search(pattern, desc):
                return {'is_plate_appearance': True,'is_at_bat': True, 'is_hit': False, 'is_walk': False, 'is_strikeout': False, 'bases_reached': 0}
        
        return None  # Skip ambiguous cases

class PitchingStatsParser:
    """Parse pitching stats and events"""
    
    def __init__(self, name_resolver: Dict[str, str]):
        self.name_resolver = name_resolver
    
    def parse_official_pitching_stats(self, soup: BeautifulSoup) -> pd.DataFrame:
        """Parse official pitching statistics"""
        pitching_tables = soup.find_all('table', {'id': lambda x: x and x.endswith('pitching')})
        if not pitching_tables:
            return pd.DataFrame()
        
        all_pitching_stats = []
        for table in pitching_tables:
            team_stats = self._parse_pitching_table(table)
            all_pitching_stats.extend(team_stats)
        
        return pd.DataFrame(all_pitching_stats) if all_pitching_stats else pd.DataFrame()
    
    def parse_pitching_events(self, soup: BeautifulSoup) -> pd.DataFrame:
        """Parse pitching events from play-by-play"""
        pbp_table = soup.find("table", id="play_by_play")
        if not pbp_table:
            return pd.DataFrame()
        
        try:
            df = pd.read_html(StringIO(str(pbp_table)))[0]
        except Exception:
            return pd.DataFrame()
        
        df = df[df['Inn'].notna() & df['Play Description'].notna() & df['Pitcher'].notna()]
        df = df[~df['Pitcher'].str.contains("Top of the|Bottom of the|inning", case=False, na=False)]
        df = df[~df['Pitcher'].str.contains("Team Totals", case=False, na=False)]
        
        events = []
        for _, row in df.iterrows():
            event = self._parse_pitching_event(row)
            if event:
                events.append(event)
        
        return pd.DataFrame(events) if events else pd.DataFrame()
    
    def _parse_pitching_table(self, table) -> List[Dict]:
        """Parse a single pitching table"""
        try:
            df = pd.read_html(StringIO(str(table)))[0]
        except Exception:
            return []
        
        df = df[df['Pitching'].notna()]
        df = df[~df['Pitching'].str.contains("Team Totals", na=False)]
        
        pitching_stats = []
        for _, row in df.iterrows():
            pitcher_name = self._clean_pitcher_name(row['Pitching'])
            if not pitcher_name:
                continue
            
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
            
            stats = {
                'pitcher_name': pitcher_name,
                'IP': safe_float(row.get('IP', 0.0)),
                'H': safe_int(row.get('H', 0)),
                'R': safe_int(row.get('R', 0)),
                'ER': safe_int(row.get('ER', 0)),
                'BB': safe_int(row.get('BB', 0)),
                'SO': safe_int(row.get('SO', 0)),
                'HR': safe_int(row.get('HR', 0)),
                'BF': safe_int(row.get('BF', 0)),
                'Pit': safe_int(row.get('Pit', 0)),
                'Dec': str(row.get('Dec', '')).strip() if pd.notna(row.get('Dec', '')) else '',
            }
            pitching_stats.append(stats)
        
        return pitching_stats
    
    def _clean_pitcher_name(self, name: str) -> str:
        """Clean pitcher name from box score"""
        if pd.isna(name):
            return ""
        
        # Remove any trailing codes AND results like ", H (1)", ", W (1-0)", ", L (0-1)", ", S (1)", etc.
        cleaned = re.sub(r"((?:[A-Z0-9]{1,3})(?:-[A-Z0-9]{1,3})*)$", "", str(name).strip())
        cleaned = re.sub(r',\s*[WLSHB]+\s*\([^)]*\)$', '', cleaned)  # Remove ", H (1)", ", W (1-0)" etc.
        return cleaned.replace("\xa0", " ").strip()
    
    def _parse_pitching_event(self, row: pd.Series) -> Optional[Dict]:
        """Parse a single pitching event"""
        pitcher_name = str(row['Pitcher']).strip()
        description = str(row['Play Description']).strip()
        pitch_count = self._parse_pitch_count(row.get('Cnt', ''))
        resolved_pitcher = self.name_resolver.get(pitcher_name, pitcher_name)
        
        outcome = self._analyze_pitching_outcome(description)
        if not outcome:
            return None
        
        return {
            'pitcher_name': resolved_pitcher,
            'description': description,
            'pitch_count': pitch_count,
            'batter_faced': 1,
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
        
        match = re.match(r'^(\d+)', str(count_str).strip())
        if match:
            try:
                return int(match.group(1))
            except ValueError:
                return 0
        return 0
    
    def _analyze_pitching_outcome(self, description: str) -> Optional[Dict]:
        """Analyze pitching outcome"""
        desc = description.lower().strip()
        outcome = {'hit_allowed': False, 'walk_allowed': False, 'strikeout': False, 'home_run_allowed': False, 'out_recorded': False}
        
        # Sacrifice flies - outs recorded
        if re.search(r'sacrifice fly|sac fly|flyball.*sacrifice fly', desc):
            outcome['out_recorded'] = True
            return outcome
        
        # Walks
        if re.search(r'^walk\b|^intentional walk', desc):
            outcome['walk_allowed'] = True
            return outcome
        
        # Strikeouts
        if re.search(r'^strikeout\b|^struck out', desc):
            outcome['strikeout'] = True
            outcome['out_recorded'] = True
            return outcome
        
        # Home runs
        if re.search(r'^home run\b|^hr\b', desc):
            outcome['hit_allowed'] = True
            outcome['home_run_allowed'] = True
            return outcome
        
        # Other hits
        hit_patterns = [r'^single\b.*(?:to|up|through)', r'^double\b.*(?:to|down)', r'^triple\b.*(?:to|down)']
        for pattern in hit_patterns:
            if re.search(pattern, desc):
                outcome['hit_allowed'] = True
                return outcome
        
        # Outs
        out_patterns = [
            r'^grounded out\b', r'^ground out\b', r'^flied out\b', r'^lined out\b',
            r'^popped out\b', r'^fouled out\b', r'^groundout\b', r'^flyout\b',
            r'^lineout\b', r'^popout\b', r'popfly\b', r'flyball:',
            r'grounded into double play|gdp\b|double play'
        ]
        
        for pattern in out_patterns:
            if re.search(pattern, desc):
                outcome['out_recorded'] = True
                return outcome
        
        # Hit by pitch
        if re.search(r'^hit by pitch|^hbp\b', desc):
            return outcome  # All False - special case
        
        return None  # Skip ambiguous cases

class NameNormalizer:
    """Handle name normalization between official stats and parsed events"""
    
    @staticmethod
    def normalize_names(official_names: set, parsed_names: set) -> Dict[str, str]:
        """Find best normalization strategy"""
        def clean_all_issues(name):
            # Remove result suffixes
            cleaned = re.sub(r',\s*[WLSHB]+\s*\([^)]*\)$', '', name)
            # Normalize unicode and whitespace  
            cleaned = unicodedata.normalize('NFKD', re.sub(r'[\s\xa0]+', ' ', cleaned).strip())
            return cleaned
        
        strategies = [
            ('unicode_nfkd', lambda x: unicodedata.normalize('NFKD', x)),
            ('clean_whitespace', lambda x: re.sub(r'[\s\xa0]+', ' ', x).strip()),
            ('full_normalize', lambda x: unicodedata.normalize('NFKD', re.sub(r'[\s\xa0]+', ' ', x).strip())),
            ('clean_all', clean_all_issues),
        ]
        
        best_resolver = {}
        best_matches = 0
        
        for strategy_name, normalize_func in strategies:
            official_normalized = {normalize_func(name): name for name in official_names}
            parsed_normalized = {normalize_func(name): name for name in parsed_names}
            matches = set(official_normalized.keys()) & set(parsed_normalized.keys())
            
            if len(matches) > best_matches:
                best_matches = len(matches)
                best_resolver = {parsed_normalized[norm]: official_normalized[norm] for norm in matches}
        
        return best_resolver

class SingleGameValidator:
    """Clean, streamlined game validator"""
    
    def __init__(self):
        self.official_parser = OfficialStatsParser()
    
    def validate_single_game(self, game_url: str) -> Dict:
        """Process and validate a single game"""
        # Fetch the page
        soup = SafePageFetcher.fetch_page(game_url)
        
        # Parse official batting stats
        official_batting = self.official_parser.parse_official_batting_stats(soup)
        
        # Parse batting events
        event_parser = EventParser(self.official_parser.name_resolver)
        parsed_events = event_parser.parse_events(soup)
        print(parsed_events)

        # Parse pitching
        pitching_parser = PitchingStatsParser(self.official_parser.name_resolver)
        official_pitching = pitching_parser.parse_official_pitching_stats(soup)
        parsed_pitching_events = pitching_parser.parse_pitching_events(soup)
        print(parsed_pitching_events)
        
        # Validate accuracy
        batting_validation = self._validate_batting_accuracy(official_batting, parsed_events)
        pitching_validation = self._validate_pitching_accuracy(official_pitching, parsed_pitching_events)
        
        return {
            'game_url': game_url,
            'official_batting_stats': official_batting,
            'parsed_events': parsed_events,
            'validation_results': batting_validation,
            'official_pitching_stats': official_pitching,
            'parsed_pitching_events': parsed_pitching_events,
            'pitching_validation_results': pitching_validation
        }
    
    def _validate_batting_accuracy(self, official_stats: pd.DataFrame, parsed_events: pd.DataFrame) -> Dict:
        """Validate batting accuracy"""
        if official_stats.empty or parsed_events.empty:
            return {'accuracy': 0, 'status': 'failed'}
        
        # Normalize names
        official_names = set(official_stats['player_name'].tolist())
        parsed_names = set(parsed_events['batter_name'].unique())
        name_resolver = NameNormalizer.normalize_names(official_names, parsed_names)
        
        # Apply name resolution
        if name_resolver:
            parsed_events = parsed_events.copy()
            parsed_events['batter_name'] = parsed_events['batter_name'].map(lambda x: name_resolver.get(x, x))
        
        # Aggregate parsed events
        parsed_stats = parsed_events.groupby('batter_name').agg({
            'is_at_bat': 'sum', 'is_hit': 'sum', 'is_walk': 'sum', 'is_strikeout': 'sum'
        }).reset_index()
        parsed_stats.columns = ['player_name', 'parsed_AB', 'parsed_H', 'parsed_BB', 'parsed_SO']
        
        # Compare stats
        return self._compare_stats(official_stats, parsed_stats, ['AB', 'H', 'BB', 'SO'])
    
    def _validate_pitching_accuracy(self, official_stats: pd.DataFrame, parsed_events: pd.DataFrame) -> Dict:
        """Validate pitching accuracy"""
        if official_stats.empty or parsed_events.empty:
            return {'accuracy': 0, 'status': 'failed'}
        
        # Normalize names
        official_names = set(official_stats['pitcher_name'].tolist())
        parsed_names = set(parsed_events['pitcher_name'].unique())
        name_resolver = NameNormalizer.normalize_names(official_names, parsed_names)
                
        # Apply name resolution
        if name_resolver:
            parsed_events = parsed_events.copy()
            parsed_events['pitcher_name'] = parsed_events['pitcher_name'].map(lambda x: name_resolver.get(x, x))
        
        # Aggregate parsed events
        parsed_stats = parsed_events.groupby('pitcher_name').agg({
            'batter_faced': 'sum', 'hit_allowed': 'sum', 'walk_allowed': 'sum',
            'strikeout': 'sum', 'home_run_allowed': 'sum'
        }).reset_index()
        parsed_stats.columns = ['pitcher_name', 'parsed_BF', 'parsed_H', 'parsed_BB', 'parsed_SO', 'parsed_HR']
        
        # Compare stats
        return self._compare_stats(official_stats, parsed_stats, ['BF', 'H', 'BB', 'SO', 'HR'], is_pitching=True)
    
    def _compare_stats(self, official: pd.DataFrame, parsed: pd.DataFrame, stat_cols: List[str], is_pitching: bool = False) -> Dict:
        """Compare official vs parsed stats"""
        name_col = 'pitcher_name' if is_pitching else 'player_name'
        comparison = pd.merge(official, parsed, on=name_col, how='inner')
        
        if comparison.empty:
            return {'accuracy': 0, 'status': 'failed', 'total_differences': 0, 'total_stats': 0, 'players_compared': 0, 'differences': []}
        
        comparison = comparison.fillna(0)
        
        # Calculate differences and track specific players with diffs
        total_diffs = 0
        total_stats = 0
        differences = []
        
        for stat in stat_cols:
            parsed_col = f'parsed_{stat}'
            if parsed_col in comparison.columns:
                comparison[f'{stat}_diff'] = comparison[parsed_col] - comparison[stat]
                diff = comparison[f'{stat}_diff'].abs().sum()
                total_diffs += diff
                total_stats += comparison[stat].sum()
        
        # Find players with differences
        for _, row in comparison.iterrows():
            player_diffs = []
            for stat in stat_cols:
                diff_col = f'{stat}_diff'
                if diff_col in comparison.columns and row[diff_col] != 0:
                    player_diffs.append(f"{stat}: {row[diff_col]:+.0f}")
            
            if player_diffs:
                differences.append({
                    'player': row[name_col],
                    'diffs': player_diffs
                })
        
        accuracy = ((total_stats - total_diffs) / total_stats * 100) if total_stats > 0 else 0
        
        return {
            'accuracy': accuracy,
            'total_differences': int(total_diffs),
            'total_stats': int(total_stats),
            'status': 'perfect' if accuracy == 100 else 'good' if accuracy >= 99 else 'needs_work',
            'players_compared': len(comparison),
            'differences': differences
        }

# Test function
def test_single_game():
    """Test the validator on a single game"""
    test_url = "https://www.baseball-reference.com/boxes/NYA/NYA202503270.shtml"
    
    validator = SingleGameValidator()
    results = validator.validate_single_game(test_url)
    
    # Extract detailed results
    batting_results = results['validation_results']
    pitching_results = results['pitching_validation_results']
    
    batting_acc = batting_results['accuracy']
    pitching_acc = pitching_results['accuracy']
    
    # Count parsed events
    batting_events = len(results['parsed_events']) if not results['parsed_events'].empty else 0
    pitching_events = len(results['parsed_pitching_events']) if not results['parsed_pitching_events'].empty else 0
    
    print(f"✅ Game validated:")
    print(f"   📊 Parsed {batting_events} batting events, {pitching_events} pitching events")
    print(f"   ⚾ Batting: {batting_acc:.1f}% ({batting_results.get('players_compared', 0)}/{len(results['official_batting_stats'])} players, {batting_results.get('total_differences', 0)} diffs/{batting_results.get('total_stats', 0)} stats)")
    print(f"   🥎 Pitching: {pitching_acc:.1f}% ({pitching_results.get('players_compared', 0)}/{len(results['official_pitching_stats'])} pitchers, {pitching_results.get('total_differences', 0)} diffs/{pitching_results.get('total_stats', 0)} stats)")
    
    # Show differences if any
    batting_diffs = batting_results.get('differences', [])
    pitching_diffs = pitching_results.get('differences', [])
    
    if batting_diffs:
        print(f"   ⚠️  Batting differences:")
        for diff in batting_diffs:
            print(f"      {diff['player']}: {', '.join(diff['diffs'])}")
    
    if pitching_diffs:
        print(f"   ⚠️  Pitching differences:")
        for diff in pitching_diffs:
            print(f"      {diff['player']}: {', '.join(diff['diffs'])}")
    
    return results

if __name__ == "__main__":
    test_single_game()