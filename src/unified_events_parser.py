"""
Unified Events Parser - Clean and Focused
========================================

Creates a single events dataframe with both batter and pitcher information.
Much shorter and cleaner than the bloated version.
"""

import pandas as pd
import re
import unicodedata
from bs4 import BeautifulSoup
from io import StringIO
from single_game_validator import SafePageFetcher
from typing import Dict, List, Optional
import uuid

class UnifiedEventsParser:
    """Parse play-by-play into unified events with both batter and pitcher info"""
    
    def __init__(self):
        self.canonical_names = set()
        self.name_resolver = {}
    
    def parse_game(self, game_url: str) -> Dict:
        """Parse a complete game into unified events and official stats"""
        soup = SafePageFetcher.fetch_page(game_url)
        
        # Parse official stats first to build name resolver
        official_batting = self._parse_official_batting(soup)
        #print(official_batting)
        official_pitching = self._parse_official_pitching(soup)
        #print(official_pitching)
        
        # Parse unified events
        game_id = self._extract_game_id(game_url)
        unified_events = self._parse_unified_events(soup, game_id)
        
        # Validate
        batting_validation = self._validate_batting(official_batting, unified_events)
        pitching_validation = self._validate_pitching(official_pitching, unified_events)
        
        return {
            'game_id': game_id,
            'official_batting': official_batting,
            'official_pitching': official_pitching,
            'unified_events': unified_events,
            'batting_validation': batting_validation,
            'pitching_validation': pitching_validation
        }
    
    def _parse_official_batting(self, soup: BeautifulSoup) -> pd.DataFrame:
        """Parse official batting stats and build name resolver"""
        # Extract canonical names from both batting and pitching tables
        self.canonical_names = self._extract_canonical_names(soup)
        self.name_resolver = self._build_name_resolver()
        
        # Parse batting tables
        batting_tables = soup.find_all('table', {'id': lambda x: x and x.endswith('batting')})
        all_stats = []
        
        for table in batting_tables:
            try:
                df = pd.read_html(StringIO(str(table)))[0]
                df = df[df['Batting'].notna()]
                df = df[~df['Batting'].str.contains("Team Totals", na=False)]
                
                for _, row in df.iterrows():
                    player_name = self._normalize_name(row['Batting'])
                    if player_name:
                        all_stats.append({
                            'player_name': player_name,
                            'AB': self._safe_int(row.get('AB', 0)),
                            'H': self._safe_int(row.get('H', 0)),
                            'BB': self._safe_int(row.get('BB', 0)),
                            'SO': self._safe_int(row.get('SO', 0)),
                            'PA': self._safe_int(row.get('PA', 0)),
                            'HR': self._extract_from_details(row, 'HR'),
                            '2B': self._extract_from_details(row, '2B'),
                            '3B': self._extract_from_details(row, '3B'),
                            'SB': self._extract_from_details(row, 'SB'),
                            'CS': self._extract_from_details(row, 'CS'),
                            'HBP': self._extract_from_details(row, 'HBP'),
                            'GDP': self._extract_from_details(row, 'GDP'),
                            'SF': self._extract_from_details(row, 'SF'),
                            'SH': self._extract_from_details(row, 'SH'),
                        })
            except Exception:
                continue
        
        return pd.DataFrame(all_stats)
    
    def _parse_official_pitching(self, soup: BeautifulSoup) -> pd.DataFrame:
        """Parse official pitching stats"""
        pitching_tables = soup.find_all('table', {'id': lambda x: x and x.endswith('pitching')})
        all_stats = []
        
        for table in pitching_tables:
            try:
                df = pd.read_html(StringIO(str(table)))[0]
                df = df[df['Pitching'].notna()]
                df = df[~df['Pitching'].str.contains("Team Totals", na=False)]
                
                for _, row in df.iterrows():
                    pitcher_name = self._normalize_name(row['Pitching'])
                    if pitcher_name:
                        all_stats.append({
                            'pitcher_name': pitcher_name,
                            'BF': self._safe_int(row.get('BF', 0)),
                            'H': self._safe_int(row.get('H', 0)),
                            'BB': self._safe_int(row.get('BB', 0)),
                            'SO': self._safe_int(row.get('SO', 0)),
                            'HR': self._safe_int(row.get('HR', 0)),
                            'PC': self._safe_int(row.get('Pit', 0)),
                        })
            except Exception:
                continue
        
        return pd.DataFrame(all_stats)
    
    def _parse_unified_events(self, soup: BeautifulSoup, game_id: str) -> pd.DataFrame:
        """Parse play-by-play into unified events"""
        pbp_table = soup.find("table", id="play_by_play")
        if not pbp_table:
            return pd.DataFrame()
        
        try:
            df = pd.read_html(StringIO(str(pbp_table)))[0]
        except Exception:
            return pd.DataFrame()
        
        # Clean data - keep ALL events, not just plate appearances
        df = df[df['Inn'].notna() & df['Play Description'].notna() & df['Pitcher'].notna()]
        df = df[~df['Batter'].str.contains("Top of the|Bottom of the", case=False, na=False)]
        
        events = []
        for _, row in df.iterrows():
            event = self._parse_single_event(row, game_id)
            if event:
                events.append(event)
        
        return pd.DataFrame(events)
    
    def _parse_single_event(self, row: pd.Series, game_id: str) -> Optional[Dict]:
        """Parse a single play-by-play row"""
        # Clean and resolve names
        batter_name = self._normalize_name(row['Batter'])
        pitcher_name = self._normalize_name(row['Pitcher'])
        description = str(row['Play Description']).strip()
        
        resolved_batter = self.name_resolver.get(batter_name, batter_name)
        resolved_pitcher = self.name_resolver.get(pitcher_name, pitcher_name)
        
        # Analyze outcome
        outcome = self._analyze_outcome(description)
        if not outcome:
            return None
        
        return {
            'event_id': str(uuid.uuid4()),
            'game_id': game_id,
            'inning': self._parse_inning(row.get('Inn', '')),
            'inning_half': self._parse_inning_half(row.get('Inn', '')),
            'batter_id': resolved_batter,
            'pitcher_id': resolved_pitcher,
            'description': description,
            'is_plate_appearance': outcome['is_plate_appearance'],
            'is_at_bat': outcome['is_at_bat'],
            'is_hit': outcome['is_hit'],
            'hit_type': outcome.get('hit_type'),
            'is_walk': outcome['is_walk'],
            'is_strikeout': outcome['is_strikeout'],
            'is_sacrifice_fly': outcome['is_sacrifice_fly'],
            'is_sacrifice_hit': outcome['is_sacrifice_hit'],
            'is_out': outcome['is_out'],
            'outs_recorded': outcome['outs_recorded'],
            'bases_reached': outcome['bases_reached'],
            'pitch_count': self._parse_pitch_count(row.get('Pit(cnt)', '')),
        }
    
    def _analyze_outcome(self, description: str) -> Optional[Dict]:
        """Analyze play outcome"""
        desc = description.lower().strip()
        outcome = {
            'is_plate_appearance': True, 'is_at_bat': False, 'is_hit': False, 'hit_type': None,
            'is_walk': False, 'is_strikeout': False, 'is_sacrifice_fly': False, 'is_sacrifice_hit': False,
            'is_out': False, 'outs_recorded': 0, 'bases_reached': 0,
        }
        
        # Sacrifice flies (not at-bats)
        if re.search(r'sacrifice fly|sac fly|flyball.*sacrifice fly', desc):
            outcome.update({'is_sacrifice_fly': True, 'is_out': True, 'outs_recorded': 1})
            return outcome
        
        # Sacrifice hits (not at-bats, e.g. sac bunts)
        if re.search(r'sacrifice bunt|sac bunt', desc):
            outcome.update({'is_sacrifice_hit': True, 'is_out': True, 'outs_recorded': 1})
            return outcome
        
        # Walks (not at-bats)
        if re.search(r'^walk\b|^intentional walk', desc):
            outcome.update({'is_walk': True, 'bases_reached': 1})
            return outcome
        
        # Hit by pitch (not at-bats)
        if re.search(r'^hit by pitch|^hbp\b', desc):
            outcome.update({'bases_reached': 1})
            return outcome
        
        # Non-plate appearance events (caught stealing, pickoffs, etc.)
        if re.search(r'caught stealing|pickoff|picked off|wild pitch|passed ball|balk', desc):
            outcome.update({'is_plate_appearance': False})
            return outcome
        
        # At-bat outcomes
        outcome['is_at_bat'] = True
        
        # Strikeouts
        if re.search(r'^strikeout\b|^struck out', desc):
            outcome.update({'is_strikeout': True, 'is_out': True, 'outs_recorded': 1})
            return outcome
        
        # Hits
        if re.search(r'^home run\b|^hr\b', desc):
            outcome.update({'is_hit': True, 'hit_type': 'home_run', 'bases_reached': 4})
            return outcome
        
        hit_patterns = [
            (r'^single\b.*(?:to|up|through)', 'single', 1),
            (r'^double\b.*(?:to|down)', 'double', 2),
            (r'^triple\b.*(?:to|down)', 'triple', 3)
        ]
        
        for pattern, hit_type, bases in hit_patterns:
            if re.search(pattern, desc):
                outcome.update({'is_hit': True, 'hit_type': hit_type, 'bases_reached': bases})
                return outcome
        
        # Outs
        if re.search(r'grounded into double play|gdp\b|double play', desc):
            outcome.update({'is_out': True, 'outs_recorded': 2})
            return outcome
        
        out_patterns = [
            r'^grounded out\b', r'^flied out\b', r'^lined out\b', r'^popped out\b',
            r'^groundout\b', r'^flyout\b', r'^lineout\b', r'^popout\b', r'^popfly\b', r'^flyball\b'
        ]
        
        for pattern in out_patterns:
            if re.search(pattern, desc):
                outcome.update({'is_out': True, 'outs_recorded': 1})
                return outcome
        
        return None
    
    def _validate_batting(self, official: pd.DataFrame, events: pd.DataFrame) -> Dict:
        """Validate batting by aggregating events"""
        if official.empty or events.empty:
            return {'accuracy': 0, 'players_compared': 0}
        
        # Aggregate events by batter
        parsed = events.groupby('batter_id').agg({
            'is_plate_appearance': 'sum',
            'is_at_bat': 'sum',
            'is_hit': 'sum',
            'is_walk': 'sum',
            'is_strikeout': 'sum'
        }).reset_index()
        
        # Add hit types (HR, 2B, 3B)
        hit_types = ['home_run', 'double', 'triple']
        for hit_type in hit_types:
            hit_agg = events[events['hit_type'] == hit_type].groupby('batter_id').size().reset_index(name=f'parsed_{hit_type.upper().replace("_", "")}')
            if hit_type == 'home_run':
                hit_agg = hit_agg.rename(columns={'parsed_HR': 'parsed_HR'})
            elif hit_type == 'double':
                hit_agg = hit_agg.rename(columns={'parsed_2B': 'parsed_2B'})
            elif hit_type == 'triple':
                hit_agg = hit_agg.rename(columns={'parsed_3B': 'parsed_3B'})
            parsed = parsed.merge(hit_agg, on='batter_id', how='left').fillna(0)
        
        # Rename for comparison
        parsed = parsed.rename(columns={
            'batter_id': 'player_name',
            'is_plate_appearance': 'parsed_PA',
            'is_at_bat': 'parsed_AB',
            'is_hit': 'parsed_H',
            'is_walk': 'parsed_BB',
            'is_strikeout': 'parsed_SO'
        })
        
        return self._compare_stats(official, parsed, ['PA', 'AB', 'H', 'BB', 'SO', 'HR', '2B', '3B'], 'player_name')
    
    def _validate_pitching(self, official: pd.DataFrame, events: pd.DataFrame) -> Dict:
        """Validate pitching by aggregating events"""
        if official.empty or events.empty:
            return {'accuracy': 0, 'players_compared': 0}
        
        # Aggregate events by pitcher
        parsed = events.groupby('pitcher_id').agg({
            'is_plate_appearance': 'sum',
            'is_hit': 'sum',
            'is_walk': 'sum',
            'is_strikeout': 'sum',
            'pitch_count': 'sum'
        }).reset_index()
        
        # Add home runs
        hr_agg = events[events['hit_type'] == 'home_run'].groupby('pitcher_id').size().reset_index(name='parsed_HR')
        parsed = parsed.merge(hr_agg, on='pitcher_id', how='left').fillna(0)
        
        # Rename for comparison
        parsed = parsed.rename(columns={
            'pitcher_id': 'pitcher_name',
            'is_plate_appearance': 'parsed_BF',
            'is_hit': 'parsed_H',
            'is_walk': 'parsed_BB',
            'is_strikeout': 'parsed_SO',
            'pitch_count': 'parsec_PC'
        })
        
        return self._compare_stats(official, parsed, ['BF', 'H', 'BB', 'SO', 'HR', 'PC'], 'pitcher_name')
    
    def _compare_stats(self, official: pd.DataFrame, parsed: pd.DataFrame, stats: List[str], name_col: str) -> Dict:
        """Compare official vs parsed stats"""
        comparison = pd.merge(official, parsed, on=name_col, how='inner')
        
        if comparison.empty:
            return {'accuracy': 0, 'players_compared': 0, 'total_differences': 0, 'differences': []}
        
        total_diffs = 0
        total_stats = 0
        differences = []
        
        # Calculate differences for each stat
        for stat in stats:
            parsed_col = f'parsed_{stat}'
            if parsed_col in comparison.columns:
                comparison[f'{stat}_diff'] = comparison[parsed_col] - comparison[stat]
                diffs = comparison[f'{stat}_diff'].abs().sum()
                total_diffs += diffs
                total_stats += comparison[stat].sum()
        
        # Find players with differences
        for _, row in comparison.iterrows():
            player_diffs = []
            for stat in stats:
                diff_col = f'{stat}_diff'
                if diff_col in comparison.columns and row[diff_col] != 0:
                    official_val = row[stat]
                    parsed_val = row[f'parsed_{stat}']
                    diff_val = row[diff_col]
                    player_diffs.append(f"{stat}: {official_val} vs {parsed_val} (diff: {diff_val:+.0f})")
            
            if player_diffs:
                differences.append({
                    'player': row[name_col],
                    'diffs': player_diffs
                })
        
        accuracy = ((total_stats - total_diffs) / total_stats * 100) if total_stats > 0 else 0
        
        return {
            'accuracy': accuracy,
            'players_compared': len(comparison),
            'total_differences': int(total_diffs),
            'total_stats': int(total_stats),
            'differences': differences
        }
    
    # Helper methods
    def _extract_canonical_names(self, soup: BeautifulSoup) -> set:
        """Extract canonical names from box score tables"""
        names = set()
        for table_type in ['batting', 'pitching']:
            tables = soup.find_all('table', {'id': lambda x: x and table_type in x.lower()})
            for table in tables:
                for row in table.find_all('tr'):
                    name_cell = row.find('th', {'data-stat': 'player'})
                    if name_cell:
                        name = self._normalize_name(name_cell.get_text(strip=True))
                        if name and name not in ['Player', 'Batting', 'Pitching']:
                            names.add(name)
        return names
    
    def _build_name_resolver(self) -> Dict[str, str]:
        """Build name resolution mapping"""
        mappings = {}
        for name in self.canonical_names:
            mappings[name] = name
            # Add abbreviated versions
            if ' ' in name:
                parts = name.split(' ')
                if len(parts) >= 2:
                    abbrev = f"{parts[0][0]}. {' '.join(parts[1:])}"
                    mappings[abbrev] = name
        return mappings
    
    def _normalize_name(self, name: str) -> str:
        """Normalize name for consistent matching"""
        if pd.isna(name) or not name:
            return ""
        
        # Unicode normalization and clean whitespace
        cleaned = unicodedata.normalize('NFKD', str(name))
        cleaned = re.sub(r'[\s\xa0]+', ' ', cleaned).strip()
        
        # Remove trailing codes and results
        cleaned = re.sub(r',\s*[WLSHB]+\s*\([^)]*\)$', '', cleaned)
        cleaned = re.sub(r"((?:[A-Z0-9]{1,3})(?:-[A-Z0-9]{1,3})*)$", "", cleaned).strip()
        
        return cleaned
    
    def _extract_game_id(self, url: str) -> str:
        """Extract game ID from URL"""
        match = re.search(r'/boxes/[A-Z]{3}/([A-Z]{3}\d{8})', url)
        return match.group(1) if match else 'unknown'
    
    def _parse_inning(self, inn_str: str) -> int:
        """Parse inning number"""
        match = re.search(r'(\d+)', str(inn_str))
        return int(match.group(1)) if match else 0
    
    def _parse_inning_half(self, inn_str: str) -> str:
        """Parse inning half"""
        inn_lower = str(inn_str).lower()
        if inn_lower.startswith('t'):
            return 'top'
        elif inn_lower.startswith('b'):
            return 'bottom'
        return ''
    
    def _parse_pitch_count(self, count_str: str) -> int:
        """Parse pitch count"""
        match = re.match(r'^(\d+)', str(count_str))
        return int(match.group(1)) if match else 0
    
    def _safe_int(self, value, default=0):
        """Safely convert to int"""
        try:
            return int(float(value)) if pd.notna(value) else default
        except (ValueError, TypeError):
            return default
    
    def _extract_from_details(self, row: pd.Series, stat: str) -> int:
        """Extract stat from Details column"""
        if 'Details' not in row.index or pd.isna(row['Details']):
            return 0
        
        details = str(row['Details'])
        match = re.search(rf"(\d+)·{stat}|(?:^|,)\s*{stat}(?:,|$)", details)
        return int(match.group(1)) if match and match.group(1) else (1 if match else 0)

# Test function
def test_unified_parser():
    """Test the unified parser"""
    test_url = "https://www.baseball-reference.com/boxes/HOU/HOU202503290.shtml"
    
    parser = UnifiedEventsParser()
    results = parser.parse_game(test_url)
    
    events = results['unified_events']
    
    print("📋 UNIFIED EVENTS SAMPLE:")
    if not events.empty:
        cols = ['batter_id', 'pitcher_id', 'inning', 'inning_half', 'description', 
                'is_plate_appearance', 'is_at_bat', 'is_hit', 'hit_type', 'bases_reached', 'pitch_count']
        print(events[cols].head(10))
        print(f"\nTotal events: {len(events)}")
    
    bat_val = results['batting_validation']
    pit_val = results['pitching_validation']
    
    print(f"\n✅ VALIDATION RESULTS:")
    print(f"   ⚾ Batting: {bat_val['accuracy']:.1f}% ({bat_val['players_compared']} players)")
    print(f"   🥎 Pitching: {pit_val['accuracy']:.1f}% ({pit_val['players_compared']} pitchers)")
    
    # Show differences for debugging
    if bat_val.get('differences'):
        print(f"\n⚠️  BATTING DIFFERENCES:")
        for diff in bat_val['differences']:
            print(f"   {diff['player']}:")
            for stat_diff in diff['diffs']:
                print(f"     {stat_diff}")
    
    if pit_val.get('differences'):
        print(f"\n⚠️  PITCHING DIFFERENCES:")
        for diff in pit_val['differences']:
            print(f"   {diff['player']}:")
            for stat_diff in diff['diffs']:
                print(f"     {stat_diff}")
    
    return results

if __name__ == "__main__":
    test_unified_parser()