from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple
from enum import Enum
import re
from datetime import datetime
import pandas as pd

class Base(Enum):
    FIRST = 1
    SECOND = 2
    THIRD = 3
    HOME = 4

class PlayOutcome(Enum):
    HIT = "hit"
    WALK = "walk"
    STRIKEOUT = "strikeout"
    GROUNDOUT = "groundout"
    FLYOUT = "flyout"
    ERROR = "error"
    SACRIFICE = "sacrifice"
    DOUBLE_PLAY = "double_play"
    TRIPLE_PLAY = "triple_play"
    HIT_BY_PITCH = "hit_by_pitch"
    FIELDERS_CHOICE = "fielders_choice"

@dataclass
class Runner:
    """Represents a runner currently on base"""
    player_name: str
    player_id: str
    current_base: Base
    at_bat_id: str  # The at-bat that put this runner on base
    batter_name: str  # Who was batting when this runner got on base
    inning: int
    inning_half: str  # 't' for top, 'b' for bottom
    
    def __hash__(self):
        return hash((self.player_name, self.player_id))

@dataclass
class AtBat:
    """Represents a single at-bat with all associated events"""
    at_bat_id: str
    batter_name: str
    batter_id: str
    pitcher_name: str
    pitcher_id: str
    inning: int
    inning_half: str
    play_description: str
    outcome: Optional[PlayOutcome] = None
    
    # Batting stats for this at-bat
    is_ab: bool = False  # Counts as official at-bat
    is_hit: bool = False
    is_single: bool = False
    is_double: bool = False
    is_triple: bool = False
    is_hr: bool = False
    is_walk: bool = False
    is_strikeout: bool = False
    is_sacrifice: bool = False
    is_hbp: bool = False
    rbis: int = 0
    
    # Runner events that happened during this at-bat
    runners_scored: List[str] = field(default_factory=list)
    stolen_bases: List[Tuple[str, Base, Base]] = field(default_factory=list)  # (player, from_base, to_base)
    caught_stealing: List[Tuple[str, Base, Base]] = field(default_factory=list)
    
    # Pitcher stats for this at-bat
    pitches_thrown: int = 0
    strikeouts_pitched: int = 0
    walks_allowed: int = 0
    hits_allowed: int = 0
    runs_allowed: int = 0
    earned_runs_allowed: int = 0

class GameState:
    """Tracks the complete state of a baseball game"""
    
    def __init__(self, game_id: str, home_team: str, away_team: str):
        self.game_id = game_id
        self.home_team = home_team
        self.away_team = away_team
        
        # Current game state
        self.runners: Dict[Base, Runner] = {}  # Base -> Runner
        self.current_inning = 1
        self.current_inning_half = 't'  # 't' for top, 'b' for bottom
        self.current_pitcher = None
        self.outs = 0
        
        # Game history
        self.at_bats: List[AtBat] = []
        self.player_stats: Dict[str, Dict] = {}  # player_name -> stats dict
        self.pitcher_stats: Dict[str, Dict] = {}  # pitcher_name -> stats dict
        
        # Player identification helpers
        self.player_name_variants: Dict[str, str] = {}  # variant -> canonical_name
        self.player_ids: Dict[str, str] = {}  # canonical_name -> player_id
        
    def add_player_variant(self, canonical_name: str, variant: str, player_id: str = None):
        """Add a name variant for a player (e.g., 'A. Judge' -> 'Aaron Judge')"""
        self.player_name_variants[variant.lower()] = canonical_name
        self.player_name_variants[canonical_name.lower()] = canonical_name
        if player_id:
            self.player_ids[canonical_name] = player_id
    
    def resolve_player_name(self, name: str) -> str:
        """Resolve a player name to its canonical form"""
        name_lower = name.lower().strip()
        
        # Direct match
        if name_lower in self.player_name_variants:
            return self.player_name_variants[name_lower]
        
        # Try fuzzy matching for common patterns
        # Remove common suffixes
        name_clean = re.sub(r'\s+(jr\.?|sr\.?|iii?|iv)$', '', name_lower)
        if name_clean in self.player_name_variants:
            return self.player_name_variants[name_clean]
        
        # Try first/last name matching
        parts = name_clean.split()
        if len(parts) >= 2:
            # Try "F. Last" -> "First Last" matching
            for canonical in self.player_name_variants.values():
                canonical_parts = canonical.lower().split()
                if (len(canonical_parts) >= 2 and 
                    parts[-1] == canonical_parts[-1] and  # Last name matches
                    parts[0].startswith(canonical_parts[0][0])):  # First initial matches
                    return canonical
        
        # If no match found, return original (but cleaned)
        return name.strip()
    
    def add_runner(self, player_name: str, base: Base, at_bat_id: str, batter_name: str):
        """Add a runner to the specified base"""
        canonical_name = self.resolve_player_name(player_name)
        player_id = self.player_ids.get(canonical_name, f"{canonical_name.lower().replace(' ', '_')}")
        
        runner = Runner(
            player_name=canonical_name,
            player_id=player_id,
            current_base=base,
            at_bat_id=at_bat_id,
            batter_name=batter_name,
            inning=self.current_inning,
            inning_half=self.current_inning_half
        )
        self.runners[base] = runner
    
    def move_runner(self, from_base: Base, to_base: Base) -> Optional[Runner]:
        """Move a runner from one base to another"""
        if from_base not in self.runners:
            return None
        
        runner = self.runners.pop(from_base)
        
        if to_base == Base.HOME:
            # Runner scored - don't add to bases
            return runner
        else:
            runner.current_base = to_base
            self.runners[to_base] = runner
            return runner
    
    def clear_bases(self):
        """Clear all runners (used for inning changes, etc.)"""
        self.runners.clear()
    
    def get_runner_at_base(self, base: Base) -> Optional[Runner]:
        """Get the runner currently at the specified base"""
        return self.runners.get(base)
    
    def process_at_bat(self, at_bat: AtBat):
        """Process a complete at-bat and update game state"""
        self.at_bats.append(at_bat)
        
        # Initialize player stats if needed
        if at_bat.batter_name not in self.player_stats:
            self.player_stats[at_bat.batter_name] = {
                'AB': 0, 'H': 0, 'R': 0, 'RBI': 0, 'BB': 0, 'SO': 0,
                '2B': 0, '3B': 0, 'HR': 0, 'SB': 0, 'CS': 0,
                'HBP': 0, 'SF': 0, 'SH': 0
            }
        
        if at_bat.pitcher_name not in self.pitcher_stats:
            self.pitcher_stats[at_bat.pitcher_name] = {
                'IP': 0.0, 'H': 0, 'R': 0, 'ER': 0, 'BB': 0, 'SO': 0,
                'HR': 0, 'BF': 0  # Batters Faced
            }
        
        # Update batter stats
        batter_stats = self.player_stats[at_bat.batter_name]
        if at_bat.is_ab:
            batter_stats['AB'] += 1
        if at_bat.is_hit:
            batter_stats['H'] += 1
        if at_bat.is_double:
            batter_stats['2B'] += 1
        if at_bat.is_triple:
            batter_stats['3B'] += 1
        if at_bat.is_hr:
            batter_stats['HR'] += 1
        if at_bat.is_walk:
            batter_stats['BB'] += 1
        if at_bat.is_strikeout:
            batter_stats['SO'] += 1
        if at_bat.is_hbp:
            batter_stats['HBP'] += 1
        if at_bat.is_sacrifice:
            batter_stats['SF'] += 1
        
        batter_stats['RBI'] += at_bat.rbis
        
        # Update pitcher stats
        pitcher_stats = self.pitcher_stats[at_bat.pitcher_name]
        pitcher_stats['BF'] += 1
        if at_bat.is_hit:
            pitcher_stats['H'] += 1
        if at_bat.is_walk:
            pitcher_stats['BB'] += 1
        if at_bat.is_strikeout:
            pitcher_stats['SO'] += 1
        if at_bat.is_hr:
            pitcher_stats['HR'] += 1
        
        # Handle stolen bases
        for player_name, from_base, to_base in at_bat.stolen_bases:
            canonical_name = self.resolve_player_name(player_name)
            if canonical_name not in self.player_stats:
                self.player_stats[canonical_name] = {
                    'AB': 0, 'H': 0, 'R': 0, 'RBI': 0, 'BB': 0, 'SO': 0,
                    '2B': 0, '3B': 0, 'HR': 0, 'SB': 0, 'CS': 0,
                    'HBP': 0, 'SF': 0, 'SH': 0
                }
            self.player_stats[canonical_name]['SB'] += 1
        
        # Handle caught stealing
        for player_name, from_base, to_base in at_bat.caught_stealing:
            canonical_name = self.resolve_player_name(player_name)
            if canonical_name not in self.player_stats:
                self.player_stats[canonical_name] = {
                    'AB': 0, 'H': 0, 'R': 0, 'RBI': 0, 'BB': 0, 'SO': 0,
                    '2B': 0, '3B': 0, 'HR': 0, 'SB': 0, 'CS': 0,
                    'HBP': 0, 'SF': 0, 'SH': 0
                }
            self.player_stats[canonical_name]['CS'] += 1
        
        # Handle runs scored
        for runner_name in at_bat.runners_scored:
            canonical_name = self.resolve_player_name(runner_name)
            if canonical_name not in self.player_stats:
                self.player_stats[canonical_name] = {
                    'AB': 0, 'H': 0, 'R': 0, 'RBI': 0, 'BB': 0, 'SO': 0,
                    '2B': 0, '3B': 0, 'HR': 0, 'SB': 0, 'CS': 0,
                    'HBP': 0, 'SF': 0, 'SH': 0
                }
            self.player_stats[canonical_name]['R'] += 1
    
    def get_batting_stats_df(self):
        """Convert player stats to DataFrame format for validation"""
        stats_list = []
        for player_name, stats in self.player_stats.items():
            stats_row = {'Name': player_name}
            stats_row.update(stats)
            stats_list.append(stats_row)
        
        return pd.DataFrame(stats_list)
    
    def get_pitching_stats_df(self):
        """Convert pitcher stats to DataFrame format"""
        stats_list = []
        for pitcher_name, stats in self.pitcher_stats.items():
            stats_row = {'Name': pitcher_name}
            stats_row.update(stats)
            stats_list.append(stats_row)
        
        return pd.DataFrame(stats_list)


class PlayParser:
    """Parse MLB play descriptions into structured data"""
    
    def __init__(self):
        # Regex patterns for different play types
        self.patterns = {
            # Hits
            'single': re.compile(r'singles?|1B', re.IGNORECASE),
            'double': re.compile(r'doubles?|2B', re.IGNORECASE),
            'triple': re.compile(r'triples?|3B', re.IGNORECASE),
            'home_run': re.compile(r'home runs?|homers?|HR', re.IGNORECASE),
            'hit': re.compile(r'singles?|doubles?|triples?|home runs?|homers?|1B|2B|3B|HR', re.IGNORECASE),
            
            # Outs
            'strikeout': re.compile(r'strikes? out|strikeout|struck out|K', re.IGNORECASE),
            'groundout': re.compile(r'grounds? out|ground out|grounds to', re.IGNORECASE),
            'flyout': re.compile(r'flies? out|fly out|pops? out|pop out|lines? out|line out', re.IGNORECASE),
            'foul_out': re.compile(r'foul out|fouls out', re.IGNORECASE),
            
            # Walks and HBP
            'walk': re.compile(r'walks?|BB|base on balls', re.IGNORECASE),
            'intentional_walk': re.compile(r'intentional walk|IBB', re.IGNORECASE),
            'hit_by_pitch': re.compile(r'hit by pitch|HBP', re.IGNORECASE),
            
            # Sacrifices
            'sacrifice_fly': re.compile(r'sacrifice fly|sac fly|SF', re.IGNORECASE),
            'sacrifice_bunt': re.compile(r'sacrifice bunt|sac bunt|SH', re.IGNORECASE),
            
            # Fielding
            'error': re.compile(r'error|E\d+', re.IGNORECASE),
            'fielders_choice': re.compile(r'fielder\'?s choice|FC', re.IGNORECASE),
            'double_play': re.compile(r'double play|DP|\d+-\d+-\d+', re.IGNORECASE),
            'triple_play': re.compile(r'triple play|TP', re.IGNORECASE),
            
            # Baserunning
            'stolen_base': re.compile(r'steals? (\w+) base|SB', re.IGNORECASE),
            'caught_stealing': re.compile(r'caught stealing|CS', re.IGNORECASE),
            'picked_off': re.compile(r'picked off|PO', re.IGNORECASE),
            
            # Scoring
            'scores': re.compile(r'scores?|score', re.IGNORECASE),
            'advances': re.compile(r'advances?|advance', re.IGNORECASE),
            
            # RBI patterns
            'rbi_explicit': re.compile(r'(\d+)\s*RBI', re.IGNORECASE),
            'run_scores': re.compile(r'(\w+(?:\s+\w+)*)\s+scores?', re.IGNORECASE),
        }
        
        # Base name mappings
        self.base_names = {
            'first': Base.FIRST, '1st': Base.FIRST, 'first base': Base.FIRST,
            'second': Base.SECOND, '2nd': Base.SECOND, 'second base': Base.SECOND,
            'third': Base.THIRD, '3rd': Base.THIRD, 'third base': Base.THIRD,
            'home': Base.HOME, 'home plate': Base.HOME
        }
    
    def parse_at_bat_outcome(self, play_description: str, batter_name: str) -> Dict:
        """Parse the main outcome of an at-bat"""
        outcome = {
            'is_ab': True,  # Default to counting as at-bat
            'is_hit': False,
            'is_single': False,
            'is_double': False,
            'is_triple': False,
            'is_hr': False,
            'is_walk': False,
            'is_strikeout': False,
            'is_sacrifice': False,
            'is_hbp': False,
            'is_error': False,
            'is_fielders_choice': False,
            'outcome_type': None
        }
        
        # Check for hits
        if self.patterns['home_run'].search(play_description):
            outcome.update({
                'is_hit': True, 'is_hr': True, 'outcome_type': PlayOutcome.HIT
            })
        elif self.patterns['triple'].search(play_description):
            outcome.update({
                'is_hit': True, 'is_triple': True, 'outcome_type': PlayOutcome.HIT
            })
        elif self.patterns['double'].search(play_description):
            outcome.update({
                'is_hit': True, 'is_double': True, 'outcome_type': PlayOutcome.HIT
            })
        elif self.patterns['single'].search(play_description):
            outcome.update({
                'is_hit': True, 'is_single': True, 'outcome_type': PlayOutcome.HIT
            })
        
        # Check for walks
        elif self.patterns['walk'].search(play_description) or self.patterns['intentional_walk'].search(play_description):
            outcome.update({
                'is_ab': False, 'is_walk': True, 'outcome_type': PlayOutcome.WALK
            })
        
        # Check for HBP
        elif self.patterns['hit_by_pitch'].search(play_description):
            outcome.update({
                'is_ab': False, 'is_hbp': True, 'outcome_type': PlayOutcome.HIT_BY_PITCH
            })
        
        # Check for sacrifices
        elif self.patterns['sacrifice_fly'].search(play_description):
            outcome.update({
                'is_ab': False, 'is_sacrifice': True, 'outcome_type': PlayOutcome.SACRIFICE
            })
        elif self.patterns['sacrifice_bunt'].search(play_description):
            outcome.update({
                'is_ab': False, 'is_sacrifice': True, 'outcome_type': PlayOutcome.SACRIFICE
            })
        
        # Check for strikeouts
        elif self.patterns['strikeout'].search(play_description):
            outcome.update({
                'is_strikeout': True, 'outcome_type': PlayOutcome.STRIKEOUT
            })
        
        # Check for other outs
        elif self.patterns['groundout'].search(play_description):
            outcome.update({
                'outcome_type': PlayOutcome.GROUNDOUT
            })
        elif self.patterns['flyout'].search(play_description) or self.patterns['foul_out'].search(play_description):
            outcome.update({
                'outcome_type': PlayOutcome.FLYOUT
            })
        
        # Check for errors
        elif self.patterns['error'].search(play_description):
            outcome.update({
                'is_error': True, 'outcome_type': PlayOutcome.ERROR
            })
        
        # Check for fielder's choice
        elif self.patterns['fielders_choice'].search(play_description):
            outcome.update({
                'is_fielders_choice': True, 'outcome_type': PlayOutcome.FIELDERS_CHOICE
            })
        
        return outcome
    
    def parse_rbis(self, play_description: str) -> int:
        """Extract RBI count from play description"""
        # Look for explicit RBI count
        rbi_match = self.patterns['rbi_explicit'].search(play_description)
        if rbi_match:
            return int(rbi_match.group(1))
        
        # Count scoring plays
        score_matches = self.patterns['run_scores'].findall(play_description)
        rbi_count = len(score_matches)
        
        # Additional logic for sacrifice flies
        if self.patterns['sacrifice_fly'].search(play_description) and rbi_count == 0:
            rbi_count = 1
        
        return rbi_count
    
    def parse_runners_scored(self, play_description: str) -> List[str]:
        """Extract names of runners who scored"""
        scored_runners = []
        
        # Look for "Player Name scores" patterns
        score_matches = self.patterns['run_scores'].findall(play_description)
        for match in score_matches:
            # Clean up the name
            name = match.strip()
            if name and not name.lower() in ['he', 'she', 'runner', 'batter']:
                scored_runners.append(name)
        
        return scored_runners
    
    def parse_stolen_bases(self, play_description: str) -> List[Tuple[str, Base, Base]]:
        """Extract stolen base information"""
        stolen_bases = []
        
        # Pattern: "Player Name steals second base"
        steal_pattern = r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+steals?\s+(\w+)(?:\s+base)?'
        matches = re.findall(steal_pattern, play_description, re.IGNORECASE)
        
        for player_name, base_name in matches:
            base_name_lower = base_name.lower()
            if base_name_lower in self.base_names:
                # Determine the base they came from
                to_base = self.base_names[base_name_lower]
                if to_base == Base.SECOND:
                    from_base = Base.FIRST
                elif to_base == Base.THIRD:
                    from_base = Base.SECOND
                elif to_base == Base.HOME:
                    from_base = Base.THIRD
                else:
                    continue  # Invalid steal
                
                stolen_bases.append((player_name, from_base, to_base))
        
        return stolen_bases
    
    def parse_caught_stealing(self, play_description: str) -> List[Tuple[str, Base, Base]]:
        """Extract caught stealing information"""
        caught_stealing = []
        
        # Pattern: "Player Name caught stealing second base"
        cs_pattern = r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+caught stealing\s+(\w+)(?:\s+base)?'
        matches = re.findall(cs_pattern, play_description, re.IGNORECASE)
        
        for player_name, base_name in matches:
            base_name_lower = base_name.lower()
            if base_name_lower in self.base_names:
                to_base = self.base_names[base_name_lower]
                if to_base == Base.SECOND:
                    from_base = Base.FIRST
                elif to_base == Base.THIRD:
                    from_base = Base.SECOND
                elif to_base == Base.HOME:
                    from_base = Base.THIRD
                else:
                    continue
                
                caught_stealing.append((player_name, from_base, to_base))
        
        return caught_stealing
    
    def parse_play_description(self, play_description: str, batter_name: str, 
                             pitcher_name: str, at_bat_id: str, inning: int, 
                             inning_half: str) -> AtBat:
        """Parse a complete play description into an AtBat object"""
        
        # Parse basic at-bat outcome
        outcome_data = self.parse_at_bat_outcome(play_description, batter_name)
        
        # Parse additional events
        rbis = self.parse_rbis(play_description)
        runners_scored = self.parse_runners_scored(play_description)
        stolen_bases = self.parse_stolen_bases(play_description)
        caught_stealing = self.parse_caught_stealing(play_description)
        
        # Create AtBat object
        at_bat = AtBat(
            at_bat_id=at_bat_id,
            batter_name=batter_name,
            batter_id=batter_name.lower().replace(' ', '_'),  # Simple ID generation
            pitcher_name=pitcher_name,
            pitcher_id=pitcher_name.lower().replace(' ', '_'),
            inning=inning,
            inning_half=inning_half,
            play_description=play_description,
            outcome=outcome_data.get('outcome_type'),
            
            # Batting stats
            is_ab=outcome_data['is_ab'],
            is_hit=outcome_data['is_hit'],
            is_single=outcome_data['is_single'],
            is_double=outcome_data['is_double'],
            is_triple=outcome_data['is_triple'],
            is_hr=outcome_data['is_hr'],
            is_walk=outcome_data['is_walk'],
            is_strikeout=outcome_data['is_strikeout'],
            is_sacrifice=outcome_data['is_sacrifice'],
            is_hbp=outcome_data['is_hbp'],
            rbis=rbis,
            
            # Runner events
            runners_scored=runners_scored,
            stolen_bases=stolen_bases,
            caught_stealing=caught_stealing,
            
            # Pitcher stats (basic)
            strikeouts_pitched=1 if outcome_data['is_strikeout'] else 0,
            walks_allowed=1 if outcome_data['is_walk'] else 0,
            hits_allowed=1 if outcome_data['is_hit'] else 0,
        )
        
        return at_bat


class GameValidator:
    """Validate parsed game data against official box scores"""
    
    def __init__(self):
        self.parser = PlayParser()
    
    def process_game_plays(self, plays_data: List[Dict], home_team: str, 
                          away_team: str, game_id: str) -> GameState:
        """Process all plays in a game and return final game state"""
        
        game_state = GameState(game_id, home_team, away_team)
        
        # Add known player variants (you'll need to populate this based on your data)
        # Example:
        # game_state.add_player_variant("Aaron Judge", "A. Judge", "judgea01")
        # game_state.add_player_variant("Giancarlo Stanton", "G. Stanton", "stantgi01")
        
        for i, play_data in enumerate(plays_data):
            # Extract required fields from your play data
            batter_name = play_data.get('batter', '').strip()
            pitcher_name = play_data.get('pitcher', '').strip()
            play_description = play_data.get('description', '').strip()
            inning = play_data.get('inning', 1)
            inning_half = play_data.get('inning_half', 't')
            
            if not batter_name or not play_description:
                continue
            
            # Generate unique at-bat ID
            at_bat_id = f"{game_id}_{i:03d}"
            
            # Parse the play description
            at_bat = self.parser.parse_play_description(
                play_description=play_description,
                batter_name=batter_name,
                pitcher_name=pitcher_name,
                at_bat_id=at_bat_id,
                inning=inning,
                inning_half=inning_half
            )
            
            # Update game state with baserunning
            self._update_baserunners(game_state, at_bat)
            
            # Process the at-bat
            game_state.process_at_bat(at_bat)
            
            # Update inning/outs if needed
            self._update_game_situation(game_state, play_data)
        
        return game_state
    
    def _update_baserunners(self, game_state: GameState, at_bat: AtBat):
        """Update base runners based on the at-bat outcome"""
        
        # Handle batter reaching base
        if at_bat.is_hit or at_bat.is_walk or at_bat.is_hbp or at_bat.is_error:
            # Determine which base the batter reaches
            if at_bat.is_hr:
                # Home run - batter and all runners score
                # Clear all bases and add runs
                pass  # Runs already counted in runners_scored
            elif at_bat.is_triple:
                game_state.add_runner(at_bat.batter_name, Base.THIRD, at_bat.at_bat_id, at_bat.batter_name)
            elif at_bat.is_double:
                game_state.add_runner(at_bat.batter_name, Base.SECOND, at_bat.at_bat_id, at_bat.batter_name)
            else:  # Single, walk, HBP, error
                game_state.add_runner(at_bat.batter_name, Base.FIRST, at_bat.at_bat_id, at_bat.batter_name)
        
        # Handle stolen bases
        for player_name, from_base, to_base in at_bat.stolen_bases:
            runner = game_state.get_runner_at_base(from_base)
            if runner and game_state.resolve_player_name(player_name) == runner.player_name:
                game_state.move_runner(from_base, to_base)
        
        # Handle caught stealing (remove runner)
        for player_name, from_base, to_base in at_bat.caught_stealing:
            runner = game_state.get_runner_at_base(from_base)
            if runner and game_state.resolve_player_name(player_name) == runner.player_name:
                game_state.runners.pop(from_base, None)
        
        # Handle runners scoring (remove from bases)
        for runner_name in at_bat.runners_scored:
            canonical_name = game_state.resolve_player_name(runner_name)
            # Find and remove the runner from bases
            for base, runner in list(game_state.runners.items()):
                if runner.player_name == canonical_name:
                    game_state.runners.pop(base)
                    break
    
    def _update_game_situation(self, game_state: GameState, play_data: Dict):
        """Update inning, outs, etc. based on play data"""
        
        # Update inning if provided
        if 'inning' in play_data:
            game_state.current_inning = play_data['inning']
        if 'inning_half' in play_data:
            game_state.current_inning_half = play_data['inning_half']
        
        # Update outs if provided
        if 'outs' in play_data:
            game_state.outs = play_data['outs']
        
        # Clear bases if inning changed
        if play_data.get('inning_changed', False):
            game_state.clear_bases()
    
    def validate_against_box_score(self, game_state: GameState, 
                                  official_batting_stats: pd.DataFrame) -> Dict:
        """Compare parsed stats against official box score"""
        
        parsed_stats = game_state.get_batting_stats_df()
        
        # Merge dataframes on player name for comparison
        comparison = self._merge_stats_for_comparison(parsed_stats, official_batting_stats)
        
        # Calculate differences
        validation_report = {
            'total_players': len(comparison),
            'perfect_matches': 0,
            'discrepancies': [],
            'missing_players': [],
            'extra_players': [],
            'stat_differences': {}
        }
        
        # Check for missing/extra players
        parsed_names = set(parsed_stats['Name'].str.lower())
        official_names = set(official_batting_stats['Name'].str.lower())
        
        validation_report['missing_players'] = list(official_names - parsed_names)
        validation_report['extra_players'] = list(parsed_names - official_names)
        
        # Compare stats for matching players
        stat_columns = ['AB', 'H', 'R', 'RBI', 'BB', 'SO', 'HR']
        
        for _, row in comparison.iterrows():
            player_name = row['Name']
            is_perfect_match = True
            player_diffs = {}
            
            for stat in stat_columns:
                parsed_col = f"{stat}_parsed"
                official_col = f"{stat}_official"
                
                if parsed_col in row and official_col in row:
                    parsed_val = row[parsed_col] if pd.notna(row[parsed_col]) else 0
                    official_val = row[official_col] if pd.notna(row[official_col]) else 0
                    
                    if parsed_val != official_val:
                        is_perfect_match = False
                        player_diffs[stat] = {
                            'parsed': parsed_val,
                            'official': official_val,
                            'diff': parsed_val - official_val
                        }
            
            if is_perfect_match:
                validation_report['perfect_matches'] += 1
            else:
                validation_report['discrepancies'].append({
                    'player': player_name,
                    'differences': player_diffs
                })
        
        # Calculate overall accuracy
        total_stats_checked = len(comparison) * len(stat_columns)
        total_correct = sum(1 for disc in validation_report['discrepancies'] 
                           for stat in stat_columns 
                           if stat not in disc['differences'])
        
        validation_report['accuracy_percentage'] = (total_correct / total_stats_checked * 100) if total_stats_checked > 0 else 0
        
        return validation_report
    
    def _merge_stats_for_comparison(self, parsed_stats: pd.DataFrame, 
                                   official_stats: pd.DataFrame) -> pd.DataFrame:
        """Merge parsed and official stats for comparison"""
        
        # Normalize player names for matching
        parsed_stats_norm = parsed_stats.copy()
        official_stats_norm = official_stats.copy()
        
        parsed_stats_norm['Name_norm'] = parsed_stats_norm['Name'].str.lower().str.strip()
        official_stats_norm['Name_norm'] = official_stats_norm['Name'].str.lower().str.strip()
        
        # Add suffixes to distinguish columns
        parsed_stats_norm = parsed_stats_norm.add_suffix('_parsed')
        parsed_stats_norm['Name'] = parsed_stats['Name']  # Keep original name
        parsed_stats_norm['Name_norm'] = parsed_stats_norm['Name_parsed'].str.lower().str.strip()
        
        official_stats_norm = official_stats_norm.add_suffix('_official')
        official_stats_norm['Name'] = official_stats['Name']  # Keep original name
        official_stats_norm['Name_norm'] = official_stats_norm['Name_official'].str.lower().str.strip()
        
        # Merge on normalized names
        merged = pd.merge(
            parsed_stats_norm, 
            official_stats_norm, 
            on='Name_norm', 
            how='outer',
            suffixes=('', '_official_dup')
        )
        
        return merged
    
    def generate_validation_report(self, validation_results: Dict) -> str:
        """Generate a human-readable validation report"""
        
        report = []
        report.append("=" * 60)
        report.append("GAME VALIDATION REPORT")
        report.append("=" * 60)
        
        # Summary
        report.append(f"Total Players: {validation_results['total_players']}")
        report.append(f"Perfect Matches: {validation_results['perfect_matches']}")
        report.append(f"Accuracy: {validation_results['accuracy_percentage']:.1f}%")
        report.append("")
        
        # Missing/Extra players
        if validation_results['missing_players']:
            report.append("MISSING PLAYERS (in official but not parsed):")
            for player in validation_results['missing_players']:
                report.append(f"  - {player}")
            report.append("")
        
        if validation_results['extra_players']:
            report.append("EXTRA PLAYERS (in parsed but not official):")
            for player in validation_results['extra_players']:
                report.append(f"  - {player}")
            report.append("")
        
        # Stat discrepancies
        if validation_results['discrepancies']:
            report.append("STAT DISCREPANCIES:")
            for disc in validation_results['discrepancies']:
                report.append(f"\n{disc['player']}:")
                for stat, diff_data in disc['differences'].items():
                    report.append(f"  {stat}: parsed={diff_data['parsed']}, "
                                f"official={diff_data['official']}, "
                                f"diff={diff_data['diff']:+d}")
        
        if validation_results['perfect_matches'] == validation_results['total_players']:
            report.append("\n🎉 PERFECT VALIDATION! All stats match exactly.")
        
        return "\n".join(report)


# Example usage function
def validate_game(game_url: str, plays_data: List[Dict], 
                 official_box_score: pd.DataFrame) -> str:
    """
    Main validation function that ties everything together
    
    Args:
        game_url: URL of the game being validated
        plays_data: List of play-by-play data dictionaries
        official_box_score: DataFrame with official batting stats
    
    Returns:
        Validation report as string
    """
    
    validator = GameValidator()
    
    # Extract game info from URL or plays_data
    game_id = game_url.split('/')[-1] if game_url else "unknown_game"
    home_team = "HOME"  # Extract from your data
    away_team = "AWAY"  # Extract from your data
    
    # Process all plays
    game_state = validator.process_game_plays(
        plays_data=plays_data,
        home_team=home_team,
        away_team=away_team,
        game_id=game_id
    )
    
    # Validate against box score
    validation_results = validator.validate_against_box_score(
        game_state=game_state,
        official_batting_stats=official_box_score
    )
    
    # Generate report
    report = validator.generate_validation_report(validation_results)
    
    return report


# Simple test to verify everything imports correctly
if __name__ == "__main__":
    print("✅ All classes imported successfully!")
    
    # Create a simple test
    test_plays = [
        {
            'batter': 'Aaron Judge',
            'pitcher': 'Shane Bieber',
            'description': 'Aaron Judge singles to center field',
            'inning': 1,
            'inning_half': 't'
        },
        {
            'batter': 'Juan Soto',
            'pitcher': 'Shane Bieber', 
            'description': 'Juan Soto walks',
            'inning': 1,
            'inning_half': 't'
        }
    ]
    
    # Create test box score
    test_box_score = pd.DataFrame({
        'Name': ['Aaron Judge', 'Juan Soto'],
        'AB': [1, 0],
        'H': [1, 0],
        'R': [0, 0],
        'RBI': [0, 0],
        'BB': [0, 1],
        'SO': [0, 0],
        'HR': [0, 0]
    })
    
    # Run test validation
    try:
        report = validate_game("test_game", test_plays, test_box_score)
        print("\n✅ Test validation completed successfully!")
        print("\nSample report:")
        print(report)
    except Exception as e:
        print(f"❌ Error during test: {e}")
        import traceback
        traceback.print_exc()