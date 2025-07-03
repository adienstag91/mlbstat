import pandas as pd
from typing import Dict, List, Tuple
import re

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
            