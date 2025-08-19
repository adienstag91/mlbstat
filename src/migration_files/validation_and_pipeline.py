# ============================================================================
# validation/player_categorizer.py - Player categorization utilities
# ============================================================================

import pandas as pd
from typing import Dict, List

def calculate_meaningful_batters(official_batting: pd.DataFrame) -> int:
    """Count batters with meaningful plate appearance activity"""
    if official_batting.empty:
        return 0
    
    meaningful_columns = ['PA', 'AB', 'H', 'BB', 'SO', 'HR', '2B', '3B', 'HBP', 'GDP', 'SF', 'SH']
    existing_columns = [col for col in meaningful_columns if col in official_batting.columns]
    
    if not existing_columns:
        return len(official_batting)
    
    meaningful_stats = official_batting[existing_columns].sum(axis=1) > 0
    return meaningful_stats.sum()

def categorize_unmatched_players(official_batting: pd.DataFrame, unmatched_names: List[str]) -> Dict:
    """Categorize unmatched players into pinch runners vs. true name mismatches"""
    
    if official_batting.empty or not unmatched_names:
        return {
        'accuracy': accuracy,
        'players_compared': len(comparison),
        'total_differences': int(total_diffs),
        'total_stats': int(total_stats),
        'differences': differences,
        'name_mismatches': mismatch_info
    }

# ============================================================================
# pipeline/game_processor.py - Main game processing pipeline
# ============================================================================

from mlb_cached_fetcher import SafePageFetcher
from parsing.name_utils import extract_canonical_names, build_name_resolver
from parsing.stats_parser import parse_official_batting, parse_official_pitching
from parsing.events_parser import parse_play_by_play_events
from parsing.game_utils import extract_game_id
from validation.stat_validator import validate_batting_stats, validate_pitching_stats

def process_single_game(game_url: str) -> Dict:
    """Process a complete game into unified events and official stats"""
    # Fetch page
    soup = SafePageFetcher.fetch_page(game_url)
    
    # Extract game metadata
    game_id = extract_game_id(game_url)
    
    # Parse official stats first to build name resolver
    official_batting = parse_official_batting(soup)
    official_pitching = parse_official_pitching(soup)
    
    # Build name resolution system
    canonical_names = extract_canonical_names(soup)
    name_resolver = build_name_resolver(canonical_names)
    
    # Parse unified events
    unified_events = parse_play_by_play_events(soup, game_id, name_resolver)
    
    # Validate
    batting_validation = validate_batting_stats(official_batting, unified_events)
    pitching_validation = validate_pitching_stats(official_pitching, unified_events)
    
    return {
        'game_id': game_id,
        'official_batting': official_batting,
        'official_pitching': official_pitching,
        'unified_events': unified_events,
        'batting_validation': batting_validation,
        'pitching_validation': pitching_validation
    }

def process_multiple_games(game_urls: List[str]) -> List[Dict]:
    """Process multiple games with error handling"""
    results = []
    
    for i, url in enumerate(game_urls):
        try:
            print(f"Processing game {i+1}/{len(game_urls)}: {url}")
            result = process_single_game(url)
            results.append(result)
        except Exception as e:
            print(f"❌ Failed to process {url}: {e}")
            # Could add to failed games list for retry
    
    return results

# ============================================================================
# pipeline/batch_validator.py - Batch validation and reporting
# ============================================================================

def validate_game_batch(game_results: List[Dict]) -> Dict:
    """Validate a batch of games and generate summary"""
    if not game_results:
        return {'accuracy': 0, 'games_processed': 0}
    
    total_batting_accuracy = 0
    total_pitching_accuracy = 0
    games_with_batting = 0
    games_with_pitching = 0
    
    for result in game_results:
        bat_val = result.get('batting_validation', {})
        pit_val = result.get('pitching_validation', {})
        
        if bat_val.get('players_compared', 0) > 0:
            total_batting_accuracy += bat_val.get('accuracy', 0)
            games_with_batting += 1
        
        if pit_val.get('players_compared', 0) > 0:
            total_pitching_accuracy += pit_val.get('accuracy', 0)
            games_with_pitching += 1
    
    avg_batting_accuracy = total_batting_accuracy / games_with_batting if games_with_batting > 0 else 0
    avg_pitching_accuracy = total_pitching_accuracy / games_with_pitching if games_with_pitching > 0 else 0
    
    return {
        'games_processed': len(game_results),
        'games_with_batting': games_with_batting,
        'games_with_pitching': games_with_pitching,
        'avg_batting_accuracy': avg_batting_accuracy,
        'avg_pitching_accuracy': avg_pitching_accuracy,
        'overall_accuracy': (avg_batting_accuracy + avg_pitching_accuracy) / 2
    }

def generate_accuracy_report(game_results: List[Dict]) -> pd.DataFrame:
    """Generate detailed CSV report of validation results"""
    report_data = []
    
    for result in game_results:
        game_id = result.get('game_id', 'unknown')
        bat_val = result.get('batting_validation', {})
        pit_val = result.get('pitching_validation', {})
        
        report_data.append({
            'game_id': game_id,
            'batting_accuracy': bat_val.get('accuracy', 0),
            'batting_players': bat_val.get('players_compared', 0),
            'batting_differences': bat_val.get('total_differences', 0),
            'pitching_accuracy': pit_val.get('accuracy', 0),
            'pitching_players': pit_val.get('players_compared', 0),
            'pitching_differences': pit_val.get('total_differences', 0),
            'events_count': len(result.get('unified_events', [])),
            'status': 'success'
        })
    
    return pd.DataFrame(report_data)

# ============================================================================
# main.py - Simple entry point (replaces your old class-based approach)
# ============================================================================

def main():
    """Main entry point - clean and simple"""
    from game_url_fetcher import GameURLFetcher
    from mlb_cached_fetcher import CachedSafePageFetcher
    
    # Initialize fetcher
    fetcher = CachedSafePageFetcher(cache_days=30)
    url_fetcher = GameURLFetcher(fetcher)
    
    # Get games to process (example: last 7 days)
    print("🔍 Fetching game URLs...")
    game_urls = url_fetcher.get_games_last_n_days(7)
    print(f"Found {len(game_urls)} games to process")
    
    # Process all games
    print("⚾ Processing games...")
    game_results = process_multiple_games(game_urls)
    
    # Validate batch
    print("✅ Validating results...")
    validation_summary = validate_game_batch(game_results)
    
    print(f"""
📊 BATCH PROCESSING COMPLETE
============================
Games processed: {validation_summary['games_processed']}
Batting accuracy: {validation_summary['avg_batting_accuracy']:.1f}%
Pitching accuracy: {validation_summary['avg_pitching_accuracy']:.1f}%
Overall accuracy: {validation_summary['overall_accuracy']:.1f}%
""")
    
    # Generate detailed report
    report_df = generate_accuracy_report(game_results)
    report_df.to_csv('batch_processing_report.csv', index=False)
    print("📄 Detailed report saved to 'batch_processing_report.csv'")
    
    return game_results

def test_single_game():
    """Test function for single game processing"""
    test_url = "https://www.baseball-reference.com/boxes/KCA/KCA202503290.shtml"
    
    print(f"🧪 Testing single game: {test_url}")
    result = process_single_game(test_url)
    
    events = result['unified_events']
    bat_val = result['batting_validation']
    pit_val = result['pitching_validation']
    
    print(f"""
📋 GAME RESULTS
===============
Game ID: {result['game_id']}
Events parsed: {len(events)}
Batting accuracy: {bat_val['accuracy']:.1f}% ({bat_val['players_compared']} players)
Pitching accuracy: {pit_val['accuracy']:.1f}% ({pit_val['players_compared']} pitchers)
""")
    
    # Show sample events
    if not events.empty:
        print("\n🎯 Sample Events:")
        cols = ['batter_id', 'pitcher_id', 'inning', 'description', 'is_hit', 'hit_type']
        available_cols = [col for col in cols if col in events.columns]
        print(events[available_cols].head(10).to_string(index=False))
    
    return result

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        test_single_game()
    else:
        main()'pinch_runners': [], 'name_mismatches': [], 'empty_stats': []}
    
    pinch_runners = []
    name_mismatches = []
    empty_stats = []
    
    for name in unmatched_names:
        player_row = official_batting[official_batting['player_name'] == name]
        
        if player_row.empty:
            name_mismatches.append(name)
            continue
        
        player_stats = player_row.iloc[0]
        
        # Define stat categories
        plate_appearance_stats = ['PA', 'AB', 'H', 'BB', 'SO', 'HR', '2B', '3B', 'HBP', 'GDP', 'SF', 'SH']
        baserunning_stats = ['R', 'SB', 'CS']
        all_stats = plate_appearance_stats + baserunning_stats
        
        # Get values that exist in the dataframe
        pa_values = [player_stats.get(stat, 0) for stat in plate_appearance_stats if stat in player_stats.index]
        br_values = [player_stats.get(stat, 0) for stat in baserunning_stats if stat in player_stats.index]
        all_values = [player_stats.get(stat, 0) for stat in all_stats if stat in player_stats.index]
        
        pa_count = player_stats.get('PA', 0)
        ab_count = player_stats.get('AB', 0)
        
        # Categorization logic
        if sum(all_values) == 0:
            empty_stats.append(name)
        elif pa_count == 0 and ab_count == 0 and sum(br_values) > 0:
            pinch_runners.append({
                'name': name,
                'stats': {stat: player_stats.get(stat, 0) for stat in baserunning_stats if stat in player_stats.index and player_stats.get(stat, 0) > 0}
            })
        elif pa_count > 0 or ab_count > 0:
            name_mismatches.append({
                'name': name,
                'pa': pa_count,
                'ab': ab_count,
                'stats': {stat: player_stats.get(stat, 0) for stat in plate_appearance_stats if stat in player_stats.index and player_stats.get(stat, 0) > 0}
            })
        else:
            name_mismatches.append({
                'name': name,
                'pa': pa_count,
                'ab': ab_count,
                'note': 'Has activity but no PA/AB'
            })
    
    return {
        'pinch_runners': pinch_runners,
        'name_mismatches': name_mismatches,
        'empty_stats': empty_stats
    }

# ============================================================================
# validation/stat_validator.py - Stats validation functions
# ============================================================================

from typing import Dict, List

def validate_batting_stats(official: pd.DataFrame, events: pd.DataFrame) -> Dict:
    """Validate batting by aggregating events"""
    if official.empty or events.empty:
        return {'accuracy': 0, 'players_compared': 0}

    # Filter to only players with meaningful baseball activity
    meaningful_columns = ['PA', 'AB', 'H', 'BB', 'SO', 'HR', '2B', '3B', 'SB', 'CS', 'HBP', 'GDP', 'SF', 'SH']
    meaningful_stats = official[meaningful_columns].sum(axis=1) > 0
    official = official[meaningful_stats]
    
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
    
    return compare_stats(official, parsed, ['PA', 'AB', 'H', 'BB', 'SO', 'HR', '2B', '3B'], 'player_name')

def validate_pitching_stats(official: pd.DataFrame, events: pd.DataFrame) -> Dict:
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
        'pitch_count': 'parsed_PC'
    })
    
    return compare_stats(official, parsed, ['BF', 'H', 'BB', 'SO', 'HR', 'PC'], 'pitcher_name')

def compare_stats(official: pd.DataFrame, parsed: pd.DataFrame, stats: List[str], name_col: str) -> Dict:
    """Compare official vs parsed stats with detailed categorization"""
    
    # Check for name mismatches before merging
    official_names = set(official[name_col].tolist())
    parsed_names = set(parsed[name_col].tolist())
    
    unmatched_official = list(official_names - parsed_names)
    unmatched_parsed = list(parsed_names - official_names)
    
    # Categorize unmatched official players
    player_categories = categorize_unmatched_players(official, unmatched_official)
    
    mismatch_info = {
        'unmatched_official_names': unmatched_official,
        'unmatched_parsed_names': unmatched_parsed,
        'total_official_players': len(official_names),
        'total_parsed_players': len(parsed_names),
        'player_categories': player_categories
    }
    
    comparison = pd.merge(official, parsed, on=name_col, how='inner')
    
    if comparison.empty:
        return {
            'accuracy': 0, 
            'players_compared': 0, 
            'total_differences': 0, 
            'differences': [],
            'name_mismatches': mismatch_info
        }
    
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
        