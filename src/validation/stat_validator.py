"""
Stats validation functions
=========================
"""

import pandas as pd
from typing import Dict, List

def categorize_unmatched_players(official_df: pd.DataFrame, unmatched_names: List[str], name_column: str = None) -> Dict:
    """Categorize unmatched players - works for both batting and pitching"""
    if official_df.empty or not unmatched_names:
        return {'pinch_runners': [], 'name_mismatches': [], 'empty_stats': []}
    
    # Auto-detect the correct column name
    if name_column is None:
        if 'player_name' in official_df.columns:
            name_column = 'player_name'
        elif 'pitcher_name' in official_df.columns:
            name_column = 'pitcher_name'
        else:
            return {'pinch_runners': [], 'name_mismatches': [], 'empty_stats': []}

    pinch_runners = []
    name_mismatches = []
    empty_stats = []
    
    for name in unmatched_names:
        # Use the correct column name instead of hardcoded 'player_name'
        if name not in official_df[name_column].values:
            continue  # Skip 0-BF pitchers
        
        player_row = official_df[official_df[name_column] == name]
        
        if player_row.empty:
            name_mismatches.append(name)
            continue
        
        player_stats = player_row.iloc[0]
        
        plate_appearance_stats = ['PA', 'AB', 'H', 'BB', 'SO', 'HR', '2B', '3B', 'HBP', 'GDP', 'SF', 'SH']
        baserunning_stats = ['R', 'SB', 'CS']
        all_stats = plate_appearance_stats + baserunning_stats
        
        pa_values = [player_stats.get(stat, 0) for stat in plate_appearance_stats if stat in player_stats.index]
        br_values = [player_stats.get(stat, 0) for stat in baserunning_stats if stat in player_stats.index]
        all_values = [player_stats.get(stat, 0) for stat in all_stats if stat in player_stats.index]
        
        pa_count = player_stats.get('PA', 0)
        ab_count = player_stats.get('AB', 0)
        
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

def compare_stats(official: pd.DataFrame, parsed: pd.DataFrame, stats: List[str], name_col: str) -> Dict:
    """Compare official vs parsed stats with detailed categorization"""
    
    official_names = set(official[name_col].tolist())
    parsed_names = set(parsed[name_col].tolist())
    
    unmatched_official = list(official_names - parsed_names)
    unmatched_parsed = list(parsed_names - official_names)
    
    player_categories = categorize_unmatched_players(official, unmatched_official, name_col)
    
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
    
    # **FIX: Ensure all stat columns are numeric in both official and parsed**
    for stat in stats:
        # Convert official stat column
        if stat in comparison.columns:
            comparison[stat] = pd.to_numeric(comparison[stat], errors='coerce').fillna(0)
        
        # Convert parsed stat column
        parsed_col = f'parsed_{stat}'
        if parsed_col in comparison.columns:
            comparison[parsed_col] = pd.to_numeric(comparison[parsed_col], errors='coerce').fillna(0)
    
    total_diffs = 0
    total_stats = 0
    differences = []
    
    for stat in stats:
        parsed_col = f'parsed_{stat}'
        if parsed_col in comparison.columns:
            comparison[f'{stat}_diff'] = comparison[parsed_col] - comparison[stat]
            diffs = comparison[f'{stat}_diff'].abs().sum()
            total_diffs += diffs
            total_stats += comparison[stat].sum()
    
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
        'differences': differences,
        'name_mismatches': mismatch_info
    }

def validate_batting_stats(official: pd.DataFrame, events: pd.DataFrame) -> Dict:
    """Validate batting by aggregating events"""
    if official.empty or events.empty:
        return {'accuracy': 0, 'players_compared': 0}

    # **FIX: Convert to numeric BEFORE any mathematical operations**
    meaningful_columns = ['PA', 'AB', 'H', 'BB', 'SO', 'HR', '2B', '3B', 'SB', 'CS', 'HBP', 'GDP', 'SF', 'SH']
    
    # Ensure all columns are numeric
    for col in meaningful_columns:
        if col in official.columns:
            official[col] = pd.to_numeric(official[col], errors='coerce').fillna(0)
    
    # Now safe to sum and compare
    meaningful_stats = official[meaningful_columns].sum(axis=1) > 0
    official = official[meaningful_stats]
    
    parsed = events.groupby('batter_name').agg({
        'is_plate_appearance': 'sum',
        'is_at_bat': 'sum',
        'is_hit': 'sum',
        'is_walk': 'sum',
        'is_strikeout': 'sum'
    }).reset_index()
    
    hit_types = ['home_run', 'double', 'triple']
    for hit_type in hit_types:
        hit_agg = events[events['hit_type'] == hit_type].groupby('batter_name').size().reset_index(name=f'parsed_{hit_type.upper().replace("_", "")}')
        if hit_type == 'home_run':
            hit_agg = hit_agg.rename(columns={'parsed_HR': 'parsed_HR'})
        elif hit_type == 'double':
            hit_agg = hit_agg.rename(columns={'parsed_DOUBLE': 'parsed_2B'})
        elif hit_type == 'triple':
            hit_agg = hit_agg.rename(columns={'parsed_TRIPLE': 'parsed_3B'})
        parsed = parsed.merge(hit_agg, on='batter_name', how='left').fillna(0)
    
    parsed = parsed.rename(columns={
        'batter_name': 'player_name',
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
    
    # **FIX: Convert to numeric BEFORE any mathematical operations**
    numeric_columns = ['BF', 'H', 'BB', 'SO', 'HR', 'R', 'ER', 'IP']
    
    # Ensure all columns are numeric
    for col in numeric_columns:
        if col in official.columns:
            official[col] = pd.to_numeric(official[col], errors='coerce').fillna(0)
    
    parsed = events.groupby('pitcher_name').agg({
        'is_plate_appearance': 'sum',
        'is_hit': 'sum',
        'is_walk': 'sum',
        'is_strikeout': 'sum',
        'pitch_count': 'sum'
    }).reset_index()
    
    hr_agg = events[events['hit_type'] == 'home_run'].groupby('pitcher_name').size().reset_index(name='parsed_HR')
    parsed = parsed.merge(hr_agg, on='pitcher_name', how='left').fillna(0)
    
    parsed = parsed.rename(columns={
        'pitcher_name': 'pitcher_name',
        'is_plate_appearance': 'parsed_BF',
        'is_hit': 'parsed_H',
        'is_walk': 'parsed_BB',
        'is_strikeout': 'parsed_SO',
        'pitch_count': 'parsed_PC'
    })
    
    return compare_stats(official, parsed, ['BF', 'H', 'BB', 'SO', 'HR'], 'pitcher_name')