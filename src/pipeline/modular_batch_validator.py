#!/usr/bin/env python3
"""
Modular Batch Validator
======================

Replaces multi_game_unified_validator.py with clean modular functions.
Processes multiple games and generates detailed CSV reports.
"""

import pandas as pd
import sys
import os
import time
from typing import List, Dict
from datetime import datetime
import traceback
from game_url_fetcher import *

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from pipeline.game_processor import process_single_game, process_multiple_games

def validate_game_batch(game_results: List[Dict]) -> Dict:
    """Validate a batch of games and generate summary statistics"""
    if not game_results:
        return {
            'games_processed': 0,
            'games_successful': 0,
            'games_failed': 0,
            'success_rate': 0.0,
            'avg_batting_accuracy': 0.0,
            'avg_pitching_accuracy': 0.0,
            'overall_accuracy': 0.0
        }
    
    successful_games = [r for r in game_results if 'batting_validation' in r and 'pitching_validation' in r]
    failed_games = len(game_results) - len(successful_games)
    
    if not successful_games:
        return {
            'games_processed': len(game_results),
            'games_successful': 0,
            'games_failed': failed_games,
            'success_rate': 0.0,
            'avg_batting_accuracy': 0.0,
            'avg_pitching_accuracy': 0.0,
            'overall_accuracy': 0.0
        }
    
    # Calculate batting accuracy
    batting_accuracies = []
    pitching_accuracies = []
    
    for result in successful_games:
        bat_val = result.get('batting_validation', {})
        pit_val = result.get('pitching_validation', {})
        
        if bat_val.get('players_compared', 0) > 0:
            batting_accuracies.append(bat_val.get('accuracy', 0))
        
        if pit_val.get('players_compared', 0) > 0:
            pitching_accuracies.append(pit_val.get('accuracy', 0))
    
    avg_batting = sum(batting_accuracies) / len(batting_accuracies) if batting_accuracies else 0
    avg_pitching = sum(pitching_accuracies) / len(pitching_accuracies) if pitching_accuracies else 0
    
    return {
        'games_processed': len(game_results),
        'games_successful': len(successful_games),
        'games_failed': failed_games,
        'success_rate': len(successful_games) / len(game_results) * 100,
        'avg_batting_accuracy': avg_batting,
        'avg_pitching_accuracy': avg_pitching,
        'overall_accuracy': (avg_batting + avg_pitching) / 2,
        'games_with_batting_data': len(batting_accuracies),
        'games_with_pitching_data': len(pitching_accuracies)
    }

def generate_detailed_report(game_results: List[Dict]) -> pd.DataFrame:
    """Generate detailed CSV report of all game validations"""
    report_data = []
    
    for result in game_results:
        game_id = result.get('game_id', 'unknown')
        
        # Handle failed games
        if 'batting_validation' not in result or 'pitching_validation' not in result:
            report_data.append({
                'game_id': game_id,
                'status': 'failed',
                'batting_accuracy': 0,
                'batting_players': 0,
                'batting_differences': 0,
                'batting_total_stats': 0,
                'pitching_accuracy': 0,
                'pitching_players': 0,
                'pitching_differences': 0,
                'pitching_total_stats': 0,
                'events_count': 0,
                'processing_time': 0,
                'error': result.get('error', 'Unknown error')
            })
            continue
        
        bat_val = result.get('batting_validation', {})
        pit_val = result.get('pitching_validation', {})
        events = result.get('pbp_events', pd.DataFrame())
        
        report_data.append({
            'game_id': game_id,
            'status': 'success',
            'batting_accuracy': round(bat_val.get('accuracy', 0), 2),
            'batting_players': bat_val.get('players_compared', 0),
            'batting_differences': bat_val.get('total_differences', 0),
            'batting_total_stats': bat_val.get('total_stats', 0),
            'pitching_accuracy': round(pit_val.get('accuracy', 0), 2),
            'pitching_players': pit_val.get('players_compared', 0),
            'pitching_differences': pit_val.get('total_differences', 0),
            'pitching_total_stats': pit_val.get('total_stats', 0),
            'events_count': len(events),
            'processing_time': round(result.get('time_to_process'),2),
            'error': ''
        })
    
    return pd.DataFrame(report_data)

def generate_player_mismatch_report(game_results: List[Dict]) -> pd.DataFrame:
    """Generate report of player name mismatches across all games"""
    mismatch_data = []
    
    for result in game_results:
        game_id = result.get('game_id', 'unknown')
        
        for validation_type in ['batting_validation', 'pitching_validation']:
            validation = result.get(validation_type, {})
            mismatches = validation.get('name_mismatches', {})
            
            # True name mismatches (players with PA/AB but not matched)
            for player_info in mismatches.get('name_mismatches', []):
                mismatch_data.append({
                    'game_id': game_id,
                    'validation_type': validation_type.replace('_validation', ''),
                    'category': 'name_mismatch',
                    'player_name': player_info.get('name', ''),
                    'pa': player_info.get('pa', 0),
                    'ab': player_info.get('ab', 0),
                    'stats': str(player_info.get('stats', {}))
                })
            
            # Pinch runners (expected non-matches)
            for player_info in mismatches.get('pinch_runners', []):
                mismatch_data.append({
                    'game_id': game_id,
                    'validation_type': validation_type.replace('_validation', ''),
                    'category': 'pinch_runner',
                    'player_name': player_info.get('name', ''),
                    'pa': 0,
                    'ab': 0,
                    'stats': str(player_info.get('stats', {}))
                })
    
    return pd.DataFrame(mismatch_data)

def save_batch_results(game_results: List[Dict], output_dir: str = "batch_results"):
    """Save comprehensive batch results to files"""
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # 1. Detailed game-by-game report
    detailed_report = generate_detailed_report(game_results)
    detailed_file = os.path.join(output_dir, f"detailed_game_report_{timestamp}.csv")
    detailed_report.to_csv(detailed_file, index=False)
    print(f"📄 Detailed report saved: {detailed_file}")
    
    # 2. Player mismatch report
    mismatch_report = generate_player_mismatch_report(game_results)
    if not mismatch_report.empty:
        mismatch_file = os.path.join(output_dir, f"player_mismatches_{timestamp}.csv")
        mismatch_report.to_csv(mismatch_file, index=False)
        print(f"📄 Mismatch report saved: {mismatch_file}")
    
    # 3. Summary statistics
    summary = validate_game_batch(game_results)
    summary_file = os.path.join(output_dir, f"batch_summary_{timestamp}.txt")
    
    with open(summary_file, 'w') as f:
        f.write(f"MLB Batch Processing Summary - {timestamp}\n")
        f.write("=" * 50 + "\n\n")
        f.write(f"Games Processed: {summary['games_processed']}\n")
        f.write(f"Successful: {summary['games_successful']}\n")
        f.write(f"Failed: {summary['games_failed']}\n")
        f.write(f"Success Rate: {summary['success_rate']:.1f}%\n\n")
        f.write(f"Batting Accuracy: {summary['avg_batting_accuracy']:.2f}%\n")
        f.write(f"Pitching Accuracy: {summary['avg_pitching_accuracy']:.2f}%\n")
        f.write(f"Overall Accuracy: {summary['overall_accuracy']:.2f}%\n\n")
        f.write(f"Games with Batting Data: {summary['games_with_batting_data']}\n")
        f.write(f"Games with Pitching Data: {summary['games_with_pitching_data']}\n")
    
    print(f"📄 Summary saved: {summary_file}")
    
    return {
        'detailed_report': detailed_file,
        'mismatch_report': mismatch_file if not mismatch_report.empty else None,
        'summary_file': summary_file
    }

def process_games_with_urls(game_urls: List[str], save_results: bool = True) -> Dict:
    """Process multiple games and generate comprehensive reports"""

    start_time = time.time()

    print(f"🚀 Starting batch processing of {len(game_urls)} games...")
    print("=" * 60)
    
    # Process all games
    game_results = []
    failed_games = []
    
    for i, url in enumerate(game_urls):
        try:
            print(f"Processing game {i+1}/{len(game_urls)}: {url}")
            result = process_single_game(url, False) 
            game_results.append(result)
            
            # Display Game Stats
            bat_acc = result['batting_validation']['accuracy']
            pit_acc = result['pitching_validation']['accuracy']
            print(f"✅ {result['game_id']}: Batting {bat_acc:.1f}%, Pitching {pit_acc:.1f}%")
            print(f"⏱️ {result['game_id']} took {result['time_to_process']:.2f} to process")

            # Show actual stat differences (these are the real issues)
            batting_differences = result['batting_validation']['differences']
            pitching_differences = result['pitching_validation']['differences']
        
            if batting_differences:
                print(f"🚨 Batting Differences: {len(batting_differences)} players")
                first_diff = batting_differences[0]
                print(f"      {first_diff['player']}: {first_diff['diffs'][0]}")
                if len(batting_differences) > 1:
                    print(f"      ... and {len(batting_differences) - 1} more")

            if pitching_differences:
                print(f"🚨 Pitching Differences: {len(pitching_differences)} pitchers")
                first_diff = pitching_differences[0]
                print(f"      {first_diff['player']}: {first_diff['diffs'][0]}")
                if len(pitching_differences) > 1:
                    print(f"      ... and {len(pitching_differences) - 1} more")

            
        except Exception as e:
            error_info = {
                'url': url,
                'game_id': f'failed_{i+1}',
                'error': str(e),
                'traceback': traceback.format_exc()
            }
            failed_games.append(error_info)
            print(f"   ❌ Failed: {str(e)}")
    
    # Generate summary
    summary = validate_game_batch(game_results)
    
    # Save results if requested
    saved_files = {}
    if save_results and game_results:
        saved_files = save_batch_results(game_results)

    batch_time = time.time() - start_time
    
    # Print summary
    print("\n" + "=" * 60)
    print("📊 BATCH PROCESSING COMPLETE")
    print("=" * 60)
    print(f"Time to Process Batch: {batch_time:.2f}")
    print(f"Games Processed: {summary['games_processed']}")
    print(f"Success Rate: {summary['success_rate']:.1f}%")
    print(f"Batting Accuracy: {summary['avg_batting_accuracy']:.2f}%")
    print(f"Pitching Accuracy: {summary['avg_pitching_accuracy']:.2f}%")
    print(f"Overall Accuracy: {summary['overall_accuracy']:.2f}%")
    
    if failed_games:
        print(f"\n❌ Failed Games ({len(failed_games)}):")
        for failed in failed_games[:5]:  # Show first 5 failures
            print(f"   {failed['url']}: {failed['error']}")
        if len(failed_games) > 5:
            print(f"   ... and {len(failed_games) - 5} more")
    
    return {
        'game_results': game_results,
        'failed_games': failed_games,
        'summary': summary,
        'saved_files': saved_files,
        'batch_time': batch_time
    }

def run_season_validation(year: str = "2025", team: str = None, max_games: int = None):
    """Run full season validation (placeholder - you'll need to adapt this to your URL fetching)"""
    
    # This is where you'd integrate with your existing game URL fetching logic
    # For now, using a sample game for demonstration
    
    #sample_urls = [
    #    "https://www.baseball-reference.com/boxes/KCA/KCA202503290.shtml",
    #    # Add more URLs here or integrate with your game_url_fetcher
    #]
    season_urls = get_games_full_season(year)
    
    if max_games:
        season_urls = season_urls[:max_games]
    
    print(f"🔍 Running {year} season validation...")
    if team:
        print(f"   Team filter: {team}")
    if max_games:
        print(f"   Max games: {max_games}")
    
    return process_games_with_urls(season_urls)

# Example usage and testing
def test_batch_validator():
    """Test the batch validator with a few games"""
    
    test_urls = [
        "https://www.baseball-reference.com/boxes/KCA/KCA202503290.shtml",
        "https://www.baseball-reference.com//boxes/NYA/NYA202505050.shtml",
        "https://www.baseball-reference.com//boxes/TEX/TEX202505180.shtml",
        "https://www.baseball-reference.com//boxes/CLE/CLE202505300.shtml",
        "https://www.baseball-reference.com//boxes/ANA/ANA202506230.shtml",
        "https://www.baseball-reference.com//boxes/ATL/ATL202506270.shtml",
        "https://www.baseball-reference.com/boxes/NYA/NYA202505170.shtml"
        # Add more test URLs here
    ]
    
    print("🧪 Testing batch validator...")
    results = process_games_with_urls(test_urls, save_results=True)
    
    print(f"\n✅ Batch validator test complete!")
    print(f"   Processed: {len(results['game_results'])} games")
    print(f"   Failed: {len(results['failed_games'])} games")
    print(f"   Files saved: {list(results['saved_files'].values())}")
    
    return results

if __name__ == "__main__":
    if len(sys.argv) > 1:
        if sys.argv[1] == "test":
            test_batch_validator()
        elif sys.argv[1] == "season":
            year = sys.argv[2] if len(sys.argv) > 2 else "2025"
            max_games = int(sys.argv[3]) if len(sys.argv) > 3 else 5
            run_season_validation(year, max_games=max_games)
    elif len(sys.argv) > 2 :
        sys.argv[1] == "team"
        team = sys.agv[2]
        max_games = int(sys.argv[3]) if len(sys.argv) > 3 else 5
        run_season_validation("2025", "NYY", max_games=max_games)
    else:
        test_batch_validator()
