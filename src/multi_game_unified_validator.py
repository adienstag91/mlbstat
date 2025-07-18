"""
Multi-Game Unified Validator
============================

Batch validator for testing the unified events parser across multiple games.
Provides comprehensive accuracy statistics and detailed reporting.
"""

import pandas as pd
import numpy as np
from typing import Dict, List
import time
import traceback
import os
from datetime import datetime, timedelta
from dataclasses import dataclass
from unified_events_parser import UnifiedEventsParser
from game_url_fetcher import GameURLFetcher

@dataclass
class MultiGameResults:
    """Container for multi-game validation results"""
    total_games: int
    successful_games: int
    failed_games: int
    batting_accuracy_scores: List[float]
    pitching_accuracy_scores: List[float]
    avg_batting_accuracy: float
    avg_pitching_accuracy: float
    game_results: List[Dict]
    failure_details: List[Dict]

class MultiGameUnifiedValidator:
    """Validate unified events parser accuracy across multiple games"""
    
    def __init__(self):
        self.unified_parser = UnifiedEventsParser()
        self.results = []
        self.failures = []
    
    def validate_multiple_games(self, game_urls: List[str], max_failures: int = 3) -> MultiGameResults:
        """Validate parsing across multiple games"""
        
        print(f"🎯 MULTI-GAME UNIFIED VALIDATION:")
        print(f"   Testing {len(game_urls)} games")
        print(f"   Max failures allowed: {max_failures}")
        print("=" * 60)
        
        batting_accuracies = []
        pitching_accuracies = []
        successful_games = 0
        failed_games = 0
        
        for i, game_url in enumerate(game_urls, 1):
            print(f"\n🎮 GAME {i}/{len(game_urls)}:")
            print(f"   URL: {game_url}")
            
            try:
                # Parse game with unified parser
                result = self.unified_parser.parse_game(game_url)
                
                # Extract validation results
                bat_val = result['batting_validation']
                pit_val = result['pitching_validation']
                
                bat_acc = bat_val['accuracy']
                pit_acc = pit_val['accuracy']
                
                events_count = len(result['unified_events']) if not result['unified_events'].empty else 0
                
                # Store results
                game_result = {
                    'game_number': i,
                    'game_url': game_url,
                    'game_id': result['game_id'],
                    'batting_accuracy': bat_acc,
                    'pitching_accuracy': pit_acc,
                    'batting_players_total': len(result['official_batting'])-len(result['official_pitching']),
                    'pitching_players_total': len(result['official_pitching']),
                    'batting_players_compared': bat_val.get('players_compared', 0),
                    'pitching_players_compared': pit_val.get('players_compared', 0),
                    'unified_events_count': events_count,
                    'batting_diffs': bat_val.get('total_differences', 0),
                    'pitching_diffs': pit_val.get('total_differences', 0),
                    'batting_stats_total': bat_val.get('total_stats', 0),
                    'pitching_stats_total': pit_val.get('total_stats', 0),
                    'status': 'success'
                }
                
                self.results.append(game_result)
                batting_accuracies.append(bat_acc)
                pitching_accuracies.append(pit_acc)
                successful_games += 1
                
                # Display results
                self._display_game_result(game_result, bat_val, pit_val)
                
                # Sleep between games to be respectful
                time.sleep(2)
                
            except Exception as e:
                failed_games += 1
                error_details = {
                    'game_number': i,
                    'game_url': game_url,
                    'error': str(e),
                    'traceback': traceback.format_exc()
                }
                
                self.failures.append(error_details)
                print(f"   ❌ FAILED: {str(e)[:100]}...")
                
                # Stop if too many failures
                if failed_games >= max_failures:
                    print(f"\n🛑 STOPPING: Reached max failures ({max_failures})")
                    break
                
                time.sleep(3)  # Longer sleep after failure
        
        # Calculate summary statistics
        avg_batting = np.mean(batting_accuracies) if batting_accuracies else 0
        avg_pitching = np.mean(pitching_accuracies) if pitching_accuracies else 0
        
        results = MultiGameResults(
            total_games=len(game_urls),
            successful_games=successful_games,
            failed_games=failed_games,
            batting_accuracy_scores=batting_accuracies,
            pitching_accuracy_scores=pitching_accuracies,
            avg_batting_accuracy=avg_batting,
            avg_pitching_accuracy=avg_pitching,
            game_results=self.results,
            failure_details=self.failures
        )
        
        self._print_summary(results)
        return results
    
    def _display_game_result(self, game_result: Dict, batting_results: Dict, pitching_results: Dict):
        """Display results for a single game"""
        
        # Show main results
        print(f"   ✅ Batting: {game_result['batting_accuracy']:.1f}% ({game_result['batting_diffs']} diffs/{game_result['batting_stats_total']} stats)")
        print(f"   ✅ Pitching: {game_result['pitching_accuracy']:.1f}% ({game_result['pitching_diffs']} diffs/{game_result['pitching_stats_total']} stats)")
        print(f"   📊 {game_result['batting_players_compared']}/{game_result['batting_players_total']} batters, {game_result['pitching_players_compared']}/{game_result['pitching_players_total']} pitchers")
        print(f"   📊 {game_result['unified_events_count']} unified events parsed")
        
        # Show differences if any (but keep them brief for multi-game output)
        batting_differences = batting_results.get('differences', [])
        pitching_differences = pitching_results.get('differences', [])
        
        if batting_differences:
            print(f"   ⚠️  Batting differences: {len(batting_differences)} players")
            # Only show first difference for brevity
            if batting_differences:
                first_diff = batting_differences[0]
                print(f"      {first_diff['player']}: {first_diff['diffs'][0]}")
                if len(batting_differences) > 1:
                    print(f"      ... and {len(batting_differences) - 1} more")
        
        if pitching_differences:
            print(f"   ⚠️  Pitching differences: {len(pitching_differences)} pitchers")
            # Only show first difference for brevity
            if pitching_differences:
                first_diff = pitching_differences[0]
                print(f"      {first_diff['player']}: {first_diff['diffs'][0]}")
                if len(pitching_differences) > 1:
                    print(f"      ... and {len(pitching_differences) - 1} more")
    
    def _print_summary(self, results: MultiGameResults):
        """Print comprehensive summary of multi-game results"""
        
        print(f"\n" + "=" * 70)
        print(f"📊 MULTI-GAME UNIFIED VALIDATION SUMMARY")
        print("=" * 70)
        
        # Overall success rate
        success_rate = (results.successful_games / results.total_games) * 100 if results.total_games > 0 else 0
        print(f"🎯 OVERALL SUCCESS:")
        print(f"   Games tested: {results.total_games}")
        print(f"   Successful: {results.successful_games}")
        print(f"   Failed: {results.failed_games}")
        print(f"   Success rate: {success_rate:.1f}%")
        
        if results.batting_accuracy_scores:
            # Batting accuracy statistics
            batting_scores = results.batting_accuracy_scores
            print(f"\n⚾ BATTING ACCURACY:")
            print(f"   Average: {results.avg_batting_accuracy:.1f}%")
            print(f"   Min: {min(batting_scores):.1f}%")
            print(f"   Max: {max(batting_scores):.1f}%")
            print(f"   Std Dev: {np.std(batting_scores):.1f}%")
            
            # Perfect games
            perfect_batting = sum(1 for score in batting_scores if score == 100.0)
            print(f"   Perfect games: {perfect_batting}/{len(batting_scores)}")
            
        if results.pitching_accuracy_scores:
            # Pitching accuracy statistics
            pitching_scores = results.pitching_accuracy_scores
            print(f"\n🥎 PITCHING ACCURACY:")
            print(f"   Average: {results.avg_pitching_accuracy:.1f}%")
            print(f"   Min: {min(pitching_scores):.1f}%")
            print(f"   Max: {max(pitching_scores):.1f}%")
            print(f"   Std Dev: {np.std(pitching_scores):.1f}%")
            
            # Perfect games
            perfect_pitching = sum(1 for score in pitching_scores if score == 100.0)
            print(f"   Perfect games: {perfect_pitching}/{len(pitching_scores)}")
        
        # Game-by-game breakdown
        if results.game_results:
            print(f"\n📋 GAME-BY-GAME BREAKDOWN:")
            print("   Game | Game ID      | Batting | Pitching | Events | Batters | Pitchers")
            print("   -----|--------------|---------|----------|--------|---------|----------")
            for game in results.game_results:
                game_id = game.get('game_id', 'unknown')
                bat_ratio = f"{game.get('batting_players_compared', 0)}/{game.get('batting_players_total', 0)}"
                pit_ratio = f"{game.get('pitching_players_compared', 0)}/{game.get('pitching_players_total', 0)}"
                events = game.get('unified_events_count', 0)
                print(f"   {game['game_number']:4d} | {game_id:14s} | {game['batting_accuracy']:6.1f}% | "
                      f"{game['pitching_accuracy']:7.1f}% | {events:6d} | {bat_ratio:>7s} | {pit_ratio:>8s}")
        
        # Failure analysis
        if results.failure_details:
            print(f"\n❌ FAILURE ANALYSIS:")
            for failure in results.failure_details:
                print(f"   Game {failure['game_number']}: {failure['error'][:80]}...")
        
        # Overall assessment
        print(f"\n🎯 ASSESSMENT:")
        if results.avg_batting_accuracy >= 99 and results.avg_pitching_accuracy >= 95:
            print("   🎉 EXCELLENT! Ready for production pipeline")
        elif results.avg_batting_accuracy >= 95 and results.avg_pitching_accuracy >= 90:
            print("   ✅ GOOD! Minor refinements needed")
        else:
            print("   🔧 NEEDS WORK! Significant improvements required")
        
        print("=" * 70)
    
    def save_detailed_results(self, results: MultiGameResults):
        """Save detailed results to CSV with timestamp in subfolder"""
        
        # Create results directory if it doesn't exist
        results_dir = "validation_results"
        os.makedirs(results_dir, exist_ok=True)
        
        # Generate timestamp for unique filenames
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if results.game_results:
            # Save detailed results
            detailed_filename = f"{results_dir}/multi_game__{timestamp}.csv"
            results_df = pd.DataFrame(results.game_results)
            numeric_columns = results_df.select_dtypes(include=[np.number]).columns
            results_df[numeric_columns] = results_df[numeric_columns].round(2)
            results_df.to_csv(detailed_filename, index=False)
            print(f"\n💾 Detailed results saved to '{detailed_filename}'")
            
            # Save summary
            summary_data = {
                'timestamp': [timestamp],
                'total_games': [results.total_games],
                'successful_games': [results.successful_games],
                'failed_games': [results.failed_games],
                'avg_batting_accuracy': [round(results.avg_batting_accuracy, 2)],
                'avg_pitching_accuracy': [round(results.avg_pitching_accuracy, 2)],
                'perfect_batting_games': [sum(1 for score in results.batting_accuracy_scores if score == 100.0)] if results.batting_accuracy_scores else [0],
                'perfect_pitching_games': [sum(1 for score in results.pitching_accuracy_scores if score == 100.0)] if results.pitching_accuracy_scores else [0],
                'batting_min': [round(min(results.batting_accuracy_scores), 2)] if results.batting_accuracy_scores else [0],
                'batting_max': [round(max(results.batting_accuracy_scores), 2)] if results.batting_accuracy_scores else [0],
                'pitching_min': [round(min(results.pitching_accuracy_scores), 2)] if results.pitching_accuracy_scores else [0],
                'pitching_max': [round(max(results.pitching_accuracy_scores), 2)] if results.pitching_accuracy_scores else [0],
            }
            summary_df = pd.DataFrame(summary_data)
            
            # Also append to a master log file for tracking progress over time
            master_log = f"{results_dir}/master_validation_log.csv"
            if os.path.exists(master_log):
                # Append to existing log
                existing_df = pd.read_csv(master_log)
                updated_df = pd.concat([existing_df, summary_df], ignore_index=True)
                updated_df.to_csv(master_log, index=False)
                print(f"📝 Results appended to master log: '{master_log}'")
            else:
                # Create new master log
                summary_df.to_csv(master_log, index=False)
                print(f"📝 Master log created: '{master_log}'")
            
            return detailed_filename
        
        return None, None

def run_batch_validation(url_list: List[str], max_failures: int = 3) -> MultiGameResults:
    """Run batch validation on multiple games"""
    
    print("🚀 STARTING BATCH UNIFIED VALIDATION")
    print("=" * 50)
    
    print(f"📋 Test games selected:")
    for i, url in enumerate(url_list, 1):
        game_id = url.split('/')[-1].replace('.shtml', '')
        print(f"   {i:2d}. {game_id}")
    
    print()
    
    # Create validator and run test
    validator = MultiGameUnifiedValidator()
    results = validator.validate_multiple_games(url_list, max_failures=max_failures)
    
    # Save results with dynamic filenames
    validator.save_detailed_results(results)
    
    return results

def run_full_season_validation(start_date: str = "2025-03-27", 
                             end_date: str = None, 
                             max_failures: int = 20) -> MultiGameResults:
    """
    Validate the entire 2025 regular season by cycling through each date.
    
    Args:
        start_date: Start of regular season (default: 2025-04-01 to skip spring training)
        end_date: End date (default: yesterday to avoid incomplete games)
        max_failures: Max failures before stopping entire validation
        
    Returns:
        MultiGameResults object with comprehensive season stats
    """
    
    print("🏆 2025 MLB SEASON VALIDATION")
    print("=" * 50)
    
    # Set end date to yesterday if not specified (avoid incomplete games)
    if end_date is None:
        end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    print(f"📅 Date range: {start_date} to {end_date}")
    print(f"🚫 Excluding spring training games")
    
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    total_days = (end_dt - start_dt).days + 1
    
    # Collect all game URLs by cycling through dates
    fetcher = GameURLFetcher()
    all_game_urls = []
    failed_dates = []
    
    print(f"\n📊 Scanning {total_days} days for completed games...")
    
    current_date = start_dt
    day_count = 0
    
    while current_date <= end_dt:
        date_str = current_date.strftime('%Y-%m-%d')
        day_count += 1
        
        try:
            print(f"  Day {day_count:3d}/{total_days}: {date_str}...", end=" ")
            
            # Use existing fetcher to get games for this date
            games = fetcher.get_games_by_date(date_str, completed_only=True)
            
            if games:
                all_game_urls.extend(games)
                print(f"✅ {len(games)} games")
            else:
                print("📭 No games")
            
            # Be respectful with requests
            time.sleep(1)
            
        except Exception as e:
            print(f"❌ Error: {str(e)[:50]}...")
            failed_dates.append(date_str)
        
        current_date += timedelta(days=1)
    
    # Summary of collection phase
    print(f"\n📋 GAME COLLECTION COMPLETE:")
    print(f"   Total games found: {len(all_game_urls)}")
    print(f"   Failed dates: {len(failed_dates)}")
    if failed_dates:
        print(f"   Failed dates: {', '.join(failed_dates[:5])}{'...' if len(failed_dates) > 5 else ''}")
    
    # Remove any potential duplicates (shouldn't happen with date-based, but safety first)
    unique_games = list(dict.fromkeys(all_game_urls))
    duplicates_removed = len(all_game_urls) - len(unique_games)
    
    if duplicates_removed > 0:
        print(f"   🔄 Removed {duplicates_removed} duplicate games")
        all_game_urls = unique_games
    
    # Now run the existing validation using all your existing functions
    print(f"\n🎯 STARTING SEASON VALIDATION OF {len(all_game_urls)} GAMES")
    print("=" * 60)
    
    # Use existing validator infrastructure
    validator = MultiGameUnifiedValidator()
    results = validator.validate_multiple_games(all_game_urls, max_failures=max_failures)
    
    # Use existing save function but with season-specific filename
    season_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Save detailed results using existing infrastructure
    validator.save_detailed_results(results)
    
    # Create additional season-specific summary
    _create_season_summary_file(results, start_date, end_date, len(all_game_urls), failed_dates)
    
    return results

def _create_season_summary_file(results: MultiGameResults, start_date: str, end_date: str, 
                               total_games: int, failed_dates: list):
    """Create a season-specific summary file"""
    
    import os
    
    # Create results directory if it doesn't exist
    results_dir = "validation_results"
    os.makedirs(results_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    summary_file = f"{results_dir}/season_summary_{timestamp}.txt"
    
    with open(summary_file, 'w') as f:
        f.write("🏆 2025 MLB REGULAR SEASON VALIDATION SUMMARY\n")
        f.write("=" * 55 + "\n\n")
        
        f.write(f"📅 Season Date Range: {start_date} to {end_date}\n")
        f.write(f"⏱️  Validation Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"🎮 Total Games Found: {total_games}\n")
        f.write(f"✅ Successfully Validated: {results.successful_games}\n")
        f.write(f"❌ Failed Games: {results.failed_games}\n")
        f.write(f"📊 Overall Success Rate: {results.successful_games/(results.successful_games + results.failed_games)*100:.1f}%\n\n")
        
        f.write("⚾ BATTING ACCURACY:\n")
        f.write(f"   Average: {results.avg_batting_accuracy:.2f}%\n")
        f.write(f"   Best: {max(results.batting_accuracy_scores):.1f}%\n")
        f.write(f"   Worst: {min(results.batting_accuracy_scores):.1f}%\n")
        f.write(f"   Perfect Games (100%): {sum(1 for x in results.batting_accuracy_scores if x == 100.0)}/{len(results.batting_accuracy_scores)}\n\n")
        
        f.write("🥎 PITCHING ACCURACY:\n")
        f.write(f"   Average: {results.avg_pitching_accuracy:.2f}%\n")
        f.write(f"   Best: {max(results.pitching_accuracy_scores):.1f}%\n")
        f.write(f"   Worst: {min(results.pitching_accuracy_scores):.1f}%\n")
        f.write(f"   Perfect Games (100%): {sum(1 for x in results.pitching_accuracy_scores if x == 100.0)}/{len(results.pitching_accuracy_scores)}\n\n")
        
        # Overall assessment
        f.write("🎯 SEASON ASSESSMENT:\n")
        if results.avg_batting_accuracy >= 99 and results.avg_pitching_accuracy >= 95:
            f.write("   🎉 EXCELLENT! Parser is production-ready for 2025 season\n")
        elif results.avg_batting_accuracy >= 95 and results.avg_pitching_accuracy >= 90:
            f.write("   ✅ GOOD! Minor refinements needed\n")
        else:
            f.write("   🔧 NEEDS WORK! Significant improvements required\n")
        
        if failed_dates:
            f.write(f"\n📅 Failed Date Collection ({len(failed_dates)} dates):\n")
            for date in failed_dates:
                f.write(f"   {date}\n")
    
    print(f"\n📝 Season summary saved to: {summary_file}")

# Usage example to add to the bottom of multi_game_unified_validator.py
def example_usage():
    """
    Example of how to use the full season validator.
    Add this to the __main__ section of multi_game_unified_validator.py
    """
    
    # Run full season validation
    season_results = run_full_season_validation(
        start_date="2025-03-27",  # Regular season start (no spring training)
        max_failures=20           # Stop after 20 failures
    )
    
    print(f"\n🏆 2025 SEASON VALIDATION COMPLETE!")
    print(f"📊 Final Results:")
    print(f"   Batting: {season_results.avg_batting_accuracy:.1f}%")
    print(f"   Pitching: {season_results.avg_pitching_accuracy:.1f}%")
    print(f"   Games: {season_results.successful_games} successful, {season_results.failed_games} failed")


if __name__ == "__main__":
    # Default: run standard validation
    # Uncomment different lines to run different tests:
    
    # Standard test (10 games)
    #results = run_batch_validation(extended=False, max_failures=3)
    
    # Extended test (15 games) 
    # results = run_batch_validation(extended=True, max_failures=5)
    
    # Quick test (3 games)
    # results = run_quick_test()
    #fetcher = GameURLFetcher()
    #url_list = fetcher.get_games_by_team("LAD")
    #results = run_batch_validation(url_list)
    #print(f"\n🎯 Final Results: {results.avg_batting_accuracy:.2f}% batting, {results.avg_pitching_accuracy:.2f}% pitching")


    # Run full season validation:
    season_results = run_full_season_validation(start_date="2025-03-27", max_failures=20)
    
    print(f"\n🏆 2025 SEASON COMPLETE!")
    print(f"Batting: {season_results.avg_batting_accuracy:.2f}%, Pitching: {season_results.avg_pitching_accuracy:.2f}%")

