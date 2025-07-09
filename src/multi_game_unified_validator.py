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
from datetime import datetime
from dataclasses import dataclass
from unified_events_parser import UnifiedEventsParser

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
                    'batting_players_total': len(result['official_batting']),
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
                game_id = game.get('game_id', 'unknown')[:12]  # Truncate for display
                bat_ratio = f"{game.get('batting_players_compared', 0)}/{game.get('batting_players_total', 0)}"
                pit_ratio = f"{game.get('pitching_players_compared', 0)}/{game.get('pitching_players_total', 0)}"
                events = game.get('unified_events_count', 0)
                print(f"   {game['game_number']:4d} | {game_id:12s} | {game['batting_accuracy']:6.1f}% | "
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
    
    def save_detailed_results(self, results: MultiGameResults, test_type: str = "standard"):
        """Save detailed results to CSV with timestamp in subfolder"""
        
        # Create results directory if it doesn't exist
        results_dir = "validation_results"
        os.makedirs(results_dir, exist_ok=True)
        
        # Generate timestamp for unique filenames
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if results.game_results:
            # Save detailed results
            detailed_filename = f"{results_dir}/multi_game_{test_type}_{timestamp}.csv"
            results_df = pd.DataFrame(results.game_results)
            results_df.to_csv(detailed_filename, index=False)
            print(f"\n💾 Detailed results saved to '{detailed_filename}'")
            
            # Save summary
            summary_data = {
                'timestamp': [timestamp],
                'test_type': [test_type],
                'total_games': [results.total_games],
                'successful_games': [results.successful_games],
                'failed_games': [results.failed_games],
                'avg_batting_accuracy': [results.avg_batting_accuracy],
                'avg_pitching_accuracy': [results.avg_pitching_accuracy],
                'perfect_batting_games': [sum(1 for score in results.batting_accuracy_scores if score == 100.0)] if results.batting_accuracy_scores else [0],
                'perfect_pitching_games': [sum(1 for score in results.pitching_accuracy_scores if score == 100.0)] if results.pitching_accuracy_scores else [0],
                'batting_min': [min(results.batting_accuracy_scores)] if results.batting_accuracy_scores else [0],
                'batting_max': [max(results.batting_accuracy_scores)] if results.batting_accuracy_scores else [0],
                'pitching_min': [min(results.pitching_accuracy_scores)] if results.pitching_accuracy_scores else [0],
                'pitching_max': [max(results.pitching_accuracy_scores)] if results.pitching_accuracy_scores else [0],
            }
            summary_df = pd.DataFrame(summary_data)
            summary_filename = f"{results_dir}/summary_{test_type}_{timestamp}.csv"
            summary_df.to_csv(summary_filename, index=False)
            print(f"📈 Summary saved to '{summary_filename}'")
            
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
            
            return detailed_filename, summary_filename
        
        return None, None

# TEST GAME URLS - Mix of different teams, dates, and scenarios
def get_test_game_urls() -> List[str]:
    """Get a diverse set of test game URLs for validation"""
    
    test_urls = [
        "https://www.baseball-reference.com/boxes/NYA/NYA202503270.shtml",  # Yankees vs Brewers
        "https://www.baseball-reference.com/boxes/LAN/LAN202503280.shtml",  # Dodgers
        "https://www.baseball-reference.com/boxes/HOU/HOU202503290.shtml",  # Astros
        "https://www.baseball-reference.com/boxes/SFN/SFN202503300.shtml",  # Giants
        "https://www.baseball-reference.com/boxes/SDN/SDN202503310.shtml",  # Padres
        "https://www.baseball-reference.com/boxes/BOS/BOS202504010.shtml",  # Red Sox
        "https://www.baseball-reference.com/boxes/ATL/ATL202504020.shtml",  # Braves
        "https://www.baseball-reference.com/boxes/CHN/CHN202504030.shtml",  # Cubs
        "https://www.baseball-reference.com/boxes/DET/DET202504040.shtml",  # Tigers
        "https://www.baseball-reference.com/boxes/TEX/TEX202504050.shtml",  # Rangers
    ]
    
    return test_urls

def get_extended_test_urls() -> List[str]:
    """Get an extended set of test URLs for comprehensive validation"""
    
    # Start with basic test URLs
    urls = get_test_game_urls()
    
    # Add more diverse scenarios
    extended_urls = [
        "https://www.baseball-reference.com/boxes/MIA/MIA202504060.shtml",  # Marlins
        "https://www.baseball-reference.com/boxes/COL/COL202504070.shtml",  # Rockies
        "https://www.baseball-reference.com/boxes/SEA/SEA202504080.shtml",  # Mariners
        "https://www.baseball-reference.com/boxes/MIN/MIN202504090.shtml",  # Twins
        "https://www.baseball-reference.com/boxes/TBA/TBA202504100.shtml",  # Rays
    ]
    
    return urls + extended_urls

def run_batch_validation(extended: bool = False, max_failures: int = 3) -> MultiGameResults:
    """Run batch validation on multiple games"""
    
    print("🚀 STARTING BATCH UNIFIED VALIDATION")
    print("=" * 50)
    
    # Get test URLs and determine test type
    if extended:
        test_urls = get_extended_test_urls()
        test_type = "extended"
        print("📋 Running EXTENDED validation (15 games)")
    else:
        test_urls = get_test_game_urls()
        test_type = "standard"
        print("📋 Running STANDARD validation (10 games)")
    
    print(f"📋 Test games selected:")
    for i, url in enumerate(test_urls, 1):
        game_id = url.split('/')[-1].replace('.shtml', '')
        print(f"   {i:2d}. {game_id}")
    
    print()
    
    # Create validator and run test
    validator = MultiGameUnifiedValidator()
    results = validator.validate_multiple_games(test_urls, max_failures=max_failures)
    
    # Save results with dynamic filenames
    validator.save_detailed_results(results, test_type)
    
    return results

def run_quick_test() -> MultiGameResults:
    """Run a quick test on just a few games"""
    print("🚀 QUICK TEST - 3 GAMES")
    print("=" * 30)
    
    quick_urls = [
        "https://www.baseball-reference.com/boxes/NYA/NYA202503270.shtml",
        "https://www.baseball-reference.com/boxes/LAN/LAN202503280.shtml", 
        "https://www.baseball-reference.com/boxes/HOU/HOU202503290.shtml",
    ]
    
    validator = MultiGameUnifiedValidator()
    results = validator.validate_multiple_games(quick_urls, max_failures=1)
    
    validator.save_detailed_results(results, "quick")
    return results

def run_custom_test(game_urls: List[str], test_name: str = "custom") -> MultiGameResults:
    """Run a custom test with specific game URLs"""
    print(f"🚀 CUSTOM TEST - {test_name.upper()}")
    print("=" * 50)
    
    print(f"📋 {len(game_urls)} games selected:")
    for i, url in enumerate(game_urls, 1):
        game_id = url.split('/')[-1].replace('.shtml', '')
        print(f"   {i:2d}. {game_id}")
    
    print()
    
    validator = MultiGameUnifiedValidator()
    results = validator.validate_multiple_games(game_urls, max_failures=2)
    
    validator.save_detailed_results(results, test_name)
    return results

if __name__ == "__main__":
    # Default: run standard validation
    # Uncomment different lines to run different tests:
    
    # Standard test (10 games)
    results = run_batch_validation(extended=False, max_failures=3)
    
    # Extended test (15 games) 
    # results = run_batch_validation(extended=True, max_failures=5)
    
    # Quick test (3 games)
    # results = run_quick_test()
    
    print(f"\n🎯 Final Results: {results.avg_batting_accuracy:.1f}% batting, {results.avg_pitching_accuracy:.1f}% pitching")