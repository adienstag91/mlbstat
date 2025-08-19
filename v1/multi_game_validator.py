"""
Multi-Game Validator - Fixed Version
===================================

Clean, working multi-game validator without variable scope issues.
"""

import pandas as pd
import numpy as np
from typing import Dict, List
import time
import traceback
from dataclasses import dataclass
from single_game_validator import SingleGameValidator

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

class MultiGameValidator:
    """Validate parsing accuracy across multiple games"""
    
    def __init__(self):
        self.single_validator = SingleGameValidator()
        self.results = []
        self.failures = []
    
    def validate_multiple_games(self, game_urls: List[str], max_failures: int = 3) -> MultiGameResults:
        """Validate parsing across multiple games"""
        
        print(f"🎯 MULTI-GAME VALIDATION:")
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
                # Validate single game
                result = self.single_validator.validate_single_game(game_url)
                
                # Extract results safely
                bat_acc = result['validation_results']['accuracy']
                pit_acc = result['pitching_validation_results']['accuracy']
                
                bat_res = result['validation_results']
                pit_res = result['pitching_validation_results']
                
                bat_events = len(result['parsed_events']) if not result['parsed_events'].empty else 0
                pit_events = len(result['parsed_pitching_events']) if not result['parsed_pitching_events'].empty else 0
                
                # Store results
                game_result = {
                    'game_number': i,
                    'game_url': game_url,
                    'batting_accuracy': bat_acc,
                    'pitching_accuracy': pit_acc,
                    'batting_players_total': len(result['official_batting_stats']),
                    'pitching_players_total': len(result['official_pitching_stats']),
                    'batting_players_compared': bat_res.get('players_compared', 0),
                    'pitching_players_compared': pit_res.get('players_compared', 0),
                    'batting_events': bat_events,
                    'pitching_events': pit_events,
                    'batting_diffs': bat_res.get('total_differences', 0),
                    'pitching_diffs': pit_res.get('total_differences', 0),
                    'batting_stats_total': bat_res.get('total_stats', 0),
                    'pitching_stats_total': pit_res.get('total_stats', 0),
                    'status': 'success'
                }
                
                self.results.append(game_result)
                batting_accuracies.append(bat_acc)
                pitching_accuracies.append(pit_acc)
                successful_games += 1
                
                # Display results
                self._display_game_result(game_result, bat_res, pit_res)
                
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
        print(f"   📊 {game_result['batting_events']} batting events, {game_result['pitching_events']} pitching events")
        
        # Show differences if any
        batting_differences = batting_results.get('differences', [])
        pitching_differences = pitching_results.get('differences', [])
        
        if batting_differences:
            print(f"   ⚠️  Batting differences:")
            for diff in batting_differences[:3]:  # Show first 3
                print(f"      {diff['player']}: {', '.join(diff['diffs'])}")
            if len(batting_differences) > 3:
                print(f"      ... and {len(batting_differences) - 3} more")
        
        if pitching_differences:
            print(f"   ⚠️  Pitching differences:")
            for diff in pitching_differences[:3]:  # Show first 3
                print(f"      {diff['player']}: {', '.join(diff['diffs'])}")
            if len(pitching_differences) > 3:
                print(f"      ... and {len(pitching_differences) - 3} more")
    
    def _print_summary(self, results: MultiGameResults):
        """Print comprehensive summary of multi-game results"""
        
        print(f"\n" + "=" * 60)
        print(f"📊 MULTI-GAME VALIDATION SUMMARY")
        print("=" * 60)
        
        # Overall success rate
        success_rate = (results.successful_games / results.total_games) * 100
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
            print("   Game | Batting | Pitching | Batters    | Pitchers")
            print("   -----|---------|----------|------------|----------")
            for game in results.game_results:
                bat_ratio = f"{game.get('batting_players_compared', 0)}/{game.get('batting_players_total', 0)}"
                pit_ratio = f"{game.get('pitching_players_compared', 0)}/{game.get('pitching_players_total', 0)}"
                print(f"   {game['game_number']:4d} | {game['batting_accuracy']:6.1f}% | "
                      f"{game['pitching_accuracy']:7.1f}% | {bat_ratio:>10s} | {pit_ratio:>8s}")
        
        # Failure analysis
        if results.failure_details:
            print(f"\n❌ FAILURE ANALYSIS:")
            for failure in results.failure_details:
                print(f"   Game {failure['game_number']}: {failure['error'][:80]}...")
        
        # Overall assessment
        print(f"\n🎯 ASSESSMENT:")
        if results.avg_batting_accuracy >= 99 and results.avg_pitching_accuracy >= 90:
            print("   🎉 EXCELLENT! Ready for production pipeline")
        elif results.avg_batting_accuracy >= 95 and results.avg_pitching_accuracy >= 80:
            print("   ✅ GOOD! Minor refinements needed")
        else:
            print("   🔧 NEEDS WORK! Significant improvements required")
        
        print("=" * 60)

# TEST GAME URLS - Mix of different teams, dates, and scenarios
def get_test_game_urls() -> List[str]:
    """Get a diverse set of test game URLs"""
    
    test_urls = [
        "https://www.baseball-reference.com/boxes/NYA/NYA202503270.shtml",  # Yankees
        "https://www.baseball-reference.com/boxes/LAN/LAN202503280.shtml",  # Dodgers
        "https://www.baseball-reference.com/boxes/HOU/HOU202503290.shtml",  # Astros
        "https://www.baseball-reference.com/boxes/SFN/SFN202503300.shtml",  # Giants  
        "https://www.baseball-reference.com/boxes/SDN/SDN202503310.shtml",  # Padres
    ]
    
    return test_urls

def run_multi_game_test():
    """Run multi-game validation test"""
    
    print("🚀 STARTING MULTI-GAME VALIDATION")
    print("=" * 50)
    
    # Get test URLs
    test_urls = get_test_game_urls()
    
    print(f"📋 Test games selected:")
    for i, url in enumerate(test_urls, 1):
        print(f"   {i}. {url}")
    
    # Create validator and run test
    validator = MultiGameValidator()
    results = validator.validate_multiple_games(test_urls, max_failures=2)
    
    # Save results for analysis
    results_df = pd.DataFrame(results.game_results)
    if not results_df.empty:
        results_df.to_csv('multi_game_results.csv', index=False)
        print(f"\n💾 Results saved to 'multi_game_results.csv'")
    
    return results

if __name__ == "__main__":
    run_multi_game_test()