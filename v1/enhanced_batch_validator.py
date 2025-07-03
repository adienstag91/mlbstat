import pandas as pd
from collections import Counter, defaultdict
import time
import random
from enhanced_mlb_parser import validate_stats
import traceback
from datetime import datetime

class EnhancedBatchValidator:
    def __init__(self):
        self.all_results = []
        self.all_discrepancies = []
        self.problematic_games = []
        self.failed_games = []
        
        # Yankees game URLs (your test set)
        self.yankees_game_urls = [
            "https://www.baseball-reference.com/boxes/NYA/NYA202503270.shtml",
            "https://www.baseball-reference.com/boxes/NYA/NYA202503290.shtml", 
            "https://www.baseball-reference.com/boxes/NYA/NYA202503300.shtml",
            "https://www.baseball-reference.com/boxes/NYA/NYA202504010.shtml",
            "https://www.baseball-reference.com/boxes/NYA/NYA202504020.shtml",
            "https://www.baseball-reference.com/boxes/NYA/NYA202504030.shtml",
            "https://www.baseball-reference.com/boxes/NYA/NYA202504040.shtml",
            "https://www.baseball-reference.com/boxes/NYA/NYA202504050.shtml",
            "https://www.baseball-reference.com/boxes/NYA/NYA202504060.shtml",
            "https://www.baseball-reference.com/boxes/NYA/NYA202504070.shtml"
        ]
        
    def analyze_single_game(self, game_url, game_number, total_games):
        """Analyze a single game with enhanced error handling"""
        
        print(f"\n🔍 [{game_number}/{total_games}] Analyzing: {game_url}")
        start_time = time.time()
        
        max_retries = 3
        retry_delay = 10
        
        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    delay = retry_delay * (attempt + 1) + random.uniform(2, 5)
                    print(f"   🔄 Retry {attempt + 1}/{max_retries} after {delay:.1f}s...")
                    time.sleep(delay)
                
                # Call the enhanced validation function
                validation_df = validate_stats(game_url)
                
                if validation_df is None or validation_df.empty:
                    print(f"   ❌ No validation data returned")
                    if attempt < max_retries - 1:
                        continue
                    else:
                        return self._create_failed_result(game_url, "No validation data returned")
                
                print(f"   ✅ Got validation DataFrame with {len(validation_df)} players")
                print(f"   ⏱️  Analysis completed in {time.time() - start_time:.1f}s")
                
                # Analyze the results
                game_result = self._analyze_validation_results(validation_df, game_url)
                return game_result
                
            except Exception as e:
                error_msg = str(e)
                print(f"   ❌ Attempt {attempt + 1} failed: {error_msg[:100]}...")
                
                if attempt < max_retries - 1:
                    # Check if it's a timeout/network error vs parsing error
                    if any(keyword in error_msg.lower() for keyword in ['timeout', 'network', 'connection', 'timed out']):
                        print(f"   🌐 Network issue detected, will retry...")
                        continue
                    else:
                        print(f"   🔧 Parsing/logic error detected")
                        # For parsing errors, don't retry as much
                        break
                else:
                    print(f"   💥 All {max_retries} attempts failed")
                    return self._create_failed_result(game_url, error_msg)
        
        return self._create_failed_result(game_url, "Max retries exceeded")
    
    def _create_failed_result(self, game_url, error_msg):
        """Create a failed result record"""
        return {
            'game_url': game_url,
            'accuracy': 0,
            'error': error_msg,
            'total_players': 0,
            'players_with_diffs': 0,
            'total_differences': 0,
            'total_stats_checked': 0,
            'failed': True
        }
    
    def _analyze_validation_results(self, validation_df, game_url):
        """Analyze validation results and extract metrics"""
        
        # Calculate accuracy
        stat_columns = ['AB_diff', 'H_diff', 'HR_diff', 'RBI_diff', 'R_diff', 
                       'BB_diff', 'SO_diff', 'SB_diff', 'CS_diff', '2B_diff', '3B_diff', 'SF_diff']
        
        total_differences = 0
        total_stats_checked = 0
        
        # Count differences more carefully
        for col in stat_columns:
            if col in validation_df.columns:
                col_diffs = validation_df[col].abs().sum()
                total_differences += col_diffs
                total_stats_checked += len(validation_df)
        
        accuracy = ((total_stats_checked - total_differences) / total_stats_checked * 100) if total_stats_checked > 0 else 0
        
        # Find players with differences
        players_with_diffs_list = []
        
        for idx, row in validation_df.iterrows():
            has_diff = False
            player_diffs = {}
            total_player_diffs = 0
            
            for col in stat_columns:
                if col in validation_df.columns:
                    diff_val = row.get(col, 0)
                    if pd.notna(diff_val) and diff_val != 0:
                        has_diff = True
                        stat_name = col.replace('_diff', '')
                        player_diffs[stat_name] = diff_val
                        total_player_diffs += abs(diff_val)
            
            if has_diff:
                player_name = row.get('batter', f'Player_{idx}')
                players_with_diffs_list.append({
                    'player': player_name,
                    'stat_differences': player_diffs,
                    'total_diffs': total_player_diffs,
                    'row_data': row
                })
                
                # Store discrepancy for later analysis
                discrepancy = {
                    'game_url': game_url,
                    'player': player_name,
                    'stat_differences': player_diffs,
                    'total_diffs': total_player_diffs
                }
                self.all_discrepancies.append(discrepancy)
        
        num_players_with_diffs = len(players_with_diffs_list)
        
        game_result = {
            'game_url': game_url,
            'accuracy': accuracy,
            'total_players': len(validation_df),
            'players_with_diffs': num_players_with_diffs,
            'total_differences': total_differences,
            'total_stats_checked': total_stats_checked,
            'failed': False
        }
        
        # Log issues if any
        if num_players_with_diffs > 0:
            self.problematic_games.append(game_result)
            print(f"   ⚠️  Found {num_players_with_diffs} players with differences:")
            for player_info in players_with_diffs_list[:3]:  # Show first 3
                print(f"     {player_info['player']}: {player_info['stat_differences']}")
            if len(players_with_diffs_list) > 3:
                print(f"     ... and {len(players_with_diffs_list) - 3} more")
        
        print(f"   📈 Accuracy: {accuracy:.1f}% | Players with diffs: {num_players_with_diffs} | Total diffs: {total_differences}")
        return game_result
    
    def run_batch_analysis(self, max_games=None, start_from=0):
        """Run analysis with enhanced reliability"""
        
        print("🚀 Starting enhanced batch analysis...")
        print(f"⏰ Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        game_urls = self.yankees_game_urls.copy()
        
        if start_from > 0:
            game_urls = game_urls[start_from:]
            print(f"📌 Starting from game {start_from + 1}")
        
        if max_games:
            game_urls = game_urls[:max_games]
            print(f"📊 Testing {len(game_urls)} games")
        else:
            print(f"📊 Testing all {len(game_urls)} remaining games")
        
        successful_count = 0
        failed_count = 0
        
        for i, game_url in enumerate(game_urls, 1):
            actual_game_number = start_from + i
            
            result = self.analyze_single_game(game_url, actual_game_number, len(self.yankees_game_urls))
            
            if result:
                self.all_results.append(result)
                if not result.get('failed', False):
                    successful_count += 1
                else:
                    failed_count += 1
                    self.failed_games.append(result)
                    print(f"   ❌ Game failed: {result['error'][:100]}...")
            
            # Adaptive delay between games
            if i < len(game_urls):  # Don't wait after last game
                if failed_count > 0:
                    # Longer delay if we've had failures
                    delay = random.uniform(15, 25)
                    print(f"   💤 Extended wait {delay:.1f}s (had failures)...")
                else:
                    delay = random.uniform(8, 12)
                    print(f"   💤 Standard wait {delay:.1f}s...")
                time.sleep(delay)
            
            # Progress update
            if i % 3 == 0 or i == len(game_urls):
                success_rate = (successful_count / (successful_count + failed_count) * 100) if (successful_count + failed_count) > 0 else 0
                print(f"\n📊 PROGRESS: {successful_count} successful, {failed_count} failed ({success_rate:.1f}% success rate)")
                
                if self.all_results:
                    successful_results = [r for r in self.all_results if not r.get('failed', False)]
                    if successful_results:
                        avg_acc = sum(r['accuracy'] for r in successful_results) / len(successful_results)
                        print(f"    Average accuracy so far: {avg_acc:.1f}%")
        
        print(f"\n⏰ Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.generate_enhanced_summary_report()
    
    def generate_enhanced_summary_report(self):
        """Generate comprehensive summary report"""
        
        print("\n" + "="*80)
        print("📊 ENHANCED BATCH VALIDATION SUMMARY REPORT")
        print("="*80)
        
        if not self.all_results:
            print("❌ No results to analyze")
            return
        
        # Overall stats
        total_games = len(self.all_results)
        successful_games = len([r for r in self.all_results if not r.get('failed', False)])
        failed_games = total_games - successful_games
        
        print(f"\n🎯 OVERALL PERFORMANCE:")
        print(f"   Total Games Attempted: {total_games}")
        print(f"   Successful: {successful_games} | Failed: {failed_games}")
        if total_games > 0:
            success_rate = successful_games / total_games * 100
            print(f"   Success Rate: {success_rate:.1f}%")
        
        # Analyze successful games
        if successful_games > 0:
            successful_results = [r for r in self.all_results if not r.get('failed', False)]
            self._analyze_successful_games(successful_results)
        
        # Analyze failed games
        if failed_games > 0:
            self._analyze_failed_games()
        
        # Discrepancy analysis
        if self.all_discrepancies:
            self._analyze_discrepancy_patterns()
        
        # Recommendations
        self._generate_recommendations()
    
    def _analyze_successful_games(self, successful_results):
        """Analyze patterns in successful games"""
        
        accuracies = [r['accuracy'] for r in successful_results]
        avg_accuracy = sum(accuracies) / len(accuracies)
        perfect_games = len([a for a in accuracies if a >= 99.99])
        
        total_stat_diffs = sum(r['total_differences'] for r in successful_results)
        total_stats_checked = sum(r['total_stats_checked'] for r in successful_results)
        
        print(f"\n📈 ACCURACY ANALYSIS:")
        print(f"   Average Accuracy: {avg_accuracy:.2f}%")
        print(f"   Near-Perfect Games (≥99.99%): {perfect_games}/{len(successful_results)} ({perfect_games/len(successful_results)*100:.1f}%)")
        print(f"   Total Stat Differences: {total_stat_diffs} out of {total_stats_checked} stats checked")
        
        # Accuracy distribution
        ranges = [
            (100, 100, "Perfect"),
            (99.5, 99.99, "Near Perfect"), 
            (98, 99.49, "Very Good"),
            (95, 97.99, "Good"),
            (90, 94.99, "Fair"),
            (0, 89.99, "Needs Work")
        ]
        
        print(f"\n📊 ACCURACY DISTRIBUTION:")
        for min_acc, max_acc, label in ranges:
            if min_acc == 100:
                count = len([a for a in accuracies if a == 100])
            else:
                count = len([a for a in accuracies if min_acc <= a <= max_acc])
            pct = count / len(accuracies) * 100 if len(accuracies) > 0 else 0
            print(f"   {label} ({min_acc:.1f}-{max_acc:.1f}%): {count} games ({pct:.1f}%)")
    
    def _analyze_failed_games(self):
        """Analyze patterns in failed games"""
        
        print(f"\n❌ FAILED GAMES ANALYSIS ({len(self.failed_games)} games):")
        
        # Categorize failure types
        failure_categories = defaultdict(list)
        
        for failed_game in self.failed_games:
            error = failed_game.get('error', 'Unknown error')
            
            if 'timeout' in error.lower() or 'timed out' in error.lower():
                failure_categories['Timeout/Network'].append(failed_game)
            elif 'no validation data' in error.lower():
                failure_categories['No Data Returned'].append(failed_game)
            elif 'parsing' in error.lower() or 'table' in error.lower():
                failure_categories['Parsing Error'].append(failed_game)
            else:
                failure_categories['Other'].append(failed_game)
        
        for category, games in failure_categories.items():
            print(f"   {category}: {len(games)} games")
            for game in games[:3]:  # Show first 3
                game_id = game['game_url'].split('/')[-1]
                print(f"     - {game_id}: {game['error'][:50]}...")
            if len(games) > 3:
                print(f"     ... and {len(games) - 3} more")
    
    def _analyze_discrepancy_patterns(self):
        """Analyze patterns in stat discrepancies"""
        
        print(f"\n🔍 DISCREPANCY ANALYSIS ({len(self.all_discrepancies)} player issues):")
        
        stat_problems = Counter()
        multi_stat_players = 0
        
        for disc in self.all_discrepancies:
            if disc['total_diffs'] > 1:
                multi_stat_players += 1
            
            for stat_name, diff_val in disc['stat_differences'].items():
                stat_problems[stat_name] += abs(diff_val)
        
        print(f"\n📊 STAT DIFFERENCE BREAKDOWN:")
        for stat, count in stat_problems.most_common():
            print(f"   {stat}: {count} total differences")
        
        print(f"\n👥 PLAYER ANALYSIS:")
        print(f"   Players with multiple stat issues: {multi_stat_players}")
        print(f"   Players with single stat issues: {len(self.all_discrepancies) - multi_stat_players}")
        
        # Show most problematic stat details
        if stat_problems:
            most_problematic_stat = stat_problems.most_common(1)[0][0]
            print(f"\n🎯 MOST PROBLEMATIC STAT: {most_problematic_stat}")
            
            problem_cases = []
            for disc in self.all_discrepancies:
                if most_problematic_stat in disc['stat_differences']:
                    diff_val = disc['stat_differences'][most_problematic_stat]
                    game_id = disc['game_url'].split('/')[-1]
                    problem_cases.append({
                        'player': disc['player'],
                        'game': game_id,
                        'diff': diff_val,
                        'all_diffs': disc['stat_differences']
                    })
            
            print(f"\n📋 SAMPLE CASES WITH {most_problematic_stat} ISSUES:")
            for i, case in enumerate(problem_cases[:5], 1):
                print(f"   {i}. {case['player']} (Game: {case['game']})")
                print(f"      {most_problematic_stat} difference: {case['diff']}")
                print(f"      All differences: {case['all_diffs']}")
    
    def _generate_recommendations(self):
        """Generate actionable recommendations"""
        
        print(f"\n💡 RECOMMENDATIONS:")
        
        successful_games = len([r for r in self.all_results if not r.get('failed', False)])
        total_games = len(self.all_results)
        
        if total_games == 0:
            print("   - No games processed successfully")
            return
        
        success_rate = successful_games / total_games * 100
        
        # Network reliability recommendations
        if success_rate < 80:
            print("   🌐 NETWORK RELIABILITY:")
            print("     - Consider increasing retry delays")
            print("     - Add more sophisticated timeout handling")
            print("     - Consider using a different user agent")
        
        # Parsing accuracy recommendations
        if successful_games > 0:
            successful_results = [r for r in self.all_results if not r.get('failed', False)]
            avg_accuracy = sum(r['accuracy'] for r in successful_results) / len(successful_results)
            
            if avg_accuracy < 95:
                print("   📈 PARSING ACCURACY:")
                print("     - Review play description parsing logic")
                print("     - Improve name resolution algorithms")
                print("     - Add more edge case handling")
            
            # Specific stat recommendations
            if self.all_discrepancies:
                stat_problems = Counter()
                for disc in self.all_discrepancies:
                    for stat_name, diff_val in disc['stat_differences'].items():
                        stat_problems[stat_name] += abs(diff_val)
                
                if stat_problems:
                    top_problem = stat_problems.most_common(1)[0][0]
                    print(f"   🎯 FOCUS AREA: {top_problem} parsing needs improvement")
        
        print("   ✅ NEXT STEPS:")
        print("     1. Focus on the most problematic stat type")
        print("     2. Review failed games for patterns")
        print("     3. Test individual problematic games in isolation")
        print("     4. Consider implementing more robust error recovery")

    def save_detailed_results(self, filename=None):
        """Save detailed results to files for further analysis"""
        
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"batch_validation_results_{timestamp}"
        
        # Save summary
        summary_data = {
            'total_games': len(self.all_results),
            'successful_games': len([r for r in self.all_results if not r.get('failed', False)]),
            'failed_games': len([r for r in self.all_results if r.get('failed', False)]),
            'all_results': self.all_results,
            'all_discrepancies': self.all_discrepancies,
            'problematic_games': self.problematic_games,
            'failed_games': self.failed_games
        }
        
        import json
        with open(f"{filename}_summary.json", 'w') as f:
            json.dump(summary_data, f, indent=2, default=str)
        
        print(f"💾 Results saved to {filename}_summary.json")
        
        # Save discrepancies as CSV for easy analysis
        if self.all_discrepancies:
            discrepancy_records = []
            for disc in self.all_discrepancies:
                base_record = {
                    'game_url': disc['game_url'],
                    'player': disc['player'],
                    'total_diffs': disc['total_diffs']
                }
                
                # Add each stat difference as a column
                for stat, diff in disc['stat_differences'].items():
                    base_record[f'{stat}_diff'] = diff
                
                discrepancy_records.append(base_record)
            
            df_discrepancies = pd.DataFrame(discrepancy_records)
            df_discrepancies.to_csv(f"{filename}_discrepancies.csv", index=False)
            print(f"💾 Discrepancies saved to {filename}_discrepancies.csv")

if __name__ == "__main__":
    validator = EnhancedBatchValidator()
    
    print("🧪 Testing Enhanced Batch Validator...")
    print("=" * 50)
    
    # Test with first 3 games to start
    print("🧪 Running test on first 3 games...")
    validator.run_batch_analysis(max_games=3)
    
    # Ask user if they want to continue
    print("\n" + "="*50)
    response = input("Continue with remaining games? (y/n): ").lower().strip()
    
    if response == 'y':
        print("\n🚀 Continuing with remaining games...")
        validator.run_batch_analysis(start_from=3)
    
    # Save results
    validator.save_detailed_results()
    
    print("\n" + "="*50)
    print("📋 SUMMARY OF ALL PROBLEMATIC PLAYERS:")
    print("="*50)
    
    if validator.all_discrepancies:
        for i, disc in enumerate(validator.all_discrepancies[:10], 1):  # Show first 10
            game_id = disc['game_url'].split('/')[-1]
            print(f"{i}. {disc['player']} (Game: {game_id})")
            print(f"   Stat differences: {disc['stat_differences']}")
            print(f"   Total diffs: {disc['total_diffs']}")
            print()
        
        if len(validator.all_discrepancies) > 10:
            print(f"... and {len(validator.all_discrepancies) - 10} more discrepancies")
    else:
        print("🎉 No discrepancies found across all games!")