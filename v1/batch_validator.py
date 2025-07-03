import pandas as pd
from collections import Counter
import time
import random
from improved_mlb_parser import validate_stats

class RobustSimpleValidator:
    def __init__(self):
        self.all_results = []
        self.all_discrepancies = []
        self.problematic_games = []
        
        # Hardcoded Yankees game URLs (from your successful test)
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
        """Analyze a single game with robust DataFrame handling"""
        print(f"\n🔍 [{game_number}/{total_games}] Analyzing: {game_url}")
        
        try:
            # Use validate_stats directly
            validation_df = validate_stats(game_url)
            
            if validation_df is None:
                print(f"❌ No validation data returned (None)")
                return None
                
            if len(validation_df) == 0:
                print(f"❌ Empty validation data returned")
                return None
            
            print(f"✅ Got validation DataFrame with {len(validation_df)} players")
            print(f"   Columns: {list(validation_df.columns)}")
            
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
                    print(f"   {col}: {col_diffs} differences")
            
            accuracy = ((total_stats_checked - total_differences) / total_stats_checked * 100) if total_stats_checked > 0 else 0
            
            # Find players with differences using a safer approach
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
                    
                    # Store discrepancy
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
                'total_stats_checked': total_stats_checked
            }
            
            if num_players_with_diffs > 0:
                self.problematic_games.append(game_result)
                print(f"⚠️  Found {num_players_with_diffs} players with differences:")
                for player_info in players_with_diffs_list:
                    print(f"     {player_info['player']}: {player_info['stat_differences']}")
            
            print(f"✅ Accuracy: {accuracy:.1f}% | Players with diffs: {num_players_with_diffs} | Total stat diffs: {total_differences}")
            return game_result
            
        except Exception as e:
            print(f"❌ Error analyzing game: {str(e)}")
            import traceback
            print(f"   Full error: {traceback.format_exc()}")
            return {
                'game_url': game_url,
                'accuracy': 0,
                'error': str(e),
                'total_players': 0,
                'players_with_diffs': 0,
                'total_differences': 0
            }
    
    def run_batch_analysis(self, max_games=None):
        """Run analysis on hardcoded Yankees games"""
        print("🚀 Starting robust simple batch analysis...")
        
        game_urls = self.yankees_game_urls.copy()
        
        if max_games:
            game_urls = game_urls[:max_games]
            print(f"📊 Testing first {max_games} games")
        else:
            print(f"📊 Testing all {len(game_urls)} games")
        
        successful_count = 0
        failed_count = 0
        
        for i, game_url in enumerate(game_urls, 1):
            result = self.analyze_single_game(game_url, i, len(game_urls))
            if result:
                self.all_results.append(result)
                if 'error' not in result:
                    successful_count += 1
                else:
                    failed_count += 1
                    print(f"   ❌ Game failed: {result['error']}")
            
            # Short delay between games
            delay = random.uniform(3, 5)
            print(f"   💤 Waiting {delay:.1f}s before next game...")
            time.sleep(delay)
            
            # Progress update
            if i % 3 == 0:
                print(f"\n📊 PROGRESS: {successful_count} successful, {failed_count} failed")
        
        self.generate_summary_report()
    
    def generate_summary_report(self):
        """Generate summary report"""
        print("\n" + "="*80)
        print("📊 ROBUST SIMPLE VALIDATION SUMMARY REPORT")
        print("="*80)
        
        if not self.all_results:
            print("❌ No results to analyze")
            return
        
        # Overall stats
        total_games = len(self.all_results)
        successful_games = len([r for r in self.all_results if 'error' not in r])
        failed_games = total_games - successful_games
        
        print(f"🎯 OVERALL PERFORMANCE:")
        print(f"   Total Games Analyzed: {total_games}")
        print(f"   Successful: {successful_games} | Failed: {failed_games}")
        if total_games > 0:
            print(f"   Success Rate: {successful_games/total_games*100:.1f}%")
        
        if successful_games > 0:
            successful_results = [r for r in self.all_results if 'error' not in r]
            accuracies = [r['accuracy'] for r in successful_results]
            avg_accuracy = sum(accuracies) / len(accuracies)
            perfect_games = len([a for a in accuracies if a >= 99.99])  # Close to perfect
            
            total_stat_diffs = sum(r['total_differences'] for r in successful_results)
            total_stats_checked = sum(r['total_stats_checked'] for r in successful_results)
            
            print(f"\n📈 ACCURACY ANALYSIS:")
            print(f"   Average Accuracy: {avg_accuracy:.2f}%")
            print(f"   Near-Perfect Games (≥99.99%): {perfect_games}/{successful_games} ({perfect_games/successful_games*100:.1f}%)")
            print(f"   Total Stat Differences: {total_stat_diffs} out of {total_stats_checked} stats checked")
            
            # Accuracy distribution
            ranges = [
                (100, 100, "Perfect"),
                (99.5, 99.99, "Near Perfect"), 
                (98, 99.49, "Very Good"),
                (95, 97.99, "Good"),
                (0, 94.99, "Needs Work")
            ]
            
            print(f"\n📊 ACCURACY DISTRIBUTION:")
            for min_acc, max_acc, label in ranges:
                if min_acc == 100:
                    count = len([a for a in accuracies if a == 100])
                else:
                    count = len([a for a in accuracies if min_acc <= a <= max_acc])
                pct = count / len(accuracies) * 100 if len(accuracies) > 0 else 0
                print(f"   {label} ({min_acc:.1f}-{max_acc:.1f}%): {count} games ({pct:.1f}%)")
        
        # Discrepancy analysis
        if self.all_discrepancies:
            self.analyze_discrepancy_patterns()
        
        # Problematic games
        if self.problematic_games:
            print(f"\n⚠️  GAMES NEEDING ATTENTION ({len(self.problematic_games)} games):")
            for game in sorted(self.problematic_games, key=lambda x: x['accuracy'])[:10]:
                print(f"   {game['accuracy']:.1f}% - {game['players_with_diffs']} players - {game['total_differences']} diffs")
                print(f"      {game['game_url']}")
        
        # Failed games
        failed_results = [r for r in self.all_results if 'error' in r]
        if failed_results:
            print(f"\n❌ FAILED GAMES ({len(failed_results)}):")
            for result in failed_results[:5]:  # Show first 5
                print(f"   {result['game_url']}")
                print(f"      Error: {result['error']}")
    
    def analyze_discrepancy_patterns(self):
        """Analyze patterns in discrepancies"""
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
        
        # Show most problematic cases
        if stat_problems:
            most_problematic_stat = stat_problems.most_common(1)[0][0]
            print(f"\n🎯 MOST PROBLEMATIC STAT: {most_problematic_stat}")
            
            print(f"\n📋 SAMPLE PLAYERS WITH {most_problematic_stat} ISSUES:")
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
            
            for i, case in enumerate(problem_cases[:10], 1):
                print(f"   {i}. {case['player']} (Game: {case['game']})")
                print(f"      All differences: {case['all_diffs']}")

if __name__ == "__main__":
    validator = RobustSimpleValidator()
    
    print("🧪 Testing most robust simple validator...")
    print("🧪 Running test on first 5 games...")
    validator.run_batch_analysis(max_games=5)
    
    print("\n" + "="*50)
    print("📋 SUMMARY OF ALL PROBLEMATIC PLAYERS:")
    print("="*50)
    
    for i, disc in enumerate(validator.all_discrepancies, 1):
        game_id = disc['game_url'].split('/')[-1]
        print(f"{i}. {disc['player']} (Game: {game_id})")
        print(f"   Stat differences: {disc['stat_differences']}")
        print(f"   Total diffs: {disc['total_diffs']}")
        print()