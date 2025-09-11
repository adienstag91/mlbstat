#!/usr/bin/env python3
"""
MLB Stats Pipeline - Modular Version
===================================

Clean, modular approach to parsing MLB play-by-play data.
Replaces the old class-based UnifiedEventsParser.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.game_processor import process_single_game, process_multiple_games

def test_single():
    """Test processing a single game"""
    test_url = "https://www.baseball-reference.com/boxes/NYA/NYA202505050.shtml"
    
    print(f"🧪 Testing single game: {test_url}")
    result = process_single_game(test_url)
    
    events = result['pbp_events']
    batting_box = result['official_batting']
    pitching_box = result['official_pitching']
    
    print(f"""
📊 GAME RESULTS
===============
Game ID: {result['game_id']}
Events parsed: {len(events)}
Batting accuracy: {result['batting_validation']['accuracy']:.1f}% ({result['batting_validation']['players_compared']} players)
Pitching accuracy: {result['pitching_validation']['accuracy']:.1f}% ({result['pitching_validation']['players_compared']} pitchers)
""")
    
    # Show sample events
    if not events.empty:
        print("🎯 Sample Events:")
        cols = ['batter_id', 'pitcher_id', 'inning', 'description', 'is_hit', 'hit_type']
        available_cols = [col for col in cols if col in events.columns]
        print(events[available_cols].head(5).to_string(index=False))
    
    # Show batting box score sample
    print("\n📊 Batting Box score sample:")
    if not batting_box.empty:
        print(batting_box.head().to_string(index=False))
    else:
        print("   No batting data found")
    
    # Show pitching box score sample  
    print("\n⚾ Pitching Box score sample:")
    if not pitching_box.empty:
        print(pitching_box.head().to_string(index=False))
    else:
        print("   No pitching data found")
    
    # Show any differences for debugging
    bat_val = result['batting_validation']
    pit_val = result['pitching_validation']
    
    if bat_val.get('differences'):
        print(f"\n⚠️  BATTING DIFFERENCES:")
        for diff in bat_val['differences'][:3]:  # Show first 3
            print(f"   {diff['player']}: {', '.join(diff['diffs'])}")
    
    if pit_val.get('differences'):
        print(f"\n⚠️  PITCHING DIFFERENCES:")
        for diff in pit_val['differences'][:3]:  # Show first 3
            print(f"   {diff['player']}: {', '.join(diff['diffs'])}")
    
    return result

def main():
    """Main entry point - process multiple games"""
    # Example with multiple games
    test_urls = [
        "https://www.baseball-reference.com/boxes/NYA/NYA202505050.shtml",
        # Add more URLs here when ready
    ]
    
    print(f"🚀 Processing {len(test_urls)} games...")
    results = process_multiple_games(test_urls)
    
    # Simple summary
    total_games = len(results)
    if total_games > 0:
        avg_batting = sum(r['batting_validation']['accuracy'] for r in results) / total_games
        avg_pitching = sum(r['pitching_validation']['accuracy'] for r in results) / total_games
        
        print(f"""
📊 BATCH COMPLETE
=================
Games processed: {total_games}
Average batting accuracy: {avg_batting:.1f}%
Average pitching accuracy: {avg_pitching:.1f}%
""")
    
    return results

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        test_single()
    else:
        main()