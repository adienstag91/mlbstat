"""
Compatibility Wrapper for UnifiedEventsParser
============================================

This maintains backward compatibility with your existing code while 
using the new modular functions under the hood.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from pipeline.game_processor import process_single_game

class UnifiedEventsParser:
    """
    DEPRECATED: Use process_single_game() function instead.
    
    This class is maintained for backward compatibility only.
    """
    
    def __init__(self):
        import warnings
        warnings.warn(
            "UnifiedEventsParser class is deprecated. Use process_single_game() function instead.",
            DeprecationWarning,
            stacklevel=2
        )
    
    def parse_game(self, game_url: str):
        """Parse game using new modular functions"""
        return process_single_game(game_url)

# For immediate migration, you can also use the function directly:
parse_game = process_single_game

def test_unified_parser():
    """Test function with same interface as before"""
    test_url = "https://www.baseball-reference.com/boxes/KCA/KCA202503290.shtml"
    
    print(f"🧪 Testing game: {test_url}")
    
    # New way (recommended)
    results = process_single_game(test_url)
    
    events = results['unified_events']
    batting_box = results['official_batting']
    pitching_box = results['official_pitching']
    
    print("\n📋 UNIFIED EVENTS SAMPLE:")
    if not events.empty:
        cols = ['batter_id', 'pitcher_id', 'inning', 'inning_half', 'description', 
                'is_plate_appearance', 'is_at_bat', 'is_hit', 'hit_type']
        available_cols = [col for col in cols if col in events.columns]
        print(events[available_cols].head(10).to_string(index=False))
        print(f"\nTotal events: {len(events)}")

    print("\n📊 Batting Box score sample:")
    if not batting_box.empty:
        print(batting_box.head())

    print("\n⚾ Pitching Box score sample:")
    if not pitching_box.empty:
        print(pitching_box.head())
    
    bat_val = results['batting_validation']
    pit_val = results['pitching_validation']
    
    print(f"\n✅ VALIDATION RESULTS:")
    print(f"   ⚾ Batting: {bat_val['accuracy']:.1f}% ({bat_val['players_compared']} players)")
    print(f"   🥎 Pitching: {pit_val['accuracy']:.1f}% ({pit_val['players_compared']} pitchers)")
    
    if bat_val.get('differences'):
        print(f"\n⚠️  BATTING DIFFERENCES:")
        for diff in bat_val['differences'][:3]:  # Show first 3
            print(f"   {diff['player']}: {', '.join(diff['diffs'])}")
    
    if pit_val.get('differences'):
        print(f"\n⚠️  PITCHING DIFFERENCES:")
        for diff in pit_val['differences'][:3]:  # Show first 3
            print(f"   {diff['player']}: {', '.join(diff['diffs'])}")
    
    return results

if __name__ == "__main__":
    test_unified_parser()
