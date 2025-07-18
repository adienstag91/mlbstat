"""
Debug Game Analyzer
==================

Deep debugging tool to analyze specific games and identify parsing issues.
"""

import pandas as pd
from unified_events_parser import UnifiedEventsParser

class GameDebugger:
    """Detailed debugging for specific games"""
    
    def __init__(self):
        self.parser = UnifiedEventsParser()
    
    def debug_game_in_detail(self, game_url: str):
        """Comprehensive debugging of a single game"""
        print(f"🔍 DEEP DEBUGGING: {game_url}")
        print("=" * 80)
        
        # Parse the game
        results = self.parser.parse_game(game_url)
        
        game_id = results['game_id']
        print(f"Game ID: {game_id}")
        print(f"Events parsed: {len(results['unified_events'])}")
        print()
        
        # Show overall accuracy
        bat_val = results['batting_validation']
        pit_val = results['pitching_validation']
        
        print(f"📊 ACCURACY SUMMARY:")
        print(f"   Batting: {bat_val['accuracy']:.1f}% ({bat_val['total_differences']} diffs/{bat_val['total_stats']} stats)")
        print(f"   Pitching: {pit_val['accuracy']:.1f}% ({pit_val['total_differences']} diffs/{pit_val['total_stats']} stats)")
        print()
        
        # Debug batting issues
        self._debug_batting_differences(results)
        
        # Debug pitching issues  
        self._debug_pitching_differences(results)
        
        # Analyze events that might be missed
        self._analyze_missed_events(results)
        
        return results
    
    def _debug_batting_differences(self, results):
        """Debug batting stat differences in detail"""
        bat_diffs = results['batting_validation'].get('differences', [])
        
        if not bat_diffs:
            print("✅ No batting differences found!")
            return
        
        print(f"⚾ BATTING DIFFERENCES ({len(bat_diffs)} players):")
        print("-" * 50)
        
        for i, diff in enumerate(bat_diffs, 1):
            player = diff['player']
            print(f"{i:2d}. {player}:")
            
            for stat_diff in diff['diffs']:
                print(f"     {stat_diff}")
            
            # Show this player's events
            player_events = results['unified_events'][
                results['unified_events']['batter_id'] == player
            ]
            
            if not player_events.empty:
                print(f"     Events for {player}:")
                for _, event in player_events.iterrows():
                    pa_marker = "PA" if event['is_plate_appearance'] else "  "
                    ab_marker = "AB" if event['is_at_bat'] else "  "
                    hit_marker = "H" if event['is_hit'] else " "
                    bb_marker = "BB" if event['is_walk'] else "  "
                    so_marker = "SO" if event['is_strikeout'] else "  "
                    
                    print(f"       {pa_marker} {ab_marker} {hit_marker} {bb_marker} {so_marker} | {event['description']}")
            else:
                print(f"     ❌ No events found for {player}")
            print()
    
    def _debug_pitching_differences(self, results):
        """Debug pitching stat differences in detail"""
        pit_diffs = results['pitching_validation'].get('differences', [])
        
        if not pit_diffs:
            print("✅ No pitching differences found!")
            return
        
        print(f"🥎 PITCHING DIFFERENCES ({len(pit_diffs)} pitchers):")
        print("-" * 50)
        
        for i, diff in enumerate(pit_diffs, 1):
            pitcher = diff['player']
            print(f"{i:2d}. {pitcher}:")
            
            for stat_diff in diff['diffs']:
                print(f"     {stat_diff}")
            
            # Show this pitcher's events
            pitcher_events = results['unified_events'][
                results['unified_events']['pitcher_id'] == pitcher
            ]
            
            if not pitcher_events.empty:
                print(f"     Events for {pitcher}:")
                for _, event in pitcher_events.iterrows():
                    pa_marker = "BF" if event['is_plate_appearance'] else "  "
                    hit_marker = "H" if event['is_hit'] else " "
                    bb_marker = "BB" if event['is_walk'] else "  "
                    so_marker = "SO" if event['is_strikeout'] else "  "
                    pc_marker = f"PC:{event['pitch_count']}" if event['pitch_count'] > 0 else ""
                    
                    print(f"       {pa_marker} {hit_marker} {bb_marker} {so_marker} {pc_marker:>6s} | {event['description']}")
            else:
                print(f"     ❌ No events found for {pitcher}")
            print()
    
    def _analyze_missed_events(self, results):
        """Look for events that might not be getting parsed"""
        all_events = results['unified_events']
        
        print(f"🔍 EVENT ANALYSIS:")
        print("-" * 30)
        
        # Count event types
        total_events = len(all_events)
        plate_appearances = all_events['is_plate_appearance'].sum()
        at_bats = all_events['is_at_bat'].sum()
        hits = all_events['is_hit'].sum()
        walks = all_events['is_walk'].sum()
        strikeouts = all_events['is_strikeout'].sum()
        sac_flies = all_events['is_sacrifice_fly'].sum()
        sac_hits = all_events['is_sacrifice_hit'].sum()
        
        print(f"Total events parsed: {total_events}")
        print(f"Plate appearances: {plate_appearances}")
        print(f"At-bats: {at_bats}")
        print(f"Hits: {hits}")
        print(f"Walks: {walks}")
        print(f"Strikeouts: {strikeouts}")
        print(f"Sacrifice flies: {sac_flies}")
        print(f"Sacrifice hits: {sac_hits}")
        print()
        
        # Show unique descriptions that might be missed
        print(f"🔍 UNIQUE EVENT DESCRIPTIONS:")
        print("-" * 40)
        
        unique_descriptions = all_events['description'].value_counts()
        print("Most common events:")
        for desc, count in unique_descriptions.head(10).items():
            print(f"   {count:2d}x: {desc}")
        
        print("\nLeast common events (might be edge cases):")
        for desc, count in unique_descriptions.tail(10).items():
            print(f"   {count:2d}x: {desc}")
    
    def compare_with_official_stats(self, results):
        """Compare parsed stats with official box score in detail"""
        print(f"\n📋 OFFICIAL VS PARSED COMPARISON:")
        print("-" * 50)
        
        # Batting comparison
        official_batting = results['official_batting']
        events = results['unified_events']
        
        print("BATTING STATS COMPARISON:")
        print("Player                    | Official PA/AB/H/BB/SO | Parsed PA/AB/H/BB/SO")
        print("-" * 75)
        
        for _, player_row in official_batting.iterrows():
            player = player_row['player_name']
            player_events = events[events['batter_id'] == player]
            
            # Official stats
            off_pa = player_row['PA']
            off_ab = player_row['AB'] 
            off_h = player_row['H']
            off_bb = player_row['BB']
            off_so = player_row['SO']
            
            # Parsed stats
            par_pa = player_events['is_plate_appearance'].sum()
            par_ab = player_events['is_at_bat'].sum()
            par_h = player_events['is_hit'].sum()
            par_bb = player_events['is_walk'].sum()
            par_so = player_events['is_strikeout'].sum()
            
            # Show comparison (highlight differences)
            pa_mark = "❌" if off_pa != par_pa else "✅"
            ab_mark = "❌" if off_ab != par_ab else "✅"
            h_mark = "❌" if off_h != par_h else "✅"
            bb_mark = "❌" if off_bb != par_bb else "✅"
            so_mark = "❌" if off_so != par_so else "✅"
            
            if off_pa != par_pa or off_ab != par_ab or off_h != par_h or off_bb != par_bb or off_so != par_so:
                print(f"{player:25s} | {off_pa:2d}/{off_ab:2d}/{off_h:2d}/{off_bb:2d}/{off_so:2d}           | {par_pa:2d}/{par_ab:2d}/{par_h:2d}/{par_bb:2d}/{par_so:2d} {pa_mark}{ab_mark}{h_mark}{bb_mark}{so_mark}")

def debug_worst_game():
    """Debug the worst performing game"""
    debugger = GameDebugger()
    
    # The worst game from your data
    worst_game_url = "https://www.baseball-reference.com/boxes/SDN/SDN202503310.shtml"
    
    results = debugger.debug_game_in_detail(worst_game_url)
    debugger.compare_with_official_stats(results)
    
    return results

if __name__ == "__main__":
    debug_worst_game()