import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from unified_events_parser import UnifiedEventsParser
from multi_game_unified_validator import MultiGameUnifiedValidator
import pandas as pd

class GameDebugger:
    def __init__(self):
        self.parser = UnifiedEventsParser()
        self.validator = MultiGameUnifiedValidator()
    
    def debug_game_in_detail(self, game_url):
        """Debug a specific game in extreme detail"""
        print(f"🔍 DEEP DEBUGGING: {game_url}")
        print("=" * 80)
        
        # Get game ID
        game_id = self.parser._extract_game_id(game_url)
        print(f"Game ID: {game_id}")
        
        # Parse events
        game_data = self.parser.parse_game(game_url)
        
        if game_data is None:
            print("Events parsed: 0")
            print("\n🚨 ZERO EVENTS PARSED - INVESTIGATING...")
            self._debug_zero_events(game_url)
            return None
        
        # Extract events DataFrame from the returned dict
        events_df = game_data.get('unified_events', None)
        if events_df is None or len(events_df) == 0:
            print("Events parsed: 0")
            print("\n🚨 ZERO EVENTS PARSED - INVESTIGATING...")
            self._debug_zero_events(game_url)
            return None
        
        print(f"Events parsed: {len(events_df)}")
        
        # Try to validate if events exist - use the game_data dict directly
        try:
            # Check if validation data is already in the game_data
            if 'batting_validation' in game_data and 'pitching_validation' in game_data:
                results = game_data
                print(f"\n📊 ACCURACY SUMMARY:")
                bat_val = results['batting_validation']
                pitch_val = results['pitching_validation']
                
                print(f"   Batting: {bat_val['accuracy']:.1f}% ({bat_val['total_differences']} diffs/{bat_val['total_stats']} stats)")
                print(f"   Pitching: {pitch_val['accuracy']:.1f}% ({pitch_val['total_differences']} diffs/{pitch_val['total_stats']} stats)")
                
                # Show differences if any
                if bat_val['differences']:
                    print(f"\n🎯 BATTING DIFFERENCES:")
                    for diff in bat_val['differences'][:5]:
                        print(f"   {diff}")
                
                if pitch_val['differences']:
                    print(f"\n🎯 PITCHING DIFFERENCES:")
                    for diff in pitch_val['differences'][:5]:
                        print(f"   {diff}")
            else:
                print(f"\n⚠️ No validation data found in game_data")
                results = None
        except Exception as e:
            print(f"⚠️ Validation analysis failed: {e}")
            results = None
        
        # Show sample events
        print(f"\n📋 SAMPLE EVENTS:")
        print(f"📊 DataFrame shape: {events_df.shape}")
        print(f"📊 Available columns: {list(events_df.columns)}")
        
        sample_events = events_df.head(5)
        for i, (_, event) in enumerate(sample_events.iterrows()):
            print(f"\n   Event {i+1}:")
            # Show key fields that are likely to exist
            key_fields = ['inning', 'half_inning', 'batter_name', 'pitcher_name', 'event_description', 
                         'is_plate_appearance', 'is_at_bat', 'is_hit']
            for field in key_fields:
                if field in event:
                    print(f"      {field}: {event[field]}")
        
        return results
    
    def _debug_zero_events(self, game_url):
        """Debug why no events were parsed from a game"""
        print("Fetching raw HTML...")
        
        # Get the raw HTML
        response = self.parser.fetcher.fetch_box_score(game_url)
        if not response:
            print("❌ Failed to fetch HTML")
            return
        
        soup = response
        print(f"✅ HTML fetched successfully")
        
        # Check if play-by-play section exists
        pbp_section = soup.find('div', {'id': 'all_play_by_play'})
        if not pbp_section:
            print("❌ No play-by-play section found (id='all_play_by_play')")
            return
        
        print("✅ Play-by-play section found")
        
        # Check for the actual play-by-play table
        pbp_table = pbp_section.find('table')
        if not pbp_table:
            print("❌ No table found in play-by-play section")
            return
        
        print("✅ Play-by-play table found")
        
        # Check for rows
        rows = pbp_table.find_all('tr')
        print(f"📊 Found {len(rows)} rows in play-by-play table")
        
        # Analyze first few rows
        print("\n🔍 ANALYZING FIRST 5 ROWS:")
        for i, row in enumerate(rows[:5]):
            cells = row.find_all(['td', 'th'])
            print(f"   Row {i}: {len(cells)} cells")
            if cells:
                cell_texts = [cell.get_text().strip() for cell in cells]
                print(f"      Content: {cell_texts}")
        
        # Check for specific event patterns
        print("\n🔍 CHECKING FOR EVENT PATTERNS:")
        event_count = 0
        for row in rows:
            cells = row.find_all('td')
            if len(cells) >= 6:  # Minimum cells for an event
                event_text = cells[5].get_text().strip() if len(cells) > 5 else ""
                if event_text and event_text not in ['', 'Play']:
                    event_count += 1
                    if event_count <= 3:  # Show first 3 events
                        print(f"   Event {event_count}: {event_text}")
        
        print(f"📊 Total potential events found: {event_count}")
        
        # Check if this is a special game type
        title = soup.find('title')
        if title:
            title_text = title.get_text()
            print(f"\n📰 Game Title: {title_text}")
            
            # Check for postponed/cancelled games
            if any(keyword in title_text.lower() for keyword in ['postponed', 'cancelled', 'suspended']):
                print("🚨 This appears to be a postponed/cancelled/suspended game")
        
        # Check for game status
        status_div = soup.find('div', class_='game_status')
        if status_div:
            status_text = status_div.get_text().strip()
            print(f"📊 Game Status: {status_text}")

def debug_worst_game():
    """Debug the worst performing game"""
    debugger = GameDebugger()
    
    # URL for the game with 0 events
    worst_game_url = "https://www.baseball-reference.com/boxes/SFN/SFN201904070.shtml"
    
    results = debugger.debug_game_in_detail(worst_game_url)
    
    if results:
        print("\n" + "="*80)
        print("🎯 DEBUGGING COMPLETE - VALIDATION SUCCESSFUL")
    else:
        print("\n" + "="*80)
        print("🚨 DEBUGGING COMPLETE - ZERO EVENTS ISSUE IDENTIFIED")

if __name__ == "__main__":
    debug_worst_game()