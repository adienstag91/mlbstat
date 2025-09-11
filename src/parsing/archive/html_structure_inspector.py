"""
HTML Structure Inspector for Batting Tables
==========================================

This will help us understand how Baseball Reference indicates substitutions in the HTML
so we can properly parse batting order and identify starters vs substitutes.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.mlb_cached_fetcher import SafePageFetcher
from bs4 import BeautifulSoup
import re

def inspect_batting_table_html(game_url: str):
    """Inspect the raw HTML structure of batting tables"""
    
    print(f"🔍 INSPECTING HTML STRUCTURE")
    print("=" * 50)
    print(f"URL: {game_url}")
    
    # Fetch the page
    soup = SafePageFetcher.fetch_page(game_url)
    
    # Find batting tables
    batting_tables = soup.find_all('table', {'id': lambda x: x and x.endswith('batting')})
    
    for table_idx, table in enumerate(batting_tables):
        team_name = "Away" if table_idx == 0 else "Home"
        print(f"\n🏟️  {team_name} Team Batting Table")
        print("-" * 30)
        
        # Get all rows
        rows = table.find_all('tr')
        print(f"Total rows: {len(rows)}")
        
        # Examine each data row
        data_rows = [row for row in rows if row.find('td')]  # Skip header rows
        
        for row_idx, row in enumerate(data_rows):
            cells = row.find_all(['td', 'th'])
            if not cells:
                continue
                
            # Get player name (first cell)
            player_cell = cells[0]
            player_text = player_cell.get_text().strip()
            
            if not player_text or 'Team Totals' in player_text:
                continue
            
            # Check for HTML indicators of substitutions
            row_class = row.get('class', [])
            row_style = row.get('style', '')
            
            # Check cell styling
            cell_class = player_cell.get('class', [])
            cell_style = player_cell.get('style', '')
            
            # Check for indentation or special formatting
            has_indent = bool(re.search(r'padding-left|margin-left|text-indent', cell_style))
            
            # Check for special characters or formatting in text
            has_special_chars = bool(re.search(r'^[\\s\\xa0]+', player_text))
            
            print(f"  Row {row_idx+1:2d}: {player_text}")
            print(f"         Row class: {row_class}")
            print(f"         Row style: {row_style}")
            print(f"         Cell class: {cell_class}")
            print(f"         Cell style: {cell_style}")
            print(f"         Has indent: {has_indent}")
            print(f"         Has leading spaces: {has_special_chars}")
            print(f"         Raw HTML: {str(player_cell)[:100]}...")
            print()

def analyze_substitution_patterns(game_url: str):
    """Analyze patterns that might indicate substitutions"""
    
    print(f"\n🔍 ANALYZING SUBSTITUTION PATTERNS")
    print("=" * 50)
    
    soup = SafePageFetcher.fetch_page(game_url)
    batting_tables = soup.find_all('table', {'id': lambda x: x and x.endswith('batting')})
    
    for table_idx, table in enumerate(batting_tables):
        team_name = "Away" if table_idx == 0 else "Home"
        print(f"\n🏟️  {team_name} Team Analysis")
        print("-" * 20)
        
        # Parse with pandas to get stats
        import pandas as pd
        from io import StringIO
        
        try:
            df = pd.read_html(StringIO(str(table)))[0]
            df = df[df['Batting'].notna()]
            df = df[~df['Batting'].str.contains("Team Totals", na=False)]
            
            print("Players with their stats:")
            for idx, row in df.iterrows():
                player_name = str(row['Batting'])
                pa = row.get('PA', 0)
                ab = row.get('AB', 0)
                
                # Indicators of being a substitute
                is_pitcher = bool(re.search(r'\\s+P\\s*$', player_name))
                is_pinch_runner = 'PR' in player_name
                has_zero_pa = pa == 0
                has_minimal_stats = pa <= 1 and ab == 0
                
                role_indicators = []
                if is_pitcher:
                    role_indicators.append("PITCHER")
                if is_pinch_runner:
                    role_indicators.append("PINCH RUNNER")
                if has_zero_pa and not is_pitcher:
                    role_indicators.append("NO PA")
                if has_minimal_stats and not is_pitcher and not is_pinch_runner:
                    role_indicators.append("MINIMAL STATS")
                
                role_str = " | ".join(role_indicators) if role_indicators else "REGULAR"
                
                print(f"  {idx+1:2d}. {player_name:30s} PA={pa:2d} AB={ab:2d} → {role_str}")
                
        except Exception as e:
            print(f"Error parsing table: {e}")

def identify_batting_order_logic(game_url: str):
    """Try to identify the actual batting order logic"""
    
    print(f"\n🎯 BATTING ORDER ANALYSIS")
    print("=" * 50)
    
    soup = SafePageFetcher.fetch_page(game_url)
    batting_tables = soup.find_all('table', {'id': lambda x: x and x.endswith('batting')})
    
    for table_idx, table in enumerate(batting_tables):
        team_name = "Away" if table_idx == 0 else "Home"
        print(f"\n🏟️  {team_name} Team Batting Order")
        print("-" * 25)
        
        import pandas as pd
        from io import StringIO
        
        try:
            df = pd.read_html(StringIO(str(table)))[0]
            df = df[df['Batting'].notna()]
            df = df[~df['Batting'].str.contains("Team Totals", na=False)]
            
            print("Proposed batting order assignment:")
            batting_order = 1
            
            for idx, row in df.iterrows():
                player_name = str(row['Batting'])
                pa = row.get('PA', 0)
                ab = row.get('AB', 0)
                
                # Skip pitchers (they don't bat in AL)
                if re.search(r'\\s+P\\s*$', player_name):
                    print(f"  --  {player_name:30s} (PITCHER - doesn't bat)")
                    continue
                
                # Skip pinch runners (they don't get batting order)
                if 'PR' in player_name:
                    print(f"  PR  {player_name:30s} (PINCH RUNNER - no batting order)")
                    continue
                
                # Assign batting order to first 9 eligible players
                if batting_order <= 9:
                    if pa > 0 or ab > 0:  # Has batting stats
                        print(f"  {batting_order:2d}  {player_name:30s} PA={pa:2d} AB={ab:2d} (STARTER)")
                        batting_order += 1
                    else:
                        print(f"  ?   {player_name:30s} PA={pa:2d} AB={ab:2d} (SUBSTITUTE?)")
                else:
                    # These are substitutes
                    if pa > 0 or ab > 0:
                        print(f"  SUB {player_name:30s} PA={pa:2d} AB={ab:2d} (replaced someone)")
                    else:
                        print(f"  --  {player_name:30s} PA={pa:2d} AB={ab:2d} (defensive sub)")
                
        except Exception as e:
            print(f"Error: {e}")

def main():
    """Main function to run all analysis"""
    
    test_url = "https://www.baseball-reference.com/boxes/KCA/KCA202503290.shtml"
    
    # Step 1: Inspect raw HTML structure
    inspect_batting_table_html(test_url)
    
    # Step 2: Analyze substitution patterns
    analyze_substitution_patterns(test_url)
    
    # Step 3: Try to identify batting order logic
    identify_batting_order_logic(test_url)
    
    print(f"\n💡 RECOMMENDATIONS:")
    print("=" * 50)
    print("1. Filter out pitchers from batting appearances entirely")
    print("2. Use PA/AB > 0 as primary indicator of actual batting participation")
    print("3. First 9 players with meaningful stats are likely the batting order 1-9")
    print("4. Players after position 9 with stats are substitutes who inherited a batting order")
    print("5. Pinch runners (PR) should not have batting orders")

if __name__ == "__main__":
    main()
