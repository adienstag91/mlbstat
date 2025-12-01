"""
Simple Game Debugger
===================

Debug games to see exactly which players have differences and why.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from pipeline.game_processor import process_game

def debug_game(game_url: str):
    """
    Debug a single game - show which players have differences and their events
    """
    print(f"\n{'='*80}")
    print(f"DEBUGGING: {game_url}")
    print(f"{'='*80}\n")
    
    # Process the game
    result = process_game(game_url)
    
    game_id = result['game_id']
    bat_val = result['validation_results']['batting']
    pit_val = result['validation_results']['pitching']
    
    print(f"Game ID: {game_id}")
    print(f"Batting Accuracy: {bat_val['accuracy']:.1f}%")
    print(f"Pitching Accuracy: {pit_val['accuracy']:.1f}%")
    print(f"Total Events: {len(result['parsing_results']['pbp_events'])}\n")
    
    # Debug batting differences
    if bat_val.get('differences'):
        print(f"\nBATTING DIFFERENCES ({len(bat_val['differences'])} players):")
        print("-"*80)
        
        for diff in bat_val['differences']:
            player = diff['player']
            print(f"\n{player}:")
            print(f"  Differences: {', '.join(diff['diffs'])}")
            
            # Get official stats
            official_row = result['parsing_results']['batting_appearances'][result['parsing_results']['batting_appearances']['player_name'] == player]
            if not official_row.empty:
                official = official_row.iloc[0]
                
                # Get parsed stats from events
                player_events = result['parsing_results']['pbp_events'][result['parsing_results']['pbp_events']['batter_name'] == player]
                
                print(f"\n  Official vs Parsed:")
                print(f"    PA:  {int(official.get('PA', 0)):2d} vs {int(player_events['is_plate_appearance'].sum()):2d}")
                print(f"    AB:  {int(official.get('AB', 0)):2d} vs {int(player_events['is_at_bat'].sum()):2d}")
                print(f"    H:   {int(official.get('H', 0)):2d} vs {int(player_events['is_hit'].sum()):2d}")
                print(f"    BB:  {int(official.get('BB', 0)):2d} vs {int(player_events['is_walk'].sum()):2d}")
                print(f"    SO:  {int(official.get('SO', 0)):2d} vs {int(player_events['is_strikeout'].sum()):2d}")
                print(f"    HR:  {int(official.get('HR', 0)):2d} vs {int((player_events['hit_type'] == 'home_run').sum()):2d}")
                print(f"    SF:  {int(official.get('SF', 0)):2d} vs {int(player_events['is_sacrifice_fly'].sum()):2d}")
                print(f"    SH:  {int(official.get('SH', 0)):2d} vs {int(player_events['is_sacrifice_hit'].sum()):2d}")
                
                # Show events
                if not player_events.empty:
                    print(f"\n  Events ({len(player_events)} total):")
                    for idx, (_, event) in enumerate(player_events.iterrows(), 1):
                        pa = "PA" if event['is_plate_appearance'] else "  "
                        ab = "AB" if event['is_at_bat'] else "  "
                        hit = "H" if event['is_hit'] else " "
                        bb = "BB" if event['is_walk'] else "  "
                        so = "SO" if event['is_strikeout'] else "  "
                        print(f"    {idx:2d}. [{pa}][{ab}][{hit}][{bb}][{so}] {event['description']}")
                else:
                    print(f"\n  No events found for this player!")
    else:
        print("\nNo batting differences!")
    
    # Debug pitching differences
    if pit_val.get('differences'):
        print(f"\n\nPITCHING DIFFERENCES ({len(pit_val['differences'])} pitchers):")
        print("-"*80)
        
        for diff in pit_val['differences']:
            pitcher = diff['player']
            print(f"\n{pitcher}:")
            print(f"  Differences: {', '.join(diff['diffs'])}")
            
            # Get official stats
            official_row = result['parsing_results']['pitching_appearances'][result['parsing_results']['pitching_appearances']['pitcher_name'] == pitcher]
            if not official_row.empty:
                official = official_row.iloc[0]
                
                # Get parsed stats from events
                pitcher_events = result['parsing_results']['pbp_events'][result['parsing_results']['pbp_events']['pitcher_name'] == pitcher]
                
                print(f"\n  Official vs Parsed:")
                print(f"    BF:  {int(official.get('BF', 0)):2d} vs {int(pitcher_events['is_plate_appearance'].sum()):2d}")
                print(f"    H:   {int(official.get('H', 0)):2d} vs {int(pitcher_events['is_hit'].sum()):2d}")
                print(f"    BB:  {int(official.get('BB', 0)):2d} vs {int(pitcher_events['is_walk'].sum()):2d}")
                print(f"    SO:  {int(official.get('SO', 0)):2d} vs {int(pitcher_events['is_strikeout'].sum()):2d}")
                print(f"    HR:  {int(official.get('HR', 0)):2d} vs {int((pitcher_events['hit_type'] == 'home_run').sum()):2d}")
                
                print(f"\n  Total events faced: {len(pitcher_events)}")
    else:
        print("\nNo pitching differences!")
    
    print(f"\n{'='*80}\n")
    
    return result


def debug_multiple_games(game_urls: list):
    """Debug multiple games"""
    print(f"\nDebugging {len(game_urls)} games...\n")
    
    for i, url in enumerate(game_urls, 1):
        print(f"\n[{i}/{len(game_urls)}]")
        try:
            debug_game(url)
        except Exception as e:
            print(f"ERROR: {e}\n")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python simple_game_debugger.py <game_url> [game_url2] [game_url3] ...")
        print("\nExample:")
        print("  python simple_game_debugger.py https://www.baseball-reference.com/boxes/ATL/ATL202010140.shtml")
        #sys.exit(1)
    
    #game_urls = sys.argv[1:]
    game_urls = [
        "https://www.baseball-reference.com/boxes/ANA/ANA201905240.shtml",
        "https://www.baseball-reference.com/boxes/ARI/ARI201906230.shtml",
        "https://www.baseball-reference.com/boxes/ARI/ARI202207270.shtml",
        "https://www.baseball-reference.com/boxes/BAL/BAL202106020.shtml",
        "https://www.baseball-reference.com/boxes/BOS/BOS201808180.shtml",
        "https://www.baseball-reference.com/boxes/CHA/CHA201804230.shtml",
        "https://www.baseball-reference.com/boxes/CHA/CHA201808210.shtml",
        "https://www.baseball-reference.com/boxes/CHA/CHA201906010.shtml",
        "https://www.baseball-reference.com/boxes/CHN/CHN201906250.shtml",
        "https://www.baseball-reference.com/boxes/CHN/CHN202205301.shtml",
        "https://www.baseball-reference.com/boxes/CIN/CIN201907180.shtml",
        "https://www.baseball-reference.com/boxes/CLE/CLE202205190.shtml",
        "https://www.baseball-reference.com/boxes/COL/COL201810070.shtml",
        "https://www.baseball-reference.com/boxes/COL/COL201909290.shtml",
        "https://www.baseball-reference.com/boxes/HOU/HOU201905220.shtml",
        "https://www.baseball-reference.com/boxes/HOU/HOU202405050.shtml",
        "https://www.baseball-reference.com/boxes/KCA/KCA201809160.shtml",
        "https://www.baseball-reference.com/boxes/LAN/LAN201806090.shtml",
        "https://www.baseball-reference.com/boxes/LAN/LAN201808250.shtml",
        "https://www.baseball-reference.com/boxes/MIA/MIA202407250.shtml",
        "https://www.baseball-reference.com/boxes/MIL/MIL201806130.shtml",
        "https://www.baseball-reference.com/boxes/MIL/MIL202205200.shtml",
        "https://www.baseball-reference.com/boxes/MIL/MIL202307040.shtml",
        "https://www.baseball-reference.com/boxes/MIN/MIN201907220.shtml",
        "https://www.baseball-reference.com/boxes/MIN/MIN201908070.shtml",
        "https://www.baseball-reference.com/boxes/NYN/NYN202205290.shtml",
        "https://www.baseball-reference.com/boxes/OAK/OAK202307010.shtml",
        "https://www.baseball-reference.com/boxes/PHI/PHI201804200.shtml",
        "https://www.baseball-reference.com/boxes/PHI/PHI201904250.shtml",
        "https://www.baseball-reference.com/boxes/PIT/PIT202209090.shtml",
        "https://www.baseball-reference.com/boxes/SDN/SDN201805040.shtml",
        "https://www.baseball-reference.com/boxes/SEA/SEA201809280.shtml",
        "https://www.baseball-reference.com/boxes/SFN/SFN201809120.shtml",
        "https://www.baseball-reference.com/boxes/SFN/SFN201904070.shtml",
        "https://www.baseball-reference.com/boxes/SFN/SFN201908090.shtml",
        "https://www.baseball-reference.com/boxes/SFN/SFN202308020.shtml",
        "https://www.baseball-reference.com/boxes/TBA/TBA201806260.shtml",
        "https://www.baseball-reference.com/boxes/TBA/TBA201807250.shtml",
        "https://www.baseball-reference.com/boxes/TBA/TBA201904240.shtml",
        "https://www.baseball-reference.com/boxes/TBA/TBA201907240.shtml",
        "https://www.baseball-reference.com/boxes/TEX/TEX201808160.shtml",
        "https://www.baseball-reference.com/boxes/TEX/TEX202209200.shtml",
        "https://www.baseball-reference.com/boxes/WAS/WAS202408280.shtml",
    ]
    
    if len(game_urls) == 1:
        debug_game(game_urls[0])
    else:
        debug_game(game_urls[0])
        #debug_multiple_games(game_urls)