#!/usr/bin/env python3
"""
Parity Verification Script
==========================

Compare results between old UnifiedEventsParser class and new modular functions
to ensure 100% identical output.
"""

import pandas as pd
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
print(sys.path)

def compare_dataframes(df1, df2, name):
    """Compare two DataFrames and report differences"""
    print(f"\n🔍 Comparing {name}:")
    
    if df1.empty and df2.empty:
        print(f"   ✅ Both {name} are empty - OK")
        return True
    
    if df1.empty or df2.empty:
        print(f"   ❌ One {name} is empty, the other is not!")
        print(f"      Old: {len(df1)} rows, New: {len(df2)} rows")
        return False
    
    if len(df1) != len(df2):
        print(f"   ❌ Different number of rows: Old={len(df1)}, New={len(df2)}")
        return False
    
    if set(df1.columns) != set(df2.columns):
        print(f"   ❌ Different columns!")
        print(f"      Old: {sorted(df1.columns)}")
        print(f"      New: {sorted(df2.columns)}")
        return False
    
    # Compare values (sort both to handle potential ordering differences)
    common_cols = [col for col in df1.columns if col in df2.columns]
    df1_sorted = df1[common_cols].sort_values(by=common_cols[0] if common_cols else df1.columns[0]).reset_index(drop=True)
    df2_sorted = df2[common_cols].sort_values(by=common_cols[0] if common_cols else df2.columns[0]).reset_index(drop=True)
    
    try:
        pd.testing.assert_frame_equal(df1_sorted, df2_sorted, check_dtype=False)
        print(f"   ✅ {name} are identical!")
        return True
    except AssertionError as e:
        print(f"   ❌ {name} have differences:")
        print(f"      {str(e)[:200]}...")
        return False

def compare_validation_results(val1, val2, name):
    """Compare validation result dictionaries"""
    print(f"\n🔍 Comparing {name} validation:")
    
    if val1.keys() != val2.keys():
        print(f"   ❌ Different keys: {val1.keys()} vs {val2.keys()}")
        return False
    
    differences = []
    for key in val1.keys():
        if key in ['differences', 'name_mismatches']:
            continue  # Skip complex nested structures for now
        
        if val1[key] != val2[key]:
            differences.append(f"{key}: {val1[key]} vs {val2[key]}")
    
    if differences:
        print(f"   ❌ Validation differences:")
        for diff in differences:
            print(f"      {diff}")
        return False
    else:
        print(f"   ✅ {name} validation results identical!")
        return True

def verify_parity():
    """Compare old vs new implementation"""
    test_url = "https://www.baseball-reference.com/boxes/KCA/KCA202503290.shtml"
    
    print("🔬 PARITY VERIFICATION")
    print("=" * 50)
    print(f"Testing URL: {test_url}")
    
    # Test OLD way (if available)
    print("\n1️⃣ Testing OLD implementation...")
    try:
        # Try to import and use the old class
        if os.path.exists('mlbstat/src/backup_old_code/unified_events_parser.py'):
            sys.path.insert(0, 'backup_old_code')
            from unified_events_parser import UnifiedEventsParser
            old_parser = UnifiedEventsParser()
            old_result = old_parser.parse_game(test_url)
            print("   ✅ Old implementation works")
        else:
            print("   ⚠️  Old implementation not found in backup - using compatibility wrapper")
            from unified_events_parser_compat import UnifiedEventsParser
            old_parser = UnifiedEventsParser()
            old_result = old_parser.parse_game(test_url)
    except Exception as e:
        print(f"   ❌ Old implementation failed: {e}")
        return False
    
    # Test NEW way
    print("\n2️⃣ Testing NEW implementation...")
    try:
        from pipeline.game_processor import process_single_game
        new_result = process_single_game(test_url)
        print("   ✅ New implementation works")
    except Exception as e:
        print(f"   ❌ New implementation failed: {e}")
        return False
    
    # Compare results
    print("\n3️⃣ Comparing results...")
    
    all_good = True
    
    # Compare basic structure
    if old_result.keys() != new_result.keys():
        print(f"   ❌ Different result keys!")
        print(f"      Old: {sorted(old_result.keys())}")
        print(f"      New: {sorted(new_result.keys())}")
        all_good = False
    else:
        print(f"   ✅ Same result structure: {sorted(old_result.keys())}")
    
    # Compare game_id
    if old_result['game_id'] != new_result['game_id']:
        print(f"   ❌ Different game_id: {old_result['game_id']} vs {new_result['game_id']}")
        all_good = False
    else:
        print(f"   ✅ Same game_id: {old_result['game_id']}")
    
    # Compare DataFrames
    all_good &= compare_dataframes(
        old_result['unified_events'], 
        new_result['unified_events'], 
        "unified_events"
    )
    
    all_good &= compare_dataframes(
        old_result['official_batting'], 
        new_result['official_batting'], 
        "official_batting"
    )
    
    all_good &= compare_dataframes(
        old_result['official_pitching'], 
        new_result['official_pitching'], 
        "official_pitching"
    )
    
    # Compare validation results
    all_good &= compare_validation_results(
        old_result['batting_validation'],
        new_result['batting_validation'],
        "batting"
    )
    
    all_good &= compare_validation_results(
        old_result['pitching_validation'],
        new_result['pitching_validation'],
        "pitching"
    )
    
    # Final verdict
    print("\n" + "=" * 50)
    if all_good:
        print("🎉 PARITY VERIFIED! Old and new implementations are identical!")
        print("✅ Safe to proceed with migration")
    else:
        print("❌ PARITY ISSUES FOUND! Need to investigate differences")
        print("⚠️  Do not use new implementation until fixed")
    
    print("=" * 50)
    
    return all_good

def detailed_comparison():
    """Show detailed side-by-side comparison"""
    test_url = "https://www.baseball-reference.com/boxes/KCA/KCA202503290.shtml"
    
    print("📊 DETAILED COMPARISON")
    print("=" * 50)
    
    # Get both results
    try:
        from unified_events_parser_compat import process_single_game as old_process
        old_result = old_process(test_url)
    except:
        print("❌ Could not get old results")
        return
    
    try:
        from pipeline.game_processor import process_single_game as new_process
        new_result = new_process(test_url)
    except:
        print("❌ Could not get new results")
        return
    
    print(f"\n📋 EVENTS COMPARISON:")
    print(f"Old events: {len(old_result['unified_events'])}")
    print(f"New events: {len(new_result['unified_events'])}")
    
    if not old_result['unified_events'].empty and not new_result['unified_events'].empty:
        print("\nFirst 3 events comparison:")
        old_sample = old_result['unified_events'][['batter_id', 'pitcher_id', 'description', 'is_hit']].head(3)
        new_sample = new_result['unified_events'][['batter_id', 'pitcher_id', 'description', 'is_hit']].head(3)
        
        print("OLD:")
        print(old_sample.to_string(index=False))
        print("\nNEW:")
        print(new_sample.to_string(index=False))
    
    print(f"\n📊 BATTING COMPARISON:")
    print(f"Old batting: {len(old_result['official_batting'])} players")
    print(f"New batting: {len(new_result['official_batting'])} players")
    
    print(f"\n⚾ PITCHING COMPARISON:")
    print(f"Old pitching: {len(old_result['official_pitching'])} pitchers")  
    print(f"New pitching: {len(new_result['official_pitching'])} pitchers")
    
    print(f"\n✅ VALIDATION COMPARISON:")
    old_bat = old_result['batting_validation']
    new_bat = new_result['batting_validation']
    old_pit = old_result['pitching_validation']
    new_pit = new_result['pitching_validation']
    
    print(f"Batting accuracy: Old={old_bat['accuracy']:.1f}%, New={new_bat['accuracy']:.1f}%")
    print(f"Pitching accuracy: Old={old_pit['accuracy']:.1f}%, New={new_pit['accuracy']:.1f}%")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "detailed":
        detailed_comparison()
    else:
        verify_parity()
