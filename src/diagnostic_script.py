#!/usr/bin/env python3
"""
Diagnostic script to find the exact source of the data type issue
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from parsing.appearances_parser import (
    parse_batting_appearances, 
    get_batting_stats_for_validation
)
from utils.url_cacher import HighPerformancePageFetcher
from parsing.parsing_utils import extract_game_id

# Test game
game_url = "https://www.baseball-reference.com/boxes/NYN/NYN202410080.shtml"

print("=" * 60)
print("DIAGNOSTIC: Finding Data Type Issue")
print("=" * 60)

# Fetch the page
fetcher = HighPerformancePageFetcher(cache_dir="cache")
soup = fetcher.fetch_page(game_url)
game_id = extract_game_id(game_url)

# Parse batting appearances
print("\n1️⃣ Parsing batting appearances...")
batting_appearances = parse_batting_appearances(soup, game_id)
print(f"   ✓ Parsed {len(batting_appearances)} rows")

# Get validation data
print("\n2️⃣ Getting batting stats for validation...")
batting_for_validation = get_batting_stats_for_validation(batting_appearances)
print(f"   ✓ Got {len(batting_for_validation)} rows for validation")

# Check the data types
print("\n3️⃣ Checking data types in validation DataFrame:")
print("\n   Column Name          | Data Type")
print("   " + "-" * 50)

for col in batting_for_validation.columns:
    dtype = batting_for_validation[col].dtype
    print(f"   {col:20} | {dtype}")

# Check specific numeric columns
print("\n4️⃣ Checking specific stat columns:")
stat_cols = ['PA', 'AB', 'H', 'BB', 'SO', 'HR', '2B', '3B']

for col in stat_cols:
    if col in batting_for_validation.columns:
        dtype = batting_for_validation[col].dtype
        sample_values = batting_for_validation[col].head(3).tolist()
        print(f"   {col:5} - dtype: {dtype:10} - samples: {sample_values}")
    else:
        print(f"   {col:5} - MISSING")

# Try the problematic operation
print("\n5️⃣ Testing the problematic operation:")
try:
    meaningful_columns = ['PA', 'AB', 'H', 'BB', 'SO', 'HR', '2B', '3B', 'SB', 'CS', 'HBP', 'GDP', 'SF', 'SH']
    existing_cols = [c for c in meaningful_columns if c in batting_for_validation.columns]
    print(f"   Existing columns: {existing_cols}")
    
    print(f"   Attempting: official[{existing_cols}].sum(axis=1) > 0")
    result = batting_for_validation[existing_cols].sum(axis=1) > 0
    print(f"   ✓ SUCCESS! Result type: {type(result)}")
    print(f"   ✓ Filtered to {result.sum()} meaningful players")
except Exception as e:
    print(f"   ❌ ERROR: {e}")
    print(f"   Error type: {type(e).__name__}")
    
    # Try to find which column is the problem
    print("\n   Testing each column individually:")
    for col in existing_cols:
        try:
            test_sum = batting_for_validation[col].sum()
            test_compare = batting_for_validation[col] > 0
            print(f"      {col:5} - sum: {test_sum:6.1f} ✓")
        except Exception as col_error:
            print(f"      {col:5} - ERROR: {col_error}")

# Check what get_batting_stats_for_validation returns
print("\n6️⃣ Inspecting get_batting_stats_for_validation function:")
print(f"   Returns a DataFrame with shape: {batting_for_validation.shape}")
print(f"   Columns: {list(batting_for_validation.columns)}")

# Show first few rows
print("\n7️⃣ First 3 rows of data:")
print(batting_for_validation.head(3))

print("\n" + "=" * 60)
print("DIAGNOSTIC COMPLETE")
print("=" * 60)
