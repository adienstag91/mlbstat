# test_new_pipeline.py
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

test_url = "https://www.baseball-reference.com/boxes/LAN/LAN202509160.shtml"

# Test the refactored parser first
print("Testing refactored appearances parser...")
from parsing.appearances_parser import test_refactored_appearances
test_refactored_appearances(test_url)
from parsing.events_parser import test_events_parser
test_events_parser(test_url)


print("\n" + "="*60 + "\n")

# Test the full database pipeline
print("Testing full database pipeline...")
from database.game_data_processor import test_separate_tables_processor
test_separate_tables_processor()