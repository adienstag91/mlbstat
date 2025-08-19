#!/usr/bin/env python3
"""
Migration Script: From UnifiedEventsParser Class to Modular Functions
====================================================================

This script helps you migrate from your current class-based approach to the 
new modular function-based approach while maintaining 100% compatibility.
"""

import os
import shutil
from pathlib import Path

def create_directory_structure():
    """Create the new modular directory structure"""
    
    # Create directories
    directories = [
        'parsing',
        'validation', 
        'pipeline',
        'database',
        'utils'
    ]
    
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        # Create __init__.py files to make them Python packages
        init_file = os.path.join(directory, '__init__.py')
        if not os.path.exists(init_file):
            with open(init_file, 'w') as f:
                f.write(f'"""{directory.title()} module"""\n')
    
    print("✅ Created directory structure:")
    for directory in directories:
        print(f"   📁 {directory}/")

def create_module_files():
    """Create the individual module files"""
    
    modules = {
        'parsing/name_utils.py': '''# Name resolution and normalization functions
from .refactored_parsing_modules import normalize_name, extract_canonical_names, build_name_resolver
''',
        'parsing/game_utils.py': '''# Game and event utilities  
from .refactored_parsing_modules import extract_game_id, parse_inning, parse_inning_half, parse_pitch_count, safe_int, generate_event_id
''',
        'parsing/outcome_analyzer.py': '''# Event outcome analysis
from .refactored_parsing_modules import analyze_outcome
''',
        'parsing/stats_parser.py': '''# Official stats parsing
from .refactored_parsing_modules import parse_official_batting, parse_official_pitching, extract_from_details
''',
        'parsing/events_parser.py': '''# Event parsing and processing
from .refactored_parsing_modules import parse_single_event, parse_play_by_play_events, fix_pitch_count_duplicates
''',
        'validation/player_categorizer.py': '''# Player categorization utilities
from .validation_and_pipeline import calculate_meaningful_batters, categorize_unmatched_players
''',
        'validation/stat_validator.py': '''# Stats validation functions
from .validation_and_pipeline import validate_batting_stats, validate_pitching_stats, compare_stats
''',
        'pipeline/game_processor.py': '''# Main game processing pipeline
from .validation_and_pipeline import process_single_game, process_multiple_games
''',
        'pipeline/batch_validator.py': '''# Batch validation and reporting
from .validation_and_pipeline import validate_game_batch, generate_accuracy_report
''',
    }
    
    for file_path, content in modules.items():
        with open(file_path, 'w') as f:
            f.write(content)
    
    print("✅ Created module files:")
    for file_path in modules.keys():
        print(f"   📄 {file_path}")

def create_compatibility_wrapper():
    """Create a compatibility wrapper so old code still works"""
    
    wrapper_code = '''"""
Compatibility Wrapper for UnifiedEventsParser
============================================

This maintains backward compatibility with your existing code while 
using the new modular functions under the hood.
"""

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

# Test function that works exactly like your old one
def test_unified_parser():
    """Test function with same interface as before"""
    test_url = "https://www.baseball-reference.com/boxes/KCA/KCA202503290.shtml"
    
    # New way (recommended)
    results = process_single_game(test_url)
    
    # Old way (still works)
    # parser = UnifiedEventsParser()
    # results = parser.parse_game(test_url)
    
    events = results['unified_events']
    batting_box = results['official_batting']
    pitching_box = results['official_pitching']
    
    print("📋 UNIFIED EVENTS SAMPLE:")
    if not events.empty:
        cols = ['batter_id', 'pitcher_id', 'inning', 'inning_half', 'description', 
                'is_plate_appearance', 'is_at_bat', 'is_hit', 'hit_type']
        available_cols = [col for col in cols if col in events.columns]
        print(events[available_cols].head(10))
        print(f"\\nTotal events: {len(events)}")

    print("\\nBatting Box score:")
    print(batting_box.head())

    print("\\nPitching Box score:")
    print(pitching_box.head())
    
    bat_val = results['batting_validation']
    pit_val = results['pitching_validation']
    
    print(f"\\n✅ VALIDATION RESULTS:")
    print(f"   ⚾ Batting: {bat_val['accuracy']:.1f}% ({bat_val['players_compared']} players)")
    print(f"   🥎 Pitching: {pit_val['accuracy']:.1f}% ({pit_val['players_compared']} pitchers)")
    
    return results

if __name__ == "__main__":
    test_unified_parser()
'''
    
    with open('unified_events_parser_compat.py', 'w') as f:
        f.write(wrapper_code)
    
    print("✅ Created compatibility wrapper: unified_events_parser_compat.py")

def create_new_main():
    """Create a clean new main.py file"""
    
    main_code = '''#!/usr/bin/env python3
"""
MLB Stats Pipeline - Modular Version
===================================

Clean, modular approach to parsing MLB play-by-play data.
Replaces the old class-based UnifiedEventsParser.
"""

from pipeline.game_processor import process_single_game, process_multiple_games
from pipeline.batch_validator import validate_game_batch, generate_accuracy_report

def main():
    """Main entry point for processing games"""
    # Example usage - replace with your actual game URLs
    test_urls = [
        "https://www.baseball-reference.com/boxes/KCA/KCA202503290.shtml",
        # Add more URLs here
    ]
    
    print("🚀 Starting MLB stats processing...")
    
    # Process games
    results = process_multiple_games(test_urls)
    
    # Validate and report
    summary = validate_game_batch(results)
    report_df = generate_accuracy_report(results)
    
    print(f"""
📊 PROCESSING COMPLETE
======================
Games: {summary['games_processed']}
Batting Accuracy: {summary['avg_batting_accuracy']:.1f}%
Pitching Accuracy: {summary['avg_pitching_accuracy']:.1f}%
""")
    
    # Save report
    report_df.to_csv('processing_report.csv', index=False)
    print("📄 Report saved to processing_report.csv")
    
    return results

def test_single():
    """Test processing a single game"""
    test_url = "https://www.baseball-reference.com/boxes/KCA/KCA202503290.shtml"
    result = process_single_game(test_url)
    
    print(f"Game {result['game_id']} processed successfully!")
    print(f"Events: {len(result['unified_events'])}")
    print(f"Batting Accuracy: {result['batting_validation']['accuracy']:.1f}%")
    print(f"Pitching Accuracy: {result['pitching_validation']['accuracy']:.1f}%")
    
    return result

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        test_single()
    else:
        main()
'''
    
    with open('main_modular.py', 'w') as f:
        f.write(main_code)
    
    print("✅ Created new main file: main_modular.py")

def backup_existing_files():
    """Backup existing files before migration"""
    
    backup_dir = 'backup_old_code'
    os.makedirs(backup_dir, exist_ok=True)
    
    files_to_backup = [
        'unified_events_parser.py',
        'multi_game_unified_validator.py',
        # Add other files you want to backup
    ]
    
    for file_name in files_to_backup:
        if os.path.exists(file_name):
            backup_path = os.path.join(backup_dir, file_name)
            shutil.copy2(file_name, backup_path)
            print(f"✅ Backed up {file_name} to {backup_path}")

def migration_instructions():
    """Print instructions for completing the migration"""
    
    instructions = """
🔧 MIGRATION COMPLETE - Next Steps:
===================================

1. **Test the new system:**
   python main_modular.py test

2. **Update your existing scripts:**
   
   OLD WAY:
   from unified_events_parser import UnifiedEventsParser
   parser = UnifiedEventsParser()
   result = parser.parse_game(url)
   
   NEW WAY:
   from pipeline.game_processor import process_single_game
   result = process_single_game(url)

3. **Gradual migration strategy:**
   - Use unified_events_parser_compat.py for immediate compatibility
   - Gradually update your scripts to use the new functions
   - Remove the compatibility wrapper when fully migrated

4. **Key benefits of new structure:**
   ✅ Easier to test individual components
   ✅ Cleaner separation of concerns  
   ✅ Ready for database integration
   ✅ More maintainable and extensible

5. **Database integration ready:**
   Your new modular structure is perfect for adding database functionality.
   The process_single_game() function returns clean data ready for insertion.

6. **Files you can now delete (after testing):**
   - unified_events_parser.py (backed up)
   - Any other old class-based files
   
Happy coding! 🚀
"""
    
    print(instructions)

def run_migration():
    """Run the complete migration process"""
    
    print("🚀 Starting migration from classes to modular functions...")
    print("=" * 60)
    
    # Step 1: Backup existing files
    print("\\n1. Backing up existing files...")
    backup_existing_files()
    
    # Step 2: Create directory structure
    print("\\n2. Creating directory structure...")
    create_directory_structure()
    
    # Step 3: Create module files
    print("\\n3. Creating module files...")
    create_module_files()
    
    # Step 4: Create compatibility wrapper
    print("\\n4. Creating compatibility wrapper...")
    create_compatibility_wrapper()
    
    # Step 5: Create new main file
    print("\\n5. Creating new main file...")
    create_new_main()
    
    # Step 6: Show instructions
    print("\\n6. Migration instructions...")
    migration_instructions()

if __name__ == "__main__":
    run_migration()
