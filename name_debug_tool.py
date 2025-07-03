"""
Name Matching Debugger for MLB Stats Parser
==========================================

This tool will help identify exactly why names aren't matching between
box score and play-by-play data.
"""

import pandas as pd
import unicodedata
import re
from typing import Dict, List, Tuple, Set

class NameMatchingDebugger:
    """Advanced debugging for name matching issues"""
    
    def __init__(self):
        self.debug_info = {}
    
    def deep_debug_names(self, official_stats: pd.DataFrame, parsed_events: pd.DataFrame) -> Dict:
        """Perform comprehensive name matching analysis"""
        
        print("🔍 DEEP NAME MATCHING DEBUG")
        print("=" * 50)
        
        # Extract name sets
        official_names = set(official_stats['player_name'].tolist()) if not official_stats.empty else set()
        parsed_names = set(parsed_events['batter_name'].unique()) if not parsed_events.empty else set()
        
        print(f"📊 Dataset overview:")
        print(f"   Official players: {len(official_names)}")
        print(f"   Parsed players: {len(parsed_names)}")
        print(f"   Exact matches: {len(official_names & parsed_names)}")
        
        # Character-level analysis
        self._analyze_character_differences(official_names, parsed_names)
        
        # Fuzzy matching analysis
        fuzzy_matches = self._find_fuzzy_matches(official_names, parsed_names)
        
        # Unicode analysis
        self._analyze_unicode_issues(official_names, parsed_names)
        
        # Name normalization suggestions
        normalization_map = self._suggest_name_normalization(official_names, parsed_names)
        
        return {
            'official_names': official_names,
            'parsed_names': parsed_names,
            'exact_matches': official_names & parsed_names,
            'fuzzy_matches': fuzzy_matches,
            'normalization_map': normalization_map,
            'debug_info': self.debug_info
        }
    
    def _analyze_character_differences(self, official_names: Set[str], parsed_names: Set[str]):
        """Analyze character-level differences between name sets"""
        
        print(f"\n🔍 CHARACTER-LEVEL ANALYSIS:")
        print("-" * 30)
        
        # Sample names for detailed analysis
        sample_official = list(official_names)[:5]
        sample_parsed = list(parsed_names)[:5]
        
        print(f"📋 OFFICIAL NAMES (first 5):")
        for name in sample_official:
            print(f"   '{name}'")
            print(f"   Length: {len(name)}")
            print(f"   Repr: {repr(name)}")
            print(f"   Bytes: {name.encode('utf-8')}")
            print(f"   Chars: {[ord(c) for c in name]}")
            print()
        
        print(f"📋 PARSED NAMES (first 5):")
        for name in sample_parsed:
            print(f"   '{name}'")
            print(f"   Length: {len(name)}")
            print(f"   Repr: {repr(name)}")
            print(f"   Bytes: {name.encode('utf-8')}")
            print(f"   Chars: {[ord(c) for c in name]}")
            print()
    
    def _find_fuzzy_matches(self, official_names: Set[str], parsed_names: Set[str]) -> List[Tuple[str, str, float]]:
        """Find potential matches using fuzzy string matching"""
        
        print(f"\n🔍 FUZZY MATCHING ANALYSIS:")
        print("-" * 30)
        
        potential_matches = []
        
        for parsed_name in parsed_names:
            best_matches = []
            
            for official_name in official_names:
                # Simple similarity metrics
                similarity_score = self._calculate_similarity(parsed_name, official_name)
                
                if similarity_score > 0.8:  # 80% similarity threshold
                    best_matches.append((official_name, similarity_score))
            
            # Sort by similarity and take top matches
            best_matches.sort(key=lambda x: x[1], reverse=True)
            
            if best_matches:
                for official_name, score in best_matches[:2]:  # Top 2 matches
                    potential_matches.append((parsed_name, official_name, score))
                    print(f"   '{parsed_name}' -> '{official_name}' (similarity: {score:.3f})")
        
        self.debug_info['fuzzy_matches'] = potential_matches
        return potential_matches
    
    def _calculate_similarity(self, name1: str, name2: str) -> float:
        """Calculate simple similarity score between two names"""
        
        # Normalize for comparison
        n1 = self._normalize_name(name1)
        n2 = self._normalize_name(name2)
        
        if n1 == n2:
            return 1.0
        
        # Simple character overlap
        chars1 = set(n1.lower())
        chars2 = set(n2.lower())
        
        if not chars1 or not chars2:
            return 0.0
        
        overlap = len(chars1 & chars2)
        total = len(chars1 | chars2)
        
        return overlap / total if total > 0 else 0.0
    
    def _normalize_name(self, name: str) -> str:
        """Normalize name for comparison"""
        
        if not name:
            return ""
        
        # Remove extra spaces and normalize unicode
        normalized = unicodedata.normalize('NFKD', name)
        normalized = re.sub(r'\s+', ' ', normalized.strip())
        
        return normalized
    
    def _analyze_unicode_issues(self, official_names: Set[str], parsed_names: Set[str]):
        """Check for unicode normalization issues"""
        
        print(f"\n🔍 UNICODE ANALYSIS:")
        print("-" * 30)
        
        def analyze_unicode_name(name: str, source: str):
            normalized_nfc = unicodedata.normalize('NFC', name)
            normalized_nfd = unicodedata.normalize('NFD', name)
            normalized_nfkc = unicodedata.normalize('NFKC', name)
            normalized_nfkd = unicodedata.normalize('NFKD', name)
            
            print(f"   {source}: '{name}'")
            print(f"      Original: {repr(name)}")
            print(f"      NFC:      {repr(normalized_nfc)}")
            print(f"      NFD:      {repr(normalized_nfd)}")
            print(f"      NFKC:     {repr(normalized_nfkc)}")
            print(f"      NFKD:     {repr(normalized_nfkd)}")
            
            # Check for non-ASCII characters
            non_ascii = [c for c in name if ord(c) > 127]
            if non_ascii:
                print(f"      Non-ASCII chars: {non_ascii}")
            
            print()
        
        # Analyze a few names from each set
        for name in list(official_names)[:2]:
            analyze_unicode_name(name, "OFFICIAL")
        
        for name in list(parsed_names)[:2]:
            analyze_unicode_name(name, "PARSED")
    
    def _suggest_name_normalization(self, official_names: Set[str], parsed_names: Set[str]) -> Dict[str, str]:
        """Suggest a normalization mapping to fix name matching"""
        
        print(f"\n🔧 NORMALIZATION SUGGESTIONS:")
        print("-" * 30)
        
        normalization_map = {}
        
        # Try different normalization strategies
        strategies = [
            ('strip_spaces', lambda x: re.sub(r'\s+', ' ', x.strip())),
            ('unicode_nfkd', lambda x: unicodedata.normalize('NFKD', x)),
            ('remove_non_ascii', lambda x: ''.join(c for c in x if ord(c) < 128)),
            ('clean_punctuation', lambda x: re.sub(r'[^\w\s]', '', x)),
        ]
        
        for strategy_name, normalize_func in strategies:
            print(f"\n   Testing strategy: {strategy_name}")
            
            official_normalized = {normalize_func(name): name for name in official_names}
            parsed_normalized = {normalize_func(name): name for name in parsed_names}
            
            matches = set(official_normalized.keys()) & set(parsed_normalized.keys())
            
            print(f"   Matches found: {len(matches)}")
            
            if matches:
                for normalized_name in list(matches)[:3]:  # Show first 3 matches
                    official_orig = official_normalized[normalized_name]
                    parsed_orig = parsed_normalized[normalized_name]
                    print(f"      '{parsed_orig}' -> '{official_orig}'")
                    normalization_map[parsed_orig] = official_orig
        
        return normalization_map
    
    def create_name_resolver(self, normalization_map: Dict[str, str]) -> Dict[str, str]:
        """Create a comprehensive name resolver"""
        
        resolver = {}
        
        # Add identity mappings
        for original, normalized in normalization_map.items():
            resolver[original] = normalized
            resolver[normalized] = normalized  # Self-mapping
        
        return resolver

# INTEGRATION FUNCTION
def debug_name_matching_in_validator():
    """
    Add this function to your SingleGameValidator class to debug name matching
    """
    
    code_to_add = '''
    def debug_name_matching(self, official_stats: pd.DataFrame, parsed_events: pd.DataFrame):
        """Debug name matching issues"""
        
        debugger = NameMatchingDebugger()
        debug_results = debugger.deep_debug_names(official_stats, parsed_events)
        
        # Apply suggested normalization
        if debug_results['normalization_map']:
            print(f"\\n🔧 APPLYING NORMALIZATION:")
            
            # Create name resolver
            name_resolver = debugger.create_name_resolver(debug_results['normalization_map'])
            
            # Apply to parsed events
            parsed_events['normalized_batter_name'] = parsed_events['batter_name'].map(
                lambda x: name_resolver.get(x, x)
            )
            
            # Try merge again
            official_names = set(official_stats['player_name'])
            normalized_names = set(parsed_events['normalized_batter_name'])
            
            new_matches = len(official_names & normalized_names)
            print(f"   New matches after normalization: {new_matches}")
            
            return name_resolver
        
        return None
    '''
    
    print("🔧 CODE TO ADD TO YOUR SingleGameValidator CLASS:")
    print(code_to_add)

if __name__ == "__main__":
    # This would be called from your main validator
    print("Name Matching Debugger Ready!")
    print("Import this into your single_game_validator.py and call:")
    print("  debugger = NameMatchingDebugger()")
    print("  debug_results = debugger.deep_debug_names(official_stats, parsed_events)")
    
    debug_name_matching_in_validator()