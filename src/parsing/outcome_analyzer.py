"""
Event outcome analysis
=====================
"""

import re
from typing import Optional, Dict

def analyze_outcome(description: str) -> Optional[Dict]:
    """Analyze play outcome - handles all types of baseball events"""
    desc = description.lower().strip()
    outcome = {
        'is_plate_appearance': True, 'is_at_bat': False, 'is_hit': False, 'hit_type': None,
        'is_walk': False, 'is_strikeout': False, 'is_sacrifice_fly': False, 'is_sacrifice_hit': False,
        'is_out': False, 'outs_recorded': 0, 'bases_reached': 0,
    }
    
    # Check for pure baserunning plays FIRST
    pure_baserunning_patterns = [
        r'caught stealing.*interference by runner',
        r'interference by runner.*caught stealing', 
        r'double play.*caught stealing.*interference',
        r'caught stealing.*double play.*interference',
        r'^interference by runner',
        r'^runner interference'
    ]
    
    for pattern in pure_baserunning_patterns:
        if re.search(pattern, desc):
            outcome.update({'is_plate_appearance': False})
            return outcome
    
    # Handle compound plays
    has_batter_action = any(pattern in desc for pattern in [
        'strikeout', 'struck out', 'single', 'double', 'triple', 'home run',
        'walk', 'grounded out', 'flied out', 'lined out', 'popped out',
        'hit by pitch', 'sacrifice'
    ])
    
    has_baserunning = any(pattern in desc for pattern in [
        'caught stealing', 'pickoff', 'picked off', 'wild pitch', 'passed ball'
    ])
    
    if has_batter_action and has_baserunning:
        desc = desc.split(',')[0].strip()
    
    # Sacrifice flies
    if re.search(r'sacrifice fly|sac fly|flyball.*sacrifice fly', desc):
        outcome.update({'is_sacrifice_fly': True, 'is_out': True, 'outs_recorded': 1})
        return outcome
    
    # Sacrifice hits
    if re.search(r'sacrifice bunt|sac bunt|bunt.*sacrifice', desc):
        outcome.update({'is_sacrifice_hit': True, 'is_out': True, 'outs_recorded': 1})
        return outcome
    
    # Walks
    if re.search(r'^walk\b|^intentional walk', desc):
        outcome.update({'is_walk': True})
        return outcome
    
    # Hit by pitch
    if re.search(r'^hit by pitch|^hbp\b', desc):
        return outcome

    # Strikeout with wild pitch/passed ball
    if re.search(r'strikeout.*wild pitch|strikeout.*passed ball|wild pitch.*strikeout|passed ball.*strikeout', desc):
        outcome.update({
            'is_at_bat': True,
            'is_strikeout': True,
            'is_out': False,
            'outs_recorded': 0})
        return outcome
        
    # Double play with strikeout
    elif re.search(r'double play.*strikeout|strikeout.*double play', desc):
        outcome.update({'is_at_bat': True, 'is_strikeout': True, 'is_out': True, 'outs_recorded': 2})
        return outcome
    
    # Pure baserunning
    elif re.search(r'caught stealing|pickoff|picked off|wild pitch|passed ball|balk', desc) and not has_batter_action:
        outcome.update({'is_plate_appearance': False})
        return outcome
    
    # At-bat outcomes
    outcome['is_at_bat'] = True

    # Reached on error
    if re.search(r'reached.*error|reached.*e\d+', desc):
        outcome.update({'is_out': False})
        return outcome

    # Reached on interference
    if re.search(r'reached.*interference', desc):
        outcome.update({'is_out': False, 'is_at_bat': False})
        return outcome
    
    # Strikeouts
    if re.search(r'^strikeout\b|^struck out|strikeout looking|strikeout swinging', desc):
        outcome.update({'is_strikeout': True, 'is_out': True})
        return outcome

    # Double plays
    if re.search(r'grounded into double play|gdp\b|double play', desc):
        outcome.update({'is_out': True, 'outs_recorded': 2})
        return outcome

    # Batter interference
    if re.search(r'interference by batter', desc):
        outcome.update({'is_out': True, 'outs_recorded': 1})
        return outcome
    
    # Other outs
    out_patterns = [
        r'grounded out\b', r'flied out\b', r'lined out\b', r'popped out\b',
        r'groundout\b', r'flyout\b', r'lineout\b', r'popout\b', r'popfly\b', r'flyball\b', r"fielder's choice\b"
    ]
    
    for pattern in out_patterns:
        if re.search(pattern, desc):
            outcome.update({'is_out': True, 'outs_recorded': 1})
            return outcome

    # Home runs
    if re.search(r'home run\b|^hr\b', desc):
        outcome.update({'is_hit': True, 'hit_type': 'home_run', 'bases_reached': 4})
        return outcome
    
    # Other hits
    hit_patterns = [
        (r'^single\b.*(?:to|up|through)', 'single', 1),
        (r'^double\b.*(?:to|down)|ground-rule double', 'double', 2),
        (r'^triple\b.*(?:to|down)', 'triple', 3)
    ]
    
    for pattern, hit_type, bases in hit_patterns:
        if re.search(pattern, desc):
            outcome.update({'is_hit': True, 'hit_type': hit_type, 'bases_reached': bases})
            return outcome
            
    return None
