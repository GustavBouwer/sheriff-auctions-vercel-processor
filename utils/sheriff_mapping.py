"""
Sheriff Mapping Utility
Handles sheriff office to UUID mapping using JSON file lookup
"""

import json
import os
from pathlib import Path

def load_sheriff_mapping():
    """Load sheriff mapping from JSON file"""
    try:
        # Get the directory of the current file
        current_dir = Path(__file__).parent.parent  # Go up to project root
        json_file_path = current_dir / 'data' / 'sheriff-mapping.json'
        
        if not json_file_path.exists():
            print(f"Warning: Sheriff mapping file not found at {json_file_path}")
            return {}
        
        with open(json_file_path, 'r', encoding='utf-8') as f:
            sheriff_data = json.load(f)
        
        # Convert list to dict for faster lookups
        mapping = {}
        for sheriff in sheriff_data:
            mapping[sheriff['sheriff_office'].lower()] = sheriff['id']
        
        return mapping
        
    except Exception as e:
        print(f"Error loading sheriff mapping: {e}")
        return {}

def get_sheriff_uuid(sheriff_office):
    """Get sheriff UUID from mapping with fuzzy matching"""
    if not sheriff_office:
        return os.getenv('DEFAULT_SHERIFF_UUID', 'f7c42d1a-2cb8-4d87-a84e-c5a0ec51d130')
    
    # Load mapping
    sheriff_mapping = load_sheriff_mapping()
    
    if not sheriff_mapping:
        print("Warning: No sheriff mapping available, using default UUID")
        return os.getenv('DEFAULT_SHERIFF_UUID', 'f7c42d1a-2cb8-4d87-a84e-c5a0ec51d130')
    
    sheriff_office_clean = sheriff_office.lower().strip()
    
    # Try exact match first
    if sheriff_office_clean in sheriff_mapping:
        return sheriff_mapping[sheriff_office_clean]
    
    # Try partial matching - find best match
    best_match = None
    best_score = 0
    
    for mapped_office, uuid in sheriff_mapping.items():
        # Simple scoring based on substring matches
        score = 0
        
        # Check if any words from the input appear in the mapping
        input_words = sheriff_office_clean.split()
        mapped_words = mapped_office.split()
        
        for input_word in input_words:
            if len(input_word) > 2:  # Only consider words longer than 2 chars
                for mapped_word in mapped_words:
                    if input_word in mapped_word or mapped_word in input_word:
                        score += len(input_word)
        
        # Also check reverse - if mapped words appear in input
        for mapped_word in mapped_words:
            if len(mapped_word) > 2:
                if mapped_word in sheriff_office_clean:
                    score += len(mapped_word)
        
        if score > best_score:
            best_score = score
            best_match = uuid
    
    if best_match and best_score > 3:  # Minimum score threshold
        return best_match
    
    # No match found, return default
    return os.getenv('DEFAULT_SHERIFF_UUID', 'f7c42d1a-2cb8-4d87-a84e-c5a0ec51d130')

def is_sheriff_associated(sheriff_uuid):
    """Check if sheriff UUID is not the default (i.e., was successfully mapped)"""
    default_uuid = os.getenv('DEFAULT_SHERIFF_UUID', 'f7c42d1a-2cb8-4d87-a84e-c5a0ec51d130')
    return sheriff_uuid != default_uuid