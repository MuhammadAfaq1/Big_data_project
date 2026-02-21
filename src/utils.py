"""
Utility functions for manifest handling and file operations
"""
import json
from pathlib import Path
from typing import Dict, List

MANIFEST_PATH = "../state/manifest.json"

def load_manifest() -> Dict:
    """
    Load the manifest file tracking processed files
    
    Returns:
        Dictionary containing manifest data
    """
    try:
        with open(MANIFEST_PATH, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return {
            "last_processed": "",
            "processed_files": [],
            "last_update": None
        }

def save_manifest(manifest: Dict) -> None:
    """
    Save the manifest file with updated processed files info
    
    Args:
        manifest: Dictionary to save as manifest
    """
    with open(MANIFEST_PATH, 'w') as f:
        json.dump(manifest, f, indent=2)

def get_new_files(manifest: Dict, inbox_path: str) -> List[str]:
    """
    Get list of new files not yet processed
    
    Args:
        manifest: Current manifest dictionary
        inbox_path: Path to inbox directory
        
    Returns:
        List of new file paths
    """
    # TODO: Implement logic to find new files
    return []
