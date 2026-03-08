"""
Utility functions for manifest handling and file operations
"""
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List

MANIFEST_PATH = Path(__file__).parent.parent / "state" / "manifest.json"

def load_manifest() -> Dict:
    """
    Load the manifest file tracking processed files
    
    Returns:
        Dictionary containing manifest data
    """
    try:
        if MANIFEST_PATH.exists():
            with open(MANIFEST_PATH, 'r') as f:
                return json.load(f)
    except Exception as e:
        print(f"Error loading manifest: {e}")
    
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
    manifest["last_update"] = datetime.now().isoformat()
    # Ensure directory exists
    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(MANIFEST_PATH, 'w') as f:
        json.dump(manifest, f, indent=2)

def get_new_files(manifest: Dict, inbox_path: str) -> List[Path]:
    """
    Get list of new files not yet processed
    
    Args:
        manifest: Current manifest dictionary
        inbox_path: Path to inbox directory
        
    Returns:
        List of Path objects for new files
    """
    inbox = Path(inbox_path)
    if not inbox.exists():
        return []
    
    processed_files = set(f.get("filename") for f in manifest.get("processed_files", []))
    
    new_files = []
    for f in inbox.glob("*.parquet"):
        if f.name not in processed_files:
            new_files.append(f)
            
    return sorted(new_files)

def update_manifest(manifest: Dict, filename: str, row_count: int) -> Dict:
    """
    Update manifest with details of a processed file
    
    Args:
        manifest: Current manifest dictionary
        filename: Name of the file processed
        row_count: Number of rows in the file
        
    Returns:
        Updated manifest dictionary
    """
    manifest["processed_files"].append({
        "filename": filename,
        "row_count": row_count,
        "processed_at": datetime.now().isoformat()
    })
    manifest["last_processed"] = filename
    return manifest
