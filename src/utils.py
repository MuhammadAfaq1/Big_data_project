"""
Utility functions for manifest handling and file operations
"""
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List

MANIFEST_PATH = Path(__file__).parent.parent / "state" / "manifest.json"
VALIDATION_LOG_PATH = Path(__file__).parent.parent / "state" / "validation_log.json"

def log_validation_error(filename: str, reason: str) -> None:
    """Logs a validation error to state/validation_log.json"""
    error_entry = {
        "filename": filename,
        "reason": reason,
        "timestamp": datetime.now().isoformat()
    }
    
    logs = []
    if VALIDATION_LOG_PATH.exists():
        try:
            with open(VALIDATION_LOG_PATH, 'r') as f:
                logs = json.load(f)
        except:
            logs = []
            
    logs.append(error_entry)
    VALIDATION_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(VALIDATION_LOG_PATH, 'w') as f:
        json.dump(logs, f, indent=2)

def validate_file_schema(df, is_spark: bool = True):
    """
    Validates that required columns are present and have expected types.
    Returns (is_valid, reason)
    """
    required_cols = ["tpep_pickup_datetime", "fare_amount", "trip_distance", "PULocationID", "DOLocationID"]

    if is_spark:
        actual_cols = {field.name: field.dataType.simpleString() for field in df.schema.fields}
        for col in required_cols:
            if col not in actual_cols:
                return False, f"Missing required column: {col}"
            
            actual_type = actual_cols[col].lower()
            if col == "tpep_pickup_datetime":
                if "timestamp" not in actual_type:
                    return False, f"Column {col} has invalid type: {actual_type}"
            elif col in ["fare_amount", "trip_distance"]:
                 if not any(t in actual_type for t in ["double", "float", "decimal"]):
                    return False, f"Column {col} has invalid type: {actual_type}"
            elif col in ["PULocationID", "DOLocationID"]:
                if not any(t in actual_type for t in ["long", "int", "integer", "short"]):
                    return False, f"Column {col} has invalid type: {actual_type}"
    else:
        # Pandas validation
        for col in required_cols:
            if col not in df.columns:
                return False, f"Missing required column: {col}"
            
            actual_type = str(df[col].dtype).lower()
            if col == "tpep_pickup_datetime":
                if "datetime" not in actual_type:
                    return False, f"Column {col} has invalid type: {actual_type}"
            elif col in ["fare_amount", "trip_distance"]:
                if "float" not in actual_type and "double" not in actual_type:
                    return False, f"Column {col} has invalid type: {actual_type}"
            elif col in ["PULocationID", "DOLocationID"]:
                if "int" not in actual_type:
                    return False, f"Column {col} has invalid type: {actual_type}"

    return True, None

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
