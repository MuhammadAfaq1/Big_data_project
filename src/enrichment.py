"""
Enrichment logic - joins taxi data with zone lookup tables
"""
from pyspark.sql import DataFrame

def enrich_with_zones(taxi_df: DataFrame, zones_df: DataFrame) -> DataFrame:
    """
    Join taxi data with zone lookup information
    
    Args:
        taxi_df: Cleaned taxi data
        zones_df: Zone lookup reference data
        
    Returns:
        Enriched DataFrame with zone information
    """
    # TODO: Implement enrichment logic
    # - Join with zone lookup by location ID
    # - Add zone names and metadata
    # - Handle null values and unmatched records
    
    return taxi_df
