"""
Enrichment logic - joins taxi data with zone lookup tables
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, unix_timestamp, round, to_date, lit, current_timestamp

def enrich_with_zones(taxi_df: DataFrame, zones_df: DataFrame) -> DataFrame:
    """
    Join taxi data with zone lookup information and add derived fields
    
    Args:
        taxi_df: Cleaned taxi data
        zones_df: Zone lookup reference data
        
    Returns:
        Enriched DataFrame with zone information and derived fields
    """
    # 1. Join for Pickup Zone
    enriched_df = taxi_df.join(
        zones_df.select(col("LocationID").alias("PULocationID"), col("Zone").alias("pickup_zone")),
        on="PULocationID",
        how="left"
    )
    
    # 2. Join for Dropoff Zone
    enriched_df = enriched_df.join(
        zones_df.select(col("LocationID").alias("DOLocationID"), col("Zone").alias("dropoff_zone")),
        on="DOLocationID",
        how="left"
    )
    
    # 3. Derived Fields
    # - trip_duration_minutes: (dropoff - pickup) / 60
    # - pickup_date: date part of pickup timestamp
    enriched_df = enriched_df.withColumn(
        "trip_duration_minutes",
        round((unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 60, 2)
    ).withColumn(
        "pickup_date",
        to_date("tpep_pickup_datetime")
    )
    
    # 4. Metadata
    # source_file and ingested_at will be added in the main loop to handle per-file metadata correctly
    # but we initialize ingested_at here if not already present
    if "ingested_at" not in enriched_df.columns:
        enriched_df = enriched_df.withColumn("ingested_at", current_timestamp())
    
    # Select and reorder fields as per requirements
    required_fields = [
        "tpep_pickup_datetime", "tpep_dropoff_datetime",
        "PULocationID", "DOLocationID",
        "pickup_zone", "dropoff_zone",
        "passenger_count", "trip_distance",
        "trip_duration_minutes", "pickup_date",
        "source_file", "ingested_at"
    ]
    
    # Filter only available fields to avoid errors if some are missing
    available_fields = [c for c in required_fields if c in enriched_df.columns]
    
    return enriched_df.select(*available_fields)
