"""
Entry point for the incremental Spark ETL job for NYC Taxi data
"""
from pyspark.sql import SparkSession
from transformations import clean_and_deduplicate
from enrichment import enrich_with_zones
from utils import load_manifest, save_manifest

def main():
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("NYC-Taxi-Incremental-ETL") \
        .getOrCreate()
    
    # Load manifest to track processed files
    manifest = load_manifest()
    
    # TODO: Implement incremental ETL pipeline
    # 1. Read new parquet files from data/inbox/
    # 2. Apply transformations (cleaning, deduplication)
    # 3. Enrich with zone lookup
    # 4. Write to data/outbox/
    # 5. Update manifest
    
    print("ETL Pipeline initialized")

if __name__ == "__main__":
    main()
