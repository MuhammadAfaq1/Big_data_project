"""
Entry point for the incremental Spark ETL job for NYC Taxi data
"""
import os
from pathlib import Path
from utils import load_manifest, save_manifest, get_new_files, update_manifest

# Constants
INBOX_DIR = Path(__file__).parent.parent / "data" / "inbox"
OUTBOX_FILE = Path(__file__).parent.parent / "data" / "outbox" / "trips_enriched.parquet"
LOOKUP_FILE = Path(__file__).parent.parent / "data" / "taxi_zone_lookup.parquet"

def main():
    # 1. Initialize environment
    import sys
    import os
    python_path = sys.executable
    os.environ['PYSPARK_PYTHON'] = python_path
    os.environ['PYSPARK_DRIVER_PYTHON'] = python_path
    
    # Ensure system32 is in PATH for taskkill and other utilities
    system32 = r"C:\Windows\System32"
    if system32 not in os.environ.get('PATH', ''):
        os.environ['PATH'] = system32 + os.pathsep + os.environ.get('PATH', '')

    # Java 17+ compatibility flags - added javax.security.auth for the "getSubject" error
    java_opts = ("--add-opens java.base/java.util=ALL-UNNAMED "
                "--add-opens java.base/java.lang=ALL-UNNAMED "
                "--add-opens java.base/java.lang.invoke=ALL-UNNAMED "
                "--add-opens java.base/java.util.concurrent=ALL-UNNAMED "
                "--add-opens java.base/java.nio=ALL-UNNAMED "
                "--add-opens java.base/sun.nio.ch=ALL-UNNAMED "
                "--add-opens java.base/sun.util.calendar=ALL-UNNAMED "
                "--add-opens java.security.jgss/sun.security.krb5=ALL-UNNAMED "
                "--add-opens java.base/java.math=ALL-UNNAMED "
                "--add-opens java.base/sun.security.action=ALL-UNNAMED "
                "--add-opens java.base/sun.net.util=ALL-UNNAMED "
                "--add-opens java.base/javax.security.auth=ALL-UNNAMED")
    os.environ['JDK_JAVA_OPTIONS'] = java_opts
    
    if not os.environ.get('HADOOP_HOME'):
        os.environ['HADOOP_HOME'] = str(Path(__file__).parent.parent)

    # 2. Load manifest
    manifest = load_manifest()
    new_files = get_new_files(manifest, str(INBOX_DIR))
    
    if not new_files:
        print("No new files to process. Exiting.")
        return

    print(f"Found {len(new_files)} new files to process: {[f.name for f in new_files]}")

    # 3. Processing with Spark (and Pandas Fallback)
    spark = None
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import lit, current_timestamp
        from transformations import clean_and_deduplicate, filter_impossible_trips
        from enrichment import enrich_with_zones

        spark = SparkSession.builder \
            .appName("NYC-Taxi-Incremental-ETL") \
            .master("local[1]") \
            .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
            .config("spark.driver.extraJavaOptions", java_opts) \
            .config("spark.executor.extraJavaOptions", java_opts) \
            .getOrCreate()
        
        print(f"Spark Session initialized ({spark.version}).")
        zones_df = spark.read.parquet(str(LOOKUP_FILE))

        for file_path in new_files:
            print(f"Processing {file_path.name} (Spark)...")
            raw_df = spark.read.parquet(str(file_path))
            input_count = raw_df.count()
            
            cleaned_df = clean_and_deduplicate(raw_df)
            cleaned_df = filter_impossible_trips(cleaned_df)
            enriched_df = cleaned_df.withColumn("source_file", lit(file_path.name)) \
                                    .withColumn("ingested_at", current_timestamp())
            final_df = enrich_with_zones(enriched_df, zones_df)
            
            final_df.write.mode("append").parquet(str(OUTBOX_FILE))
            
            # Update state
            manifest = update_manifest(manifest, file_path.name, input_count)
            save_manifest(manifest)
            print(f" - {file_path.name} processed successfully.")

    except Exception as e:
        print(f" ! Spark execution failed: {e}")
        print(" ! Reverting to Pandas/PyArrow implementation for local Windows compatibility...")
        
        import pandas as pd
        import numpy as np
        # Reload manifest in case some files were already processed by Spark before the crash
        manifest = load_manifest()
        processed_set = set(f["filename"] for f in manifest["processed_files"])
        files_to_process = [f for f in new_files if f.name not in processed_set]
        
        if not files_to_process:
            print("All files were already processed by Spark before the crash.")
            return

        zones_pdf = pd.read_parquet(str(LOOKUP_FILE))

        for file_path in files_to_process:
            print(f"Processing {file_path.name} (Pandas Fallback)...")
            pdf = pd.read_parquet(str(file_path))
            input_count = len(pdf)
            
            # Mirror logic
            pdf = pdf.dropna(subset=['tpep_pickup_datetime', 'tpep_dropoff_datetime'])
            pdf = pdf[(pdf['passenger_count'] > 0) & (pdf['trip_distance'] > 0)]
            pdf = pdf[(pdf['PULocationID'] >= 1) & (pdf['PULocationID'] <= 263)]
            pdf = pdf[(pdf['DOLocationID'] >= 1) & (pdf['DOLocationID'] <= 263)]
            pdf = pdf.drop_duplicates(subset=["VendorID", "tpep_pickup_datetime", "PULocationID", "DOLocationID"])

            # Impossible trip filtering (Pandas mirror of Spark logic)
            pdf['tpep_pickup_datetime'] = pd.to_datetime(pdf['tpep_pickup_datetime'])
            pdf['tpep_dropoff_datetime'] = pd.to_datetime(pdf['tpep_dropoff_datetime'])
            duration_hours = (pdf['tpep_dropoff_datetime'] - pdf['tpep_pickup_datetime']).dt.total_seconds() / 3600.0
            pdf['__trip_duration_hours'] = duration_hours

            valid_duration_mask = pdf['__trip_duration_hours'] > 0
            # Avoid division-by-zero / NaN issues by using where with np.inf
            speed_mph = pdf['trip_distance'] / pdf['__trip_duration_hours'].where(valid_duration_mask, np.nan)

            mask_total_amount = pdf['total_amount'] >= 0
            mask_speed = (~valid_duration_mask) | (speed_mph <= 100)
            pdf = pdf[mask_total_amount & mask_speed].drop(columns=['__trip_duration_hours'])
            
            pdf = pdf.merge(zones_pdf[['LocationID', 'Zone']].rename(columns={'LocationID': 'PULocationID', 'Zone': 'pickup_zone'}), on='PULocationID', how='left')
            pdf = pdf.merge(zones_pdf[['LocationID', 'Zone']].rename(columns={'LocationID': 'DOLocationID', 'Zone': 'dropoff_zone'}), on='DOLocationID', how='left')
            
            pdf['tpep_pickup_datetime'] = pd.to_datetime(pdf['tpep_pickup_datetime'])
            pdf['tpep_dropoff_datetime'] = pd.to_datetime(pdf['tpep_dropoff_datetime'])
            pdf['trip_duration_minutes'] = ((pdf['tpep_dropoff_datetime'] - pdf['tpep_pickup_datetime']).dt.total_seconds() / 60.0).round(2)
            pdf['pickup_date'] = pdf['tpep_pickup_datetime'].dt.date
            pdf['source_file'] = file_path.name
            pdf['ingested_at'] = pd.Timestamp.now()
            
            OUTBOX_FILE.parent.mkdir(parents=True, exist_ok=True)
            if OUTBOX_FILE.exists():
                existing = pd.read_parquet(str(OUTBOX_FILE))
                pdf = pd.concat([existing, pdf], ignore_index=True)
            
            pdf.to_parquet(str(OUTBOX_FILE), index=False)
            
            # Update state
            manifest = update_manifest(manifest, file_path.name, input_count)
            save_manifest(manifest)
            print(f" - {file_path.name} processed successfully via fallback.")

    print("ETL Pipeline complete.")

if __name__ == "__main__":
    main()
