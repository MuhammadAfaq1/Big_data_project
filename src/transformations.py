"""
Data cleaning and deduplication logic for NYC Taxi data
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_timestamp

def clean_and_deduplicate(df: DataFrame) -> DataFrame:
    """
    Clean and remove duplicates from NYC Taxi data
    
    Args:
        df: Input Spark DataFrame
        
    Returns:
        Cleaned and deduplicated DataFrame
    """
    # 1. Type Casting
    df = df.withColumn("tpep_pickup_datetime", to_timestamp(col("tpep_pickup_datetime"))) \
           .withColumn("tpep_dropoff_datetime", to_timestamp(col("tpep_dropoff_datetime"))) \
           .withColumn("passenger_count", col("passenger_count").cast("int")) \
           .withColumn("trip_distance", col("trip_distance").cast("double")) \
           .withColumn("PULocationID", col("PULocationID").cast("int")) \
           .withColumn("DOLocationID", col("DOLocationID").cast("int"))
    
    # 2. Data Cleaning Rules
    # - Remove null values in critical fields
    # - Ensure passenger count and trip distance are positive
    # - Ensure LocationIDs are within valid range (1-263)
    df_cleaned = df.filter(
        (col("tpep_pickup_datetime").isNotNull()) &
        (col("tpep_dropoff_datetime").isNotNull()) &
        (col("passenger_count") > 0) &
        (col("trip_distance") > 0) &
        (col("PULocationID") >= 1) & (col("PULocationID") <= 263) &
        (col("DOLocationID") >= 1) & (col("DOLocationID") <= 263)
    )
    
    # 3. Deduplication
    # Defined key: (VendorID, tpep_pickup_datetime, PULocationID, DOLocationID)
    df_deduped = df_cleaned.dropDuplicates(["VendorID", "tpep_pickup_datetime", "PULocationID", "DOLocationID"])
    
    return df_deduped
