"""
Data cleaning and deduplication logic for NYC Taxi data
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_timestamp, unix_timestamp


def clean_and_deduplicate(df: DataFrame) -> DataFrame:
    """
    Clean and remove duplicates from NYC Taxi data
    
    Args:
        df: Input Spark DataFrame
        
    Returns:
        Cleaned and deduplicated DataFrame
    """
    # 1. Type Casting
    df = (
        df.withColumn(
            "tpep_pickup_datetime", to_timestamp(col("tpep_pickup_datetime"))
        )
        .withColumn(
            "tpep_dropoff_datetime", to_timestamp(col("tpep_dropoff_datetime"))
        )
        .withColumn("passenger_count", col("passenger_count").cast("int"))
        .withColumn("trip_distance", col("trip_distance").cast("double"))
        .withColumn("PULocationID", col("PULocationID").cast("int"))
        .withColumn("DOLocationID", col("DOLocationID").cast("int"))
    )
    
    # 2. Data Cleaning Rules
    # - Remove null values in critical fields
    # - Ensure passenger count and trip distance are positive
    # - Ensure LocationIDs are within valid range (1-263)
    df_cleaned = df.filter(
        (col("tpep_pickup_datetime").isNotNull())
        & (col("tpep_dropoff_datetime").isNotNull())
        & (col("passenger_count") > 0)
        & (col("trip_distance") > 0)
        & (col("PULocationID") >= 1)
        & (col("PULocationID") <= 263)
        & (col("DOLocationID") >= 1)
        & (col("DOLocationID") <= 263)
    )
    
    # 3. Deduplication
    # Defined key: (VendorID, tpep_pickup_datetime, PULocationID, DOLocationID)
    df_deduped = df_cleaned.dropDuplicates(
        ["VendorID", "tpep_pickup_datetime", "PULocationID", "DOLocationID"]
    )
    
    return df_deduped


def filter_impossible_trips(df: DataFrame) -> DataFrame:
    """
    Filter out physically impossible or clearly invalid trips.
    
    Rules:
    - Negative total_amount
    - Implied average speed greater than 100 mph
    
    The speed rule uses pickup/dropoff timestamps and trip_distance.
    """
    # Temporary helper column for trip duration in hours
    df_with_duration = df.withColumn(
        "_trip_duration_hours",
        (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 3600.0,
    )

    # Protect against zero or negative duration before computing speed
    valid_duration = col("_trip_duration_hours") > 0
    valid_speed = (col("trip_distance") / col("_trip_duration_hours")) <= 100

    df_filtered = df_with_duration.filter(
        (col("total_amount") >= 0) & (~valid_duration | valid_speed)
    ).drop("_trip_duration_hours")

    return df_filtered
