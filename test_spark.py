print("Starting script...")

from pyspark.sql import SparkSession
import time

print("Import successful")

spark = SparkSession.builder \
    .appName("Test") \
    .getOrCreate()

print("Spark created")
print("Spark version:", spark.version)

print("Spark UI should be available now...")
print("Keeping Spark alive for 120 seconds...")

time.sleep(120)
