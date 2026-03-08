"""
performance monitoring for the nyc taxi etl job
tracks how long each stage takes and logs it so we can compare
initial vs incremental runs later
"""

import json
import time
import os
from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


def get_spark():
    # shuffle partitions set to 8 because default 200 is overkill locally
    # also turned on AQE so spark can handle the join better on its own
    spark = (
        SparkSession.builder
        .appName("NYC_Taxi_ETL")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "logs/spark-events")
        .config("spark.ui.port", "4040")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


class StageTimer:
    def __init__(self, label):
        self.label = label
        self.start = 0.0
        self.end = 0.0

    @property
    def elapsed_seconds(self):
        return round(self.end - self.start, 3)

    def __enter__(self):
        self.start = time.perf_counter()
        return self

    def __exit__(self, *_):
        self.end = time.perf_counter()


def log_physical_plan(df, label):
    # useful for checking if spark is doing a broadcast join or a shuffle
    # want to see BroadcastHashJoin for the zones lookup, not SortMergeJoin
    print(f"\n===== PHYSICAL PLAN: {label} =====")
    df.explain()
    print("=" * 40)


class PerformanceMonitor:

    LOG_PATH = "logs/performance_log.json"

    def __init__(self, run_type="initial"):
        self.run_type = run_type
        self.run_id = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        self.stages = []
        self._run_start = time.perf_counter()

    def record(self, label, elapsed, row_count=None, notes=""):
        entry = {
            "stage": label,
            "elapsed_seconds": elapsed,
            "row_count": row_count,
            "notes": notes,
        }
        self.stages.append(entry)
        rc_str = f"  rows={row_count:,}" if row_count is not None else ""
        print(f"[PERF] {label:<35} {elapsed:>7.3f}s{rc_str}")

    def total_elapsed(self):
        return round(time.perf_counter() - self._run_start, 3)

    def save(self):
        os.makedirs(os.path.dirname(self.LOG_PATH), exist_ok=True)

        existing = []
        if os.path.exists(self.LOG_PATH):
            with open(self.LOG_PATH, "r") as f:
                existing = json.load(f)

        existing.append({
            "run_id": self.run_id,
            "run_type": self.run_type,
            "total_elapsed_seconds": self.total_elapsed(),
            "stages": self.stages,
        })

        with open(self.LOG_PATH, "w") as f:
            json.dump(existing, f, indent=2)

        print(f"[PERF] saved to {self.LOG_PATH}")

    def print_summary(self):
        print(f"\n--- Summary ({self.run_type}) ---")
        for s in self.stages:
            rc = f"{s['row_count']:,}" if s["row_count"] is not None else "—"
            print(f"  {s['stage']:<30} {s['elapsed_seconds']:>7.3f}s   rows: {rc}")
        print(f"  {'TOTAL':<30} {self.total_elapsed():>7.3f}s")
        print()

    def compare_runs(self):
        if not os.path.exists(self.LOG_PATH):
            print("no log file yet")
            return

        with open(self.LOG_PATH) as f:
            all_runs = json.load(f)

        def last_of(rtype):
            hits = [r for r in all_runs if r["run_type"] == rtype]
            return hits[-1] if hits else None

        initial = last_of("initial")
        incremental = last_of("incremental")

        print("\n--- initial vs incremental ---")
        print(f"  {'stage':<30} {'initial':>10}  {'incremental':>12}")

        si = {s["stage"]: s for s in (initial["stages"] if initial else [])}
        sinc = {s["stage"]: s for s in (incremental["stages"] if incremental else [])}
        names = list(si.keys()) + [k for k in sinc if k not in si]

        for name in names:
            ti   = f"{si[name]['elapsed_seconds']:.3f}s"   if name in si   else "—"
            tinc = f"{sinc[name]['elapsed_seconds']:.3f}s" if name in sinc else "—"
            print(f"  {name:<30} {ti:>10}  {tinc:>12}")

        ti_total   = f"{initial['total_elapsed_seconds']:.3f}s"     if initial     else "—"
        tinc_total = f"{incremental['total_elapsed_seconds']:.3f}s" if incremental else "—"
        print(f"  {'TOTAL':<30} {ti_total:>10}  {tinc_total:>12}")


def timed_read_parquet(spark, path, monitor):
    with StageTimer("read_parquet") as t:
        df = spark.read.parquet(path)
        count = df.count()
    monitor.record("read_parquet", t.elapsed_seconds, count)
    log_physical_plan(df, "read_parquet")
    return df


def timed_clean(df, monitor):
    # dropping rows that don't make sense
    # R1: passenger count should exist and be > 0
    # R2: trip distance shouldn't be negative
    # R3: pickup time can't be after dropoff
    # R4: negative total_amount is invalid
    # dedup key: pickup+dropoff times, locations, and amount
    with StageTimer("clean_and_dedup") as t:
        cleaned = (
            df
            .filter(F.col("passenger_count").isNotNull() & (F.col("passenger_count") > 0))
            .filter(F.col("trip_distance").isNotNull() & (F.col("trip_distance") >= 0))
            .filter(F.col("tpep_pickup_datetime") <= F.col("tpep_dropoff_datetime"))
            .filter(F.col("total_amount") >= 0)
            .dropDuplicates([
                "tpep_pickup_datetime",
                "tpep_dropoff_datetime",
                "PULocationID",
                "DOLocationID",
                "total_amount",
            ])
        )
        count = cleaned.count()
    monitor.record("clean_and_dedup", t.elapsed_seconds, count)
    log_physical_plan(cleaned, "after_clean")
    return cleaned


def timed_enrich(trips, zones, monitor):
    # zones table is only 265 rows so broadcasting it avoids a shuffle
    # confirmed BroadcastHashJoin in physical plan output
    pu = zones.select(
        F.col("LocationID").alias("PULocationID"),
        F.col("Zone").alias("pickup_zone"),
        F.col("Borough").alias("pickup_borough"),
    )
    do = zones.select(
        F.col("LocationID").alias("DOLocationID"),
        F.col("Zone").alias("dropoff_zone"),
        F.col("Borough").alias("dropoff_borough"),
    )

    with StageTimer("enrich_join") as t:
        enriched = (
            trips
            .join(F.broadcast(pu), on="PULocationID", how="left")
            .join(F.broadcast(do), on="DOLocationID", how="left")
        )
        count = enriched.count()
    monitor.record("enrich_join", t.elapsed_seconds, count)
    log_physical_plan(enriched, "after_enrich")
    return enriched


def timed_derive_fields(df, source_file, monitor):
    ingested_at = datetime.utcnow().isoformat()
    with StageTimer("derive_fields") as t:
        result = (
            df
            .withColumn(
                "trip_duration_minutes",
                (F.unix_timestamp("tpep_dropoff_datetime") -
                 F.unix_timestamp("tpep_pickup_datetime")) / 60.0
            )
            .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
            .withColumn("source_file", F.lit(source_file))
            .withColumn("ingested_at", F.lit(ingested_at))
        )
        count = result.count()
    monitor.record("derive_fields", t.elapsed_seconds, count)
    return result


def timed_write(df, output_path, monitor):
    # partitioning by pickup_date so incremental runs only touch new partitions
    # dynamic overwrite so we don't wipe old data on rerun
    with StageTimer("write_parquet") as t:
        (
            df.write
            .mode("overwrite")
            .option("partitionOverwriteMode", "dynamic")
            .partitionBy("pickup_date")
            .parquet(output_path)
        )
    monitor.record("write_parquet", t.elapsed_seconds)


if __name__ == "__main__":
    import sys

    run_type = sys.argv[1] if len(sys.argv) > 1 else "initial"

    spark = get_spark()
    monitor = PerformanceMonitor(run_type=run_type)

    INBOX  = "data/inbox/"
    ZONES  = "data/inbox/taxi_zone_lookup.parquet"
    OUTBOX = "data/outbox/trips_enriched.parquet"

    trips_raw = timed_read_parquet(spark, INBOX, monitor)
    zones_df  = spark.read.parquet(ZONES)

    trips_clean    = timed_clean(trips_raw, monitor)
    trips_enriched = timed_enrich(trips_clean, zones_df, monitor)
    trips_final    = timed_derive_fields(trips_enriched, INBOX, monitor)

    timed_write(trips_final, OUTBOX, monitor)

    monitor.print_summary()
    monitor.save()
    monitor.compare_runs()

    print("Spark UI -> http://localhost:4040")