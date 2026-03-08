## Correctness Report – Impossible Trips Scenario

This report documents how the pipeline handles clearly invalid or “impossible” NYC taxi trips, in addition to the baseline cleaning and deduplication rules.

### 1. Baseline Cleaning & Deduplication

- Drop rows with null `tpep_pickup_datetime` or `tpep_dropoff_datetime`
- Enforce:
  - `passenger_count > 0`
  - `trip_distance > 0`
  - `PULocationID` and `DOLocationID` in \[1, 263]
- Deduplicate using key: `(VendorID, tpep_pickup_datetime, PULocationID, DOLocationID)`

### 2. Custom Scenario: Impossible Trips

We add an extra transformation that targets *physically impossible* trips:

- **Rule 1 – Negative total amount**  
  Filter out any row where `total_amount < 0`.

- **Rule 2 – Unrealistic speed (> 100 mph)**  
  Compute trip duration in hours using pickup and dropoff timestamps:

  \[
  \text{duration\_hours} =
  \frac{\text{dropoff\_ts} - \text{pickup\_ts}}{3600}
  \]

  Then compute implied average speed:

  \[
  \text{speed\_mph} = \frac{\text{trip\_distance}}{\text{duration\_hours}}
  \]

  - If `duration_hours <= 0`, the row is treated as invalid duration and kept out of the speed rule (handled by the baseline timestamp checks).
  - If `speed_mph > 100`, the row is filtered as an “impossible trip”.

Implementation locations:
- Spark: `filter_impossible_trips` in `src/transformations.py`
- Pandas fallback: mirrored logic in `src/main.py`

### 3. Sample “Bad Rows” and Outcomes

| Example ID | VendorID | pickup time           | dropoff time          | passenger_count | trip_distance | total_amount | Notes                                           | Kept? |
|-----------:|---------:|-----------------------|-----------------------|----------------:|--------------:|-------------:|------------------------------------------------|:-----:|
| 1          | 1        | 2024-01-01 08:00:00   | 2024-01-01 08:10:00   | 0               | 1.2           | 8.50         | Zero passengers                                |  No   |
| 2          | 2        | 2024-01-01 09:00:00   | 2024-01-01 09:20:00   | 1               | 3.5           | 12.75        | `PULocationID = 300` (out of range)           |  No   |
| 3          | 1        | 2024-01-01 10:00:00   | 2024-01-01 10:15:00   | 1               | 2.1           | -5.00        | Negative total_amount                          |  No   |
| 4          | 1        | 2024-01-01 11:00:00   | 2024-01-01 11:30:00   | 1               | 120.0         | 250.00       | \~30 minutes for 120 miles \(\approx 240 mph\) |  No   |
| 5          | 2        | 2024-01-01 12:00:00   | 2024-01-01 12:30:00   | 1               | 5.0           | 18.00        | Normal trip                                    | Yes   |

This table can be used as evidence of correctness: any run of the pipeline that includes rows like 1–4 in the raw input must *not* contain them in the final `trips_enriched.parquet` output.

