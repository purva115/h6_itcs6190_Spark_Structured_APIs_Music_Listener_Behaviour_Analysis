# Music Listener Behavior Analysis with Spark Structured API

## Overview

Analyze user listening habits and music trends from a fictional streaming platform using PySpark. This project provides insights into genre preferences, song popularity, and listener engagement through scalable data processing with Spark Structured APIs.

## Dataset Description

Two CSV files are used:

* **listening\_logs.csv** — Contains user listening activity with columns: `user_id`, `song_id`, `timestamp`, `duration_sec`.
* **songs\_metadata.csv** — Contains song details with columns: `song_id`, `title`, `artist`, `genre`, `mood`.

> Tip: For realistic testing, include at least 1,000 listening log records and 50+ unique songs in `songs_metadata.csv`.

## Repository Structure

```
├── datagen.py                # Generates sample datasets
├── main.py                   # Main analysis script
├── requirements.txt          # Python dependencies
├── README.md                 # Project documentation
├── listening_logs.csv        # User listening activity
├── songs_metadata.csv        # Song details and metadata
└── outputs/
    ├── user_favorite_genres/
    ├── avg_listen_time_per_song/
    ├── genre_loyalty_scores/
    └── night_owl_users/
```

## Output Directory Structure

```
outputs/
├── user_favorite_genres/
├── avg_listen_time_per_song/
├── genre_loyalty_scores/
└── night_owl_users/
```

## Tasks and Outputs

1. **User's Favourite Genre**: Identifies each user's most listened-to genre. Output: `outputs/user_favorite_genres/` (CSV per partition).
2. **Average Listen Time Per Song**: Calculates average play duration for each song. Output: `outputs/avg_listen_time_per_song/`.
3. **Genre Loyalty Score**: Computes the fraction of each user's plays that are from their favorite genre and flags users above a loyalty threshold. Output: `outputs/genre_loyalty_scores/`.
4. **Night Owl Users**: Detects users active between 12 AM and 5 AM (local time). Output: `outputs/night_owl_users/`.

## Execution Instructions

1. Install Python and PySpark (prefer a virtual environment):

   ```bash
   pip install pyspark
   ```
2. Place `listening_logs.csv` and `songs_metadata.csv` in the project directory.
3. Run the analysis:

   ```bash
   python main.py
   ```
4. Results will be saved as CSV files in the `outputs/` folder.

## Analysis Workflow

1. **Load Data**: Read both CSV files into Spark DataFrames using `spark.read.csv(..., header=True, inferSchema=True)`.
2. **Prepare Data**: Parse timestamps to Spark `TimestampType`, extract hour of day, and join listening logs with song metadata on `song_id`.
3. **Run Analysis**:

   * Determine each user's favorite genre by counting plays per genre and selecting the highest.
   * Calculate average listen time per song using `groupBy(song_id)` and `avg(duration_sec)`.
   * Compute genre loyalty scores: for each user, `loyalty = plays_in_favorite_genre / total_plays`. Filter by a configurable threshold (e.g., `0.5`).
   * Identify night owl users: filter play records where the hour is between 0 and 4 (inclusive) and list users with >= 1 play in that window (or use a minimum-play threshold).
4. **Save Results**: Write each DataFrame to CSV with `df.coalesce(1).write.mode("overwrite").csv(output_path, header=True)` (coalesce optional depending on dataset size).

## Errors and Resolutions

**Loyalty Score Output Issue:**
If the genre loyalty score output is empty when using a strict threshold (e.g., `0.8`), it likely means the test dataset does not contain users with such concentrated listening patterns. Lowering the threshold to something like `0.5` or adjusting the minimum-play filter often yields meaningful results. Always inspect intermediate counts (e.g., per-user total plays) to set an appropriate threshold.

## Implementation Notes

* Use Spark functions (`from pyspark.sql.functions import col, hour, avg, row_number, desc`) and window functions to compute rankings per user.
* Consider handling duplicate or very short listen events (e.g., `duration_sec < 5`) by filtering them out as noise.
* If timestamps are in UTC and you want local-time analysis, convert timezones before extracting the hour.

## Example `main.py` (outline)

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, avg, count, row_number
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("MusicAnalysis").getOrCreate()

# Load
logs = spark.read.csv("listening_logs.csv", header=True, inferSchema=True)
songs = spark.read.csv("songs_metadata.csv", header=True, inferSchema=True)

# Prepare
logs = logs.withColumn("ts", col("timestamp").cast("timestamp")).withColumn("hour", hour(col("ts")))
joined = logs.join(songs, on="song_id", how="left")

# User favorite genre
genre_counts = joined.groupBy("user_id", "genre").count()
window = Window.partitionBy("user_id").orderBy(desc("count"))
user_fav = genre_counts.withColumn("rn", row_number().over(window)).filter(col("rn") == 1).select("user_id", "genre")
user_fav.write.mode("overwrite").csv("outputs/user_favorite_genres", header=True)

# Average listen time per song
avg_time = joined.groupBy("song_id", "title", "artist").agg(avg("duration_sec").alias("avg_duration_sec"))
avg_time.write.mode("overwrite").csv("outputs/avg_listen_time_per_song", header=True)

# Genre loyalty score (example threshold 0.5)
user_total = joined.groupBy("user_id").count().withColumnRenamed("count", "total_plays")
fav_with_counts = genre_counts.join(user_fav, ["user_id", "genre"]).select("user_id", "count")
fav_with_counts = fav_with_counts.withColumnRenamed("count", "fav_plays")
loyalty = fav_with_counts.join(user_total, "user_id").withColumn("loyalty", col("fav_plays") / col("total_plays"))
loyalty.filter(col("loyalty") >= 0.5).write.mode("overwrite").csv("outputs/genre_loyalty_scores", header=True)

# Night owl users
night_plays = joined.filter(col("hour").between(0, 4))
night_users = night_plays.groupBy("user_id").count().filter(col("count") >= 1).select("user_id")
night_users.write.mode("overwrite").csv("outputs/night_owl_users", header=True)

spark.stop()
```

## Notes

* The example code is an outline; adapt it to your schema, data quality constraints, and cluster resources.
* For large datasets, avoid `.coalesce(1)` to prevent driver overload; write partitioned outputs instead.

---

*Generated README for the Music Listener Behavior Analysis project.*
