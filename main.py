import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import os

# Hadoop home for Spark
os.environ['HADOOP_HOME'] = r"C:\hadoop"
# Add bin to PATH so Spark can find winutils.exe
os.environ['PATH'] += os.pathsep + os.path.join(os.environ['HADOOP_HOME'], 'bin')

spark = SparkSession.builder.appName("MusicAnalysis").getOrCreate()

# Generate unique output folder using timestamp
timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
base_output_path = f"output_{timestamp}"

# Load datasets
listening_logs = spark.read.csv("listening_logs.csv", header=True, inferSchema=True)
songs_metadata = spark.read.csv("songs_metadata.csv", header=True, inferSchema=True)

# Task 1: User Favorite Genres
logs_with_genre = listening_logs.join(songs_metadata, on="song_id", how="left")
user_genre_counts = logs_with_genre.groupBy("user_id", "genre").agg(count("*").alias("plays"))
window_user = Window.partitionBy("user_id").orderBy(col("plays").desc())
favourite_genre = user_genre_counts.withColumn("rank", row_number().over(window_user)) \
                                   .filter(col("rank") == 1) \
                                   .select("user_id", "genre", "plays")
favourite_genre.show(truncate=False)
# Save Task 1
favourite_genre.write.csv(f"{base_output_path}/user_favorite_genres")

# Task 2: Average Listen Time
avg_listen_time = listening_logs.groupBy("song_id") \
                               .agg(avg("duration_sec").alias("avg_duration_sec")) \
                               .join(songs_metadata.select("song_id", "title", "artist"), on="song_id", how="left")
avg_listen_time.show(truncate=False)
# Save Task 2
avg_listen_time.write.csv(f"{base_output_path}/avg_listen_time_per_song")

# Task 3: Genre Loyalty Scores

# Check if listening_logs has data
print("Listening logs count:", listening_logs.count())
listening_logs.show(5)

# Check if songs_metadata has data
print("Songs metadata count:", songs_metadata.count())
songs_metadata.show(5)

# Check distinct genres
print("===============")
songs_metadata.select("genre").distinct().show(10)

# Check if joins return anything
joined = listening_logs.join(songs_metadata, "song_id", "inner")
print("Joined count:", joined.count())
joined.show(5)

# Total plays per user
total_plays = logs_with_genre.groupBy("user_id").agg(count("*").alias("total_plays"))

# Plays per genre per user
genre_plays = logs_with_genre.groupBy("user_id", "genre").agg(count("*").alias("genre_plays"))

# Debug: Show loyalty scores before filtering
user_loyalty_debug = favourite_genre.join(genre_plays, on=["user_id", "genre"]) \
                                    .join(total_plays, on="user_id") \
                                    .withColumn("loyalty_score", col("genre_plays") / col("total_plays"))

print("=== Debug Loyalty Scores (before filtering) ===")
user_loyalty_debug.show(truncate=False)

# Now apply the threshold (> 0.8)
user_loyalty = user_loyalty_debug.filter(col("loyalty_score") > 0.5) \
                                 .select("user_id", "genre", "loyalty_score")

print("=== Users with Genre Loyalty > 0.8 ===")
user_loyalty.show(truncate=False)


# Save Task 3
user_loyalty.write.csv(f"{base_output_path}/genre_loyalty_scores")

# Task 4: Night Owl Users
logs_with_hour = listening_logs.withColumn("hour", hour(to_timestamp("timestamp")))
night_listeners = logs_with_hour.filter((col("hour") >= 0) & (col("hour") < 5)) \
                                .groupBy("user_id") \
                                .agg(count("*").alias("night_plays")) \
                                .orderBy(col("night_plays").desc())
night_listeners.show(truncate=False)
# Save Task 4
night_listeners.write.csv(f"{base_output_path}/night_owl_users")

print(f"\nAll outputs saved under folder: {base_output_path}")
