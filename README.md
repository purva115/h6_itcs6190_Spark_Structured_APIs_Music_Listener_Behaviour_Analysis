Music Streaming Analysis Using Spark Structured APIs
Overview
This project analyzes user listening behavior and music trends using Apache Spark Structured APIs. The analysis processes structured data from a fictional music streaming platform to gain insights into genre preferences, song popularity, and listener engagement patterns.
Course: ITCS 6190/8190 - Cloud Computing for Data Analysis, Fall 2025
Instructor: Marco Vieira
Dataset Description
The project utilizes two primary datasets:
1. listening_logs.csv
Contains user listening activity logs with the following structure:

user_id: Unique identifier for each user
song_id: Unique identifier for each song
timestamp: Date and time when the song was played (format: YYYY-MM-DD HH:MM:SS)
duration_sec: Duration in seconds for which the song was played

2. songs_metadata.csv
Contains metadata about songs with the following structure:

song_id: Unique identifier for each song
title: Title of the song
artist: Name of the artist
genre: Genre classification (e.g., Pop, Rock, Jazz)
mood: Mood category (e.g., Happy, Sad, Energetic, Chill)

Repository Structure
├── README.md
├── main.py                     # Main analysis script
├── input_generator.py          # Data generation script
├── inputs/
│   ├── listening_logs.csv      # User listening logs
│   └── songs_metadata.csv      # Song metadata
└── outputs/
    ├── user_favorite_genres/
    ├── avg_listen_time_per_song/
    ├── genre_loyalty_scores/
    └── night_owl_users/
Output Directory Structure
outputs/
├── user_favorite_genres/       # Each user's most listened-to genre
├── avg_listen_time_per_song/   # Average listening duration per song
├── genre_loyalty_scores/       # Users with loyalty score > 0.8
└── night_owl_users/           # Users active between 12 AM - 5 AM
Tasks and Outputs
Task 1: User Favorite Genres

Objective: Identify the most listened-to genre for each user
Method: Count song plays per genre for each user
Output: CSV file with user_id and their favorite genre

Task 2: Average Listen Time Per Song

Objective: Calculate average listening duration for each song
Method: Compute mean duration across all plays for each song
Output: CSV file with song_id and average listen time

Task 3: Genre Loyalty Score

Objective: Calculate loyalty score for users (proportion of plays in favorite genre)
Method: Filter users with loyalty score > 0.8
Output: CSV file with users and their loyalty scores

Task 4: Night Owl Users

Objective: Identify users who listen to music between 12 AM and 5 AM
Method: Filter listening logs by timestamp range
Output: CSV file with users active during late night hours

Execution Instructions
Prerequisites
Before starting the assignment, ensure you have the following software installed and properly configured on your machine:

Python 3.x:

Download and Install Python
Verify installation:



bash     python3 --version

PySpark:

Install using pip:



bash     pip install pyspark

Apache Spark:

Ensure Spark is installed. You can download it from the Apache Spark Downloads page.
Verify installation by running:



bash     spark-submit --version
Running the Analysis Tasks
Running Locally

Generate the Input Data:

bash   python3 input_generator.py

Execute the Analysis:

bash   spark-submit main.py
or
bash   python3 main.py

Verify the Outputs:
Check the outputs/ directory for the resulting files:

bash   ls outputs/
Alternative Execution Methods
Using Jupyter Notebook (if using .ipynb format):
bashjupyter notebook main.ipynb
Using GitHub Codespaces:

Open the repository in GitHub Codespaces
Install dependencies:

bash   pip install pyspark

Run the analysis script

Implementation Approach
Data Loading and Preparation

Utilized Spark's spark.read.csv() to load datasets
Applied appropriate schemas and handled null values
Ensured data quality and consistency

Data Analysis Techniques

Filtering: Applied time-based filters for night owl analysis
Aggregation: Used groupBy operations for genre counting and averaging
Joins: Combined listening logs with song metadata
Transformations: Applied window functions and calculations for loyalty scores

Performance Considerations

Optimized join operations using appropriate broadcast hints
Utilized Spark's lazy evaluation for efficient processing
Implemented proper partitioning strategies for large datasets

Results Summary
Key Findings

Genre Distribution: Analysis of user preferences across different music genres
Listening Patterns: Identification of peak listening times and user behavior
User Segmentation: Classification of users based on loyalty and listening habits
Engagement Metrics: Average listening durations and genre preferences

Data Quality

Dataset contains a minimum of 100 records as required
All output files are properly formatted and saved in CSV format
Results are validated for accuracy and completeness

Errors and Resolutions
Common Issues and Solutions

Import Errors:

   Error: No module named 'pyspark'
   Solution: pip install pyspark

Spark Context Issues:

   Error: Cannot call methods on a stopped SparkContext
   Solution: Ensure proper SparkSession initialization and cleanup

File Path Issues:

   Error: Path does not exist
   Solution: Verify input file paths and create output directories

Memory Issues:

   Error: OutOfMemoryError
   Solution: Adjust Spark configuration for memory allocation
Technologies Used

Apache Spark: Distributed data processing framework
PySpark: Python API for Spark
Python: Primary programming language
CSV: Data format for input and output files

Assignment Submission
This project fulfills the requirements for Hands-on L6: Spark Structured APIs assignment including:

✅ Complete analysis of user listening behavior
✅ Implementation using Spark Structured APIs
✅ Proper output file generation in required format
✅ Well-structured repository with all required files
✅ Comprehensive README documentation
✅ Dataset with minimum 100 records

