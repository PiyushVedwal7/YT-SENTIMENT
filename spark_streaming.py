from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split
import time
import gc
import logging
from youtube_fetch_comments import fetch_comments

# Configure logging for better error tracking
logging.basicConfig(level=logging.INFO)

def start_streaming(api_key, video_id):
    # Create Spark session with memory configurations
    spark = SparkSession.builder \
        .appName("StreamApp") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.python.worker.memory", "2g") \
        .getOrCreate()

    data = []

    def get_comments():
        try:
            comments = fetch_comments(api_key, video_id)
            for comment in comments[2:]:
                data.append((comment,))
        except Exception as e:
            logging.error(f"Error fetching comments: {e}")

    while True:
        try:
            # Fetch the comments and create DataFrame
            get_comments()

            if data:
                # Limit the number of comments per cycle (e.g., process only 5 comments per batch)
                df = spark.createDataFrame(data[:5], ["comment"])

                # Split words and count occurrences
                words_df = df.select(explode(split(df.comment, " ")).alias("word"))
                word_count = words_df.groupBy("word").count()

                # Show word count
                word_count.show()

                # Clear memory after processing the batch
                data.clear()  # Clear the list to release memory
                gc.collect()  # Trigger garbage collection to free memory
            else:
                logging.info("No comments fetched")
                
        except Exception as e:
            logging.error(f"Error occurred while processing DataFrame: {e}")

        # Wait for a bit before fetching new comments
        time.sleep(10)

