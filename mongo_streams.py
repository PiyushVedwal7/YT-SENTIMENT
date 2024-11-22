from pyspark.sql import SparkSession
from pymongo import MongoClient

def start_spark_streaming(input_dir, mongo_uri, db_name, collection_name):
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("YouTubeCommentsStreaming") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    # Initialize MongoDB Client
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]

    # Read the streaming data from input directory
    comment_stream = spark.readStream \
        .format("text") \
        .load(input_dir)

    # Function to write the data to MongoDB
    def write_to_mongo(batch_df, batch_id):
        # Convert the Spark DataFrame to Pandas for easy insertion
        batch_data = batch_df.toPandas()
        # Insert each row into MongoDB
        collection.insert_many(batch_data.to_dict("records"))

    # Write the raw comments to MongoDB in batches
    query = comment_stream.writeStream \
        .outputMode("append") \
        .foreachBatch(write_to_mongo) \
        .start()

    # Await termination of the query
    query.awaitTermination()

# Parameters
input_dir = "temp_dir/streaming_dir"
mongo_uri = "mongodb://localhost:27017"
db_name = "spark_yt"
collection_name = "comments"

# Start the Spark Streaming job
start_spark_streaming(input_dir, mongo_uri, db_name, collection_name)
