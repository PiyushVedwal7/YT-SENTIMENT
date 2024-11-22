from flask import Flask, render_template, jsonify
from threading import Thread
from pyspark.sql import SparkSession
from spark_streaming import start_streaming
# from spark_structure_stream import start_streaming
from stream_comments import start_spark_streaming

app = Flask(__name__)

processed_data = []

# Initialize Spark session
spark = SparkSession.builder.appName("FlaskApp").getOrCreate()

@app.route("/")
def home():
    return render_template("index.html")


# @app.route("/data")
# def data():
#     API_KEY = "AIzaSyBMYZRIVOvvrjKKIY64bDT3sgMxw15Xqvc"
#     VIDEO_ID = "4gulVzzh82g"

#     comment_thread = Thread(target=start_streaming, args=(API_KEY, VIDEO_ID))
#     comment_thread.start()

#     return jsonify(processed_data)


@app.route('/data')
def get_data():
    # Query the in-memory table created by PySpark
    try:
        df = spark.sql("SELECT word, count FROM word_counts ORDER BY count DESC")
        data = df.collect()
        # Convert data to a JSON-serializable format
        result = [{"word": row.word, "count": row.count} for row in data]
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)})

if __name__ == "__main__":
    # Run the Spark streaming job in a separate thread
    spark_thread = Thread(target=start_spark_streaming, args=("AIzaSyBMYZRIVOvvrjKKIY64bDT3sgMxw15Xqvc", "4gulVzzh82g"))
    spark_thread.start()

    # Start the Flask app
    app.run(debug=True)


if __name__ == "__main__":
    app.run(debug=True)