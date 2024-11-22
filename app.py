from flask import Flask, render_template
from pymongo import MongoClient
from textblob import TextBlob
import pandas as pd

app = Flask(__name__)

# MongoDB Connection
client = MongoClient('mongodb://localhost:27017/')
db = client['spark_yt']  # Replace with your database name
collection = db['comments']  # Replace with your collection name

@app.route('/')
def index():
    # Fetch comments from MongoDB
    comments_cursor = collection.find({}, {'_id': 1, 'value': 1})  # Include both _id and value fields
    
    # Debugging: print comments fetched from MongoDB
    print("Fetched comments from MongoDB:")
    for comment in comments_cursor:
        print(comment)  # Debugging: print each comment

    # Reset the cursor to iterate over it again
    comments_cursor = collection.find({}, {'_id': 1, 'value': 1})

    # Convert MongoDB cursor to a DataFrame
    comments_data = []
    for comment in comments_cursor:
        comments_data.append({
            '_id': str(comment['_id']),  # Convert ObjectId to string
            'value': comment['value']    # Text value of the comment
        })
    
    # Convert to DataFrame
    comments_df = pd.DataFrame(comments_data)
    
    # Debugging: print the DataFrame to check if data is correct
    print("Dataframe created:")
    print(comments_df)

    # Ensure DataFrame has necessary data
    if not comments_df.empty and 'value' in comments_df.columns:
        # Apply sentiment analysis
        comments_df['polarity'] = comments_df['value'].apply(lambda x: TextBlob(x).sentiment.polarity)
        comments_df['sentiment'] = comments_df['polarity'].apply(
            lambda polarity: 'Positive' if polarity > 0 else 'Negative' if polarity < 0 else 'Neutral'
        )
    else:
        comments_df = pd.DataFrame(columns=['_id', 'value', 'polarity', 'sentiment'])
    
    # Convert DataFrame back to JSON-like structure
    comments = comments_df.to_dict(orient='records')

    # Debugging: print the final comments to pass to the template
    print("Final comments to render:")
    print(comments)

    # Pass comments to the template
    return render_template('index.html', comments=comments)

if __name__ == '__main__':
    app.run(debug=True)
