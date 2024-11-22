import requests
import time
import os
import uuid

def fetch_comments(api_key, video_id, output_dir):
    """
    Fetch comments from a YouTube video and write them to a file.
    """
    URL = "https://www.googleapis.com/youtube/v3/commentThreads"
    params = {
        "part": "snippet",
        "videoId": video_id,
        "key": api_key,
        "maxResults": 20
    }

    try:
        response = requests.get(URL, params)
        response.raise_for_status()  # Raise an error for HTTP status codes 4xx or 5xx

        # Extract comments
        comments = [
            item["snippet"]["topLevelComment"]["snippet"]["textDisplay"]
            for item in response.json().get("items", [])
        ]

        # Write comments to a file
        if comments:
            os.makedirs(output_dir, exist_ok=True)  # Ensure the directory exists
            file_path = os.path.join(output_dir, f"{uuid.uuid4()}.txt")
            with open(file_path, "w", encoding="utf-8") as f:  # Use utf-8 encoding
                for comment in comments:
                    f.write(comment + "\n")
            print(f"Comments written to: {file_path}")
        else:
            print("No comments found.")
    except requests.exceptions.RequestException as e:
        print(f"Error fetching comments: {e}")

def simulate_streaming(api_key, video_id, output_dir, duration=120):
    """
    Simulate streaming by periodically fetching comments for a specified duration.
    """
    start_time = time.time()
    while time.time() - start_time < duration:
        fetch_comments(api_key, video_id, output_dir)
        time.sleep(60)  # Fetch comments every 10 seconds

# API Key and Video ID
API_KEY = "AIzaSyBIDQ9a5Q4nkxeII_lwba4hLXYYjArUGC4"
VIDEO_ID = "4gulVzzh82g"

# Output Directory for storing streamed comments
OUTPUT_DIR = "temp_dir/streaming_dir"

# Start streaming simulation
simulate_streaming(API_KEY, VIDEO_ID, OUTPUT_DIR, duration=120)  # Simulate for 2 minutes
