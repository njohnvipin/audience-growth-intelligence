import os
import json
import requests
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
CHANNEL_ID = os.getenv("CHANNEL_ID")

RAW_PATH = "data/raw"

def youtube_get(url, params):
    params["key"] = YOUTUBE_API_KEY
    r = requests.get(url, params=params)
    r.raise_for_status()
    return r.json()

def get_uploads_playlist(channel_id):

    data = youtube_get(
        "https://www.googleapis.com/youtube/v3/channels",
        {"part": "contentDetails", "id": channel_id}
    )

    return data["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

def get_video_ids(playlist_id):

    video_ids = []
    next_page = None

    while True:

        data = youtube_get(
            "https://www.googleapis.com/youtube/v3/playlistItems",
            {
                "part": "snippet",
                "playlistId": playlist_id,
                "maxResults": 50,
                "pageToken": next_page
            }
        )

        for item in data["items"]:
            video_ids.append(item["snippet"]["resourceId"]["videoId"])

        next_page = data.get("nextPageToken")

        if not next_page:
            break

    return video_ids


def get_video_details(video_ids):

    videos = []

    for i in range(0, len(video_ids), 50):

        batch = video_ids[i:i+50]

        data = youtube_get(
            "https://www.googleapis.com/youtube/v3/videos",
            {
                "part": "snippet,statistics",
                "id": ",".join(batch)
            }
        )

        videos.extend(data["items"])

    return videos


def extract():

    playlist = get_uploads_playlist(CHANNEL_ID)

    video_ids = get_video_ids(playlist)

    videos = get_video_details(video_ids)

    os.makedirs(RAW_PATH, exist_ok=True)

    file = f"{RAW_PATH}/youtube_raw_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

    with open(file, "w") as f:
        json.dump(videos, f, indent=2)

    print("Raw data saved:", file)

    return file