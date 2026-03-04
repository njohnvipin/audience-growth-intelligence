import os
import requests
from datetime import date
from dotenv import load_dotenv
import psycopg

load_dotenv()

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
CHANNEL_ID = os.getenv("CHANNEL_ID")


# -----------------------------
# Helper
# -----------------------------
def yyyymmdd(d: date) -> int:
    return int(d.strftime("%Y%m%d"))


def youtube_get(url: str, params: dict) -> dict:
    params = dict(params)
    params["key"] = YOUTUBE_API_KEY

    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()

    return r.json()


# -----------------------------
# Get uploads playlist
# -----------------------------
def get_uploads_playlist_id(channel_id: str):

    data = youtube_get(
        "https://www.googleapis.com/youtube/v3/channels",
        {"part": "contentDetails", "id": channel_id},
    )

    items = data.get("items", [])

    if not items:
        raise ValueError("Channel not found")

    return items[0]["contentDetails"]["relatedPlaylists"]["uploads"]


# -----------------------------
# Get ALL video IDs
# -----------------------------
def get_all_video_ids_from_playlist(playlist_id):

    video_ids = []
    next_page = None

    while True:

        params = {
            "part": "contentDetails",
            "playlistId": playlist_id,
            "maxResults": 50,
        }

        if next_page:
            params["pageToken"] = next_page

        data = youtube_get(
            "https://www.googleapis.com/youtube/v3/playlistItems",
            params,
        )

        for item in data["items"]:
            video_ids.append(
                item["contentDetails"]["videoId"]
            )

        next_page = data.get("nextPageToken")

        if not next_page:
            break

    return video_ids


# -----------------------------
# Get video metadata
# -----------------------------
def get_video_details(video_ids):

    videos = []

    for i in range(0, len(video_ids), 50):

        batch = video_ids[i:i + 50]

        data = youtube_get(
            "https://www.googleapis.com/youtube/v3/videos",
            {
                "part": "snippet,statistics",
                "id": ",".join(batch),
            },
        )

        for item in data["items"]:

            video_id = item["id"]
            title = item["snippet"]["title"]

            stats = item.get("statistics", {})

            view_count = int(stats.get("viewCount", 0))
            like_count = int(stats.get("likeCount", 0))
            comment_count = int(stats.get("commentCount", 0))

            videos.append({
                "video_id": video_id,
                "title": title,
                "view_count": view_count,
                "like_count": like_count,
                "comment_count": comment_count
            })

    return videos


# -----------------------------
# Load dimension table
# -----------------------------
def load_dim_video(conn, videos):

    with conn.cursor() as cur:

        for v in videos:

            cur.execute(
                """
                INSERT INTO warehouse.dim_video (video_id, title)

                VALUES (%s, %s)

                ON CONFLICT (video_id)
                DO UPDATE SET title = EXCLUDED.title
                """,
                (v["video_id"], v["title"])
            )

    conn.commit()


# -----------------------------
# Load daily snapshot
# -----------------------------
def load_fact_snapshot(conn, videos):

    today = date.today()
    date_id = yyyymmdd(today)

    with conn.cursor() as cur:

        for v in videos:

            # prevent duplicate snapshot
            cur.execute(
                """
                SELECT 1
                FROM warehouse.fact_video_snapshot_daily f
                JOIN warehouse.dim_video d
                ON f.video_sk = d.video_sk
                WHERE f.date_id = %s
                AND d.video_id = %s
                """,
                (date_id, v["video_id"])
            )

            exists = cur.fetchone()

            if exists:
                continue

            cur.execute(
                """
                INSERT INTO warehouse.fact_video_snapshot_daily
                (date_id, video_sk, view_count, like_count, comment_count, snapshot_ts)

                SELECT
                    %s,
                    video_sk,
                    %s,
                    %s,
                    %s,
                    NOW()

                FROM warehouse.dim_video
                WHERE video_id = %s
                """,
                (
                    date_id,
                    v["view_count"],
                    v["like_count"],
                    v["comment_count"],
                    v["video_id"]
                )
            )

    conn.commit()


# -----------------------------
# Main ETL pipeline
# -----------------------------
if __name__ == "__main__":

    print("Fetching uploads playlist...")

    uploads_playlist = get_uploads_playlist_id(CHANNEL_ID)

    print("Fetching video IDs...")

    video_ids = get_all_video_ids_from_playlist(uploads_playlist)

    print(f"Total videos found: {len(video_ids)}")

    print("Fetching video metadata...")

    videos = get_video_details(video_ids)

    print("Connecting to warehouse...")

    conn = psycopg.connect(
        host=os.getenv("PGHOST"),
        port=os.getenv("PGPORT"),
        dbname=os.getenv("PGDATABASE"),
        user=os.getenv("PGUSER"),
        password=os.getenv("PGPASSWORD")
    )

    print("Updating dim_video...")

    load_dim_video(conn, videos)

    print("Loading daily snapshot...")

    load_fact_snapshot(conn, videos)

    conn.close()

    print("Snapshot ETL completed successfully")