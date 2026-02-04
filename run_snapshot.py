import os
import math
import requests
from datetime import date, datetime
from dotenv import load_dotenv
import psycopg

load_dotenv()

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
CHANNEL_ID = os.getenv("CHANNEL_ID")

def yyyymmdd(d: date) -> int:
    return int(d.strftime("%Y%m%d"))

def youtube_get(url: str, params: dict) -> dict:
    params = dict(params)
    params["key"] = YOUTUBE_API_KEY
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def get_uploads_playlist_id(channel_id: str) -> str:
    data = youtube_get(
        "https://www.googleapis.com/youtube/v3/channels",
        {"part": "contentDetails", "id": channel_id}
    )
    items = data.get("items", [])
    if not items:
        raise ValueError("Channel not found or API key invalid.")
    return items[0]["contentDetails"]["relatedPlaylists"]["uploads"]

def get_all_video_ids_from_playlist(playlist_id: str, max_videos: int = 50) -> list[str]:
    video_ids = []
    page_token = None

    while True and len(video_ids) < max_videos:
        data = youtube_get(
            "https://www.googleapis.com/youtube/v3/playlistItems",
            {
                "part": "contentDetails",
                "playlistId": playlist_id,
                "maxResults": 50,
                **({"pageToken": page_token} if page_token else {})
            }
        )
        for it in data.get("items", []):
            vid = it["contentDetails"].get("videoId")
            if vid:
                video_ids.append(vid)
                if len(video_ids) >= max_videos:
                    break

        page_token = data.get("nextPageToken")
        if not page_token:
            break

    return video_ids

def chunked(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def get_video_details(video_ids: list[str]) -> list[dict]:
    details = []
    for chunk in chunked(video_ids, 50):
        data = youtube_get(
            "https://www.googleapis.com/youtube/v3/videos",
            {"part": "snippet,statistics", "id": ",".join(chunk)}
        )
        details.extend(data.get("items", []))
    return details

def parse_video(item: dict) -> dict:
    vid = item["id"]
    snip = item.get("snippet", {})
    stats = item.get("statistics", {})

    title = snip.get("title")
    published_at = snip.get("publishedAt")  # ISO string

    # counts come as strings
    views = int(stats.get("viewCount", 0))
    likes = int(stats.get("likeCount", 0)) if "likeCount" in stats else 0
    comments = int(stats.get("commentCount", 0)) if "commentCount" in stats else 0

    return {
        "video_id": vid,
        "title": title,
        "published_at": published_at,
        "view_count": views,
        "like_count": likes,
        "comment_count": comments,
    }

def main():
    if not YOUTUBE_API_KEY:
        raise ValueError("Missing YOUTUBE_API_KEY in .env")
    if not CHANNEL_ID:
        raise ValueError("Missing CHANNEL_ID in .env")

    # 1) Extract
    uploads_playlist = get_uploads_playlist_id(CHANNEL_ID)
    video_ids = get_all_video_ids_from_playlist(uploads_playlist, max_videos=50)
    video_items = get_video_details(video_ids)
    rows = [parse_video(it) for it in video_items]

    # 2) Connect DB
    conninfo = (
        f"host={os.getenv('PGHOST')} "
        f"port={os.getenv('PGPORT')} "
        f"dbname={os.getenv('PGDATABASE')} "
        f"user={os.getenv('PGUSER')} "
        f"password={os.getenv('PGPASSWORD')}"
    )

    today = date.today()
    date_id = yyyymmdd(today)

    with psycopg.connect(conninfo) as conn:
        with conn.cursor() as cur:
            # 3) Upsert dim_date
            cur.execute(
                """
                INSERT INTO warehouse.dim_date (date_id, date_value, year, month, day)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (date_id) DO NOTHING;
                """,
                (date_id, today, today.year, today.month, today.day)
            )

            # 4) Upsert dim_video + insert snapshot facts
            for r in rows:
                # Upsert dim_video and get surrogate key
                cur.execute(
                    """
                    INSERT INTO warehouse.dim_video (video_id, title, published_at)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (video_id) DO UPDATE
                    SET title = EXCLUDED.title,
                        published_at = EXCLUDED.published_at
                    RETURNING video_sk;
                    """,
                    (r["video_id"], r["title"], r["published_at"])
                )
                video_sk = cur.fetchone()[0]

                # Insert snapshot (idempotent)
                cur.execute(
                    """
                    INSERT INTO warehouse.fact_video_snapshot_daily
                      (date_id, video_sk, view_count, like_count, comment_count)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (date_id, video_sk) DO UPDATE
                    SET view_count = EXCLUDED.view_count,
                        like_count = EXCLUDED.like_count,
                        comment_count = EXCLUDED.comment_count,
                        snapshot_ts = CURRENT_TIMESTAMP;
                    """,
                    (date_id, video_sk, r["view_count"], r["like_count"], r["comment_count"])
                )

        conn.commit()

    print(f"Snapshot captured for {len(rows)} videos on {today.isoformat()} (date_id={date_id}).")

if __name__ == "__main__":
    main()
