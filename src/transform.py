import os
import json
import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable
from deltalake.writer import write_deltalake

PROCESSED_FOLDER = "data/processed"
DELTA_PATH = "data/lakehouse/youtube_delta"


def transform(raw_file):

    print("Transforming data...")

    # Load JSON
    with open(raw_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Handle both JSON structures
    if isinstance(data, dict):
        items = data.get("items", [])
    elif isinstance(data, list):
        items = data
    else:
        items = []

    records = []

    for item in items:

        if not isinstance(item, dict):
            continue

        snippet = item.get("snippet", {})
        stats = item.get("statistics", {})

        records.append({
            "video_id": item.get("id"),
            "title": snippet.get("title"),
            "published_at": snippet.get("publishedAt"),
            "view_count": int(stats.get("viewCount", 0)),
            "like_count": int(stats.get("likeCount", 0)),
            "comment_count": int(stats.get("commentCount", 0))
        })

    df = pd.DataFrame(records)

    if df.empty:
        print("No records found")
        return df

    # Cleaning
    df["title"] = df["title"].fillna("Unknown")
    df["published_at"] = pd.to_datetime(df["published_at"])

    df["date_id"] = df["published_at"].dt.strftime("%Y%m%d").astype(int)

    df["year"] = df["published_at"].dt.year
    df["month"] = df["published_at"].dt.month
    df["day"] = df["published_at"].dt.day

    df = df.drop_duplicates(subset=["video_id", "date_id"])

    # Save parquet
    os.makedirs(PROCESSED_FOLDER, exist_ok=True)

    parquet_path = os.path.join(PROCESSED_FOLDER, "youtube_processed.parquet")
    df.to_parquet(parquet_path, index=False)

    print("Processed data saved:", parquet_path)

    # Convert to Arrow
    table = pa.Table.from_pandas(df)

    os.makedirs("data/lakehouse", exist_ok=True)

    # Create or merge Delta Lake
    if not os.path.exists(DELTA_PATH):

        write_deltalake(
            DELTA_PATH,
            table,
            mode="overwrite"
        )

        print("Delta table created")

    else:

        dt = DeltaTable(DELTA_PATH)

        (
            dt.merge(
                table,
                predicate="target.video_id = source.video_id AND target.date_id = source.date_id",
                source_alias="source",
                target_alias="target"
            )
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
        )

        print("Delta merge completed")

    return df