import os
import psycopg
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()


def connect():
    return psycopg.connect(
        host=os.getenv("PGHOST"),
        port=os.getenv("PGPORT"),
        dbname=os.getenv("PGDATABASE"),
        user=os.getenv("PGUSER"),
        password=os.getenv("PGPASSWORD")
    )


def load(df):

    conn = connect()

    with conn.cursor() as cur:

        for _, row in df.iterrows():

            # -----------------------------
            # Insert or update dim_video
            # -----------------------------
            cur.execute("""
                INSERT INTO warehouse.dim_video (video_id, title, published_at)
                VALUES (%s, %s, %s)
                ON CONFLICT (video_id)
                DO UPDATE SET title = EXCLUDED.title
                RETURNING video_sk
            """,
            (row.video_id, row.title, row.published_at)
            )

            video_sk = cur.fetchone()[0]

            # -----------------------------
            # Prepare date fields
            # -----------------------------
            date_str = str(row.date_id)

            date_value = datetime.strptime(date_str, "%Y%m%d").date()

            year = int(date_str[:4])
            month = int(date_str[4:6])
            day = int(date_str[6:])

            # -----------------------------
            # Insert into dim_date
            # -----------------------------
            cur.execute("""
                INSERT INTO warehouse.dim_date
                (date_id, date_value, year, month, day)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (date_id) DO NOTHING
            """,
            (
                row.date_id,
                date_value,
                year,
                month,
                day
            )
            )

            # -----------------------------
            # Insert snapshot fact
            # -----------------------------
            cur.execute("""
                INSERT INTO warehouse.fact_video_snapshot_daily
                (video_sk, date_id, view_count, like_count, comment_count)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (date_id, video_sk)
                DO UPDATE
                SET
                    view_count = EXCLUDED.view_count,
                    like_count = EXCLUDED.like_count,
                    comment_count = EXCLUDED.comment_count
            """,
            (
                video_sk,
                row.date_id,
                row.view_count,
                row.like_count,
                row.comment_count
            )
            )

    conn.commit()
    conn.close()

    print("Warehouse load completed")