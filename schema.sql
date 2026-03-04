
CREATE TABLE warehouse.dim_date (
    date_id     INTEGER PRIMARY KEY,          -- 20260203
    date_value  DATE NOT NULL,                -- 2026-02-03
    year        INTEGER NOT NULL,
    month       INTEGER NOT NULL,
    day         INTEGER NOT NULL
);

CREATE TABLE warehouse.dim_video (
    video_sk SERIAL PRIMARY KEY,
    video_id VARCHAR(50) NOT NULL UNIQUE,
    title TEXT,
    published_at TIMESTAMP WITH TIME ZONE,
    channel_id VARCHAR(50),
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE warehouse.fact_video_snapshot_daily (
    date_id        INTEGER NOT NULL,
    video_sk       INTEGER NOT NULL,
    view_count     BIGINT NOT NULL DEFAULT 0,
    like_count     BIGINT NOT NULL DEFAULT 0,
    comment_count  BIGINT NOT NULL DEFAULT 0,
    snapshot_ts    TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (date_id, video_sk),
    FOREIGN KEY (date_id)  REFERENCES warehouse.dim_date(date_id),
    FOREIGN KEY (video_sk) REFERENCES warehouse.dim_video(video_sk)
);