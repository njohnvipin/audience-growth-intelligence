CREATE SCHEMA IF NOT EXISTS warehouse;

CREATE TABLE IF NOT EXISTS warehouse.dim_date (
  date_id INT PRIMARY KEY,          -- YYYYMMDD
  date_value DATE NOT NULL,
  year INT NOT NULL,
  month INT NOT NULL,
  day INT NOT NULL
);

CREATE TABLE IF NOT EXISTS warehouse.dim_video (
  video_sk SERIAL PRIMARY KEY,
  video_id TEXT UNIQUE NOT NULL,
  title TEXT,
  published_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS warehouse.fact_video_snapshot_daily (
  date_id INT NOT NULL REFERENCES warehouse.dim_date(date_id),
  video_sk INT NOT NULL REFERENCES warehouse.dim_video(video_sk),
  view_count BIGINT NOT NULL DEFAULT 0,
  like_count BIGINT NOT NULL DEFAULT 0,
  comment_count BIGINT NOT NULL DEFAULT 0,
  snapshot_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (date_id, video_sk)
);
