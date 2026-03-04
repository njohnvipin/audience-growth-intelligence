import os
from dotenv import load_dotenv
import psycopg
import pandas as pd
import streamlit as st
import plotly.express as px

# -----------------------------
# Load environment variables
# -----------------------------
load_dotenv()

# -----------------------------
# Page title
# -----------------------------
st.title("YouTube Channel Analytics Dashboard")

# -----------------------------
# Connect to PostgreSQL
# -----------------------------
conn = psycopg.connect(
    host=os.getenv("PGHOST"),
    port=os.getenv("PGPORT"),
    dbname=os.getenv("PGDATABASE"),
    user=os.getenv("PGUSER"),
    password=os.getenv("PGPASSWORD")
)

# -----------------------------
# Query Data Warehouse
# -----------------------------
query = """
SELECT
    d.date_value,
    v.title,
    f.view_count,
    f.like_count,
    f.comment_count
FROM warehouse.fact_video_snapshot_daily f
JOIN warehouse.dim_video v
ON f.video_sk = v.video_sk
JOIN warehouse.dim_date d
ON f.date_id = d.date_id
"""

df = pd.read_sql(query, conn)

df["date_value"] = pd.to_datetime(df["date_value"])

# -----------------------------
# Sidebar Filters
# -----------------------------
st.sidebar.header("Filters")

start_date = st.sidebar.date_input(
    "Start Date",
    df["date_value"].min()
)

end_date = st.sidebar.date_input(
    "End Date",
    df["date_value"].max()
)

df = df[
    (df["date_value"] >= pd.to_datetime(start_date)) &
    (df["date_value"] <= pd.to_datetime(end_date))
]

video = st.sidebar.selectbox(
    "Select Video",
    df["title"].unique()
)

filtered = df[df["title"] == video]

# -----------------------------
# KPI Metrics
# -----------------------------
st.subheader("Channel Summary")

total_views = df["view_count"].max()
total_likes = df["like_count"].max()
total_comments = df["comment_count"].max()
total_videos = df["title"].nunique()

col1, col2, col3, col4 = st.columns(4)

col1.metric("Total Views", f"{total_views:,}")
col2.metric("Total Likes", f"{total_likes:,}")
col3.metric("Total Comments", f"{total_comments:,}")
col4.metric("Total Videos", total_videos)

# -----------------------------
# Views Over Time
# -----------------------------
st.subheader("Views Over Time")

fig_views = px.line(
    filtered,
    x="date_value",
    y="view_count",
    markers=True
)

st.plotly_chart(fig_views, use_container_width=True)

# -----------------------------
# Likes vs Comments
# -----------------------------
st.subheader("Likes vs Comments")

fig_interaction = px.line(
    filtered,
    x="date_value",
    y=["like_count", "comment_count"],
    markers=True
)

st.plotly_chart(fig_interaction, use_container_width=True)

# -----------------------------
# Engagement Rate
# -----------------------------
filtered["engagement"] = (
    (filtered["like_count"] + filtered["comment_count"])
    / filtered["view_count"] * 100
)

st.subheader("Engagement Rate (%)")

fig_engagement = px.line(
    filtered,
    x="date_value",
    y="engagement",
    markers=True
)

st.plotly_chart(fig_engagement, use_container_width=True)

# -----------------------------
# Top Videos
# -----------------------------
st.subheader("Top 10 Videos by Views")

top_videos = (
    df.groupby("title")["view_count"]
    .max()
    .sort_values(ascending=False)
    .head(10)
    .reset_index()
)

fig_top = px.bar(
    top_videos,
    x="title",
    y="view_count"
)

st.plotly_chart(fig_top, use_container_width=True)

# -----------------------------
# Viral Videos (Growth)
# -----------------------------
df = df.sort_values(["title", "date_value"])
df["view_growth"] = df.groupby("title")["view_count"].diff()

viral = (
    df.groupby("title")["view_growth"]
    .sum()
    .sort_values(ascending=False)
    .head(10)
    .reset_index()
)

st.subheader("🔥 Fastest Growing Videos")

fig_viral = px.bar(
    viral,
    x="title",
    y="view_growth"
)

st.plotly_chart(fig_viral, use_container_width=True)