from prefect import flow, task
from extract_youtube import extract
from transform import transform
from load_warehouse import load


@task(retries=3, retry_delay_seconds=60)
def extract_task():
    print("Extracting data from YouTube API...")
    raw_file = extract()
    return raw_file


@task
def transform_task(raw_file):
    print("Transforming data...")
    df = transform(raw_file)
    return df


@task
def load_task(df):
    print("Loading data to warehouse...")
    load(df)


@flow(name="youtube-analytics-pipeline")
def youtube_pipeline():

    raw_file = extract_task()

    df = transform_task(raw_file)

    load_task(df)


if __name__ == "__main__":
    youtube_pipeline()