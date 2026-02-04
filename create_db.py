import os
from dotenv import load_dotenv
import psycopg

load_dotenv()

def main():
    # Connect to the default maintenance DB (postgres) to create your project DB
    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5432")
    user = os.getenv("PGUSER", "postgres")
    password = os.getenv("PGPASSWORD")
    target_db = os.getenv("PGDATABASE", "youtube_growth_dw")

    if not password:
        raise ValueError("PGPASSWORD is missing in .env")

    conninfo = f"host={host} port={port} dbname=postgres user={user} password={password}"

    with psycopg.connect(conninfo, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (target_db,))
            exists = cur.fetchone() is not None

            if exists:
                print(f"Database '{target_db}' already exists.")
            else:
                cur.execute(f'CREATE DATABASE "{target_db}";')
                print(f"Database '{target_db}' created successfully.")

if __name__ == "__main__":
    main()
