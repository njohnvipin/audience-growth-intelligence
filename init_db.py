import os
from dotenv import load_dotenv
import psycopg

load_dotenv()

def main():
    conninfo = (
        f"host={os.getenv('PGHOST')} "
        f"port={os.getenv('PGPORT')} "
        f"dbname={os.getenv('PGDATABASE')} "
        f"user={os.getenv('PGUSER')} "
        f"password={os.getenv('PGPASSWORD')}"
    )

    with psycopg.connect(conninfo) as conn:
        with conn.cursor() as cur:
            with open("schema.sql", "r", encoding="utf-8") as f:
                cur.execute(f.read())
        conn.commit()

    print("✅ Database initialized (schemas + tables created).")

if __name__ == "__main__":
    main()
