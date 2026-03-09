from extract_youtube import extract
from transform import transform
from load_warehouse import load

def main():

    print("Starting pipeline...")

    raw_file = extract()

    df = transform(raw_file)

    load(df)

    print("Pipeline completed successfully.")

if __name__ == "__main__":
    main()