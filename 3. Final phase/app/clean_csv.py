import pandas as pd
import os

# Input and output file paths
RAW_FILE = os.getenv("RAW_FILE", "data/IoT_data.csv")
CLEAN_FILE = os.getenv("CLEAN_FILE", "data/cleaned_IoT_data.csv")

def clean_csv(input_file: str, output_file: str):
    # Load CSV, drop blank lines
    df = pd.read_csv(input_file, header=0, skip_blank_lines=True)

    # Drop extra unnamed or misaligned columns
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

    # Normalize column names
    df.columns = df.columns.str.strip().str.lower()

    # Drop fully empty rows
    df = df.dropna(how='all')

    # Save the cleaned dataset for ingestion
    df.to_csv(output_file, index=False)

    print(f" Cleaned CSV saved at {output_file}")
    print(f"Columns: {df.columns.tolist()}")
    print(f"Total rows: {len(df)}")

if __name__ == "__main__":
    clean_csv(RAW_FILE, CLEAN_FILE)
  