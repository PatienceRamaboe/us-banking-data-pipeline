
import csv
from pathlib import Path

# Dataset folder
data_folder = Path.home() / "Downloads" / "ieee-fraud-detection" 

files = [
    "train_transaction.csv",
    "train_identity.csv"
]

for filename in files:
    file_path = data_folder / filename

    print("\n" + "=" * 60)
    print(f"FILE: {filename}")
    print("=" * 60)

    if not file_path.exists():
        print("File not found. Check the folder location and filename.")
        continue

    # File size
    size_mb = file_path.stat().st_size / (1024 * 1024)
    print(f"File size: {size_mb:.2f} MB")

    # Read a small sample without loading the whole file
    with open(file_path, "r", encoding="utf-8", newline="") as file:
        reader = csv.reader(file)

        columns = next(reader)
        print(f"\nNumber of columns: {len(columns)}")
        print("\nColumn names:")
        print(columns)

        print("\nFirst 5 records:")
        for row_number, row in enumerate(reader):
            print(row)
            if row_number == 4:
                break

