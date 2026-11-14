
import csv
import math
from collections import Counter

FILE_PATH = "data/raw/2025.csv"
SAMPLE_LIMIT = 100_000

counts = Counter()
seen = {}

with open(FILE_PATH, "r", encoding="utf-8-sig", newline="") as file:
    reader = csv.DictReader(file)

    for i, row in enumerate(reader):
        if i >= SAMPLE_LIMIT:
            break

        counts["rows"] += 1

        # Price validation
        try:
            prices = [
                float(row["Min_Price"]),
                float(row["Max_Price"]),
                float(row["Modal_Price"])
            ]

            if not all(math.isfinite(p) for p in prices):
                counts["non_numeric_or_non_finite"] += 1
            else:
                min_p, max_p, modal_p = prices

                if min_p <= 0:
                    counts["non_positive_min"] += 1

                if max_p <= 0:
                    counts["non_positive_max"] += 1

                if modal_p <= 0:
                    counts["non_positive_modal"] += 1

                if not (min_p <= modal_p <= max_p):
                    counts["invalid_price_range"] += 1

        except (ValueError, TypeError):
            counts["non_numeric_or_non_finite"] += 1

        # Duplicate investigation
        key = (
            row["Arrival_Date"],
            row["State"],
            row["District"],
            row["Market"],
            row["Commodity_Code"],
            row["Variety"],
            row["Grade"]
        )

        if key in seen:
            counts["duplicate_rows"] += 1

            if counts["duplicate_rows"] <= 5:
                print("\nDUPLICATE MATCH:")
                print("Previous:", seen[key])
                print("Current: ", row)
        else:
            seen[key] = row

print("\n========== VALIDATION REPORT ==========")

for name, count in counts.items():
    print(f"{name}: {count}")

print("=======================================")
