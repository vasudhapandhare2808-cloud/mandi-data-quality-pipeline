
# Mandi Data Quality Pipeline

## Dataset

Source: Kaggle India Mandi Daily Commodity Prices
File examined: 2025.csv
Sample size: 100,000 records

## Columns

- State
- District
- Market
- Commodity
- Variety
- Grade
- Arrival_Date
- Min_Price
- Max_Price
- Modal_Price
- Commodity_Code

## Preliminary Data Quality Findings

| Issue | Records |
|---|---:|
| Non-positive minimum price | 133 |
| Non-positive maximum price | 38 |
| Invalid price ordering | 38 |

These findings are based on the first 100,000
records of the 2025 dataset.

## Proposed Validation Rules

1. Reject missing mandatory fields.
2. Reject non-numeric or non-finite prices.
3. Reject non-positive prices.
4. Reject records where Min_Price > Modal_Price.
5. Reject records where Modal_Price > Max_Price.
6. Detect duplicate natural keys.
7. Flag statistically unusual price movements.
