import pandas as pd
import logging


def convert_to_gregorian(year_buddhist):
    return year_buddhist - 543

def convert_to_datetime(date_str):
    try:
        date_part, time_part = date_str.split(' ') if ' ' in date_str else (date_str, "00:00:00")
        date_parts = date_part.split('-') if '-' in date_part else date_part.split('/')

        if len(date_parts) != 3:
            raise ValueError("Invalid date format")

        if int(date_parts[0]) > 2500:
            year_buddhist = int(date_parts[0])
            month = int(date_parts[1])
            day = int(date_parts[2])
            year_gregorian = convert_to_gregorian(year_buddhist)
        else:
            year_gregorian = int(date_parts[0])
            month = int(date_parts[1])
            day = int(date_parts[2])

        date_time_str = f"{year_gregorian}-{month:02d}-{day:02d} {time_part}"
        return pd.to_datetime(date_time_str, format='%Y-%m-%d %H:%M:%S')

    except Exception as e:
        logging.error(f" Error processing date: {date_str} - {e}")
        return pd.NaT

df = pd.read_csv("/dataset/transactions.csv")

# Renamed 'date' to 'transaction_date'
df.rename(columns={"date": "transaction_date"}, inplace=True)

# Convert to DATETIME
df["transaction_date"] = df["transaction_date"].astype(str).apply(convert_to_datetime)

# Fix 'Null' and nagative
df["price"] = df["price"].fillna(0).apply(lambda x: max(x, 0))
df["quantity"] = df["quantity"].fillna(0).apply(lambda x: max(x, 0))

# Export as CSV file
df.to_csv("transactions_cleaned.csv", index=False)