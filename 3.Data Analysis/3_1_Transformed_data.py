import pandas as pd
import logging

# ฟังก์ชันแปลงปี พ.ศ. เป็น ค.ศ.
def convert_to_gregorian(year_buddhist):
    return year_buddhist - 543

# ฟังก์ชันแปลงวันที่จาก พ.ศ. เป็น ค.ศ. และแปลงเป็น datetime
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

# โหลดข้อมูลต้นฉบับ
df = pd.read_csv("/dataset/transactions.csv")

# เปลี่ยนชื่อคอลัมน์ 'date' เป็น 'transaction_date'
df.rename(columns={"date": "transaction_date"}, inplace=True)

# แปลงวันที่จาก พ.ศ. เป็น ค.ศ.
df["transaction_date"] = df["transaction_date"].astype(str).apply(convert_to_datetime)

# แก้ไขค่าที่เป็น null และค่าติดลบ
df["price"] = df["price"].fillna(0).apply(lambda x: max(x, 0))
df["quantity"] = df["quantity"].fillna(0).apply(lambda x: max(x, 0))

# บันทึกข้อมูลที่สะอาดแล้ว
df.to_csv("transactions_cleaned.csv", index=False)

print(" Data cleaned and saved as transactions_cleaned.csv")
