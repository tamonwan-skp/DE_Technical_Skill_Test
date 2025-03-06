import pandas as pd
from datetime import datetime

# โหลดข้อมูลที่ผ่านการ clean แล้ว
df = pd.read_csv("transactions_cleaned.csv")

# คำนวณยอดขายรวมและรายได้รวมต่อสินค้า
product_sales = df.groupby("product_id").agg(
    total_quantity_sold=("quantity", "sum"),
    total_revenue=("price", "sum")
).reset_index()

print(" ยอดขายรวมและรายได้รวมต่อสินค้า:")
print(product_sales.head())

# หาธุรกรรมล่าสุดของแต่ละผู้ใช้และจำนวนวันที่ผ่านมา
df["transaction_date"] = pd.to_datetime(df["transaction_date"])
latest_transactions = df.groupby("customer_id")["transaction_date"].max().reset_index()
latest_transactions["days_ago"] = (datetime.now() - latest_transactions["transaction_date"]).dt.days

print("\n ธุรกรรมล่าสุดของแต่ละผู้ใช้:")
print(latest_transactions.head())

# บันทึกผลลัพธ์
product_sales.to_csv("product_sales_summary.csv", index=False)
latest_transactions.to_csv("latest_transactions_summary.csv", index=False)
import pandas as pd
from datetime import datetime

# โหลดข้อมูลที่ผ่านการ clean แล้ว
df = pd.read_csv("transactions_cleaned.csv")

# คำนวณยอดขายรวมและรายได้รวมต่อสินค้า
product_sales = df.groupby("product_id").agg(
    total_quantity_sold=("quantity", "sum"),
    total_revenue=("price", "sum")
).reset_index()

print(" ยอดขายรวมและรายได้รวมต่อสินค้า:")
print(product_sales.head())

# หาธุรกรรมล่าสุดของแต่ละผู้ใช้และจำนวนวันที่ผ่านมา
df["transaction_date"] = pd.to_datetime(df["transaction_date"])
latest_transactions = df.groupby("customer_id")["transaction_date"].max().reset_index()
latest_transactions["days_ago"] = (datetime.now() - latest_transactions["transaction_date"]).dt.days

print("\n ธุรกรรมล่าสุดของแต่ละผู้ใช้:")
print(latest_transactions.head())

# บันทึกผลลัพธ์
product_sales.to_csv("product_sales_summary.csv", index=False)
latest_transactions.to_csv("latest_transactions_summary.csv", index=False)

print("\n Data processing completed. Results saved as CSV files.")
print("\n Data processing completed. Results saved as CSV files.")
