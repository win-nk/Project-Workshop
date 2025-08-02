# Data Collection
from dotenv import load_dotenv
import os
import sqlalchemy
import pandas as pd
import requests


# Step 1) อ่านข้อมูลจาก MySQL database
load_dotenv() #คำสั่ง load_dotenv() เป็นการอ่านไฟล์ .env เข้ามาในตัวแปร environment variable แล้วใช้ os.getenv() เพื่ออ่านค่าของ variable แต่ละตัวอีกที
class Config:
    load_dotenv()
    MYSQL_HOST = os.getenv("MYSQL_HOST")
    MYSQL_PORT = os.getenv("MYSQL_PORT")  
    MYSQL_USER = os.getenv("MYSQL_USER")
    MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
    MYSQL_DB = 'r2de3'
    MYSQL_CHARSET = 'utf8mb4'


engine = sqlalchemy.create_engine(
    "mysql+pymysql://{user}:{password}@{host}:{port}/{db}".format(
        user=Config.MYSQL_USER,
        password=Config.MYSQL_PASSWORD,
        host=Config.MYSQL_HOST,
        port=Config.MYSQL_PORT,
        db=Config.MYSQL_DB,
    )
)

# Query Table (วิธีที่ 1: sqlalchemy)
with engine.connect() as connection:
  product_result = connection.execute(sqlalchemy.text(f"SELECT * FROM product ")).fetchall()
product = pd.DataFrame(product_result)

# Query Table (วิธีที่ 2: Pandas) -- better
customer = pd.read_sql("SELECT * FROM r2de3.customer", engine)
transaction = pd.read_sql("SELECT * FROM transaction", engine)


# Join tables: product & customer & transaction
merged_transaction = (
    transaction
    .merge(product, how="left", left_on="ProductNo", right_on="ProductNo")
    .merge(customer, how="left", left_on="CustomerNo", right_on="CustomerNo")
)


# Step 2) ดึงข้อมูลการแปลงค่าเงินจาก API ด้วย Requests
url = "https://r2de3-currency-api-vmftiryt6q-as.a.run.app/gbp_thb"
r = requests.get(url)
r.status_code
result_conversion_rate = r.json()
conversion_rate = pd.DataFrame(result_conversion_rate)
conversion_rate = conversion_rate.drop(columns=['id'])
conversion_rate['date'] = pd.to_datetime(conversion_rate['date'])
conversion_rate.info()


# Step 3) Join the data มารวมข้อมูลกัน
final_df = merged_transaction.merge(conversion_rate, how = 'left', left_on=  'Date', right_on= 'date' ) 
final_df["total_amount"] = final_df["Price"] * final_df["Quantity"]
final_df["thb_amount"] = final_df["total_amount"] * final_df["gbp_thb"]
'''
# อีกวิธีหนึ่ง คือ การใช้ function apply ของ DataFrame
def convert_rate(price, rate):
  return price * rate

# ใช้ lambda function แบบเขียน function การคำนวณไปเลยในบรรทัดเดียว
final_df["thb_amount"] = final_df.apply(lambda row: row["total_amount"] * row["gbp_thb"], axis=1)

# ใช้ lambda function แบบเรียกใช้ function ที่มีอยู่แล้ว (ประกาศ convert_rate ไว้ด้านบน)
final_df["thb_amount"] = final_df.apply(lambda row: convert_rate(row["total_amount"], row["gbp_thb"]), axis=1)
final_df
'''
# drop column ที่ไม่ใช้ axis = 1 ↓ หมายถึง drop column (ถ้า axis=0 → จะใช้ drop row ได้)
final_df = final_df.drop(["date", "gbp_thb"], axis=1)
final_df.columns #List ชื่อ column ทั้งหมด

# เปลี่ยนชื่อ column ให้เป็นตัวพิมพ์เล็ก และเปลี่ยนชื่อ column ที่ลงท้ายด้วย No ให้เป็น _id
final_df.columns = ['transaction_id', 'date', 'product_id', 'price', 'quantity', 'customer_id',
       'product_name', 'customer_country', 'customer_name', 'total_amount','thb_amount']

# Step 4) Output ไฟล์ผลลัพธ์
final_df.to_parquet("output.parquet", index=False)
# final_df.to_csv("output.csv", index=False)
print("Generated Succesfully")

# Read file
# check_parquet = pd.read_parquet("output.parquet")
