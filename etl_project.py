import sqlite3
import pandas as pd

CSV_FILE = "sales_raw.csv"
DB_FILE = "sales.db"


def create_sample_data():
    raw = """order_id,date,product,quantity,price
1,2026-10-01,Pen,10,5
2,01/10/2026,Book,2,120
2,01/10/2026,Book,2,120
3,2026-10-02,pen,,5
4,2026-10-03,Bag,1,
5,2026-10-03,Book,3,120
"""
    with open(CSV_FILE, "w") as f:
        f.write(raw)


# 1. EXTRACT
def extract(path):
    df = pd.read_csv(path)
    print(f"Extract: {len(df)} வரிசைகள் எடுக்கப்பட்டன")
    return df


# 2. TRANSFORM
def transform(df):
    df = df.drop_duplicates()
    df["product"] = df["product"].str.strip().str.title()
    df["date"] = pd.to_datetime(df["date"], format="mixed", dayfirst=False)
    df["quantity"] = df["quantity"].fillna(1)
    df = df.dropna(subset=["price"])
    df["quantity"] = df["quantity"].astype(int)
    df["total"] = df["quantity"] * df["price"]
    print(f"Transform: {len(df)} சுத்தமான வரிசைகள்")
    return df


# 3. LOAD
def load(df, db):
    with sqlite3.connect(db) as conn:
        df.to_sql("sales", conn, if_exists="replace", index=False)
    print(f"Load: {db} -ல் சேமிக்கப்பட்டது")


def report(db):
    with sqlite3.connect(db) as conn:
        q = """SELECT product, SUM(quantity) AS units, SUM(total) AS revenue
               FROM sales GROUP BY product ORDER BY revenue DESC"""
        print(pd.read_sql(q, conn))


if __name__ == "__main__":
    create_sample_data()
    data = extract(CSV_FILE)
    clean = transform(data)
    load(clean, DB_FILE)
    report(DB_FILE)