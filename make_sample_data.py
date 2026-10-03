"""inbox folder-ல் தவறுகள் உள்ள sample CSV உருவாக்கும் (practice-க்கு)."""
import os
from datetime import datetime

BASE = os.path.dirname(os.path.abspath(__file__))
INBOX = os.path.join(BASE, "inbox")
os.makedirs(INBOX, exist_ok=True)

rows = """emp_id,first_name,last_name,department,salary,hire_date
1,  arun ,kumar,it,55000,2021-04-12
2,priya,SELVAM,hr,48000,2020-01-05
3,Karthik,raj,Finance,62000,2019-07-23
3,Karthik,raj,Finance,62000,2019-07-23
4,divya,Lakshmi,it,abc,2022-02-10
5,Mohan,das,sales,51000,not-a-date
,Ravi,kumar,it,45000,2021-09-01
6,Anitha,Devi,hr,47000,2023-03-15
"""

name = f"employees_{datetime.now():%Y%m%d_%H%M%S}.csv"
path = os.path.join(INBOX, name)
with open(path, "w", encoding="utf-8") as f:
    f.write(rows)
print("உருவாக்கப்பட்டது:", path)
