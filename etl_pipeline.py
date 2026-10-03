"""
Snowflake ELT Pipeline (company-style)

Flow:
  inbox/*.csv  --EXTRACT-->  pandas
               --LOAD-->     Snowflake RAW
               --TRANSFORM-> CLEAN -> FINAL   (SQL, Snowflake-க்குள்ளேயே = ELT)
               --TEST-->     data quality checks
               --ARCHIVE-->  processed file archive folder-க்கு

Run:  py etl_pipeline.py
"""
import glob
import logging
import os
import shutil
import sys
import uuid

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# ---------------- CONFIG ----------------
BASE = os.path.dirname(os.path.abspath(__file__))
INBOX = os.path.join(BASE, "inbox")
ARCHIVE = os.path.join(BASE, "archive")
FAILED = os.path.join(BASE, "failed")
LOGS = os.path.join(BASE, "logs")
for d in (INBOX, ARCHIVE, FAILED, LOGS):
    os.makedirs(d, exist_ok=True)

DB = "ETL_DB"
WAREHOUSE = "ETL_WH"
REQUIRED_COLS = ["EMP_ID", "FIRST_NAME", "LAST_NAME", "DEPARTMENT", "SALARY", "HIRE_DATE"]

# Password code-ல் எழுத வேண்டாம். Environment variables-ல் வைக்கவும்.
SF_ACCOUNT = os.environ.get("SNOWFLAKE_ACCOUNT")
SF_USER = os.environ.get("SNOWFLAKE_USER")
SF_PASSWORD = os.environ.get("SNOWFLAKE_PASSWORD")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOGS, "pipeline.log"), encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("etl")


# ---------------- HELPERS ----------------
def connect():
    if not all([SF_ACCOUNT, SF_USER, SF_PASSWORD]):
        raise SystemExit(
            "SNOWFLAKE_ACCOUNT / SNOWFLAKE_USER / SNOWFLAKE_PASSWORD "
            "environment variables set செய்யவும்."
        )
    return snowflake.connector.connect(
        account=SF_ACCOUNT, user=SF_USER, password=SF_PASSWORD,
        warehouse=WAREHOUSE, database=DB,
    )


def audit(conn, batch_id, step, status, detail=""):
    """ஒவ்வொரு படியையும் AUDIT.PIPELINE_LOG-ல் எழுது."""
    log.info("[%s] %s - %s %s", batch_id[:8], step, status, detail)
    conn.cursor().execute(
        f"INSERT INTO {DB}.AUDIT.PIPELINE_LOG (BATCH_ID, STEP, STATUS, DETAIL) "
        "VALUES (%s, %s, %s, %s)",
        (batch_id, step, status, str(detail)[:1000]),
    )


def scalar(conn, sql, params=None):
    return conn.cursor().execute(sql, params).fetchone()[0]


# ---------------- 1. EXTRACT ----------------
def extract(path):
    df = pd.read_csv(path, dtype=str)           # எல்லாவற்றையும் text ஆக படி
    df.columns = [c.strip().upper() for c in df.columns]
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"CSV-ல் இந்த columns இல்லை: {missing}")
    return df[REQUIRED_COLS]


# ---------------- 2. LOAD (RAW) ----------------
def load_raw(conn, df, file_name, batch_id):
    df = df.copy()
    df["SOURCE_FILE"] = file_name
    df["BATCH_ID"] = batch_id
    df["LOAD_TS"] = pd.Timestamp.now()
    success, _, nrows, _ = write_pandas(
        conn, df, "EMPLOYEES_RAW", database=DB, schema="RAW"
    )
    if not success:
        raise RuntimeError("RAW load தோல்வி")
    return nrows


# ---------------- 3. TRANSFORM (RAW -> CLEAN -> FINAL) ----------------
CLEAN_SQL = f"""
CREATE OR REPLACE TABLE {DB}.CLEAN.EMPLOYEES AS
SELECT
    TRY_TO_NUMBER(TRIM(EMP_ID))            AS EMP_ID,
    INITCAP(TRIM(FIRST_NAME))              AS FIRST_NAME,
    INITCAP(TRIM(LAST_NAME))               AS LAST_NAME,
    UPPER(TRIM(DEPARTMENT))                AS DEPARTMENT,
    TRY_TO_NUMBER(TRIM(SALARY), 12, 2)     AS SALARY,
    TRY_TO_DATE(TRIM(HIRE_DATE))           AS HIRE_DATE,
    LOAD_TS
FROM {DB}.RAW.EMPLOYEES_RAW
WHERE TRY_TO_NUMBER(TRIM(EMP_ID)) IS NOT NULL
  AND TRY_TO_NUMBER(TRIM(SALARY), 12, 2) > 0
  AND TRY_TO_DATE(TRIM(HIRE_DATE)) IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY TRY_TO_NUMBER(TRIM(EMP_ID)) ORDER BY LOAD_TS DESC
) = 1
"""

FINAL_SQL = f"""
CREATE OR REPLACE TABLE {DB}.FINAL.DEPT_SALARY_SUMMARY AS
SELECT DEPARTMENT,
       COUNT(*)            AS EMP_COUNT,
       ROUND(AVG(SALARY),2) AS AVG_SALARY,
       MAX(SALARY)         AS MAX_SALARY
FROM {DB}.CLEAN.EMPLOYEES
GROUP BY DEPARTMENT
"""


def transform(conn):
    conn.cursor().execute(CLEAN_SQL)
    conn.cursor().execute(FINAL_SQL)


# ---------------- 4. TESTS ----------------
def run_tests(conn, batch_id, expected_rows):
    failures = []

    # Test 1: Row count - CSV rows = RAW-ல் இந்த batch rows
    raw_cnt = scalar(conn, f"SELECT COUNT(*) FROM {DB}.RAW.EMPLOYEES_RAW WHERE BATCH_ID = %s", (batch_id,))
    if raw_cnt != expected_rows:
        failures.append(f"Row count mismatch: CSV={expected_rows}, RAW={raw_cnt}")

    # Test 2: Null check - முக்கிய columns
    nulls = scalar(conn, f"""SELECT COUNT(*) FROM {DB}.CLEAN.EMPLOYEES
        WHERE EMP_ID IS NULL OR SALARY IS NULL OR HIRE_DATE IS NULL OR DEPARTMENT IS NULL""")
    if nulls:
        failures.append(f"CLEAN-ல் {nulls} rows-ல் NULL உள்ளது")

    # Test 3: Duplicate check
    dups = scalar(conn, f"""SELECT COUNT(*) FROM (
        SELECT EMP_ID FROM {DB}.CLEAN.EMPLOYEES GROUP BY EMP_ID HAVING COUNT(*) > 1)""")
    if dups:
        failures.append(f"{dups} duplicate EMP_ID")

    # Test 4: Schema check - RAW table columns மாறவில்லை
    cols = {r[0] for r in conn.cursor().execute(
        f"SELECT COLUMN_NAME FROM {DB}.INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_SCHEMA='RAW' AND TABLE_NAME='EMPLOYEES_RAW'").fetchall()}
    need = set(REQUIRED_COLS) | {"SOURCE_FILE", "BATCH_ID", "LOAD_TS"}
    if not need.issubset(cols):
        failures.append(f"RAW schema மாறியுள்ளது: missing {need - cols}")

    # Test 5: Reconciliation - FINAL மொத்தம் = CLEAN மொத்தம்
    clean_cnt = scalar(conn, f"SELECT COUNT(*) FROM {DB}.CLEAN.EMPLOYEES")
    final_cnt = scalar(conn, f"SELECT COALESCE(SUM(EMP_COUNT),0) FROM {DB}.FINAL.DEPT_SALARY_SUMMARY")
    if clean_cnt != final_cnt:
        failures.append(f"CLEAN={clean_cnt} but FINAL={final_cnt}")

    # தகவல் மட்டும்: எத்தனை rows reject ஆனது
    total_raw = scalar(conn, f"SELECT COUNT(DISTINCT TRIM(EMP_ID)) FROM {DB}.RAW.EMPLOYEES_RAW")
    audit(conn, batch_id, "REJECTED_INFO", "INFO",
          f"RAW unique ids={total_raw}, CLEAN={clean_cnt}")
    return failures


# ---------------- ORCHESTRATION ----------------
def process_file(conn, path):
    file_name = os.path.basename(path)
    batch_id = str(uuid.uuid4())
    try:
        audit(conn, batch_id, "START", "OK", file_name)

        df = extract(path)
        audit(conn, batch_id, "EXTRACT", "OK", f"{len(df)} rows")

        n = load_raw(conn, df, file_name, batch_id)
        audit(conn, batch_id, "LOAD_RAW", "OK", f"{n} rows")

        transform(conn)
        audit(conn, batch_id, "TRANSFORM", "OK", "CLEAN + FINAL refreshed")

        failures = run_tests(conn, batch_id, len(df))
        if failures:
            raise AssertionError("; ".join(failures))
        audit(conn, batch_id, "TESTS", "PASSED", "5/5")

        shutil.move(path, os.path.join(ARCHIVE, file_name))
        audit(conn, batch_id, "END", "SUCCESS", file_name)
    except Exception as e:
        log.exception("Pipeline தோல்வி")
        audit(conn, batch_id, "END", "FAILED", e)
        shutil.move(path, os.path.join(FAILED, file_name))
        # இங்கே company-ல் email/Slack alert அனுப்புவார்கள்


def main():
    files = sorted(glob.glob(os.path.join(INBOX, "*.csv")))
    if not files:
        log.info("inbox-ல் புதிய file இல்லை.")
        return
    conn = connect()
    try:
        for f in files:
            process_file(conn, f)
    finally:
        conn.close()


if __name__ == "__main__":
    main()

