\# Snowflake ELT Pipeline



A beginner-friendly ELT pipeline built with Python and Snowflake.



\## Architecture



inbox/\*.csv -> Extract (pandas) -> Load to RAW -> Transform (SQL) -> CLEAN -> FINAL

Data quality tests -> AUDIT log



| Layer | Purpose |

|-------|---------|

| RAW   | Data exactly as received (all columns STRING) |

| CLEAN | Trimmed, typed, de-duplicated, invalid rows removed |

| FINAL | Report-ready department salary summary |

| AUDIT | Step-by-step pipeline run log |



\## Data quality tests (5)



1\. Row count: CSV rows = rows loaded into RAW

2\. No NULLs in key columns of CLEAN

3\. No duplicate EMP\_ID

4\. RAW schema has all expected columns

5\. Reconciliation: FINAL total = CLEAN total



\## Setup



1\. Run 01\_snowflake\_setup.sql once in a Snowflake worksheet.

2\. Install dependencies: py -m pip install -r requirements.txt

3\. Set credentials as environment variables (never hard-code them):

&#x20;  SNOWFLAKE\_ACCOUNT, SNOWFLAKE\_USER, SNOWFLAKE\_PASSWORD

4\. Generate a sample CSV: py make\_sample\_data.py

5\. Run the pipeline: py etl\_pipeline.py



\## Tech stack



Python, pandas, Snowflake, SQL

