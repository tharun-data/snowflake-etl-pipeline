# Snowflake ELT Pipeline

A beginner-friendly ELT pipeline built with Python and Snowflake.

## Architecture

```
inbox/*.csv -> Extract (pandas) -> Load to RAW -> Transform (SQL) -> CLEAN -> FINAL
Data quality tests -> AUDIT log
```

| Layer | Purpose |
|-------|---------|
| RAW   | Data exactly as received (all columns STRING) |
| CLEAN | Trimmed, typed, de-duplicated, invalid rows removed |
| FINAL | Report-ready department salary summary |
