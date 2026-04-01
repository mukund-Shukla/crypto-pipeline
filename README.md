# Crypto Market Analytics Pipeline

An end-to-end ETL pipeline that extracts live cryptocurrency market data from the CoinGecko API, transforms it using Python and pandas, and loads it into Snowflake for time-series analytics.

Built as a portfolio project demonstrating real-world data engineering patterns — pagination, retry logic, idempotency, data quality checks, partitioned storage, and backfill capability.

---

## Table of Contents

- [What This Project Does](#what-this-project-does)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Tech Stack](#tech-stack)
- [How to Run It](#how-to-run-it)
- [Business Questions Answered](#business-questions-answered)
- [Data Model](#data-model)
- [Design Decisions and Trade-offs](#design-decisions-and-trade-offs)
- [What I Would Add Next](#what-i-would-add-next)

---

## What This Project Does

This pipeline runs daily to collect the top 250 cryptocurrencies by market cap and store a complete snapshot in Snowflake. Over time, the table accumulates a full history of daily market conditions — enabling trend analysis, dominance shifts, and volatility tracking across any date range.

The pipeline handles the full data lifecycle:

- **Extraction** — fetches paginated API responses with retry logic and rate limiting
- **Transformation** — cleans nulls, enforces types, deduplicates, and adds derived metrics
- **Loading** — bulk loads to Snowflake with idempotency guarantees and load verification
- **Observability** — every run is recorded in a `pipeline_runs` metadata table in Snowflake

---

## Architecture

```
CoinGecko REST API
        |
        | HTTP GET (paginated, 100 coins/page, 3 pages)
        |
   [ extract.py ]
   - Retry with exponential backoff
   - Rate limiting between pages
   - Saves raw JSON (immutable, timestamped)
        |
        v
raw/date=YYYY-MM-DD/data_YYYYMMDD_HHMMSS.json
        |
        |
   [ transform.py ]
   - Data quality checks
   - Null handling, type casting, deduplication
   - Derived metrics (market dominance, momentum, etc.)
   - Saves as Parquet (columnar, partitioned by date)
        |
        v
processed/date=YYYY-MM-DD/data_YYYYMMDD_HHMMSS.parquet
        |
        |
   [ load.py ]
   - Idempotency check (skip if date already loaded)
   - Bulk load via write_pandas (Snowflake COPY INTO)
   - Row count verification
   - Records run in pipeline_runs metadata table
        |
        v
Snowflake
  - crypto_analytics   (daily snapshot fact table)
  - pipeline_runs      (pipeline audit log)

All stages orchestrated by pipeline.py
```

---

## Project Structure

```
crypto-pipeline/
|
+-- src/
|   +-- extract.py       # Stage 1: API extraction
|   +-- transform.py     # Stage 2: Cleaning and enrichment
|   +-- load.py          # Stage 3: Snowflake loading
|   +-- pipeline.py      # Orchestrator and entry point
|
+-- raw/                 # Raw JSON files (gitignored)
|   +-- date=2026-03-30/
|       +-- data_20260330_090000.json
|
+-- processed/           # Transformed Parquet files (gitignored)
|   +-- date=2026-03-30/
|       +-- data_20260330_090015.parquet
|
+-- logs/
|   +-- pipeline.log     # Persistent log of all runs
|
+-- .env                 # Snowflake credentials (gitignored)
+-- config.yaml          # All pipeline configuration
+-- requirements.txt     # Python dependencies
+-- .gitignore
+-- README.md
```

---

## Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| Language | Python 3.12 | Pipeline logic |
| API Client | requests | HTTP calls to CoinGecko |
| Transformation | pandas, pyarrow | Data cleaning and Parquet writing |
| Storage (raw) | Local filesystem | Immutable raw JSON, date-partitioned |
| Storage (processed) | Local filesystem | Cleaned Parquet, date-partitioned |
| Data Warehouse | Snowflake | Analytics-ready snapshot table |
| Config | PyYAML | Externalised configuration |
| Credentials | python-dotenv | Environment variable management |
| Logging | Python logging | File + terminal logging with severity |

---

## How to Run It

### Prerequisites

- Python 3.8+
- Snowflake free trial account (snowflake.com)
- CoinGecko API — no key required

### Setup

```bash
# 1. Clone the repo
git clone https://github.com/mukund_shukla/crypto-pipeline.git
cd crypto-pipeline

# 2. Create and activate virtual environment
python -m venv venv
source venv/bin/activate        # Mac/Linux
venv\Scripts\activate           # Windows

# 3. Install dependencies
pip install -r requirements.txt

# 4. Create your .env file
cp .env.example .env
# Fill in your Snowflake credentials in .env

# 5. Create Snowflake warehouse and database
# Run this in your Snowflake worksheet:
# CREATE WAREHOUSE IF NOT EXISTS dev_wh WITH WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60;
# CREATE DATABASE IF NOT EXISTS crypto_db;
```

### Environment variables (.env)

```
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_WAREHOUSE=dev_wh
SNOWFLAKE_DATABASE=crypto_db
SNOWFLAKE_SCHEMA=public
```

> Your Snowflake account identifier is in your login URL.
> Example: `https://abc12345.us-east-1.snowflakecomputing.com` → account is `abc12345.us-east-1`

### Running the pipeline

```bash
# Full daily run — extract + transform + load
python src/pipeline.py

# Explicit full run
python src/pipeline.py run

# Transform and load only (raw data must already exist for that date)
python src/pipeline.py transform 2026-03-30

# Backfill from a date to today
python src/pipeline.py backfill 2026-03-01

# Backfill a specific date range
python src/pipeline.py backfill 2026-03-01 2026-03-30
```

### Running individual stages (for development and debugging)

```bash
# Test extraction only
python src/extract.py

# Test transformation only (runs today's date)
python src/transform.py

# Test transformation for a specific date
python src/transform.py 2026-03-30

# Test load only
python src/load.py

# Test load for a specific date
python src/load.py 2026-03-30
```

### Verifying the pipeline ran successfully

After a full run, check Snowflake:

```sql
-- Confirm data loaded for today
SELECT COUNT(*), load_date
FROM crypto_analytics
GROUP BY load_date
ORDER BY load_date DESC;

-- Check pipeline run history
SELECT load_date, rows_loaded, status, loaded_at, notes
FROM pipeline_runs
ORDER BY loaded_at DESC
LIMIT 10;
```

---

## Business Questions Answered

Once the pipeline has run for several days, the following questions become answerable:

### Q1 — What is the total crypto market cap and how did it change day over day?

```sql
SELECT
    load_date,
    ROUND(SUM(market_cap) / 1e12, 3)      AS total_market_cap_trillion,
    ROUND(LAG(SUM(market_cap) / 1e12)
          OVER (ORDER BY load_date), 3)   AS prev_day_trillion,
    ROUND(
        (SUM(market_cap) - LAG(SUM(market_cap))
            OVER (ORDER BY load_date))
        / LAG(SUM(market_cap))
            OVER (ORDER BY load_date) * 100
    , 2)                                   AS day_over_day_change_pct
FROM crypto_analytics
GROUP BY load_date
ORDER BY load_date DESC
LIMIT 30;
```

### Q2 — How has Bitcoin's price trended over the last 30 days?

The daily snapshot table accumulates history — enabling time-series queries across any date range.

```sql
SELECT
    load_date,
    current_price,
    pct_change_24h,
    pct_change_7d,
    momentum
FROM crypto_analytics
WHERE coin_id = 'bitcoin'
  AND load_date >= DATEADD(day, -30, CURRENT_DATE)
ORDER BY load_date ASC;
```

### Q3 — What percentage of the market is in a downtrend today?

The `momentum` derived column classifies each coin into a trend category based on
24h and 7d price change. This gives a market sentiment snapshot in a single query.

```sql
SELECT
    momentum,
    COUNT(*)                                           AS coin_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct_of_market,
    ROUND(SUM(market_cap), 0)                          AS total_market_cap_usd
FROM crypto_analytics
WHERE load_date = CURRENT_DATE
GROUP BY momentum
ORDER BY coin_count DESC;
```

### Q4 — Which coins are consistently in the top 20 by market cap over the last 7 days?

```sql
SELECT
    coin_id,
    name,
    COUNT(DISTINCT load_date)          AS days_in_top20,
    ROUND(AVG(market_cap_rank), 1)     AS avg_rank,
    ROUND(AVG(current_price), 2)       AS avg_price_7d
FROM crypto_analytics
WHERE load_date >= DATEADD(day, -7, CURRENT_DATE)
  AND market_cap_rank <= 20
GROUP BY coin_id, name
HAVING COUNT(DISTINCT load_date) = 7   -- present every single day
ORDER BY avg_rank ASC;
```

---

## Data Model

### crypto_analytics

One row per coin per day. The table grows by 250 rows with each daily run.

| Column | Type | Description |
|---|---|---|
| coin_id | VARCHAR | CoinGecko unique identifier (e.g. 'bitcoin') |
| symbol | VARCHAR | Ticker symbol (e.g. 'BTC') |
| name | VARCHAR | Display name |
| current_price | FLOAT | Price in USD at extraction time |
| market_cap | FLOAT | Total market capitalisation in USD |
| market_cap_rank | INT | Rank by market cap (1 = largest) |
| total_volume | FLOAT | 24h trading volume in USD |
| circulating_supply | FLOAT | Coins currently in circulation |
| total_supply | FLOAT | Total coins ever created (0 if unknown) |
| pct_change_1h | FLOAT | Price change % in last 1 hour |
| pct_change_24h | FLOAT | Price change % in last 24 hours |
| pct_change_7d | FLOAT | Price change % in last 7 days |
| pct_from_ath | FLOAT | % below all-time high |
| market_dominance_pct | FLOAT | This coin's % share of total market cap |
| volume_to_market_cap | FLOAT | Liquidity ratio (volume / market cap) |
| supply_circulation_ratio | FLOAT | Circulating / total supply |
| momentum | VARCHAR | Trend label: strong_uptrend, mild_uptrend, neutral, mild_downtrend, strong_downtrend |
| last_updated_at | TIMESTAMP_TZ | When CoinGecko last updated this coin |
| transformed_at | TIMESTAMP_TZ | When the transformation stage ran |
| load_date | DATE | Partition key — the date this row was loaded |

**Table grain:** One row per `coin_id` per `load_date`. The combination is unique.

**Idempotency:** The `load_date` column is used as the idempotency key. Before every load, the pipeline checks if rows exist for that date. If they do, the load is skipped. To force a reload for a specific date:

```sql
DELETE FROM crypto_analytics WHERE load_date = '2026-03-30';
-- Then re-run: python src/pipeline.py transform 2026-03-30
```

### pipeline_runs

Audit log of every pipeline execution.

| Column | Type | Description |
|---|---|---|
| run_id | VARCHAR | Auto-generated UUID |
| load_date | DATE | Which date this run processed |
| rows_loaded | INT | Number of rows loaded (0 for skips and failures) |
| loaded_at | TIMESTAMP_TZ | When this run executed |
| status | VARCHAR | SUCCESS, SKIPPED, or FAILED |
| notes | VARCHAR | Failure message or skip reason |

---

## Design Decisions and Trade-offs

### Raw data is immutable

**Decision:** Raw JSON files are never overwritten or modified after being written.
Each run creates a new file with a timestamp in the filename.

**Why:** If transformation code has a bug, the raw files are the safety net.
Reprocessing is always possible without re-calling the API.
Re-calling CoinGecko for a historical date would return today's prices, not that date's prices —
the raw files are the only true historical record.

**Trade-off:** Storage grows with every run. For daily runs over a year, this is negligible.
At hourly frequency or 5000+ coins, a retention policy would be needed.

---

### One snapshot per day (daily grain)

**Decision:** The pipeline loads one set of 250 rows per day, not one per run.
Subsequent runs on the same day are skipped.

**Why:** CoinGecko free tier is not a real-time feed.
The use case is daily trend analysis — not intraday price tracking.
A clearly defined grain (one row per coin per day) makes queries predictable and simple.

**Trade-off:** Intraday price swings are not captured.
If the business requirement changed to hourly snapshots,
the grain would change to `coin_id + snapshot_timestamp`
and the idempotency logic would need updating.

---


### Hive-style date partitioning from day one

**Decision:** Both raw and processed folders use `date=YYYY-MM-DD/` naming.

**Why:** This is the convention understood natively by AWS Athena, Spark, and Glue.
When this project is extended to use S3 as the storage layer,
the folder structure requires zero changes —
query engines will automatically use partitions to skip irrelevant data.

**Trade-off:** Slightly more complex folder structure than a flat directory.
Worth it for forward compatibility with cloud data lake tools.

---

### Local storage instead of S3

**Decision:** Raw and processed files are stored on the local filesystem.

**Why:** Keeps the project self-contained and runnable without AWS credentials.

**Trade-off:** Does not survive a machine crash.
Not suitable for production — production would use S3 or GCS.
This is documented as the primary "what I would add next" item.

---

### Config-driven, no hardcoding

**Decision:** Every value that might change — API URL, coin count, retry attempts,
file paths, log level — lives in `config.yaml`.

**Why:** The same code runs in development and production with different configs.
Changing the coin count from 250 to 500 means editing one line, not hunting through code.

**Trade-off:** Adds a config file to manage.
Worth it for any project that runs more than once or in more than one environment.

---

## What I Would Add Next

**Airflow scheduling** — replace manual execution with a daily Airflow DAG
that triggers `pipeline.py run` on a schedule and retries on failure.

**Move storage to AWS S3** — replace local `raw/` and `processed/` directories
with S3 buckets. The Hive partition structure is already compatible —
only the path prefix in config needs updating.

**Alerting** — send a Slack or email notification when `pipeline_runs`
records a FAILED status. Currently failures are only visible in logs and the metadata table.

**Data retention policy** — add a cleanup step that deletes raw JSON files
older than 90 days while keeping processed Parquet files indefinitely.
At daily frequency the storage impact is small,
but building the habit of retention management is good engineering practice.

---

## Author

Built by Mukund Shukla as part of a data engineering portfolio.
Stack: Python · pandas · Snowflake · pyarrow · CoinGecko API