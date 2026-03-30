import os
import logging
import yaml
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# ── Load config and environment ───────────────────────────────
env_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=env_path)
# print("ENV PATH:", env_path)
# print("EXISTS:", env_path.exists())

# from dotenv import dotenv_values
# print("ENV CONTENT:", dotenv_values(env_path))


with open("config.yaml") as f:
    config = yaml.safe_load(f)

# ── Logging ───────────────────────────────────────────────────
logging.basicConfig(
    level=config["logging"]["level"],
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.FileHandler(config["logging"]["log_file"]),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# STEP 1 — SNOWFLAKE CONNECTION
# ─────────────────────────────────────────────────────────────

def get_connection()->snowflake.connector.SnowflakeConnection:
    # Establishes and returns a connection to Snowflake using credentials from environment variables
    required_env_vars = ["SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD", "SNOWFLAKE_ACCOUNT", "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_DATABASE", "SNOWFLAKE_SCHEMA"]
    
    # Check all required vars are present before attempting connection. Better to fail with a clear message than a cryptic Snowflake error
    
    missing  = [var for var in required_env_vars if  not  os.getenv(var)]
    if missing :
        raise EnvironmentError(f"Missing required environment variables: {', '.join(missing  )}")
    
    try:
        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA")
        )
        logger.info(f"Successfully connected to Snowflake - " f"Database: {os.getenv('SNOWFLAKE_DATABASE')}, Schema: {os.getenv('SNOWFLAKE_SCHEMA')}")
        return conn
    
    except Exception as e:
        logger.error(f"Failed to connect to Snowflake: {e}")
        raise

def create_table_if_not_exists(conn:snowflake.connector.SnowflakeConnection)->None:
    # Creates the target table in Snowflake if it doesn't already exist. This is idempotent and can be safely run before every load.
    create_crypto_table = f"""
    CREATE TABLE IF NOT EXISTS crypto_analytics  (
            coin_id                  VARCHAR,
            symbol                   VARCHAR,
            name                     VARCHAR,
            current_price            FLOAT,
            market_cap               FLOAT,
            market_cap_rank          INT,
            total_volume             FLOAT,
            circulating_supply       FLOAT,
            total_supply             FLOAT,
            max_supply               FLOAT,
            all_time_high            FLOAT,
            pct_from_ath             FLOAT,
            pct_change_1h            FLOAT,
            pct_change_24h           FLOAT,
            pct_change_7d            FLOAT,
            market_dominance_pct     FLOAT,
            volume_to_market_cap     FLOAT,
            supply_circulation_pct   FLOAT,
            momentum                 VARCHAR,
            last_updated_at          TIMESTAMP_TZ,
            transformed_at           TIMESTAMP_TZ,
            load_date                  DATE
    )
    """

    # Metadata table — tracks every load that has ever run
    # This is how we know if a date has already been loaded

    create_pipeline_runs_table="""
    CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id          VARCHAR DEFAULT UUID_STRING(),
            load_date       DATE,
            rows_loaded     INT,
            loaded_at       TIMESTAMP_TZ,
            status          VARCHAR,
            notes           VARCHAR
            )"""

    cursor=conn.cursor()
    try:
        cursor.execute(create_crypto_table)
        logger.info("Table crypto_analytics is ready.")

        cursor.execute(create_pipeline_runs_table)
        logger.info("Table pipeline_runs is ready.")
    except Exception as e:
        logger.error(f"Error creating table '{create_crypto_table}': {e}")
        raise

    finally:
        cursor.close()


# ─────────────────────────────────────────────────────────────
# STEP 3 — IDEMPOTENCY CHECK
# ─────────────────────────────────────────────────────────────

def already_loaded(conn:snowflake.connector.SnowflakeConnection,load_date:str)->bool:
    # Checks if data for the given load_date already exists in the target table. This prevents duplicate loads and ensures idempotency.
    query="Select count(*) from crypto_analytics where load_date=%s"
    cursor=conn.cursor()
    try:
        cursor.execute(query,(load_date,))
        count=cursor.fetchone()[0]
        if count>0:
            logger.warning(f"Data for load_date {load_date} already exists in crypto_analytics (count={count}). Skipping load to prevent duplicates." 
                           f"To reload, delete existing records for this date first." f"DELETE from crypto_analytics where load_date='{load_date}'")
            return True
        
        logger.info(f"No existing data found for load_date {load_date}. Safe to proceed with load.")
        return False
    
    except Exception as e:
        logger.error(f"Error checking for existing data for load_date {load_date}: {e}")
        raise

    finally:
        cursor.close()


# ─────────────────────────────────────────────────────────────
# STEP 4 — READ PROCESSED PARQUET
# ─────────────────────────────────────────────────────────────


def get_latest_processed_file(date_str:str)->Path:
    # Finds the latest processed Parquet file for the given date. This is the file that will be loaded into Snowflake.
    
    partition_dir=Path(config["storage"]["processed_dir"])/f"date={date_str}"

    if not partition_dir.exists():
        raise FileNotFoundError(f"No processed data found for date {date_str} in {partition_dir}. Cannot proceed with load.Run transform.py first")

    files=sorted(partition_dir.glob("market_data_*.parquet"))  # Sort by filename (which includes timestamp) to get the latest file

    if not files:
        raise FileNotFoundError(f"No processed Parquet files found in {partition_dir}. Cannot proceed with load. Run transform.py first")

    latest=files[-1]
    
    logger.info(f"Found {len(files)} processed files for date {date_str}. Latest file: {latest.name}")
    return latest

def read_processed_data(filepath:Path,load_date:str)->pd.DataFrame:
    # Reads the processed Parquet file into a DataFrame and adds load_date column. This is the data that will be loaded into Snowflake.
    try:
        df=pd.read_parquet(filepath)
        df["load_date"]=load_date  # Add load_date column to the DataFrame so we can use it in Snowflake for partitioning and idempotency checks
        df.columns=[col.upper() for col in df.columns]  # Snowflake convention is uppercase column names
        logger.info(f"Read {len(df)} records from processed file {filepath.name}")
        return df
    except Exception as e:
        logger.error(f"Error reading processed Parquet file {filepath}: {e}")
        raise
        
        
# ─────────────────────────────────────────────────────────────
# STEP 5 — LOAD TO SNOWFLAKE
# ─────────────────────────────────────────────────────────────
def load_to_snowflake(conn:snowflake.connector.SnowflakeConnection,df:pd.DataFrame,table_name:str)->int:
    # Loads the DataFrame into Snowflake using the write_pandas utility. Returns the number of rows loaded.
    try:
        success, num_chunks, num_rows, _ = write_pandas(conn, df, table_name=table_name.upper(),overwrite=False,use_logical_type=True) 

        if not success:
            raise RuntimeError(f"write_pandas returned sucess=False for {table_name}")
        
        logger.info(f"Loaded {num_rows} rows into Snowflake table {table_name} in {num_chunks} chunks.")
        return num_rows
    
    except Exception as e:
        logger.error(f"Failed to load data into '{table_name}':{e}")
        raise



# ─────────────────────────────────────────────────────────────
# STEP 6 — LOAD VERIFICATION
# ─────────────────────────────────────────────────────────────

def verify_load(conn: snowflake.connector.SnowflakeConnection,load_date: str, expected_rows: int) -> bool:
    """
    After loading, query Snowflake to confirm the row count
    matches what was loaded.
    Never assume a load succeeded just because no error was thrown.
    Returns True if verification passes, False otherwise.
    """
    query  = "SELECT COUNT(*) FROM crypto_analytics WHERE load_date = %s"
    cursor = conn.cursor()

    try:
        cursor.execute(query, (load_date,))
        actual_rows = cursor.fetchone()[0]

        if actual_rows == expected_rows:
            logger.info(
                f"Load verification PASSED — "
                f"expected {expected_rows}, found {actual_rows} in Snowflake"
            )
            return True
        else:
            logger.error(
                f"Load verification FAILED — "
                f"expected {expected_rows}, found {actual_rows} in Snowflake"
            )
            return False

    finally:
        cursor.close()



# ─────────────────────────────────────────────────────────────
# STEP 7 — RECORD PIPELINE RUN
# ─────────────────────────────────────────────────────────────

def record_pipeline_run(
    conn: snowflake.connector.SnowflakeConnection,
    load_date: str,
    rows_loaded: int,
    status: str,
    notes: str = None
) -> None:
    """
    Insert a record into pipeline_runs after every load attempt.
    This gives you a complete history of every run —
    successes, failures, and skips — queryable from Snowflake.
    """
    query = """
        INSERT INTO pipeline_runs (load_date, rows_loaded, loaded_at, status, notes)
        VALUES (%s, %s, %s, %s, %s)
    """
    cursor = conn.cursor()
    try:
        cursor.execute(query, (
            load_date,
            rows_loaded,
            datetime.now(),
            status,
            notes
        ))
        logger.info(f"Pipeline run recorded — status: {status}, rows: {rows_loaded}")
    finally:
        cursor.close()


# ─────────────────────────────────────────────────────────────
# MASTER LOAD FUNCTION
# ─────────────────────────────────────────────────────────────

def load(date_str: str = None) -> None:
    """
    Full load pipeline for a given date.
    If no date provided, uses today.
    Flow:
    1. Connect to Snowflake
    2. Create tables if they don't exist
    3. Check if this date is already loaded (idempotency)
    4. Read processed Parquet
    5. Load to Snowflake
    6. Verify row count
    7. Record the run in pipeline_runs
    """

    if date_str is None:
        date_str = datetime.now().strftime("%Y-%m-%d")

    logger.info(f"Starting load for date: {date_str}")
    conn = get_connection()

    try:
        # Step 1 — Ensure tables exist
        create_table_if_not_exists(conn)

        # Step 2 — Idempotency check
        if already_loaded(conn, date_str):
            record_pipeline_run(
                conn, date_str, 0, "SKIPPED",
                "Data already exists for this date"
            )
            return

        # Step 3 — Read processed file
        filepath = get_latest_processed_file(date_str)
        df       = read_processed_data(filepath, date_str)

        # Step 4 — Load
        rows_loaded = load_to_snowflake(conn, df, "crypto_analytics")

        # Step 5 — Verify
        verified = verify_load(conn, date_str, rows_loaded)

        if verified:
            record_pipeline_run(conn, date_str, rows_loaded, "SUCCESS")
            logger.info(f"Load complete for {date_str} — {rows_loaded} rows")
        else:
            record_pipeline_run(
                conn, date_str, rows_loaded, "FAILED",
                "Row count mismatch after load"
            )
            raise RuntimeError(
                f"Load verification failed for {date_str}. "
                f"Check pipeline_runs table for details."
            )

    except Exception as e:
        # Record the failure before re-raising
        # so pipeline_runs always has an entry for every attempt
        try:
            record_pipeline_run(conn, date_str, 0, "FAILED", str(e))
        except Exception:
            pass   # don't let metadata recording failure hide the real error
        logger.error(f"Load failed for {date_str}: {e}")
        raise

    finally:
        # Always close the connection — even if something crashed
        conn.close()
        logger.info("Snowflake connection closed")


# ─────────────────────────────────────────────────────────────
# Run standalone for testing
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    date = sys.argv[1] if len(sys.argv) > 1 else None
    load(date)