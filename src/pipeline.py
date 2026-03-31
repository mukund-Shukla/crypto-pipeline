import sys
import logging
import yaml
from datetime import datetime, timedelta,timezone,date
from pathlib import Path


# ── Load config ───────────────────────────────────────────────
with open("config.yaml") as f:
    config = yaml.safe_load(f)

# ── Logging ───────────────────────────────────────────────────
logging.basicConfig(
    level=config["logging"]["level"],
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(config["logging"]["log_file"]),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ── Import pipeline stages ────────────────────────────────────
# Each stage is imported as a module — pipeline.py only calls
# the master function from each. It never touches internal logic.


from extract import fetch_market_data,save_raw_data
from transform import transform,backfill as transform_backfill
from load import load


# ─────────────────────────────────────────────────────────────
# Each function runs one stage and handles its failure cleanly.
# Separating them means a failure in extract doesn't silently
# skip transform — it fails loudly with context.
# ─────────────────────────────────────────────────────────────

def run_extract() -> Path:
    """
    Run extraction stage.
    Returns path to saved raw file.
    Raises if extraction fails — pipeline should not continue.
    """
    logger.info("=" * 40)
    logger.info("STAGE 1: EXTRACT")
    logger.info("=" * 40)
    try:
        data=fetch_market_data()
        raw_path=save_raw_data(data)
        logger.info(f"Extracted data saved to {raw_path}")
        return raw_path
    except Exception as e:
        logger.error(f"Extract stage failed: {e}")
        raise

def run_transform(date_str:str)->Path:
    """
    Run transformation stage.
    Returns path to saved transformed file.
    Raises if transformation fails — pipeline should not continue.
    """
    logger.info("=" * 40)
    logger.info("STAGE 2: TRANSFORM")
    logger.info("=" * 40)
    try:
        processed_path=transform(date_str)
        logger.info(f"Transformed data saved to {processed_path}")
        return processed_path
    except Exception as e:
        logger.error(f"Transform stage failed: {e}")
        raise


def run_load(date_str:str)->None:
    """
    Run loading stage.
    Raises if loading fails — pipeline should not continue.
    """
    logger.info("=" * 40)
    logger.info("STAGE 3: LOAD")
    logger.info("=" * 40)
    try:
        load(date_str)
        logger.info(f"Data loaded successfully for {date_str}.")
    except Exception as e:
        logger.error(f"Load stage failed: {e}")
        raise

# ─────────────────────────────────────────────────────────────
# PIPELINE MODES
# ─────────────────────────────────────────────────────────────

def run_full_pipeline():
    """
    Run the full pipeline: Extract → Transform → Load for today
    Each stage must succeed for the next to run.
    """
    today = datetime.now().strftime("%Y-%m-%d")

    logger.info("=" * 40)
    logger.info("CRYPTO PIPELINE — FULL RUN")
    logger.info(f"Date : {today}")
    logger.info(f"Start: {datetime.now(timezone.utc).isoformat()}")
    logger.info("=" * 40)
    
    start_time = datetime.now()

    try:
        # Stage 1 — Extract
        # Must succeed before transform can run
        run_extract()
        # Stage 2 — Transform
        # Must succeed before load can run
        run_transform(today)
        # Stage 3 — Load
        run_load(today)

        # ── Pipeline summary ──────────────────────────────────
        duration = (datetime.now() - start_time).seconds
        logger.info("=" * 40)
        logger.info("PIPELINE COMPLETE")
        logger.info(f"Date    : {today}")
        logger.info(f"Duration: {duration}s")
        logger.info(f"Status  : SUCCESS")
        logger.info("=" * 40)
    
    except Exception as e:
        duration = (datetime.now() - start_time).seconds
        logger.error("=" * 40)
        logger.error("PIPELINE FAILED")
        logger.error(f"Date    : {today}")
        logger.error(f"Duration: {duration}s")
        logger.error(f"Error   : {e}")
        logger.error("=" * 40)

        raise

def run_transform_only(date_str:str=None):
    """
    Transform + load only — skips extraction.
    Used when raw data already exists but needs reprocessing.
    For example: bug in transform logic, schema change.
    """

    if date_str is None:
        date_str = datetime.now().strftime("%Y-%m-%d")

    logger.info("=" * 40)
    logger.info("CRYPTO PIPELINE — TRANSFORM + LOAD ONLY")
    logger.info(f"Date : {date_str}")

    run_transform(date_str)
    run_load(date_str)
    logger.info(f"Transform + Load complete for {date_str}.")
    logger.info("=" * 40)

def run_backfill(start_date:str,end_date:str=None):
    """
    Backfill mode — reprocess a date range.

    IMPORTANT: Backfill only runs transform + load.
    It never re-extracts. Raw data must already exist.
    Reason: CoinGecko returns current prices — calling it
    today for a historical date gives today's prices,
    not March 1st's prices. Raw files ARE the historical record.

    Flow per date:
      1. Check raw data exists
      2. Transform (creates new processed Parquet)
      3. Load to Snowflake
         - If already loaded: skipped by load()'s idempotency check
         - If not loaded: loads fresh

    To reload a date that's already in Snowflake:
      DELETE FROM crypto_analytics WHERE load_date = 'YYYY-MM-DD';
      Then re-run backfill for that date.
    """
    start=date.fromisoformat(start_date)
    end = date.fromisoformat(end_date) if end_date else date.today()

    if start>end:
        raise ValueError(f"{start_date} is after {end_date} — invalid range.")
    
    total_days=(end-start).days+1

    logger.info("=" * 40)
    logger.info("CRYPTO PIPELINE — BACKFILL")
    logger.info(f"From : {start_date}")
    logger.info(f"To   : {end}")
    logger.info(f"Days : {total_days}")
    logger.info("=" * 40)

    success = []
    skipped = []
    failed  = []

    current=start
    while current<=end:
        date_str=current.strftime("%Y-%m-%d")
        logger.info(f"Processiong {date_str}...")
        # Check raw data exists before attempting
        raw_partition=Path(config["storage"]["raw_dir"])/f"date={date_str}"
        if not raw_partition.exists():
            logger.warning(f"Raw data for {date_str} not found — skipping. Run extract for this date first")
            skipped.append(date_str)
            current+=timedelta(days=1)
            continue
        try:
            run_transform(date_str)
            run_load(date_str)
            success.append(date_str)
            logger.info(f"Backfill successful for {date_str}.")
        except Exception as e:
            logger.error(f"Backfill failed for {date_str}: {e}")
            failed.append(date_str)

        current+=timedelta(days=1)

    # ── Backfill summary ──────────────────────────────────────

    logger.info("=" * 40)
    logger.info("BACKFILL SUMMARY")
    logger.info(f"  Total   : {total_days} days")
    logger.info(f"  Success : {len(success)}")
    logger.info(f"  Skipped : {len(skipped)} — no raw data")
    logger.info(f"  Failed  : {len(failed)}")
    if skipped:
        logger.warning(f"  Skipped dates : {skipped}")
    if failed:
        logger.error(f"  Failed dates  : {failed}")
    logger.info("=" * 40)

    # If any dates failed, exit with error so schedulers know
    if failed:
        raise RuntimeError(
            f"Backfill completed with {len(failed)} failure(s): {failed}"
        )


# ─────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────

def print_usage():
    print("""
Usage:
  python src/pipeline.py                                  → full run today
  python src/pipeline.py run                              → full run today
  python src/pipeline.py transform 2026-03-30            → transform + load only
  python src/pipeline.py backfill 2026-03-01             → backfill from date to today
  python src/pipeline.py backfill 2026-03-01 2026-03-30  → backfill date range
    """)


if __name__ == "__main__":

    # No arguments — normal daily run
    if len(sys.argv) == 1 or sys.argv[1] == "run":
        run_full_pipeline()

    elif sys.argv[1] == "transform":
        # Transform + load only for a specific date
        # Usage: python src/pipeline.py transform 2026-03-30
        if len(sys.argv) < 3:
            print("Error: 'transform' mode requires a date argument")
            print("Usage: python src/pipeline.py transform 2026-03-30")
            sys.exit(1)
        run_transform_only(sys.argv[2])

    elif sys.argv[1] == "backfill":
        # Backfill a date range
        # Usage: python src/pipeline.py backfill 2026-03-01
        # Usage: python src/pipeline.py backfill 2026-03-01 2026-03-30
        if len(sys.argv) < 3:
            print("Error: 'backfill' mode requires at least a start date")
            print("Usage: python src/pipeline.py backfill 2026-03-01")
            sys.exit(1)
        start = sys.argv[2]
        end   = sys.argv[3] if len(sys.argv) > 3 else None
        run_backfill(start, end)

    else:
        print(f"Unknown command: '{sys.argv[1]}'")
        print_usage()
        sys.exit(1)