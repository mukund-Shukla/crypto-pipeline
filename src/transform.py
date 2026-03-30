import json
import logging
import sys
import yaml
import pandas as pd
from datetime import date, datetime, timezone
from pathlib import Path

# Loading configuration from YAML file
with open('config.yaml') as f:
    config = yaml.safe_load(f)


# Setting up logging

logging.basicConfig(
    level=config["logging"]["level"],
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    handlers=[
        logging.FileHandler(config["logging"]["log_file"]),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


#Step 1 - Reading raw JSON data

    # Returns the most recently written raw file for a given date.
    # Filename format is data_YYYYMMDD_HHMMSS.json — sorting
    # alphabetically gives chronological order automatically.

def get_latest_raw_file(date_str: str) -> Path:
    partition_dir = Path(config["storage"]["raw_dir"]) / f"date={date_str}"

    if not partition_dir.exists():
        raise FileNotFoundError(f"No raw data found for date {date_str} in {partition_dir}, run extraction logic first")

    files=sorted(partition_dir.glob("market_data_*.json"))

    if not files:
        raise FileNotFoundError(f"Partition folder exists but no raw data files found in {partition_dir}")   

    latest_file=files[-1]
    logger.info(f"Found {len(files)} raw files for {date_str}. Using latest: {latest_file.name}")
    return latest_file

def load_raw(filepath:Path)->pd.DataFrame:
    
    
    with open(filepath,"r") as f:
        wrapper=json.load(f)

    extracted_at=wrapper["extracted_at"]
    record_count=wrapper["record_count"]
    data=wrapper["data"]

    if "data" not in wrapper:
        raise ValueError("Invalid raw file format")

    logger.info(f"Loaded {record_count} records from {filepath.name}, extracted at {extracted_at}")
    
    df=pd.DataFrame(data)
    return df


# STEP 2 - Data Quality Checks 

# Run these BEFORE transformation.
# If the raw data is fundamentally broken, fail loudly here
# rather than silently producing bad transformed data.


def run_quality_checks(df:pd.DataFrame)->None:

    logger.info("Running data quality checks on raw DataFrame")
    issues=[]

    #Check 1 - DataFrame is not empty

    if df.empty:
        raise ValueError("DataFrame is empty. No data to transform.")
    
    #Check 2 - Required columns are present

    required_columns=["id","symbol","name","current_price","market_cap","total_volume","market_cap_rank"]

    missing=[col for col in required_columns if col not in df.columns]


    #Check 3- Minimum row count - if we expect 250 coins but only got 10, something is likely wrong.

    if len(df)<10:
        raise ValueError(f"DataFrame has only {len(df)} rows, which is suspiciously low. Expected at least 10.")
    
    
    #Check 4- No duplicate coin IDs (warn,don't fail - some duplicates can be expected if we fetch multiple pages and trim to top N after)

    duplicate_count=df["id"].duplicated().sum()
    if duplicate_count > 0:
        issues.append(f"Found {duplicate_count} duplicate coin IDs in raw data. ")

    # Check 5 - Null rate in critical columns - if more than 10% of rows have nulls in key fields, that's a red flag.

    for col in ["current_price","market_cap"]:
        null_pct=df[col].isnull().mean()*100
        if null_pct > 10:
            issues.append(f"Column '{col}' has {null_pct:.2f}% null values, which is above the 10% threshold.")

    #Check 6 - Prices should be positive numbers - if we have negative prices, that's a clear data issue.

    if "current_price" in df.columns:
        negative_prices=(pd.to_numeric(df["current_price"], errors='coerce') < 0).sum()
        if negative_prices > 0:
            issues.append(f"Found {negative_prices} rows with negative 'current_price', which is invalid.")

    # Log all issues found, or log that all checks passed.
    if issues:
        for issue in issues:
            logger.warning("Data quality issues found in raw data:\n" + "\n".join(issue))
        else:
            logger.info("All data quality checks passed successfully.")

# STEP 3 - Data Transformation. Each function does one specific transformation, reusable and independently testable, and we can chain them together in a main transform function.

    # Keep only the columns we need and rename them to be clean.
    # Don't carry every API field forward — only what's useful.

def select_and_rename_columns(df:pd.DataFrame)->pd.DataFrame:
    column_map={
        "id":                                        "coin_id",
        "symbol":                                    "symbol",
        "name":                                      "name",
        "current_price":                             "current_price",
        "market_cap":                                "market_cap",
        "market_cap_rank":                           "market_cap_rank",
        "total_volume":                              "total_volume",
        "circulating_supply":                        "circulating_supply",
        "total_supply":                              "total_supply",
        "max_supply":                                "max_supply",
        "ath":                                       "all_time_high",
        "ath_change_percentage":                     "pct_from_ath",
        "price_change_percentage_1h_in_currency":    "pct_change_1h",
        "price_change_percentage_24h_in_currency":   "pct_change_24h",
        "price_change_percentage_7d_in_currency":    "pct_change_7d",
        "last_updated":                              "last_updated_at"
    }

    # Only keep columns that actually exist in the DataFrame
    # API responses sometimes omit optional fields
    
    existing_map={k:v for k,v in column_map.items() if k in df.columns}  #dictionary comprehension to filter column_map to only include keys that are in df.columns
    df=df[list(existing_map.keys())].rename(columns=existing_map)

    logger.info(f"Selected {len(df.columns)} and renamed them. Final columns: {df.columns.tolist()}")
    return df


def cast_types(df:pd.DataFrame)->pd.DataFrame:
    # Cast columns to appropriate types for analysis and storage.
    # Ex. current_price should be float, market_cap should be int, last_updated_at should be datetime, etc.
    # API returns everything as loose Python types — we enforce a strict schema.errors='coerce' means invalid values become NaN instead of crashing.

    numeric_cols=[
        "current_price","market_cap","total_volume",
        "circulating_supply","total_supply","max_supply",
        "all_time_high","pct_from_ath","pct_change_1h",
        "pct_change_24h","pct_change_7d"
        ]
    
    for col in numeric_cols:
        if col in df.columns:
            before_nulls=df[col].isnull().sum() # count how many nulls we have before coercion, to log if we are losing a lot of data due to type issues    
            df[col]=pd.to_numeric(df[col], errors='coerce')
            after_nulls=df[col].isnull().sum() # count how many nulls we have after coercion

            new_nulls=after_nulls-before_nulls
            if new_nulls > 0:
                logger.warning(f"Column '{col}': Coercion to numeric introduced {new_nulls} new null values, which may indicate data quality issues in the raw data.")
        
    
    # Timestamps - API returns ISO strings, we convert to pandas datetime for easier analysis later. errors='coerce' means invalid timestamps become NaT instead of crashing.

    if "last_updated_at" in df.columns:
        df["last_updated_at"]=pd.to_datetime(df["last_updated_at"], errors='coerce')    
        
    # Integer

    if "market_cap_rank" in df.columns:
        df["market_cap_rank"]=pd.to_numeric(df["market_cap_rank"], errors='coerce').astype("Int64") # nullable integer type in pandas

    logger.info("Casted columns to appropriate types.")
    return df


def handle_nulls(df:pd.DataFrame)->pd.DataFrame:
    # Handle null values in critical columns. We have options:
    # - Never silently drop rows — log every decision.
    # - Fill nulls with a placeholder value (ex. 0 for market_cap if we want to keep them but mark as unknown)
    # - Leave nulls as-is and handle them in analysis later (sometimes it's better to keep them as nulls rather than filling with potentially misleading values)

    inital_rows=len(df)

    #Supply fields : 0 is a valid substitute for nulls in supply fields, as it indicates no supply. We log how many nulls we are filling.

    for col in ["circulating_supply","total_supply","max_supply"]:
        if col in df.columns:
            null_count=df[col].isnull().sum()
            if null_count > 0:
                df[col]=df[col].fillna(0)
                logger.info(f"Filled {null_count} nulls in '{col}' with 0")

    # Price change fields: 0 means no change — again, we log how many nulls we are filling.

    for col in ["pct_change_1h","pct_change_24h","pct_change_7d"]:
        if col in df.columns:
            null_count=df[col].isnull().sum()
            if null_count > 0:
                df[col]=df[col].fillna(0)
                logger.info(f"Filled {null_count} nulls in '{col}' with 0")

    # Critical columns: If current_price or market_cap is null, that's a problem — we drop those rows but log how many we are dropping.
    critical_cols=["current_price","market_cap"]
    existing_critical=[col for col in critical_cols if col in df.columns]
    df=df.dropna(subset=existing_critical)

    dropped=inital_rows-len(df)
    if dropped > 0:
        logger.warning(f"Dropped {dropped} rows due to nulls in critical columns: {existing_critical}")
    else:  
        logger.info("No nulls found in critical columns, no rows dropped.")
    
    return df


def remove_duplicates(df:pd.DataFrame)->pd.DataFrame:

    # Remove duplicate coins by coin_id, keeping the one with the highest market cap (assuming it's the most accurate and up-to-date).
    # We log how many duplicates we found and removed.

    inital_rows=len(df)
    df=df.drop_duplicates(subset="coin_id", keep="first") # keep the first occurrence, which should be the one with the highest market cap if we fetched pages in order
    removed=inital_rows-len(df)
    if removed > 0:
        logger.warning(f"Removed {removed} duplicate rows based on 'coin_id'.")
    else:
        logger.info("No duplicate 'coin_id' found, no rows removed.")

    return df

def add_derived_metrics(df:pd.DataFrame)->pd.DataFrame:

    # Adding new columns computed from existing ones. This is the value-add layer — insights that weren't in the raw API.

    # 1 Market dominance percentage - what percentage of the total crypto market cap does each coin represent? This is a key metric in crypto analysis.
    total_market_cap=df["market_cap"].sum()
    df["market_dominance_pct"]=(df["market_cap"]/total_market_cap*100).round(4)

    # 2 Volume to market cap ratio - how much trading volume is there relative to the size of the coin? This can indicate how actively traded a coin is compared to its size.
    df["volume_to_market_cap"]=df["total_volume"]/df["market_cap"].replace(0,pd.NA).round(4) # avoiding division by zero, and round for cleaner presentation

    # 3 Supply circulation percentage - what % of total supply is circulating?
    df["supply_circulation_pct"]=(df["circulating_supply"]/df["total_supply"].replace(0,pd.NA)*100).round(4) 

    # 4 Momentum signal: simple label based on 24h and 7d price changes. This is a very basic example of a derived metric that combines multiple fields into a single insight.
    def momentum_label(row):
        change_24h=row.get("pct_change_24h",0) or 0
        change_7d=row.get("pct_change_7d",0) or 0
        if change_24h > 5 and change_7d > 10:
            return "Strong Bullish"
        elif change_24h < -5 and change_7d < -10:
            return "Strong Bearish"
        elif change_24h > 0 and change_7d > 0:
            return "mildly uptrend"
        elif change_24h < 0 and change_7d < 0:
            return "mildly downtrend"
        else:
            return "Neutral"
        
    df["momentum"]=df.apply(momentum_label, axis=1)

    # Processing timestamp to a consistent format (ex. UTC) if needed - in this case we assume API returns in UTC already, but we could add logic here to convert if not.

    df["transformed_at"]=datetime.now() # add a timestamp for when the transformation was done 

    #to put timezoen in utc, uncomment the following line and make sure to import pytz at the top of the file
    # df["transformed_at"]=datetime.now(timezone.utc)

    logger.info("Added derived metrics: market_dominance_pct, volume_to_market_cap, supply_circulation_pct, momentum, and transformed_at timestamp.")
    return df


# Step 4 - Save as Parquet 

    # Save transformed DataFrame as Parquet, partitioned by date.
    # Parquet is columnar — much faster than CSV for analytical queries.
    # No overwrite: uses timestamp in filename same as raw layer.

def save_processed_data(df:pd.DataFrame,date_str:str)->Path:
    timestamp=datetime.now().strftime("%Y-%m-%d_%H%M%S") # e.g. 20260329_143022
    partition_dir=Path(config["storage"]["processed_dir"])/f"date={date_str}"
    partition_dir.mkdir(parents=True, exist_ok=True) # create directory if it doesn't exist

    filepath=partition_dir/f"market_data_{timestamp}.parquet"
    if df.empty:
        logger.warning("DataFrame is empty. Skipping save.")
        return None

    df.to_parquet(filepath, index=False,engine="pyarrow") # we specify pyarrow engine for better performance and compatibility
    logger.info(f"Saved transformed data to {filepath} with {len(df)} records.")
    return filepath


# Step 5 - Transformation Summary

def log_transformation_summary(df_raw:pd.DataFrame,df_final:pd.DataFrame)->None:
    logger.info("-" * 50)
    logger.info("Transformation Summary:")
    logger.info(f"Rows before : {len(df_raw)}")
    logger.info(f"Rows after : {len(df_final)}")
    logger.info(f"Rows dropped: {len(df_raw)-len(df_final)}")
    logger.info(f"Columns after: {len(df_final.columns)}")
    logger.info(f"Null counts:")
    for col in df_final.columns:
        null_count = df_final[col].isnull().sum()
        if null_count > 0:
            logger.info(f"  {col}: {null_count} nulls remaining")
    logger.info("-" * 50)


# Transform function that chains all steps together

    # Full transformation pipeline for a given date.
    # If no date provided, uses today's date.
    # Returns path to the saved Parquet file.

def transform(date_str:str=None)->Path:

    if date_str is None:
        date_str=datetime.now().strftime("%Y-%m-%d")
    
    logger.info(f"Starting transformation for date {date_str}")

    # 1. Read
    raw_filepath=get_latest_raw_file(date_str)
    df_raw=load_raw(raw_filepath)
    df=df_raw.copy() # we keep the original raw DataFrame untouched for logging and comparison in the summary

    # 2. Quality Checks
    run_quality_checks(df)

    # 3. Transformations
    df=select_and_rename_columns(df)
    df=cast_types(df)
    df=handle_nulls(df)
    df=remove_duplicates(df)
    df=add_derived_metrics(df)

    # 4. Summary
    log_transformation_summary(df_raw, df)

    # 5. Save
    output_path=save_processed_data(df, date_str)

    logger.info(f"Transformation completed for date {date_str}. Output saved to {output_path}")
    return output_path
    

# Backfill logic - if we want to run transformation for a range of past dates, 
# we could add a function that loops through a list of date strings and calls transform() for each one. This allows us to easily backfill historical data if needed.

def backfill(start_date:str,end_date:str)->None:
    start=date.fromisoformat(start_date)
    end=date.fromisoformat(end_date) if end_date else date.today()

    if start>end:
        raise ValueError(f"Start date {start_date} must be before end date {end_date}")

    total_days=(end-start).days + 1
    logger.info(f"Starting backfill from {start_date} to {end_date} ({total_days} days)")

    success=[]
    skipped=[]
    failed=[]

    current=start
    while current <= end:
        date_str=current.strftime("%Y-%m-%d")
        try:
            #Checking if raw data exists for this date before attempting transformation
            partition_dir=Path(config["storage"]["raw_dir"])/f"date={date_str}"
            if not partition_dir.exists():
                logger.warning(f"No raw data found for {date_str}, skipping transformation for this date.")
                skipped.append(date_str)
                current += pd.Timedelta(days=1) # move to the next day
                continue

            logger.info(f"Processing transformation for {date_str}")
            transform(date_str)
            success.append(date_str)

        except Exception as e:
            logger.error(f"Error processing transformation for {date_str}: {e}")
            failed.append(date_str)

        current += pd.Timedelta(days=1) # move to the next day

# Summary of backfill results
    logger.info("-" * 50)
    logger.info("Backfill Summary:")
    logger.info(f"Total days: {total_days}")
    logger.info(f"Successful transformations: {len(success)}")
    logger.info(f"Skipped (no raw data): {len(skipped)}")
    logger.info(f"Failed transformations: {len(failed)}")
    if skipped:
        logger.info(f"Skipped dates: {skipped}")
    if failed:
        logger.info(f"Failed dates: {failed}")
    logger.info("-" * 50)



# ─────────────────────────────────────────────────────────────
# Run standalone for testing
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    try:
        if len(sys.argv)==1:
            path=transform() # run for today's date if no arguments
            print(f"Transformation completed. Output saved to {path}")
        elif sys.argv[1]=="backfill":
            start_date=sys.argv[2]
            end_date=sys.argv[3] if len(sys.argv)>3 else None
            backfill(start_date, end_date)
        else:
            print("Invalid arguments. To run transformation for today: python transform.py. To backfill: python transform.py backfill YYYY-MM-DD YYYY-MM-DD")

    except Exception as e:
        print(f"Error: {e}")