import os
import sys
import logging
import json
import polars as pl
from google.cloud import storage
from config import TOKENS, BUCKET_NAME, RAW_FOLDER
from utils import decode_row, extract_field

# ---------------- LOGGING ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

def token_processor(token_def):
    """
    1. Clean
    2. Decode inputs to JSON string
    3. Overwrite transfer parameters
    4. Split into Transfers vs Non-Transfers
    5. Save to Google Cloud Storage
    """
    symbol = token_def.symbol
    decimals = token_def.decimals

    logger.info(f"--- Processing Data for {symbol} ---")

    # 1 --- Clean & Prepare DataFrame ---
    try:
        raw_glob_path = f"gs://{BUCKET_NAME}/{RAW_FOLDER}/token={symbol}/**/*.parquet"
        logger.info(f"Scanning path: {raw_glob_path}")

        # hive_partitioning=False prevents crashes from messy Google Cloud Storage directory trees
        lf = pl.scan_parquet(raw_glob_path, hive_partitioning=False)

    except Exception as e:
        logger.warning(f"Error Loading for {symbol}: {e}")
        return

    # Collect Entire DataFrame and Deduplicate
    try:
        df = lf.collect()
        if df.is_empty():
            logger.info(f"No successful transactions for {symbol}.")
            return
        
        df = df.unique(subset=["hash"], keep="first")
        logger.info(f"Loaded {len(df)} rows for processing")
    except Exception as e:
        logger.error(f"Failed collecting dataframe: {e}")
        return

    # Column Datatypes casting & Date Generation
    try:
        df = df.with_columns([
            pl.col("blockNumber").cast(pl.Int64),
            pl.col("timeStamp").cast(pl.Int64),
            pl.col("hash").str.to_lowercase(),
            pl.col("from").str.to_lowercase(),
            pl.col("to").str.to_lowercase(),
            pl.col("value").cast(pl.Float64),
            pl.col("input").str.to_lowercase(),
            pl.col("methodId").str.to_lowercase(),
            pl.col("functionName").str.to_lowercase(),
        ])
        
        # Re-derive 'month' from timeStamp since hive_partitioning=False ignores the folder names
        df = df.with_columns(
            pl.from_epoch(pl.col("timeStamp"), time_unit="s")
            .dt.strftime("%Y-%m")
            .alias("month")
        )

    except Exception as e:
        logger.error(f"Column preparation failed: {e}")
        return


    # 2 --- DECODING DataFrame ---
    logger.info("Decoding smart contract inputs...")
    try:
        # We dump the dict to a JSON string because Polars Structs require a uniform schema
        df = df.with_columns(
            pl.struct(["input", "functionName"])
            .map_elements(
                lambda r: json.dumps(decode_row(r["input"], r["functionName"])), 
                return_dtype=pl.Utf8
            )
            .alias("decoded_params")
        )
    except Exception as e:
        logger.error(f"Decoding failed: {e}")
        return


    # 3 --- OVERWRITE TRANSFER COLUMNS ---
    # Only match exact transfer or transferfrom signatures
    is_transfer_condition = (
        pl.col("functionName").str.starts_with("transfer(") |
        pl.col("functionName").str.starts_with("transferfrom(")
    )
    # Rename original transaction sender to 'caller'
    df = df.rename({"from": "caller"})

    logger.info("Overwriting decoded parameters for transfers...")
    try:
        # Extract fields temporarily
        df = df.with_columns([
            pl.col("decoded_params").map_elements(lambda x: extract_field(x, "to"), return_dtype=pl.Utf8).alias("decoded_to"),
            pl.col("decoded_params").map_elements(lambda x: extract_field(x, "from"), return_dtype=pl.Utf8).alias("decoded_from"),
            pl.col("decoded_params").map_elements(lambda x: extract_field(x, "value", "amount"), return_dtype=pl.Float64).alias("decoded_value")
        ])

        # Overwrite logic
        df = df.with_columns([
            # FROM LOGIC: Use decoded_from if available (transferFrom). Otherwise use caller (transfer).
            pl.when(is_transfer_condition & pl.col("decoded_from").is_not_null())
              .then(pl.col("decoded_from").str.to_lowercase())
              .when(is_transfer_condition)
              .then(pl.col("caller"))
              .otherwise(pl.col("caller")) # Fallback for non-transfers
              .alias("from"),
              
            # TO LOGIC: Use decoded_to if available. Otherwise keep the original 'to' (contract address).
            pl.when(is_transfer_condition & pl.col("decoded_to").is_not_null())
              .then(pl.col("decoded_to").str.to_lowercase())
              .otherwise(pl.col("to"))
              .alias("to"),
              
            # VALUE LOGIC: Normalize decoded value. Otherwise keep original native value.
            pl.when(is_transfer_condition & pl.col("decoded_value").is_not_null())
              .then(pl.col("decoded_value") / (10 ** decimals))
              .otherwise(pl.col("value"))
              .alias("value")
        ]).drop(["decoded_to", "decoded_from", "decoded_value"])
        
    except Exception as e:
        logger.error(f"Failed to overwrite transfer columns: {e}")
        return

    # Save samples to Google Cloud Storage
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    try:
        methodid_unique_df = df.unique(subset=["methodId"], keep="first")
        methodid_unique_df = methodid_unique_df.select(['functionName','decoded_params','hash','methodId','input'])

        sample_local_path = f"/tmp/{symbol}_method_sample.csv"
        sample_blob_path = f"samples/token={symbol}/methodid_unique_sample.csv"
    
        methodid_unique_df.write_csv(sample_local_path)
        bucket.blob(sample_blob_path).upload_from_filename(sample_local_path)
        os.remove(sample_local_path)
        
        logger.info(f"Sample [methodId] Saved -> gs://{BUCKET_NAME}/{sample_blob_path}")
    except Exception as e:
        logger.warning(f"Could not generate sample CSV for {symbol}: {e}")

    # 4 --- SPLIT DATASET ---
    datasets = {
        "processed_normal_transfers": df.filter(is_transfer_condition).drop(['decoded_params','functionName','methodId','input']),
        "processed_normal_txs_notransfers": df.filter(~is_transfer_condition)
    }

    # 5 --- EXPORT TO GOOGLE CLOUD STORAGE ---
    logger.info(f"{symbol} | Splitting dataset into Transfers vs Non-Transfers...")
    logger.info(f"Schema: {datasets['processed_normal_transfers'].schema}")

    for folder_name, target_df in datasets.items():
        if target_df.is_empty():
            logger.info(f"No data for {folder_name} for token {symbol}.")
            continue
            
        logger.info(f"Saving {len(target_df)} rows to {folder_name}...")
                    
        for part_df in target_df.partition_by("month"):
            month_str = part_df["month"][0]
            if month_str is None:
                continue
            
            filename = "data.parquet"
            blob_path = f"{folder_name}/token={symbol}/month={month_str}/{filename}"
            local_path = f"/tmp/{symbol}_{month_str}_{folder_name}.parquet"
            
            part_df.write_parquet(local_path)
            blob = bucket.blob(blob_path)
            blob.upload_from_filename(local_path)
            os.remove(local_path)

def main():
    for token in TOKENS:
        token_processor(token)
        logger.info(f"Processed {token.symbol}.")


if __name__ == "__main__":
    main()