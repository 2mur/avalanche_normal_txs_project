import json
import logging
import requests
import time
import os
import re
import polars as pl
from google.cloud import storage
from config import SCAN_API_KEY, BASE_URL, DROPCOLS_INGEST, MAX_RESULT_SIZE

logger = logging.getLogger(__name__)
STATE_FILE_PATH = "state/global_state.json"

# --- DATA CLEANERS ---
def trim_ingestion(buffer: list):
    """
    Takes a large list of dicts:
    1. Filters to only necessary columns.
    2.1 Cleans function names (removes args).
    2.2 Filters out 'approve' functions based on name.
    4. Adds 'month' column for monthly partitioning.
    """
    if not buffer: return None

    df = pl.DataFrame(buffer)

    if df.is_empty(): return None

    # 2. Smart Filtering: Remove 'approve' based on functionName
    if "functionName" in df.columns:
        initial_count = len(df)

        # 1. Filter out exact matches to "approve"        
        df = df.filter(
            pl.col("functionName")
            .fill_null("")
            .str.split("(").list.get(0)  # Take first part
            .str.to_lowercase()
            .str.strip_chars()           # Remove whitespace
            != "approve"
        )
        
        filtered_count = len(df)
        dropped = initial_count - filtered_count
        if dropped > 0:
            logger.info(f"Filtered {dropped} 'approve' txs (by name).")

    #2. filter out isError == 1
    if not df.is_empty():
        initial_count = len(df)
        df = df.filter(pl.col("isError") != "1")
        filtered_count = len(df)
        dropped = initial_count - filtered_count
        if dropped > 0:
                logger.info(f"Filtered {dropped} isError == 1")
    
    if df.is_empty():
        logger.info("Buffer empty after filtering.")
        return None
    # 3. Add Monthly Partition Columns
    df = df.with_columns(
        pl.from_epoch(pl.col("timeStamp").cast(pl.Int64)).alias("dt")
    ).with_columns(
        pl.col("dt").dt.strftime("%Y-%m").alias("month")
    ).drop("dt")

    # 4. Drop Columns
    df = df.drop(DROPCOLS_INGEST)
    #existing = [c for c in KEEP_COLS if c in df.columns]
    return df
    
# --- GCS HELPERS ---
def get_gcs_client():
    return storage.Client()

def load_state(bucket):
    """Loads the ingestion state (cursor) from GCS."""
    blob = bucket.blob("state/global_state.json")
    if blob.exists():
        return json.loads(blob.download_as_text())
    return {"tokens": {}}

def save_state(bucket, state):
    """Saves the ingestion state to GCS."""
    blob = bucket.blob("state/global_state.json")
    blob.upload_from_string(json.dumps(state, indent=2), content_type="application/json")

def save_buffer(df: pl.DataFrame, symbol: str, bucket_name: str, prefix: str):
    """
    Docstring for save_buffer
    
    > Partitions by Month.
    > Saves to GCS.
    """
    # 6. Save (One file per Month found in buffer)
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    run_ts = int(time.time())

    for part_df in df.partition_by("month"):
        month_str = part_df["month"][0]

        # 🔍 Log schema once per partition
        logger.info(f"{symbol} | Month: {month_str}")
        logger.info(f"Schema: {part_df.schema}")

        # Naming: {prefix}_{timestamp}.parquet
        filename = f"{prefix}_{run_ts}.parquet"
        local_path = f"/tmp/{symbol}_{filename}"

        # Partition Structure: token=XY/month=YYYY-MM/
        blob_path = f"raw_normal_data/token={symbol}/month={month_str}/{filename}"

        part_df.write_parquet(local_path)

        blob = bucket.blob(blob_path)
        blob.upload_from_filename(local_path)

        logger.info(f"Saved {len(part_df)} rows to {blob_path}")

        os.remove(local_path)

# --- API HELPERS ---
def get_chain_tip():
    """Fetches the latest block number."""
    params = {
        "module": "proxy",
        "action": "eth_blockNumber",
        "apikey": SCAN_API_KEY
    }
    try:
        response = requests.get(BASE_URL, params=params, timeout=10)
        data = response.json()
        if "result" in data:
            return int(data["result"], 16)
        return 0
    except Exception as e:
        logger.error(f"Chain tip error: {e}")
        return 0
    
def get_contract_creation_block(address):
    """
    Two-Step fetch to find the genesis block of a contract.
    1. Get Creation Transaction Hash.
    2. Get Block Number of that Transaction.
    https://api.routescan.io/v2/network/mainnet/evm/43114/etherscan/api
    ?module=contract
    &action=getcontractcreation
    &contractaddresses=0xf5b969064b91869fBF676ecAbcCd1c5563F591d0
    &apikey=YourApiKeyToken
    """
    # Step 1: Get Creation Tx Hash
    params_create = {
        "module": "contract", "action": "getcontractcreation",
        "contractaddresses": address, "apikey": SCAN_API_KEY
    }
    try:
        time.sleep(0.201) # Rate limit wait
        resp = requests.get(BASE_URL, params=params_create, timeout=10).json()
        
        if resp["status"] == "0" or not resp["result"]:
            logger.warning(f"Could not find creation tx for {address}. (Might be too old or proxy?)")
            return 0
            
        # Result is a list, we asked for one address
        tx_hash = resp["result"][0]["txHash"]
        
        # Step 2: Get Transaction Details to find Block Number
        params_tx = {
            "module": "proxy", "action": "eth_getTransactionByHash",
            "txhash": tx_hash, "apikey": SCAN_API_KEY
        }

        time.sleep(0.201) # Rate limit wait
        resp_tx = requests.get(BASE_URL, params=params_tx, timeout=10).json()
        
        if not resp_tx.get("result"):
             return 0
             
        creation_block = int(resp_tx["result"]["blockNumber"], 16)
        return creation_block

    except Exception as e:
        logger.error(f"Error finding creation block for {address}: {e}")
        return 0
    
def fetch_normal_batch(address, start_block, end_block, sort="asc"):
    """Fetches a batch of transactions.
    # ?module=account
    # &action=txlist
    # &address=0xddbd2b932c763ba5b1b7ae3b362eac3e8d40121a
    # &startblock=0
    # &endblock=99999999
    # &page=1
    # &offset=10
    # &sort=asc
    # &apikey=YourApiKeyToken
    """
    #module and action predefined for normal transactions
    params = {
        "module": "account",
        "action": "txlist",
        "address": address,
        "startblock": start_block,
        "endblock": end_block,
        "sort": sort,
        "offset": MAX_RESULT_SIZE,
        "apikey": SCAN_API_KEY
    }
    
    for attempt in range(2):
        try:
            time.sleep(0.201) # Rate limit wait
            response = requests.get(BASE_URL, params=params, timeout=10)
            data = response.json()
            
            if data["status"] == "1":
                return data["result"]
            elif data["message"] == "No transactions found":
                return []
            else:
                logger.warning(f"API Error: {data['message']} - {data['result'][:20]}")
                
        except Exception as e:
            logger.error(f"Request failed: {e}")
            
    return []

# --- DECODER HELPERS ---
def parse_signature(signature):
    """
    Parses a signature like 'transferfrom(address from, address to, uint256 value) returns (bool)'
    Returns a list of tuples: [('address', 'from'), ('address', 'to'), ('uint256', 'value')]
    """
    # Replace pd.isna with standard None and type checking
    if signature is None or not isinstance(signature, str) or signature.strip() == '':
        return []
    
    # Extract the part inside the first set of parentheses
    match = re.search(r'\((.*?)\)', signature)
    if not match:
        return []
    
    params_str = match.group(1)
    if not params_str.strip():
        return []
    
    # Split by comma to get individual parameters
    params = params_str.split(',')
    parsed_params = []
    
    for param in params:
        parts = param.strip().split(' ')
        if len(parts) >= 2:
            param_type = parts[0]
            param_name = parts[-1]
        else:
            param_type = parts[0]
            # Fallback name if the ABI lacks parameter names
            param_name = f"param_{len(parsed_params)}"
        parsed_params.append((param_type, param_name))
        
    return parsed_params

def decode_row(input_hex, function_name):
    """
    Decodes the hex input data based on the extracted types from function_name.
    Returns a dictionary of decoded values.
    """
    params = parse_signature(function_name)
    
    # Replace pd.isna with standard None and type checking
    if input_hex is None or not isinstance(input_hex, str) or len(input_hex) < 10:
        return {}
        
    # Strip "0x" + 8 chars of methodId
    hex_data = input_hex[10:]
    decoded = {}
    
    for i, (p_type, p_name) in enumerate(params):
        # Every static ABI parameter is padded to 32 bytes (64 hex characters)
        start_idx = i * 64
        end_idx = start_idx + 64
        chunk = hex_data[start_idx:end_idx]
        
        if len(chunk) < 64:
            decoded[p_name] = f"Error: chunk too short ({chunk})"
            continue
            
        try:
            if 'address' in p_type:
                # Addresses are 20 bytes (40 hex chars), padded on the left
                decoded[p_name] = '0x' + chunk[-40:]
            elif 'uint' in p_type or 'int' in p_type:
                # Cast hex to base-16 integer
                decoded[p_name] = int(chunk, 16)
            elif 'bool' in p_type:
                # Cast hex to boolean
                decoded[p_name] = bool(int(chunk, 16))
            elif 'bytes' in p_type and '[' not in p_type and 'bytes' != p_type:
                # Static bytes (e.g. bytes32)
                decoded[p_name] = '0x' + chunk
            else:
                # Dynamic types (string, bytes, arrays) use pointers (offsets). 
                # For basic normal tx decoding, we store the raw hex offset.
                decoded[p_name] = f"Raw/Pointer: 0x{chunk}"
        except Exception as e:
            decoded[p_name] = f"Failed to decode: {e}"
            
    return decoded

def extract_field(json_str, *keys):
    """Safely extract keys (like 'to', 'from', 'value', 'amount') from a JSON string."""
    if not json_str:
        return None
    try:
        d = json.loads(json_str)
        for k in keys:
            if k in d and d[k] is not None:
                return d[k]
    except Exception:
        pass
    return None