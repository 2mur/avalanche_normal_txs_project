import logging
import sys
from config import TOKENS, BUCKET_NAME, BATCHES_PER_RUN, MAX_RESULT_SIZE
from utils import (
    get_gcs_client, 
    load_state, 
    save_state,
    trim_ingestion, 
    save_buffer,
    fetch_normal_batch, 
    get_chain_tip, 
    get_contract_creation_block
)

# ---------------- LOGGING ----------------
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

def run_incremental(token, t_state, chain_tip):
    """Fetches NEW transactions (Forward from max_ingested)."""

    start_block = t_state.get("max_ingested_block", 0) + 1
    
    # Safety: Don't go beyond chain tip
    if start_block > chain_tip: 
        return
    
    total_docs = 0
    current_start = start_block
    
    for _ in range(BATCHES_PER_RUN):
        logger.info(f"{token.symbol} [Inc]: Fetching batch from block {current_start} to {chain_tip}")
        
        batch = fetch_normal_batch(token.address, current_start, chain_tip, "asc")
        
        if not batch:
            break
        
        buffer = trim_ingestion(batch)

        if buffer is None:
            logger.info("Buffer empty after trimming. Ending incremental.")
            break
        
        save_buffer(buffer, token.symbol, BUCKET_NAME, prefix="inc")
        del buffer

        total_docs += len(batch)
        
        max_block_in_batch = max(int(t["blockNumber"]) for t in batch)
        t_state["max_ingested_block"] = max_block_in_batch
        
        if len(batch) < MAX_RESULT_SIZE:
            break
            
        # OVERLAP FIX: Start exactly on the max block to catch cut-off txs
        if current_start == max_block_in_batch:
            # Stuck Guard: The entire 10k batch was inside one single block
            current_start = max_block_in_batch + 1 
        else:
            # Overlap: We will fetch the boundary block again and rely on dedup
            current_start = max_block_in_batch
        
        # Safety: If we passed the tip
        if current_start > chain_tip:
            break
            
    if total_docs > 0:
        logger.info(f"{token.symbol} [Inc]: Ingested {total_docs} txs. New Head: {t_state['max_ingested_block']}")


def run_token_init(token, t_state, chain_tip):
    """Bootstraps a completely new token with its first 10k recent transactions."""
    
    creation_block = get_contract_creation_block(token.address)
    t_state["creation_block"] = creation_block

    logger.info(f"--- INITIALIZING {token.symbol} ---")
    logger.info(f"{token.symbol} [Init]: Fetching 10,000 most recent transactions...")
    
    # Fetch descending to grab the absolute newest 10k
    batch = fetch_normal_batch(token.address, 0, chain_tip, "desc")
    
    if batch:
        buffer = trim_ingestion(batch)
        if buffer is not None:
            save_buffer(buffer, token.symbol, BUCKET_NAME, prefix="init")
            del buffer
        
        # Establish Cursors
        t_state["max_ingested_block"] = max(int(t["blockNumber"]) for t in batch)
        min_b = min(int(t["blockNumber"]) for t in batch)
        t_state["min_ingested_block"] = min_b
        
        # Check if we hit the bottom immediately
        if min_b <= creation_block and creation_block > 0:
            t_state["history_status"] = "filled"
            
        t_state["initialized"] = True
        logger.info(f"{token.symbol} [Init]: Success! Head: {t_state['max_ingested_block']}, Floor: {t_state['min_ingested_block']}")
    else:
        logger.warning(f"{token.symbol} [Init]: No transactions found. Marking initialized to prevent infinite loops.")
        t_state["initialized"] = True


def run_backfill(token, t_state):
    """Fetches OLD transactions (Backward from min_ingested)."""
    
    if t_state.get("history_status") == "filled":
        return

    # FIX: Must define creation_block before checking it
    creation_block = t_state.get("creation_block", 0)
    
    if creation_block == 0:
        creation_block = get_contract_creation_block(token.address)
        if creation_block > 0:
            t_state["creation_block"] = creation_block
            logger.info(f"{token.symbol}: Creation block found at {creation_block}")
        else:
            creation_block = 0 

    current_floor = t_state.get("min_ingested_block", 0)
    
    # Check if finished before starting loop
    if creation_block > 0 and current_floor <= creation_block:
        t_state["history_status"] = "filled"
        logger.info(f"{token.symbol} [Backfill]: Reached creation block. History complete.")
        return

    logger.info(f"{token.symbol} [Backfill]: Walking back from {current_floor} (Target: {creation_block})")

    total_docs = 0
    
    for _ in range(BATCHES_PER_RUN):
        target_start = creation_block if creation_block > 0 else 0
        
        logger.info(f"{token.symbol} [Backfill]: Fetching batch from block {current_floor} down to {target_start}")

        batch = fetch_normal_batch(token.address, target_start, current_floor, "desc")
        
        if not batch:
            if target_start > 0:
                t_state["history_status"] = "filled"
                t_state["min_ingested_block"] = target_start
                logger.info(f"{token.symbol} [Backfill]: No data returned. Marking filled.")
            break
            
        buffer = trim_ingestion(batch)
        if buffer is None:
            logger.info("Buffer empty after trimming. Ending backfill.")
            break
            
        save_buffer(buffer, token.symbol, BUCKET_NAME, prefix="bf")
        del buffer

        total_docs += len(batch)
        
        min_block_in_batch = min(int(t["blockNumber"]) for t in batch)
        t_state["min_ingested_block"] = min_block_in_batch 
        
        if len(batch) < MAX_RESULT_SIZE:
            t_state["history_status"] = "filled"
            t_state["min_ingested_block"] = creation_block
            logger.info(f"{token.symbol} [Backfill]: Partial page received. History finished.")
            break
            
        # OVERLAP FIX: End exactly on the min block to catch cut-off txs
        if current_floor == min_block_in_batch:
            # Stuck Guard: The entire 10k batch was inside one single block
            next_floor = min_block_in_batch - 1 
        else:
            # Overlap: We will fetch the boundary block again and rely on dedup
            next_floor = min_block_in_batch
            
        if next_floor <= creation_block and creation_block > 0:
            t_state["history_status"] = "filled"
            t_state["min_ingested_block"] = creation_block
            logger.info(f"{token.symbol} [Backfill]: Crossed creation block. History finished.")
            break
            
        current_floor = next_floor

    if total_docs > 0:
        logger.info(f"{token.symbol} [Backfill]: Saved {total_docs} txs. New Floor: {t_state['min_ingested_block']}")


def main():
    print("--- PIPELINE STARTING ---")
    
    client = get_gcs_client()
    bucket = client.bucket(BUCKET_NAME)
    state = load_state(bucket)
    
    try:
        chain_tip = get_chain_tip()
        logger.info(f"Chain Tip: {chain_tip}")
    except Exception as e:
        logger.error(f"Failed to get chain tip: {e}")
        return

    for token in TOKENS:
        # Check and set default state
        if token.symbol not in state["tokens"]:
            state["tokens"][token.symbol] = {
                "initialized": False,
                "max_ingested_block": 0,
                "min_ingested_block": 0,
                "history_status": "unfilled",
                "creation_block": 0
            }
        
        t_state = state["tokens"][token.symbol]
        
        # --- INITIALIZATION PHASE ---
        if not t_state.get("initialized", False):
            try:
                run_token_init(token, t_state, chain_tip)
                save_state(bucket, state)  # Save after init
            except Exception as e:
                logger.error(f"Initialization failed for {token.symbol}: {e}")
                continue  # Skip to next token if init fails
        else:
            # --- NORMAL INGESTION PHASE ---
            try:
                run_incremental(token, t_state, chain_tip)
            except Exception as e:
                logger.error(f"Incremental failed for {token.symbol}: {e}")

            try:
                # FIX: Removed chain_tip to match function signature
                run_backfill(token, t_state) 
            except Exception as e:
                logger.error(f"Backfill failed for {token.symbol}: {e}")

            save_state(bucket, state)

    print("--- PIPELINE FINISHED ---")


if __name__ == "__main__":
    main()