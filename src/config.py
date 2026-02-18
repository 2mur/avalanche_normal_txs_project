
import os

# --- INFRASTRUCTURE ---
# Defaults to your project bucket if not set
BUCKET_NAME = os.getenv("BUCKET_NAME")
PROJECT_ID = os.getenv("PROJECT_ID")

# --- API KEYS ---
# We use SCAN_API_KEY everywhere.
SCAN_API_KEY = os.getenv("SCAN_API_KEY") 
BASE_URL = "https://api.routescan.io/v2/network/mainnet/evm/43114/etherscan/api"
# ?module=account
# &action=txlist
# &address=0x...
# &startblock=0
# &endblock=99999999
# &page=1
# &offset=10
# &sort=asc
# &apikey=YourApiKeyToken

# ---------------- CONFIG ----------------
BATCHES_PER_RUN = 10     # Keep small to avoid timeouts
MAX_RESULT_SIZE = 10000  # Etherscan limit

# --- FOLDER PATHS ---
RAW_FOLDER = "raw_normal_data"
PROCESSED_FOLDER = "processed_normal_data"
REPORTS_FOLDER = "reports"

# Columns to keep for clean data
KEEP_COLS = {
    "blockNumber", "timeStamp", "hash", "from", 
    "input", "functionName", "methodId", "isError"
}

DROPCOLS_INGEST = ["nonce", "transactionIndex", "gas", "gasPrice", "gasUsed",
                   "cumulativeGasUsed", "txreceipt_status", "confirmations", "contractAddress", "isError"]

# --- TOKEN DEFINITIONS ---
class TokenDef:
    def __init__(self, symbol, address, decimals=18):
        self.symbol = symbol
        self.address = address.lower()
        self.decimals = decimals

TOKENS = [
    TokenDef(symbol="ARENA", address="0xB8d7710f7d8349A506b75dD184F05777c82dAd0C", decimals=18), # Example
    TokenDef(symbol="JOE", address="0x6e84a6216eA6dACC71eE8E6b0a5B7322EEbC0fDd", decimals=18),
    TokenDef(symbol="NOCHILL", address="0xAcFb898Cff266E53278cC0124fC2C7C94C8cB9a5", decimals=18),
    TokenDef(symbol="COQ", address="0x420FcA0121DC28039145009570975747295f2329", decimals=18),
    TokenDef(symbol="KET", address="0xFFFF003a6BAD9b743d658048742935fFFE2b6ED7", decimals=18),
]

'''
raw normal tx:
   {
      "blockNumber": "78315790",
      "blockHash": "0x7df22e971df153ce0e828beaae867e461158f1cf551182b96c35df9960faa8e3",
      "timeStamp": "1771352294",
      "hash": "0x1cdc84fc186b7544484585f43dadc71c6d79102991168a0e8583f71af6d7164f",
      "nonce": "198",
      "transactionIndex": "25",
      "from": "0x731d3d5c016b7e04c43bb4dc3598ea90a6c37e81",
      "to": "0xb8d7710f7d8349a506b75dd184f05777c82dad0c",
      "value": "0",
      "gas": "46397",
      "gasPrice": "100611974",
      "input": "0x095ea7b3000000000000000000000000effb809d99142ce3b51c1796c096f5b01b4aaec400000000000000000000000000000000000000000000000a162d5c1822c0caab",
      "methodId": "0x095ea7b3",
      "functionName": "approve(address spender, uint256 value) returns (bool)",
      "contractAddress": "",
      "cumulativeGasUsed": "1156540",
      "txreceipt_status": "1",
      "gasUsed": "46015",
      "confirmations": "608",
      "isError": "0"
    },

schema after ingestion:
    Schema: OrderedDict([
    ('blockNumber', String), 
    ('blockHash', String), 
    ('timeStamp', String), 
    ('hash', String), 
    ('from', String), 
    ('to', String), 
    ('value', String), 
    ('input', String), 
    ('methodId', String), 
    ('functionName', String),
    ('month', String)])

schema after processing:
OrderedDict([
('blockNumber', Int64), 
('blockHash', String), 
('timeStamp', Int64), 
('hash', String), 
('from', String), 
('to', String), 
('value', Float64), 
('input', String), 
('methodId', String), 
('functionName', String), 
('month', String)])
'''