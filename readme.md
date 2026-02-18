# Token Clinic: Lakehouse Data Pipelines

A professional-grade, environment-driven Data Lakehouse pipeline designed to index, decode, and analyze EVM-compatible blockchain token transactions.

Built with **Python**, **Polars**, and **Google Cloud Storage (GCS)**, this architecture is designed to handle API pagination limits, mitigate schema drift, and organize data into highly performant Monthly Hive Partitions.

## Key Features

* **Dual-Cursor Ingestion Engine:** Bootstraps new tokens with the 10,000 most recent transactions, then simultaneously drives an **Incremental** (forward-moving) cursor to catch live data and a **Backfill** (backward-moving) cursor to index historical data down to the contract's creation block.
* **Pagination Overlap & Deduplication:** Intelligently handles Etherscan's hard 10,000-record API limits by overlapping boundary blocks and applying global deduplication, ensuring zero data loss at the seams.
* **Dynamic Hex-Decoding:** Parses raw Ethereum transaction calldata (`input`) against dynamic, 32-byte-slot JSON rules to extract clean methods, senders, receivers, and token amounts without needing full ABIs.
* **Monthly Hive Partitioning:** Transforms raw JSON/Parquet dumps into a clean, query-optimized Silver Layer partitioned by month (`token=SYMBOL/month=YYYY-MM/data.parquet`).
* **Schema Drift Immunity:** Utilizes `gcsfs` and Polars' diagonal concatenation to merge raw files seamlessly, even if upstream API columns change order or disappear.
* **Automated EDA Reporting:** Generates interactive, standalone HTML reports using Plotly to track monthly transaction volume, user growth, and function call distributions.

## Project Structure

```text
├── src/
│   ├── config.py       # Centralized environment & token configuration
│   ├── ingest.py       # Pipeline 1: Raw Data Ingestion (Dual-Cursor)
│   ├── process.py      # Pipeline 2: Silver Layer Transformation & Decoding
│   ├── report.py       # Pipeline 3: Gold Layer Analytics & HTML Generation
│   └── utils.py        # Core API, GCS, and data cleaning helpers
├── .gitignore          # Secures secrets and local data folders
├── deploy.ps1          # Cloud Run deployment script
└── README.md

```

---

## Pipeline Operations

The system is broken down into three decoupled pipelines that can be scheduled independently via Google Cloud Run Jobs or cron.

### 1. The Ingestor (`src/ingest.py`)

Fetches raw transaction data.

* **Initialization:** If a token is new, it seeds the state with the 10,000 most recent transactions.
* **Incremental:** Pushes the `max_ingested_block` forward to the current chain tip.
* **Backfill:** Pulls the `min_ingested_block` backward until it hits the contract's genesis block.

### 2. The Processor (`src/process.py`)

Transforms the Bronze layer (Raw) into the Silver layer (Processed).

* Scans all raw parquets via GCSFS.
* Deduplicates overlapping boundary blocks.
* Executes the `decode_row` logic to parse hex data.
* Writes out partitioned Hive structures.
* *Bonus:* Generates a CSV sample of decoded methods to `samples/` for easy manual auditing.

### 3. The Reporter (`src/report.py`)

Generates the Gold layer.

* Uses Polars' Hive-aware lazy scanning to rapidly query the Silver layer.
* Computes aggregate statistics (Unique users, volume, function distributions).
* Uploads a standalone `_eda.html` dashboard directly to the GCS `reports/` folder.