# Token Clinic: Lakehouse Data Pipelines

```
Download an HTML report from /reports and open in your preferred browser.
```

A Data Lakehouse pipeline designed to index, decode, analyze, and visualize EVM-compatible blockchain token transactions.

Built with **Python**, **Polars**, **Plotly**, and **Google: Worfkflows, Cloud Storage, Cloud Run**, this architecture is designed to handle API pagination limits, mitigate schema drift, organize data into highly performant Monthly Hive Partitions, and serve interactive visual analytics.

## Key Features

* **Dual-Cursor Ingestion Engine:** Bootstraps new tokens with the 10,000 most recent transactions, then simultaneously drives an **Incremental** (forward-moving) cursor to catch live data and a **Backfill** (backward-moving) cursor to index historical data down to the contract's creation block.
* **Pagination Overlap & Deduplication:** Intelligently handles Etherscan's hard 10,000-record API limits by overlapping boundary blocks and applying global deduplication, ensuring zero data loss at the seams.
* **Dynamic Hex-Decoding:** Parses raw Ethereum transaction calldata (`input`) against dynamic, 32-byte-slot JSON rules to extract clean methods, senders, receivers, and token amounts without needing full ABIs.
* **Monthly Hive Partitioning:** Transforms raw JSON/Parquet dumps into a clean, query-optimized Silver Layer partitioned by month (`token=SYMBOL/month=YYYY-MM/data.parquet`).
* **Advanced Behavioral Profiling:** Classifies wallet addresses into Wealth Classes (Whales, Retail, Dust) and Action Classes (Accumulating, Distributing) based on running ledger balances.

## Project Structure

```text
├── src/
│   ├── config.py        # Centralized environment & token configuration
│   ├── ingest.py        # Pipeline 1: Raw Data Ingestion (Dual-Cursor)
│   ├── process.py       # Pipeline 2: Silver Layer Transformation & Decoding
│   ├── report.py        # Pipeline 3: Gold Layer Analytics & EDA
│   ├── users.py         # Pipeline 4: Behavioral Profiling & User Dashboards
│   └── utils.py         # Core API, GCS, and data cleaning helpers
├── reports/             # HTML Pyplot reports
├── .gitignore           # Secures secrets and local data folders
├── deploy.ps1           # Cloud deployment script
├── Dockerfile           # Image
├── workflow.yaml        # Cloud Workflows
└── README.md

```

---

## Pipeline Operations

The system is broken down into modular pipelines that can be scheduled independently. In a professional setup, you want to trigger a pipeline via Google Cloud Run Jobs or cron.

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

### 3. The Reporter (`src/report.py`)

Generates the initial Gold layer.

* Uses Polars' Hive-aware lazy scanning to rapidly query the Silver layer.
* Computes aggregate statistics (Unique users, volume, function distributions).
* Uploads a standalone `_eda.html` dashboard directly to the Google Cloud Storage `reports/` folder.

### 4. The Profiler (`src/users.py`)

Handles entity resolution and wallet-level behavioral classification.

* **Lifetime Aggregations:** Calculates total inbound/outbound tx counts, volumes, and net flows for every historical address.
* **Daily Ledger:** Transforms transaction deltas into a cumulative running balance per user, per day.
* **Behavioral Matrix:** Classifies wallets strictly based on mathematical thresholds (e.g., >0.1% supply = Whale) and action shifts (Holding vs. Distributing).
* **Quadrant Dashboard:** Generates a custom HTML UI with frosted-glass CSS, Plotly stacked area charts, and top 50 user statistic cards.