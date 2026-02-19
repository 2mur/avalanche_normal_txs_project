import os
import sys
import logging
import polars as pl
import plotly.graph_objects as go
import plotly.express as px
import plotly.io as pio
from google.cloud import storage
from config import TOKENS, BUCKET_NAME, PROCESSED_FOLDER, REPORTS_FOLDER

# ---------------- LOGGING ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ---------------- CONFIGURATION ----------------
DUST_THRESHOLD = 1/100_000_000       # (< 0.000_000_1%) tokens = "Out/Dust"
RETAIL_THRESHOLD = 1/100_000    # (< 0.000_1%) tokens = "Retail"
WHALE_THRESHOLD = 1/1_000    # (owns >0.1%) tokens = "Whale"

def polars_to_html_table(df: pl.DataFrame) -> str:
    if df.is_empty():
        return "<p>No data available</p>"

    # Enforce .2f maximum for float columns
    float_cols = [col for col, dtype in zip(df.columns, df.dtypes) if dtype in (pl.Float64, pl.Float32)]
    if float_cols:
        df = df.with_columns([pl.col(c).round(2) for c in float_cols])

    headers = "".join(f"<th>{col}</th>" for col in df.columns)
    rows = []

    for row in df.iter_rows():
        cells = "".join(f"<td>{str(val)}</td>" for val in row)
        rows.append(f"<tr>{cells}</tr>")

    return f"""
    <table>
        <thead><tr>{headers}</tr></thead>
        <tbody>{"".join(rows)}</tbody>
    </table>
    """

def generate_user_stats(df: pl.DataFrame) -> pl.DataFrame:
    """Step 1: Aggregate Lifetime User Stats"""
    in_stats = df.group_by("to").agg([
        pl.len().alias("in_tx_count"),
        pl.sum("value").alias("in_vol"),
        pl.min("date_utc").alias("first_in_date"),
        pl.max("date_utc").alias("last_in_date")
    ]).rename({"to": "address"})

    out_stats = df.group_by("from").agg([
        pl.len().alias("out_tx_count"),
        pl.sum("value").alias("out_vol"),
        pl.min("date_utc").alias("first_out_date"),
        pl.max("date_utc").alias("last_out_date")
    ]).rename({"from": "address"})

    user_stats = in_stats.join(out_stats, on="address", how="outer").fill_null(0)
    
    user_stats = user_stats.with_columns([
        (pl.col("in_tx_count") + pl.col("out_tx_count")).alias("total_tx_count"),
        (pl.col("in_vol") + pl.col("out_vol")).alias("total_volume"),
        (pl.col("in_vol") - pl.col("out_vol")).alias("current_balance_approx"),
        pl.lit("unknown").alias("entity_type")
    ])
    
    return user_stats

def generate_daily_ledger(df: pl.DataFrame) -> pl.DataFrame:
    """Step 2: The 'Ledger' Transformation"""
    debits = df.select([
        pl.col("from").alias("address"),
        (pl.col("value") * -1).alias("delta"),
        pl.col("date_utc")
    ])

    credits = df.select([
        pl.col("to").alias("address"),
        pl.col("value").alias("delta"),
        pl.col("date_utc")
    ])

    ledger = pl.concat([debits, credits]).sort(["address", "date_utc"])

    daily_ledger = ledger.group_by(["address", "date_utc"]).agg(
        pl.sum("delta").alias("daily_change")
    ).sort(["address", "date_utc"])

    daily_ledger = daily_ledger.with_columns(
        pl.col("daily_change").cum_sum().over("address").alias("balance")
    )
    
    return daily_ledger

def classify_behavior(ledger_df: pl.DataFrame, supply: int) -> pl.DataFrame:
    """Step 3: User/Date Level Classification"""
    return ledger_df.with_columns([
        pl.when(pl.col("balance") < DUST_THRESHOLD * supply).then(pl.lit("Out/Dust"))
          .when(pl.col("balance") < RETAIL_THRESHOLD * supply).then(pl.lit("Retail"))
          .when(pl.col("balance") >= WHALE_THRESHOLD * supply).then(pl.lit("Whale"))
          .otherwise(pl.lit("Holder"))
          .alias("wealth_class"),

        pl.when(pl.col("balance") < DUST_THRESHOLD * supply).then(pl.lit("Out"))
          .otherwise(pl.lit("In"))
          .alias("status_simple"),
          
        pl.col("balance").shift(1).over("address").fill_null(0).alias("prev_balance")
    ]).with_columns([
        pl.when(pl.col("prev_balance") == 0).then(pl.lit("Just Joined"))
          .when(pl.col("balance") > pl.col("prev_balance")).then(pl.lit("Accumulating"))
          .when(pl.col("balance") < DUST_THRESHOLD * supply).then(pl.lit("Completely Sold"))
          .when(pl.col("balance") < pl.col("prev_balance")).then(pl.lit("Distributing"))
          .otherwise(pl.lit("Holding"))
          .alias("action_class")
    ]).drop("prev_balance")

def generate_users_dashboard(symbol: str, user_stats_df: pl.DataFrame, ledger_df: pl.DataFrame, bucket: storage.Bucket):
    """Step 4: Generate and Upload HTML Dashboard"""
    logger.info(f"Generating Users Dashboard for {symbol}...")

    # Extract the absolute latest state of every user
    latest_state = ledger_df.sort(["address", "date_utc"]).group_by("address").last()
    
    # KPIs
    total_users = len(latest_state)
    whales = len(latest_state.filter(pl.col("wealth_class") == "Whale"))
    retail = len(latest_state.filter(pl.col("wealth_class") == "Retail"))
    out_dust = len(latest_state.filter(pl.col("wealth_class") == "Out/Dust"))

    template_style = "plotly_dark"

    # 1. Wealth Distribution (Pie)
    wealth_dist = latest_state.group_by("wealth_class").len()
    fig_wealth = go.Figure(data=[go.Pie(
        labels=wealth_dist["wealth_class"].to_list(), 
        values=wealth_dist["len"].to_list(), 
        hole=.4,
        marker_colors=['#f88', '#3498db', '#95a5a6', '#2ecc71']
    )])
    fig_wealth.update_layout(title="Current Network Wealth Distribution", template=template_style, paper_bgcolor='rgba(0,0,0,0)')

    # 2. Current Action Distribution (Bar)
    action_dist = latest_state.group_by("action_class").len()
    fig_action = px.bar(
        x=action_dist["action_class"].to_list(), 
        y=action_dist["len"].to_list(),
        labels={"x": "Behavior", "y": "Number of Users"},
        color_discrete_sequence=['#f88']
    )
    fig_action.update_layout(title="What are users doing right now?", template=template_style, paper_bgcolor='rgba(0,0,0,0)')

    # 3. Behavioral Trends Over Time (Stacked Area)
    behavior_time = ledger_df.group_by(["date_utc", "action_class"]).len().sort("date_utc")
    try:
        matrix = behavior_time.pivot(values="len", index="date_utc", columns="action_class").fill_null(0).sort("date_utc")
        fig_behavior_time = go.Figure()
        for col in [c for c in matrix.columns if c != "date_utc"]:
            fig_behavior_time.add_trace(go.Scatter(
                x=matrix["date_utc"].to_list(), y=matrix[col].to_list(),
                name=col, mode='lines', stackgroup='one'
            ))
        fig_behavior_time.update_layout(title="Daily Network Behavior (Stacked)", yaxis_title="Active Addresses", template=template_style, paper_bgcolor='rgba(0,0,0,0)')
        html_behavior_time = pio.to_html(fig_behavior_time, full_html=False, include_plotlyjs=False)
    except Exception as e:
        logger.warning(f"Skipping time series behavior chart: {e}")
        html_behavior_time = f"<p style='text-align:center;'>Not enough data for behavioral time series.</p>"

    # 4. Top 50 Holders Table
    top_50 = latest_state.sort("balance", descending=True).head(50).select(["address", "balance", "wealth_class", "action_class"])

    # Build HTML
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>{symbol} User & Behavioral Analytics</title>
        <style>
            body {{
                margin: 0;
                font-family: Inter, -apple-system, sans-serif;
                background-color: #292929;
                background-image: radial-gradient(#6c4d4d 1px, transparent 0);
                background-size: 20px 20px;
                color: #fff;
                min-height: 100vh;
            }}
            .app {{ padding: 2rem; max-width: 1400px; margin: 0 auto; }}
            .title {{ text-align: center; font-size: 2rem; margin-bottom: 2rem; color: #fff; }}
            .subtitle {{ text-align: center; font-size: 1rem; color: #aaa; margin-bottom: 2rem; }}
            .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 2rem; margin-bottom: 2rem; }}
            .grid-2 {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(500px, 1fr)); gap: 2rem; margin-bottom: 2rem; }}
            .card {{
                background-color: #3a3a3a; border: 2px solid #444; border-radius: 12px;
                padding: 1.5rem; display: flex; flex-direction: column; align-items: center;
                justify-content: center; transition: transform 0.2s ease, border-color 0.2s ease;
            }}
            .card:hover {{ border-color: #f77; transform: scale(1.02); }}
            .stat-label {{ font-size: 1rem; color: #aaa; text-transform: uppercase; margin-bottom: 0.5rem; text-align: center; }}
            .stat-value {{ font-size: 2.2rem; color: #f88; font-weight: bold; text-align: center; }}
            .chart-wrapper {{ background-color: #3a3a3a; border: 2px solid #444; border-radius: 12px; padding: 1rem; margin-bottom: 2rem; }}
            .table-wrapper {{ height: 500px; overflow-y: auto; background-color: #3a3a3a; border: 2px solid #444; border-radius: 12px; padding: 1rem; margin-bottom: 2rem; }}
            table {{ width: 100%; border-collapse: collapse; text-align: left; }}
            th {{ padding: 12px; border-bottom: 2px solid #666; color: #f88; position: sticky; top: 0; background: #3a3a3a; z-index: 10; }}
            td {{ padding: 12px; border-bottom: 1px solid #444; color: #ddd; }}
            ::-webkit-scrollbar {{ width: 8px; height: 8px; }}
            ::-webkit-scrollbar-track {{ background: #292929; border-radius: 4px; }}
            ::-webkit-scrollbar-thumb {{ background: #555; border-radius: 4px; }}
            ::-webkit-scrollbar-thumb:hover {{ background: #f88; }}
        </style>
    </head>
    <body>
        <div id="app" class="app">
            <h1 class="title">{symbol} User Behavioral Analytics</h1>
            <p class="subtitle">Entity resolution, ledger profiling, and wealth distribution</p>

            <div class="grid">
                <div class="card">
                    <div class="stat-label">Total Historical Addresses</div>
                    <div class="stat-value">{total_users:,}</div>
                </div>
                <div class="card">
                    <div class="stat-label">Current Whales (>100k)</div>
                    <div class="stat-value">{whales:,}</div>
                </div>
                <div class="card">
                    <div class="stat-label">Current Retail Users</div>
                    <div class="stat-value">{retail:,}</div>
                </div>
                <div class="card">
                    <div class="stat-label">Out / Dust Accounts</div>
                    <div class="stat-value">{out_dust:,}</div>
                </div>
            </div>

        
            <div class="chart-wrapper" style="margin-bottom: 0;">
                {pio.to_html(fig_wealth, full_html=False, include_plotlyjs='cdn')}
            </div>

            <div class="chart-wrapper" style="margin-bottom: 0;">
                {pio.to_html(fig_action, full_html=False, include_plotlyjs=False)}
            </div>
        

            <h2 class="title" style="font-size: 1.5rem; margin-top: 3rem;">Behavioral Shifts Over Time</h2>
            <div class="chart-wrapper">
                {html_behavior_time}
            </div>

            <h2 class="title" style="font-size: 1.5rem; margin-top: 3rem;">Top 50 Current Holders</h2>
            <div class="table-wrapper">
                {polars_to_html_table(top_50)}
            </div>
        </div>
    </body>
    </html>
    """
    
    blob_name = f"{REPORTS_FOLDER}/{symbol}_users_eda.html"
    bucket.blob(blob_name).upload_from_string(html_content, content_type="text/html")
    logger.info(f"User Dashboard securely saved to: gs://{BUCKET_NAME}/{blob_name}")


def process_users(token_def):
    symbol = token_def.symbol
    logger.info(f"--- Building User Profiles for {symbol} ---")

    try:
        path = f"gs://{BUCKET_NAME}/processed_normal_transfers/token={symbol}/**/*.parquet"
        lf = pl.scan_parquet(path, hive_partitioning=False)
        
        lf = lf.with_columns(
            pl.from_epoch(pl.col("timeStamp"), time_unit="s").dt.date().alias("date_utc")
        )
        
        df = lf.collect()
        if df.is_empty():
            logger.warning("No data found.")
            return

    except Exception as e:
        logger.error(f"Failed to load transfers: {e}")
        return

    try:
        user_stats_df = generate_user_stats(df)
        ledger_df = generate_daily_ledger(df)
        ledger_df = classify_behavior(ledger_df, token_def.supply)

    except Exception as e:
        logger.error(f"Failed processing logic: {e}")
        return

    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    
    # --- SAVE TO GOOGLE CLOUD STORAGE ---
    local_stats = f"/tmp/{symbol}_users.parquet"
    user_stats_df.write_parquet(local_stats)
    bucket.blob(f"user_analytics/token={symbol}/lifetime_stats.parquet").upload_from_filename(local_stats)
    os.remove(local_stats)
    
    # Save partitioned Ledger 
    ledger_df = ledger_df.with_columns(pl.col("date_utc").dt.strftime("%Y-%m").alias("month_str"))
    logger.info(f"Saving Ledger ({len(ledger_df)} rows)...")
    
    for part_df in ledger_df.partition_by("month_str"):
        m = part_df["month_str"][0]
        local_ledger = f"/tmp/{symbol}_ledger_{m}.parquet"
        part_df.write_parquet(local_ledger)
        # Using your updated path
        bucket.blob(f"ledger/token={symbol}/month={m}/data.parquet").upload_from_filename(local_ledger)
        os.remove(local_ledger)

    # --- GENERATE DASHBOARD ---
    generate_users_dashboard(symbol, user_stats_df, ledger_df, bucket)

    logger.info("User Profiling Complete.")


def main():
    for token in TOKENS:
        process_users(token)

if __name__ == "__main__":
    main()