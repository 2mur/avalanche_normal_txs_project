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
        (pl.col("in_vol") - pl.col("out_vol")).abs().alias("net_flow"),
        pl.lit("unknown").alias("entity_type")
    ])
    logger.info("generate_user_stats schema: %s", user_stats.schema)

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
    logger.info("generate_daily_ledger schema: %s", daily_ledger.schema)
    return daily_ledger

def classify_behavior(ledger_df: pl.DataFrame, supply: int) -> pl.DataFrame:
    """Step 3: User/Date Level Classification"""
    df = ledger_df.with_columns([
        pl.when(pl.col("balance") < DUST_THRESHOLD * supply).then(pl.lit("Dust"))
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

    logger.info("classify_behavior schema: %s", df.schema)
    return df

def generate_user_cards_html(user_stats_df: pl.DataFrame) -> str:
    """Helper to generate the top 50 user quadrant cards"""
    if user_stats_df.is_empty():
        return "<p>No top users available.</p>"

    top_50 = user_stats_df.sort("current_balance_approx", descending=True).head(50)
    
    cards_html = []
    for row in top_50.iter_rows(named=True):
        addr = row.get("address", "Unknown")
        
        in_tx = f"{row.get('in_tx_count', 0):,}"
        in_vol = f"{row.get('in_vol', 0):,.2f}"
        f_in = str(row.get('first_in_date', 'N/A'))
        l_in = str(row.get('last_in_date', 'N/A'))
        
        out_tx = f"{row.get('out_tx_count', 0):,}"
        out_vol = f"{row.get('out_vol', 0):,.2f}"
        f_out = str(row.get('first_out_date', 'N/A'))
        l_out = str(row.get('last_out_date', 'N/A'))
        
        tot_tx = f"{row.get('total_tx_count', 0):,}"
        tot_vol = f"{row.get('total_volume', 0):,.2f}"
        bal = f"{row.get('current_balance_approx', 0):,.2f}"
        net = f"{row.get('net_flow', 0):,.2f}"

        card = f"""
        <div class="user-card frosted">
            <div class="uc-header" title="{addr}">{addr}</div>
            <div class="uc-body">
                <div class="uc-quad">
                    <strong>Inbound</strong><br/>
                    Tx Count: {in_tx}<br/>
                    Vol: {in_vol}<br/>
                    First: {f_in}<br/>
                    Last: {l_in}
                </div>
                <div class="uc-quad">
                    <strong>Outbound</strong><br/>
                    Tx Count: {out_tx}<br/>
                    Vol: {out_vol}<br/>
                    First: {f_out}<br/>
                    Last: {l_out}
                </div>
            </div>
            <div class="uc-footer">
                <div><strong>Total Tx:</strong> {tot_tx}</div>
                <div><strong>Total Vol:</strong> {tot_vol}</div>
                <div><strong>Balance:</strong> {bal}</div>
                <div><strong>Net Flow:</strong> {net}</div>
            </div>
        </div>
        """
        cards_html.append(card)

    return f'<div class="user-cards-grid">{"".join(cards_html)}</div>'

def generate_users_dashboard(symbol: str, user_stats_df: pl.DataFrame, ledger_df: pl.DataFrame, bucket: storage.Bucket):
    """Step 4: Generate and Upload HTML Dashboard"""
    logger.info(f"Generating Users Dashboard for {symbol}...")

    latest_state = ledger_df.sort(["address", "date_utc"]).group_by("address").last()
    
    total_users = len(latest_state)
    whales = len(latest_state.filter(pl.col("wealth_class") == "Whale"))
    retail = len(latest_state.filter(pl.col("wealth_class") == "Retail"))
    out_dust = len(latest_state.filter(pl.col("wealth_class") == "Dust"))

    template_style = "plotly_dark"
    graphs_html = []

    try:
        # 1. Top 30 Addresses by Max Balance (Line)
        top_30_addrs = ledger_df.group_by("address").agg(pl.max("balance").alias("max_b")).sort("max_b", descending=True).head(30)["address"].to_list()
        g1_df = ledger_df.filter(pl.col("address").is_in(top_30_addrs)).sort(["address", "date_utc"])
        fig1 = px.line(g1_df, x="date_utc", y="balance", color="address", title="Top 30 Addresses by Max Balance")
        fig1.update_layout(template=template_style, paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
        graphs_html.append(pio.to_html(fig1, full_html=False, include_plotlyjs='cdn'))

        # 2. Wealth Class Counts (Side-by-Side Bar)
        g2_df = ledger_df.group_by(["date_utc", "wealth_class"]).len().sort("date_utc")
        fig2 = px.bar(g2_df, x="date_utc", y="len", color="wealth_class", barmode="stack", title="Wealth Class Counts (Grouped)")
        fig2.update_layout(template=template_style, paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
        graphs_html.append(pio.to_html(fig2, full_html=False, include_plotlyjs=False))

        # 3. Wealth Class Counts (100% Stacked Bar)
        fig3 = px.bar(g2_df, x="date_utc", y="len", color="wealth_class", title="Wealth Class Composition (%)")
        fig3.update_layout(barmode='stack', barnorm='percent', template=template_style, paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
        graphs_html.append(pio.to_html(fig3, full_html=False, include_plotlyjs=False))

        # 4. Simple Status In/Out (Positive/Negative Bar)
        g4_df = ledger_df.group_by(["date_utc", "status_simple"]).len()
        g4_df = g4_df.with_columns(
            pl.when(pl.col("status_simple") == "Out").then(pl.col("len") * -1).otherwise(pl.col("len")).alias("plot_val")
        ).sort("date_utc")
        fig4 = px.bar(g4_df, x="date_utc", y="plot_val", color="status_simple", title="Network Status (In vs Out)")
        fig4.update_layout(template=template_style, paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
        graphs_html.append(pio.to_html(fig4, full_html=False, include_plotlyjs=False))

        # 5. Action Class (100% Stacked Bar)
        g5_df = ledger_df.group_by(["date_utc", "action_class"]).len().sort("date_utc")
        fig5 = px.bar(g5_df, x="date_utc", y="len", color="action_class", title="Action Class Composition (%)")
        fig5.update_layout(barmode='stack', barnorm='percent', template=template_style, paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
        graphs_html.append(pio.to_html(fig5, full_html=False, include_plotlyjs=False))

    except Exception as e:
        logger.warning(f"Error generating Plotly graphs: {e}")
        graphs_html.append(f"<p style='text-align:center;'>Error generating charts: {e}</p>")

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
                background-color: #1a1a1a;
                background-image: radial-gradient(#4a3b3b 1px, transparent 0);
                background-size: 20px 20px;
                color: #fff;
                min-height: 100vh;
            }}
            .app {{ padding: 2rem; max-width: 1400px; margin: 0 auto; }}
            .title {{ text-align: center; font-size: 2rem; margin-bottom: 2rem; color: #fff; }}
            .subtitle {{ text-align: center; font-size: 1rem; color: #aaa; margin-bottom: 2rem; }}
            
            /* Frosted Glass Base Class */
            .frosted {{
                background-color: rgba(45, 45, 45, 0.6);
                backdrop-filter: blur(12px);
                -webkit-backdrop-filter: blur(12px);
                border: 1px solid rgba(255, 255, 255, 0.1);
                box-shadow: 0 4px 15px rgba(0, 0, 0, 0.2);
            }}

            /* KPI Grid */
            .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 2rem; margin-bottom: 2rem; }}
            .card {{
                border-radius: 12px;
                padding: 1.5rem; display: flex; flex-direction: column; align-items: center;
                justify-content: center; transition: transform 0.2s ease, border-color 0.2s ease;
            }}
            .card:hover {{ border-color: rgba(255, 119, 119, 0.5); transform: scale(1.02); }}
            .stat-label {{ font-size: 1rem; color: #ccc; text-transform: uppercase; margin-bottom: 0.5rem; text-align: center; }}
            .stat-value {{ font-size: 2.2rem; color: #f88; font-weight: bold; text-align: center; }}
            
            /* Chart Layouts */
            .chart-wrapper {{ 
                border-radius: 12px; 
                padding: 1rem; 
                margin-bottom: 2rem; 
            }}
            
            /* User Cards Grid */
            .user-cards-grid {{
                display: grid;
                grid-template-columns: repeat(auto-fill, minmax(350px, 1fr));
                gap: 1.5rem;
                margin-top: 1rem;
            }}
            .user-card {{
                border-radius: 12px;
                display: flex;
                flex-direction: column;
                overflow: hidden;
                transition: transform 0.2s ease;
            }}
            .user-card:hover {{ transform: translateY(-3px); border-color: rgba(255, 119, 119, 0.3); }}
            .uc-header {{
                background-color: rgba(0, 0, 0, 0.3);
                padding: 0.75rem 1rem;
                font-weight: bold;
                color: #f88;
                text-align: center;
                border-bottom: 1px solid rgba(255, 255, 255, 0.05);
                font-family: monospace;
            }}
            .uc-body {{
                display: grid;
                grid-template-columns: 1fr 1fr;
                flex-grow: 1;
            }}
            .uc-quad {{
                padding: 1rem;
                font-size: 0.85rem;
                color: #ddd;
                line-height: 1.5;
            }}
            .uc-quad:first-child {{ border-right: 1px solid rgba(255, 255, 255, 0.05); }}
            .uc-quad strong {{ color: #fff; display: inline-block; margin-bottom: 4px; border-bottom: 1px solid #555; }}
            .uc-footer {{
                background-color: rgba(0, 0, 0, 0.2);
                border-top: 1px solid rgba(255, 255, 255, 0.05);
                padding: 1rem;
                display: grid;
                grid-template-columns: 1fr 1fr;
                gap: 0.75rem;
                font-size: 0.85rem;
                color: #ddd;
            }}
            .uc-footer div strong {{ color: #aaa; }}
        </style>
    </head>
    <body>
        <div id="app" class="app">
            <h1 class="title">{symbol} User Behavioral Analytics</h1>
            <p class="subtitle">Entity resolution, ledger profiling, and wealth distribution</p>

            <div class="grid">
                <div class="card frosted">
                    <div class="stat-label">Total Historical Addresses</div>
                    <div class="stat-value">{total_users:,}</div>
                </div>
                <div class="card frosted">
                    <div class="stat-label">Current Whales (>100k)</div>
                    <div class="stat-value">{whales:,}</div>
                </div>
                <div class="card frosted">
                    <div class="stat-label">Current Retail Users</div>
                    <div class="stat-value">{retail:,}</div>
                </div>
                <div class="card frosted">
                    <div class="stat-label">Out / Dust Accounts</div>
                    <div class="stat-value">{out_dust:,}</div>
                </div>
            </div>

            <h2 class="title" style="font-size: 1.5rem; margin-top: 3rem;">Ledger Behavior Metrics</h2>
            {"".join([f'<div class="chart-wrapper frosted">{chart}</div>' for chart in graphs_html])}

            <h2 class="title" style="font-size: 1.5rem; margin-top: 3rem;">Top 50 Current Holders</h2>
            {generate_user_cards_html(user_stats_df)}
            
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
    
    ledger_df = ledger_df.with_columns(pl.col("date_utc").dt.strftime("%Y-%m").alias("month_str"))
    logger.info(f"Saving Ledger ({len(ledger_df)} rows)...")
    
    for part_df in ledger_df.partition_by("month_str"):
        m = part_df["month_str"][0]
        local_ledger = f"/tmp/{symbol}_ledger_{m}.parquet"
        part_df.write_parquet(local_ledger)
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