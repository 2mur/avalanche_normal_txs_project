import sys
import logging
import polars as pl
import plotly.graph_objects as go
import plotly.express as px
import plotly.io as pio
from google.cloud import storage
from config import TOKENS, BUCKET_NAME, REPORTS_FOLDER

# ---------------- LOGGING ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def polars_to_html_table(df: pl.DataFrame) -> str:
    if df.is_empty():
        return "<p>No data available</p>"

    # Automatically enforce .2f maximum for all float columns
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


def generate_eda_report(token_symbol: str):
    logger.info(f"Generating Visual Analytics Report for {token_symbol}...")

    # 1. Native Polars Scanning targeting ONLY transfers in Google Cloud Storage
    path_glob = f"gs://{BUCKET_NAME}/processed_normal_transfers/token={token_symbol}/**/*.parquet"
    
    try:
        lf = pl.scan_parquet(path_glob, hive_partitioning=False)

        # Collect necessary columns
        lf = lf.with_columns([
            pl.from_epoch(pl.col("timeStamp"), time_unit="s").dt.date().alias("date_utc"),
            pl.from_epoch(pl.col("timeStamp"), time_unit="s").dt.strftime("%Y-%m").alias("month_str"),
            pl.from_epoch(pl.col("timeStamp"), time_unit="s").dt.day().cast(pl.Utf8).alias("day_of_month")
        ])

        df = lf.collect()

        if df.is_empty():
            logger.warning(f"No transfer data found for {token_symbol}.")
            return

    except Exception as e:
        logger.error(f"Error loading data for {token_symbol}: {e}")
        return

    # ---------------- GLOBAL STATS ----------------
    min_date = df["date_utc"].min()
    max_date = df["date_utc"].max()
    total_txs = len(df)

    u_caller = df["caller"].n_unique()
    u_from = df["from"].n_unique()
    u_to = df["to"].n_unique()

    min_val = df["value"].min()
    max_val = df["value"].max()
    mean_val = df["value"].mean()
    median_val = df["value"].median()

    # ---------------- LEADERBOARD ----------------
    addresses_df = pl.concat([
        df.select(pl.col("from").alias("address")),
        df.select(pl.col("to").alias("address"))
    ])
    
    top_50_addresses = (
        addresses_df.group_by("address")
        .len()
        .sort("len", descending=True)
        .head(100)
        .rename({"len": "Transfer Count"})
    )

    # ---------------- TRUE CUMULATIVE UNIQUES LOGIC ----------------
    new_callers = df.group_by("caller").agg(pl.col("month_str").min().alias("month_str")).group_by("month_str").len().rename({"len": "new_callers"})
    new_senders = df.group_by("from").agg(pl.col("month_str").min().alias("month_str")).group_by("month_str").len().rename({"len": "new_senders"})
    new_receivers = df.group_by("to").agg(pl.col("month_str").min().alias("month_str")).group_by("month_str").len().rename({"len": "new_receivers"})

    # ---------------- MONTHLY AGGREGATION ----------------
    monthly_aggs = (
        df.group_by("month_str")
        .agg([
            pl.len().alias("tx_count"),
            pl.col("value").sum().alias("total_volume"),
            pl.col("value").mean().alias("avg_transfer_size"),
            pl.col("value").median().alias("med_transfer_size"),
            pl.col("caller").n_unique().alias("active_callers"),
            pl.col("from").n_unique().alias("active_senders"),
            pl.col("to").n_unique().alias("active_receivers"),
        ])
        .join(new_callers, on="month_str", how="left")
        .join(new_senders, on="month_str", how="left")
        .join(new_receivers, on="month_str", how="left")
        .fill_null(0)
        .sort("month_str")
        .with_columns([
            pl.col("tx_count").cum_sum().alias("cum_tx_count"),
            pl.col("total_volume").cum_sum().alias("cum_volume"),
            pl.col("new_callers").cum_sum().alias("total_unique_callers"),
            pl.col("new_senders").cum_sum().alias("total_unique_senders"),
            pl.col("new_receivers").cum_sum().alias("total_unique_receivers")
        ])
    )
    months = monthly_aggs["month_str"].to_list()

    # ---------------- PLOTS ----------------
    template_style = "plotly_dark"

    fig_monthly = go.Figure()
    fig_monthly.add_trace(go.Bar(x=months, y=monthly_aggs["tx_count"].to_list(), name="Transactions", marker_color='#f88'))
    fig_monthly.add_trace(go.Scatter(x=months, y=monthly_aggs["total_volume"].to_list(), name="Total Volume", yaxis="y2", mode="lines+markers", line=dict(color='#ccc', width=3)))
    fig_monthly.update_layout(title="Monthly Activity & Volume", yaxis=dict(title="Transactions"), yaxis2=dict(title="Volume", overlaying="y", side="right"), template=template_style, paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')

    fig_cum_activity = go.Figure()
    fig_cum_activity.add_trace(go.Scatter(x=months, y=monthly_aggs["cum_tx_count"].to_list(), name="Total Transactions", mode="lines", fill='tozeroy', line=dict(color='#f88', width=3)))
    fig_cum_activity.add_trace(go.Scatter(x=months, y=monthly_aggs["cum_volume"].to_list(), name="Total Volume", yaxis="y2", mode="lines", line=dict(color='#ccc', width=3)))
    fig_cum_activity.update_layout(title="Cumulative Growth: Transactions & Volume", yaxis=dict(title="Total Transactions"), yaxis2=dict(title="Total Volume Moved", overlaying="y", side="right"), template=template_style, paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')

    fig_addresses = go.Figure()
    fig_addresses.add_trace(go.Scatter(x=months, y=monthly_aggs["active_callers"].to_list(), mode='lines+markers', name='Active Callers'))
    fig_addresses.add_trace(go.Scatter(x=months, y=monthly_aggs["active_senders"].to_list(), mode='lines+markers', name='Active Senders'))
    fig_addresses.add_trace(go.Scatter(x=months, y=monthly_aggs["active_receivers"].to_list(), mode='lines+markers', name='Active Receivers'))
    fig_addresses.update_layout(title="Monthly Active Participants", yaxis_title="Active Addresses per Month", template=template_style, paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')

    fig_cum_users = go.Figure()
    fig_cum_users.add_trace(go.Scatter(x=months, y=monthly_aggs["total_unique_callers"].to_list(), mode='lines', name='Total Unique Callers', line=dict(width=3)))
    fig_cum_users.add_trace(go.Scatter(x=months, y=monthly_aggs["total_unique_senders"].to_list(), mode='lines', name='Total Unique Senders', line=dict(width=3)))
    fig_cum_users.add_trace(go.Scatter(x=months, y=monthly_aggs["total_unique_receivers"].to_list(), mode='lines', name='Total Unique Receivers', line=dict(width=3)))
    fig_cum_users.update_layout(title="Cumulative Network Adoption", yaxis_title="Total Addresses", template=template_style, paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')

    fig_sizes = go.Figure()
    fig_sizes.add_trace(go.Bar(x=months, y=monthly_aggs["avg_transfer_size"].to_list(), name='Avg Transfer Size', marker_color='#f88'))
    fig_sizes.add_trace(go.Scatter(x=months, y=monthly_aggs["med_transfer_size"].to_list(), name='Median Transfer Size', yaxis='y2', mode='lines+markers', line=dict(color='#ccc', width=2)))
    fig_sizes.update_layout(title="Transfer Size Dynamics Over Time", yaxis=dict(title="Average Size"), yaxis2=dict(title="Median Size", overlaying="y", side="right", type="log"), template=template_style, paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')

    # --- PIVOT FIX APPLIED HERE ---
    heatmap_df = df.group_by(["month_str", "day_of_month"]).len()
    try:
        # Replaced 'on' with 'columns' for Polars < 1.0.0
        matrix = heatmap_df.pivot(values="len", index="month_str", columns="day_of_month").fill_null(0).sort("month_str")
        day_cols = sorted([col for col in matrix.columns if col != "month_str"], key=int)
        z_data = [list(row) for row in matrix.select(day_cols).iter_rows()]

        fig_heat = px.imshow(z_data, labels=dict(x="Day of Month", y="Month", color="Transactions"), x=day_cols, y=matrix["month_str"].to_list(), color_continuous_scale="Reds", aspect="auto")
        fig_heat.update_layout(title="Transaction Heatmap: Month vs Day of Month", template=template_style, paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
        html_heat = pio.to_html(fig_heat, full_html=False, include_plotlyjs=False)
    except Exception as e:
        logger.warning(f"Heatmap generation skipped: {e}")
        html_heat = f"<p style='text-align:center;'>Data distribution insufficient for Heatmap. ({e})</p>"

    # ---------------- HTML TEMPLATE ----------------
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>{token_symbol} Transfer Analytics</title>
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
            .app {{
                padding: 2rem;
                max-width: 1400px;
                margin: 0 auto;
            }}
            .title {{
                text-align: center;
                font-size: 2rem;
                margin-bottom: 2rem;
                color: #fff;
            }}
            .subtitle {{
                text-align: center;
                font-size: 1rem;
                color: #aaa;
                margin-bottom: 2rem;
            }}
            .grid {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
                gap: 2rem;
                margin-bottom: 2rem;
            }}
            .grid-2 {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(500px, 1fr));
                gap: 2rem;
                margin-bottom: 2rem;
            }}
            .card {{
                background-color: #3a3a3a;
                border: 2px solid #444;
                border-radius: 12px;
                padding: 1.5rem;
                display: flex;
                flex-direction: column;
                align-items: center;
                justify-content: center;
                transition: transform 0.2s ease, border-color 0.2s ease;
            }}
            .card:hover {{
                border-color: #f77;
                transform: scale(1.02);
            }}
            .stat-label {{
                font-size: 1rem;
                color: #aaa;
                text-transform: uppercase;
                margin-bottom: 0.5rem;
                text-align: center;
            }}
            .stat-value {{
                font-size: 2.2rem;
                color: #f88;
                font-weight: bold;
                text-align: center;
            }}
            .chart-wrapper {{
                background-color: #3a3a3a;
                border: 2px solid #444;
                border-radius: 12px;
                padding: 1rem;
                margin-bottom: 2rem;
            }}
            .table-wrapper {{
                height: 500px;
                overflow-y: auto;
                background-color: #3a3a3a;
                border: 2px solid #444;
                border-radius: 12px;
                padding: 1rem;
                margin-bottom: 2rem;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                text-align: left;
            }}
            th {{
                padding: 12px;
                border-bottom: 2px solid #666;
                color: #f88;
                position: sticky;
                top: 0;
                background: #3a3a3a;
                z-index: 10;
            }}
            td {{
                padding: 12px;
                border-bottom: 1px solid #444;
                color: #ddd;
            }}
            ::-webkit-scrollbar {{ width: 8px; height: 8px; }}
            ::-webkit-scrollbar-track {{ background: #292929; border-radius: 4px; }}
            ::-webkit-scrollbar-thumb {{ background: #555; border-radius: 4px; }}
            ::-webkit-scrollbar-thumb:hover {{ background: #f88; }}
        </style>
    </head>
    <body>
        <div id="app" class="app">
            <h1 class="title">{token_symbol} Standard Transfers Analytics</h1>
            <p class="subtitle">Period: {min_date} to {max_date}</p>

            <div class="grid">
                <div class="card">
                    <div class="stat-label">Total Transfers</div>
                    <div class="stat-value">{total_txs:,}</div>
                </div>
                <div class="card">
                    <div class="stat-label">Total Unique Callers</div>
                    <div class="stat-value">{u_caller:,}</div>
                </div>
                <div class="card">
                    <div class="stat-label">Total Unique Senders</div>
                    <div class="stat-value">{u_from:,}</div>
                </div>
                <div class="card">
                    <div class="stat-label">Total Unique Receivers</div>
                    <div class="stat-value">{u_to:,}</div>
                </div>
            </div>

            <div class="grid">
                <div class="card">
                    <div class="stat-label">Min Transfer</div>
                    <div class="stat-value">{min_val:,.2f}</div>
                </div>
                <div class="card">
                    <div class="stat-label">Max Transfer</div>
                    <div class="stat-value">{max_val:,.2f}</div>
                </div>
                <div class="card">
                    <div class="stat-label">Avg Transfer</div>
                    <div class="stat-value">{mean_val:,.2f}</div>
                </div>
                <div class="card">
                    <div class="stat-label">Median Transfer</div>
                    <div class="stat-value">{median_val:,.2f}</div>
                </div>
            </div>

            <h2 class="title" style="font-size: 1.5rem; margin-top: 3rem;">Monthly Time Series Analysis</h2>
            <div class="chart-wrapper">
                {pio.to_html(fig_monthly, full_html=False, include_plotlyjs='cdn')}
            </div>
            
            <div class="chart-wrapper">
                {pio.to_html(fig_addresses, full_html=False, include_plotlyjs=False)}
            </div>

            <div class="chart-wrapper">
                {pio.to_html(fig_sizes, full_html=False, include_plotlyjs=False)}
            </div>

            <h2 class="title" style="font-size: 1.5rem; margin-top: 3rem;">Activity Distribution</h2>
            
            <div class="chart-wrapper">
                {html_heat}
            </div>

            <h2 class="title" style="font-size: 1.5rem; margin-top: 3rem;">Growth & Cumulative Metrics</h2>
            
            <div class="chart-wrapper">
                {pio.to_html(fig_cum_activity, full_html=False, include_plotlyjs=False)}
            </div>

            <div class="chart-wrapper">
                {pio.to_html(fig_cum_users, full_html=False, include_plotlyjs=False)}
            </div>
            
            <h2 class="title" style="font-size: 1.5rem; margin-top: 3rem;">Top 100 Most Active Addresses</h2>
            <p class="subtitle">(Aggregated transfer count acting as sender or receiver)</p>
            <div class="table-wrapper">
                {polars_to_html_table(top_50_addresses)}
            </div>

            <h2 class="title" style="font-size: 1.5rem; margin-top: 3rem;">In-Depth Monthly Metrics</h2>
            <div class="table-wrapper">
                {polars_to_html_table(monthly_aggs.drop(["new_callers", "new_senders", "new_receivers"]))}
            </div>
        </div>
    </body>
    </html>
    """
    
    # ---------------- SAVE TO GOOGLE CLOUD STORAGE ----------------
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    blob_name = f"{REPORTS_FOLDER}/{token_symbol}_transfers_eda.html"
    bucket.blob(blob_name).upload_from_string(
        html_content,
        content_type="text/html"
    )

    logger.info(f"Report securely saved to Google Cloud Storage: gs://{BUCKET_NAME}/{blob_name}")

def main():
    for token in TOKENS:
        generate_eda_report(token.symbol)

if __name__ == "__main__":
    main()