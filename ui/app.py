"""
Swing Trading Dashboard
=======================
Visualises data from market.duckdb — impulses, funnel state, watchlist and more.
"""

import os
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────
DB_PATH = Path(__file__).parent.parent / "swing_trading_1" / "data" / "market.duckdb"

STATE_COLORS = {
    "impulse":       "#f97316",   # orange
    "consolidating": "#3b82f6",   # blue
    "watchlist":     "#22c55e",   # green
    "fallout":       "#ef4444",   # red
}
STATE_EMOJI = {
    "impulse":       "🔥",
    "consolidating": "🫧",
    "watchlist":     "✅",
    "fallout":       "💀",
}
BULL_COLOR = "#22c55e"
BEAR_COLOR = "#ef4444"


# ──────────────────────────────────────────────────────────────────────────────
# DB helpers — cached so every tab doesn't re-query
# ──────────────────────────────────────────────────────────────────────────────
@st.cache_resource(show_spinner=False)
def get_conn():
    if not DB_PATH.exists():
        return None
    return duckdb.connect(str(DB_PATH), read_only=True)


@st.cache_data(ttl=300, show_spinner=False)
def q(sql: str, params=None) -> pd.DataFrame:
    conn = get_conn()
    if conn is None:
        return pd.DataFrame()
    if params:
        return conn.execute(sql, params).df()
    return conn.execute(sql).df()


# ──────────────────────────────────────────────────────────────────────────────
# Page setup
# ──────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Swing Radar",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS
st.markdown("""
<style>
    .metric-card {
        background: #1e293b;
        border-radius: 12px;
        padding: 16px 20px;
        border-left: 4px solid;
        margin-bottom: 4px;
    }
    .badge {
        display: inline-block;
        padding: 2px 10px;
        border-radius: 9999px;
        font-size: 12px;
        font-weight: 600;
    }
    .tag-impulse       { background:#f97316; color:white; }
    .tag-consolidating { background:#3b82f6; color:white; }
    .tag-watchlist     { background:#22c55e; color:white; }
    .tag-fallout       { background:#ef4444; color:white; }
    .tag-bull          { background:#22c55e; color:white; }
    .tag-bear          { background:#ef4444; color:white; }
    .stTabs [data-baseweb="tab"] { font-size: 15px; }
</style>
""", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────
# Sidebar
# ──────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 📡 Swing Radar")
    st.markdown("---")

    # Last run info
    run_log = q("""
        SELECT run_date, status, tickers_processed, candles_written, impulses_found, ran_at
        FROM run_log ORDER BY run_date DESC LIMIT 1
    """)
    if not run_log.empty:
        last = run_log.iloc[0]
        status_color = "🟢" if last["status"] == "success" else "🔴"
        st.markdown(f"**Last Run:** {status_color} {last['run_date']}")
        st.markdown(f"**Tickers scanned:** {last['tickers_processed']:,}")
        st.markdown(f"**Impulses found:** {last['impulses_found']}")
        st.markdown(f"**Candles written:** {last['candles_written']:,}")
    else:
        st.warning("No pipeline runs found in DB yet.")

    st.markdown("---")
    st.markdown("**View date**")
    max_date_row = q("SELECT MAX(snapshot_date) as d FROM funnel_snapshots")
    max_date = date.today()
    if not max_date_row.empty:
        raw = max_date_row.iloc[0]["d"]
        # Guard against None, NaT, and any pandas NA-like values
        try:
            converted = pd.to_datetime(raw)
            if converted is not pd.NaT and not pd.isnull(converted):
                max_date = converted.date()
        except Exception:
            pass
    selected_date = st.date_input("Snapshot date", value=max_date)

    st.markdown("---")
    st.markdown("**Direction filter**")
    dir_filter = st.multiselect("", ["BULL", "BEAR"], default=["BULL", "BEAR"])


# ──────────────────────────────────────────────────────────────────────────────
# Header KPIs
# ──────────────────────────────────────────────────────────────────────────────
st.markdown("## 📡 Swing Radar Dashboard")

counts = q("""
    SELECT state, COUNT(*) as n
    FROM funnel_snapshots
    WHERE snapshot_date = ?
    GROUP BY state
""", [selected_date])

count_map = dict(zip(counts["state"], counts["n"])) if not counts.empty else {}

col1, col2, col3, col4 = st.columns(4)
col1.metric("🔥 Impulses today",    count_map.get("impulse", 0))
col2.metric("🫧 Consolidating",     count_map.get("consolidating", 0))
col3.metric("✅ Watchlist (ready)", count_map.get("watchlist", 0))
col4.metric("💀 Fallen out",        count_map.get("fallout", 0))

st.markdown("---")

# ──────────────────────────────────────────────────────────────────────────────
# Tabs
# ──────────────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "✅ Watchlist",
    "🔥 Impulse Scanner",
    "🫧 Funnel Pipeline",
    "📈 Stock Deep-Dive",
    "📋 Run Log",
])


# ┌─────────────────────────────────┐
# │  TAB 1 — Watchlist              │
# └─────────────────────────────────┘
with tab1:
    st.subheader(f"Stocks ready for entry — {selected_date}")

    wl = q("""
        SELECT
            fs.ticker,
            fs.impulse_date,
            fs.stable_days,
            fs.day0_high,
            fs.day0_volume,
            i.direction,
            i.change_pct,
            i.open  AS impulse_open,
            i.close AS impulse_close
        FROM funnel_snapshots fs
        LEFT JOIN impulse_signals i
            ON fs.ticker = i.ticker AND fs.impulse_date = i.trade_date
        WHERE fs.snapshot_date = ? AND fs.state = 'watchlist'
        ORDER BY i.change_pct DESC
    """, [selected_date])

    if not dir_filter:
        wl = wl[wl["direction"].isin([])]
    else:
        wl = wl[wl["direction"].isin(dir_filter)]

    if wl.empty:
        st.info("No watchlist stocks for this date. Try a different date or run the pipeline.")
    else:
        wl["clean_ticker"] = wl["ticker"].str.replace(".NS", "", regex=False)
        wl["Days held"] = wl["stable_days"]
        wl["Impulse %"] = wl["change_pct"].map(lambda x: f"+{x:.1f}%" if x > 0 else f"{x:.1f}%")
        wl["Day 0 High"] = wl["day0_high"].map(lambda x: f"₹{x:,.2f}")
        wl["Direction"] = wl["direction"].map(
            lambda d: f'<span class="badge tag-bull">▲ BULL</span>'
            if d == "BULL"
            else f'<span class="badge tag-bear">▼ BEAR</span>'
        )
        wl["Ticker"] = wl["clean_ticker"]

        # Donut chart of bull vs bear
        dir_counts = wl["direction"].value_counts().reset_index()
        dir_counts.columns = ["direction", "count"]
        col_a, col_b = st.columns([2, 1])
        with col_a:
            st.markdown(wl[["Ticker", "Impulse %", "Day 0 High", "Days held", "Direction"]].to_html(
                escape=False, index=False
            ), unsafe_allow_html=True)
        with col_b:
            fig_donut = px.pie(
                dir_counts, values="count", names="direction",
                color="direction",
                color_discrete_map={"BULL": BULL_COLOR, "BEAR": BEAR_COLOR},
                hole=0.55,
                title="Bull vs Bear split",
            )
            fig_donut.update_traces(textinfo="value+percent")
            fig_donut.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                showlegend=True,
                height=300,
                margin=dict(t=40, b=0, l=0, r=0),
            )
            st.plotly_chart(fig_donut, use_container_width=True)

        # Top impulse movers bar chart
        st.markdown("#### Top movers on Impulse Day")
        fig_bar = px.bar(
            wl.head(20).sort_values("change_pct"),
            x="change_pct", y="clean_ticker",
            orientation="h",
            color="direction",
            color_discrete_map={"BULL": BULL_COLOR, "BEAR": BEAR_COLOR},
            labels={"change_pct": "Change %", "clean_ticker": "Ticker"},
            text="change_pct",
        )
        fig_bar.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
        fig_bar.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            height=max(300, len(wl.head(20)) * 28),
            margin=dict(l=80, r=40, t=10, b=40),
            xaxis_title="Impulse Day Change %",
            yaxis_title="",
        )
        st.plotly_chart(fig_bar, use_container_width=True)


# ┌─────────────────────────────────┐
# │  TAB 2 — Impulse Scanner        │
# └─────────────────────────────────┘
with tab2:
    st.subheader("🔥 Impulse Scanner — Top Movers")

    days_back = st.slider("Show impulses from last N days", 1, 30, 7, key="imp_days")
    since = date.today() - timedelta(days=days_back)

    impulses = q("""
        SELECT ticker, trade_date, open, close, change_pct, direction, interval
        FROM impulse_signals
        WHERE trade_date >= ?
        ORDER BY trade_date DESC, change_pct DESC
    """, [since])

    if impulses.empty:
        st.info("No impulse data found for this period.")
    else:
        if dir_filter:
            impulses = impulses[impulses["direction"].isin(dir_filter)]

        impulses["clean_ticker"] = impulses["ticker"].str.replace(".NS", "", regex=False)
        impulses["Date"] = pd.to_datetime(impulses["trade_date"]).dt.strftime("%b %d")

        col1, col2 = st.columns([3, 2])

        with col1:
            # Heatmap: tickers vs dates coloured by change_pct
            pivot = impulses.pivot_table(
                index="clean_ticker", columns="Date", values="change_pct", aggfunc="first"
            ).fillna(0)
            # Limit to top 40 by max abs move
            top_tickers = impulses.groupby("clean_ticker")["change_pct"].apply(
                lambda x: x.abs().max()
            ).nlargest(40).index
            pivot = pivot.loc[pivot.index.isin(top_tickers)]

            fig_heat = px.imshow(
                pivot,
                color_continuous_scale="RdYlGn",
                color_continuous_midpoint=0,
                aspect="auto",
                title="Impulse Heatmap (% change — top 40 tickers)",
                labels={"color": "Change %"},
            )
            fig_heat.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                height=max(400, len(pivot) * 18),
                margin=dict(t=40, b=40, l=80, r=20),
                xaxis_tickangle=-45,
            )
            st.plotly_chart(fig_heat, use_container_width=True)

        with col2:
            # Daily impulse count bar
            daily = impulses.groupby(["Date", "direction"]).size().reset_index(name="count")
            fig_daily = px.bar(
                daily, x="Date", y="count", color="direction",
                color_discrete_map={"BULL": BULL_COLOR, "BEAR": BEAR_COLOR},
                barmode="group",
                title="Daily Impulse Count",
            )
            fig_daily.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                height=320,
                margin=dict(t=40, b=40, l=20, r=20),
                xaxis_tickangle=-45,
                legend_title_text="",
            )
            st.plotly_chart(fig_daily, use_container_width=True)

            # Distribution of change_pct
            fig_hist = px.histogram(
                impulses, x="change_pct", color="direction",
                color_discrete_map={"BULL": BULL_COLOR, "BEAR": BEAR_COLOR},
                nbins=30,
                title="Distribution of Impulse Moves",
            )
            fig_hist.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                height=280,
                margin=dict(t=40, b=40, l=20, r=20),
                bargap=0.05,
                legend_title_text="",
            )
            st.plotly_chart(fig_hist, use_container_width=True)

        # Raw table (collapsible)
        with st.expander("Raw impulse data"):
            show = impulses[["clean_ticker", "trade_date", "direction", "change_pct", "open", "close"]].copy()
            show.columns = ["Ticker", "Date", "Direction", "Change %", "Open", "Close"]
            show["Change %"] = show["Change %"].map(lambda x: f"+{x:.2f}%" if x > 0 else f"{x:.2f}%")
            show["Open"]  = show["Open"].map(lambda x: f"₹{x:,.2f}")
            show["Close"] = show["Close"].map(lambda x: f"₹{x:,.2f}")
            st.dataframe(show, use_container_width=True)


# ┌─────────────────────────────────┐
# │  TAB 3 — Funnel Pipeline        │
# └─────────────────────────────────┘
with tab3:
    st.subheader(f"🫧 Full Funnel — {selected_date}")

    funnel_data = q("""
        SELECT
            fs.ticker, fs.state, fs.stable_days, fs.day0_high, fs.day0_volume,
            fs.failure_reason, fs.impulse_date,
            i.direction, i.change_pct
        FROM funnel_snapshots fs
        LEFT JOIN impulse_signals i
            ON fs.ticker = i.ticker AND fs.impulse_date = i.trade_date
        WHERE fs.snapshot_date = ?
        ORDER BY fs.state, i.change_pct DESC
    """, [selected_date])

    if funnel_data.empty:
        st.info("No funnel data for this date.")
    else:
        if dir_filter:
            funnel_data = funnel_data[funnel_data["direction"].isin(dir_filter) | funnel_data["direction"].isna()]

        funnel_data["clean_ticker"] = funnel_data["ticker"].str.replace(".NS", "", regex=False)

        # Funnel chart
        state_order = ["impulse", "consolidating", "watchlist", "fallout"]
        funnel_counts = funnel_data.groupby("state").size().reset_index(name="count")
        funnel_counts["state"] = pd.Categorical(funnel_counts["state"], categories=state_order, ordered=True)
        funnel_counts = funnel_counts.sort_values("state")

        col_f1, col_f2 = st.columns([1, 2])
        with col_f1:
            fig_funnel = go.Figure(go.Funnel(
                y=[f"{STATE_EMOJI.get(s, '')} {s.capitalize()}" for s in funnel_counts["state"]],
                x=funnel_counts["count"],
                textinfo="value+percent initial",
                marker=dict(color=[STATE_COLORS.get(s, "#888") for s in funnel_counts["state"]]),
            ))
            fig_funnel.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                height=320,
                margin=dict(t=10, b=10, l=10, r=10),
                title="Conversion Funnel",
            )
            st.plotly_chart(fig_funnel, use_container_width=True)

        with col_f2:
            # Scatter: change_pct vs stable_days, coloured by state
            fig_scatter = px.scatter(
                funnel_data.dropna(subset=["change_pct"]),
                x="stable_days",
                y="change_pct",
                color="state",
                color_discrete_map=STATE_COLORS,
                hover_name="clean_ticker",
                hover_data={"direction": True, "day0_high": ":.2f"},
                size_max=12,
                title="Stable days vs Impulse strength",
                labels={"stable_days": "Stable Days", "change_pct": "Impulse Change %"},
            )
            fig_scatter.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                height=320,
                margin=dict(t=40, b=20, l=20, r=20),
            )
            st.plotly_chart(fig_scatter, use_container_width=True)

        # Per-state cards
        st.markdown("#### Stocks by state")
        for state in ["watchlist", "consolidating", "impulse", "fallout"]:
            subset = funnel_data[funnel_data["state"] == state]
            if subset.empty:
                continue
            color = STATE_COLORS[state]
            emoji = STATE_EMOJI[state]
            with st.expander(f"{emoji} **{state.capitalize()}** — {len(subset)} stocks", expanded=(state == "watchlist")):
                cols = st.columns(5)
                for idx, row in subset.iterrows():
                    c = cols[idx % 5]
                    direction = row.get("direction", "")
                    dir_badge = "▲" if direction == "BULL" else "▼" if direction == "BEAR" else ""
                    pct = row.get("change_pct")
                    pct_str = f"{pct:+.1f}%" if pd.notna(pct) else ""
                    stable = int(row["stable_days"]) if pd.notna(row["stable_days"]) else 0
                    progress = "█" * stable + "░" * (4 - stable)
                    reason = row.get("failure_reason", "") or ""
                    c.markdown(
                        f"""**{row['clean_ticker']}**  
{dir_badge} {pct_str}  
`{progress}` d{stable}  
<small style='color:#94a3b8'>{reason[:30]}</small>""",
                        unsafe_allow_html=True,
                    )


# ┌─────────────────────────────────┐
# │  TAB 4 — Stock Deep Dive        │
# └─────────────────────────────────┘
with tab4:
    st.subheader("📈 Stock Deep-Dive")

    all_tickers = q("SELECT DISTINCT ticker FROM impulse_signals ORDER BY ticker")
    ticker_options = all_tickers["ticker"].str.replace(".NS", "", regex=False).tolist() if not all_tickers.empty else []

    if not ticker_options:
        st.info("No tickers found in the database.")
    else:
        selected_ticker = st.selectbox("Select a stock", ticker_options)
        full_ticker = selected_ticker + ".NS"

        col_a, col_b = st.columns(2)

        with col_a:
            # Funnel journey timeline
            journey = q("""
                SELECT snapshot_date, state, stable_days, failure_reason
                FROM funnel_snapshots
                WHERE ticker = ?
                ORDER BY snapshot_date DESC
                LIMIT 60
            """, [full_ticker])

            if not journey.empty:
                journey["snapshot_date"] = pd.to_datetime(journey["snapshot_date"])
                journey["state_num"] = journey["state"].map(
                    {"impulse": 1, "consolidating": 2, "watchlist": 3, "fallout": 0}
                )
                fig_journey = px.scatter(
                    journey, x="snapshot_date", y="state",
                    color="state",
                    color_discrete_map=STATE_COLORS,
                    hover_data={"stable_days": True, "failure_reason": True},
                    title=f"{selected_ticker} — Funnel Journey",
                    size_max=12,
                    category_orders={"state": ["watchlist", "consolidating", "impulse", "fallout"]},
                )
                fig_journey.update_traces(marker=dict(size=14))
                fig_journey.update_layout(
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    height=280,
                    margin=dict(t=40, b=40, l=20, r=20),
                    showlegend=False,
                    yaxis_title="",
                )
                st.plotly_chart(fig_journey, use_container_width=True)
            else:
                st.info("No funnel history for this ticker.")

        with col_b:
            # OHLCV candlestick
            candles = q("""
                SELECT datetime, open, high, low, close, volume
                FROM candles WHERE ticker = ?
                ORDER BY datetime DESC LIMIT 60
            """, [full_ticker])

            if not candles.empty:
                candles = candles.sort_values("datetime")
                fig_candle = make_subplots(
                    rows=2, cols=1, shared_xaxes=True,
                    row_heights=[0.7, 0.3], vertical_spacing=0.02
                )
                fig_candle.add_trace(go.Candlestick(
                    x=candles["datetime"],
                    open=candles["open"],
                    high=candles["high"],
                    low=candles["low"],
                    close=candles["close"],
                    name="Price",
                    increasing_line_color=BULL_COLOR,
                    decreasing_line_color=BEAR_COLOR,
                ), row=1, col=1)
                colors_vol = [BULL_COLOR if c >= o else BEAR_COLOR
                              for c, o in zip(candles["close"], candles["open"])]
                fig_candle.add_trace(go.Bar(
                    x=candles["datetime"], y=candles["volume"],
                    marker_color=colors_vol, name="Volume", showlegend=False,
                ), row=2, col=1)
                fig_candle.update_layout(
                    title=f"{selected_ticker} — Price & Volume",
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    height=360,
                    margin=dict(t=40, b=20, l=20, r=20),
                    xaxis_rangeslider_visible=False,
                )
                st.plotly_chart(fig_candle, use_container_width=True)
            else:
                st.info("No candle data for this ticker.")

        # Impulse history table
        imp_history = q("""
            SELECT trade_date, direction, open, close, change_pct
            FROM impulse_signals WHERE ticker = ?
            ORDER BY trade_date DESC
        """, [full_ticker])

        if not imp_history.empty:
            st.markdown("#### Impulse History")
            imp_history["change_pct"] = imp_history["change_pct"].map(
                lambda x: f"+{x:.2f}%" if x > 0 else f"{x:.2f}%"
            )
            imp_history["open"]  = imp_history["open"].map(lambda x: f"₹{x:,.2f}")
            imp_history["close"] = imp_history["close"].map(lambda x: f"₹{x:,.2f}")
            imp_history.columns = ["Date", "Direction", "Open", "Close", "Change %"]
            st.dataframe(imp_history, use_container_width=True, hide_index=True)


# ┌─────────────────────────────────┐
# │  TAB 5 — Run Log                │
# └─────────────────────────────────┘
with tab5:
    st.subheader("📋 Pipeline Run Log")

    full_log = q("""
        SELECT run_date, status, tickers_processed, candles_written, impulses_found, ran_at, error
        FROM run_log ORDER BY run_date DESC LIMIT 90
    """)

    if full_log.empty:
        st.info("No run log entries yet.")
    else:
        full_log["run_date"] = pd.to_datetime(full_log["run_date"])

        col_l1, col_l2 = st.columns(2)

        with col_l1:
            # Impulses found over time
            fig_imp = px.area(
                full_log.sort_values("run_date"),
                x="run_date", y="impulses_found",
                title="Impulses Found per Day",
                color_discrete_sequence=["#f97316"],
            )
            fig_imp.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                height=250,
                margin=dict(t=40, b=20, l=20, r=20),
            )
            st.plotly_chart(fig_imp, use_container_width=True)

        with col_l2:
            # Candles written over time
            fig_can = px.bar(
                full_log.sort_values("run_date"),
                x="run_date", y="candles_written",
                title="Candles Written per Day",
                color_discrete_sequence=["#3b82f6"],
            )
            fig_can.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                height=250,
                margin=dict(t=40, b=20, l=20, r=20),
            )
            st.plotly_chart(fig_can, use_container_width=True)

        # Status badges
        success_count = (full_log["status"] == "success").sum()
        fail_count    = (full_log["status"] == "failed").sum()
        st.markdown(
            f"**{success_count}** successful runs &nbsp; | &nbsp; **{fail_count}** failed runs "
            f"(last 90 days)"
        )

        # Table with error column
        show_log = full_log.copy()
        show_log["Status"] = show_log["status"].map(
            lambda s: "🟢 success" if s == "success" else "🔴 failed"
        )
        show_log = show_log.rename(columns={
            "run_date": "Date",
            "tickers_processed": "Tickers",
            "candles_written": "Candles",
            "impulses_found": "Impulses",
            "ran_at": "Ran At",
            "error": "Error",
        })
        st.dataframe(
            show_log[["Date", "Status", "Tickers", "Candles", "Impulses", "Ran At", "Error"]],
            use_container_width=True,
            hide_index=True,
        )

        # Failed runs with errors
        failed = full_log[full_log["status"] == "failed"]
        if not failed.empty:
            with st.expander("🔴 Failed runs detail"):
                for _, row in failed.iterrows():
                    st.error(f"**{row['run_date'].date()}** — {row['error']}")
