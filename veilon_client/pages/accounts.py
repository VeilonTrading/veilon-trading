import streamlit as st
import pandas as pd
import altair as alt
import plotly.graph_objects as go
import asyncio
from datetime import datetime, timezone
from veilon_client.pages.routes import CHECKOUT_PAGE
from veilon_client.pages.routes import DASHBOARD_PAGE
from veilon_client.static.elements.metrics import metric_tile, empty_tile
from veilon_core.repositories.users import get_or_create_user_from_oidc
from veilon_core.repositories.accounts import get_accounts_for_user
from veilon_core.services.account_deployment_handler import handle_account_deployment
from veilon_core.services.improved_account_flow import get_improved_lifecycle_manager

def get_plan_specifications(account_id: int) -> dict:
    """
    Get the plan specifications for an account.
    Returns profit_target_pct, max_drawdown_pct, etc.
    
    Values are returned as decimals (e.g., 0.10 for 10%)
    """
    from veilon_core.repositories.db import execute_query
    
    # Get the most recent trading period for THIS account to determine phase
    period_rows = execute_query(
        """
        SELECT tp.period_type, a.plan_id, a.metaapi_account_id
        FROM trading_periods tp
        JOIN accounts a ON a.id = tp.account_id
        WHERE tp.account_id = %s
        ORDER BY tp.start_time DESC
        LIMIT 1
        """,
        (account_id,),
        fetch_results=True,
    )
    
    if not period_rows:
        # No trading period - try to get from account directly
        account_rows = execute_query(
            """
            SELECT plan_id FROM accounts WHERE id = %s
            """,
            (account_id,),
            fetch_results=True,
        )
        if not account_rows:
            return None
        plan_id = account_rows[0]['plan_id']
        period_type = 'phase_1'  # Default to phase 1
    else:
        plan_id = period_rows[0]['plan_id']
        period_type = period_rows[0]['period_type']
    
    # Map period_type to phase_number or phase_type
    if period_type == 'phase_1':
        phase_condition = "ps.phase_number = 1"
    elif period_type == 'funded':
        phase_condition = "ps.phase_type = 'funded'"
    else:
        phase_condition = "ps.phase_number = 1"  # Default
    
    spec_rows = execute_query(
        f"""
        SELECT 
            profit_target_pct,
            max_drawdown_pct,
            daily_drawdown_pct,
            time_limit_days,
            min_trading_days,
            profit_split_pct,
            name,
            phase_type
        FROM plan_specifications ps
        WHERE ps.plan_id = %s
        AND {phase_condition}
        LIMIT 1
        """,
        (plan_id,),
        fetch_results=True,
    )
    
    if not spec_rows:
        return None
    
    row = spec_rows[0]
    
    # Convert percentages from DB format (10.00 = 10%) to decimal (0.10)
    return {
        'profit_target_pct': float(row['profit_target_pct']) / 100 if row['profit_target_pct'] else 0.10,
        'max_drawdown_pct': float(row['max_drawdown_pct']) / 100 if row['max_drawdown_pct'] else 0.10,
        'daily_drawdown_pct': float(row['daily_drawdown_pct']) / 100 if row['daily_drawdown_pct'] else None,
        'time_limit_days': row['time_limit_days'],
        'min_trading_days': row['min_trading_days'],
        'profit_split_pct': float(row['profit_split_pct']) / 100 if row['profit_split_pct'] else None,
        'name': row['name'],
        'phase_type': row['phase_type'],
    }

def get_fresh_account_status(account_id: int) -> tuple[str, str]:
    """
    Query fresh status and phase directly from database
    This ensures we have the latest status after deployment
    """
    from veilon_core.repositories.db import execute_query
    
    rows = execute_query(
        """
        SELECT status, phase
        FROM accounts
        WHERE id = %s
        """,
        (account_id,),
        fetch_results=True
    )
    
    if not rows:
        return (None, None)
    
    return (rows[0].get('status'), rows[0].get('phase'))

def hide_streamlit_header():
    """Hide the default Streamlit header and chart toolbars"""
    st.markdown(
        """
        <style>
        /* Hide the Streamlit header */
        header[data-testid="stHeader"] {
            display: none;
        }
        
        /* Hide the hamburger menu */
        #MainMenu {
            visibility: hidden;
        }
        
        /* Hide the "Made with Streamlit" footer */
        footer {
            visibility: hidden;
        }
        
        /* Reduce top padding since header is gone */
        .block-container {
            padding-top: 1rem;
        }
        
        /* Hide chart toolbar on hover */
        [data-testid="stElementToolbar"] {
            display: none !important;
        }
        
        /* Hide Vega chart actions menu (download PNG, etc.) */
        details[title="Click to view actions"],
        .vega-actions {
            display: none !important;
        }
        </style>
        """,
        unsafe_allow_html=True
    )

def render_start_evaluation_button(account_id: int, metaapi_account_id: str):
    """
    Show 'Start Evaluation' button
    Only shows warning if user clicks and account has open positions
    """
    
    # Check for any previous error stored in session state
    error_key = f"start_eval_error_{account_id}"
    
    if error_key in st.session_state and st.session_state[error_key]:
        st.warning("⚠️ **Account is not flat**")
        st.caption(st.session_state[error_key])
    
    st.space("medium")
    st.write("**Ready to begin your evaluation?**")
    st.caption("Ensure all positions are closed in your MT4/5 terminal before starting.")
    st.space("small")
    if st.button("🚀 Start Evaluation", type="secondary", use_container_width=True, key="start_eval_btn"):
        # Clear any previous error
        st.session_state[error_key] = None
        
        with st.spinner("Checking account status..."):
            lifecycle_manager = get_improved_lifecycle_manager()
            
            # Run async function - pass both account_id and metaapi_account_id
            result = asyncio.run(
                lifecycle_manager.attempt_start_evaluation(account_id, metaapi_account_id)
            )
            
            if result['success']:
                st.success(result['success_message'])
                st.balloons()
                st.rerun()
            else:
                # Store error in session state to show warning on rerun
                st.session_state[error_key] = result['error_message']
                st.rerun()

def get_account_status(account: dict) -> tuple[str, str]:
    """
    Determine account status badge text and color.
    Returns: (badge_text, badge_color)
    """
    passed_at = account.get("passed_at")
    funded_at = account.get("funded_at")
    closed_at = account.get("closed_at")
    is_enabled = account.get("is_enabled", True)
    in_review = account.get("in_review", False)
    
    # Failed: account is closed
    if closed_at is not None:
        return ("Failed", "red")
    
    # In Review: not closed but under review
    if in_review:
        return ("In Review", "orange")
    
    # Funded: passed, funded, active, not in review
    if funded_at is not None and is_enabled and not in_review:
        return ("Funded", "green")
    
    # Phase 1: active incomplete challenge
    # passed_at == NULL, funded_at == NULL, closed_at == NULL, is_enabled == TRUE, in_review == FALSE
    if passed_at is None and funded_at is None and closed_at is None and is_enabled and not in_review:
        return ("Phase 1", "blue")
    
    # Default fallback
    return ("Disabled", "gray")

def format_balance_label(balance: float) -> str:
    """Format balance as '10k', '50k', '100k', etc."""
    if balance >= 1_000_000:
        return f"{balance / 1_000_000:.0f}M"
    elif balance >= 1_000:
        return f"{balance / 1_000:.0f}k"
    else:
        return f"{balance:.0f}"

def account_summary_tile(account: dict):
    """Render the account summary tile with plan name, account ID, and status."""
    account_id = account.get("id", "")
    balance = float(account.get("balance", 0))
    plan_name = account.get("plan_name", "Unknown Plan")
    
    # Extract plan type from plan_name (e.g., "50k 1-Step" -> "50k 1-Step Evaluation")
    # If plan_name doesn't contain "Evaluation", add it
    if "Evaluation" not in plan_name:
        title = f"{plan_name} Evaluation"
    else:
        title = plan_name
    
    badge_text, badge_color = get_account_status(account)
    
    metric_tile(
        key="stat-1-tile",
        title=title,
        value=f"#{account_id}",
        value_size="1.85rem",
        footer_badge=badge_text,
        footer_badge_color=badge_color,
        title_padding_bottom="0.5rem",
    )

def get_user_id() -> int:
    email = st.user.email.strip().lower()
    user = get_or_create_user_from_oidc(
        email=email,
        given_name=getattr(st.user, "given_name", None),
        family_name=getattr(st.user, "family_name", None),
    )
    return user["id"]

def get_user_accounts(user_id: int) -> list[dict]:
    return get_accounts_for_user(user_id)

def build_account_label_map(accounts: list[dict]) -> tuple[dict[str, dict], list[str], bool]:
    if not accounts:
        return {}, ["No accounts available"], True

    label_to_account = {str(a["id"]): a for a in accounts}
    return label_to_account, list(label_to_account.keys()), False

def get_trading_period_for_account(account_id: int) -> dict:
    """
    Get the most recent trading period for a specific account.
    Returns start_time, end_time, and metaapi_account_id.
    """
    from veilon_core.repositories.db import execute_query
    
    rows = execute_query(
        """
        SELECT 
            tp.start_time, 
            tp.end_time, 
            tp.metaapi_account_id,
            tp.period_type,
            tp.status
        FROM trading_periods tp
        WHERE tp.account_id = %s
        ORDER BY tp.start_time DESC 
        LIMIT 1
        """,
        (account_id,),
        fetch_results=True,
    )
    
    if not rows:
        return None
    
    return {
        'start_time': rows[0]['start_time'],
        'end_time': rows[0]['end_time'],
        'metaapi_account_id': rows[0]['metaapi_account_id'],
        'period_type': rows[0]['period_type'],
        'status': rows[0]['status'],
    }

def get_latest_ohlc_bar(account_id: int):
    """Get the most recent completed OHLC bar for this account's trading period"""
    from veilon_core.repositories.db import execute_query
    
    # Get the trading period for this account
    period = get_trading_period_for_account(account_id)
    if not period:
        return None
    
    metaapi_account_id = period['metaapi_account_id']
    start_time = period['start_time']
    end_time = period['end_time']
    
    # Query OHLC bars within this trading period
    if end_time:
        rows = execute_query(
            """
            SELECT bar_time, open, high, low, close
            FROM equity_ohlc_1min
            WHERE metaapi_account_id = %s
            AND bar_time >= %s
            AND bar_time <= %s
            ORDER BY bar_time DESC
            LIMIT 1
            """,
            (metaapi_account_id, start_time, end_time),
            fetch_results=True,
        )
    else:
        rows = execute_query(
            """
            SELECT bar_time, open, high, low, close
            FROM equity_ohlc_1min
            WHERE metaapi_account_id = %s
            AND bar_time >= %s
            ORDER BY bar_time DESC
            LIMIT 1
            """,
            (metaapi_account_id, start_time),
            fetch_results=True,
        )
    
    if not rows:
        return None
    
    return {
        'bar_time': rows[0]['bar_time'],
        'open': float(rows[0]['open']),
        'high': float(rows[0]['high']),
        'low': float(rows[0]['low']),
        'close': float(rows[0]['close'])
    }

def get_first_equity(account_id: int):
    """Get the first equity value for this account's most recent trading period"""
    from veilon_core.repositories.db import execute_query
    
    # Get the trading period for this account
    period = get_trading_period_for_account(account_id)
    if not period:
        return None
    
    metaapi_account_id = period['metaapi_account_id']
    start_time = period['start_time']
    
    # Get first OHLC bar for this period
    first_bar_rows = execute_query(
        """
        SELECT open
        FROM equity_ohlc_1min
        WHERE metaapi_account_id = %s
        AND bar_time >= %s
        ORDER BY bar_time ASC
        LIMIT 1
        """,
        (metaapi_account_id, start_time),
        fetch_results=True,
    )
    
    if not first_bar_rows:
        return None
    
    return float(first_bar_rows[0]['open'])

@st.fragment(run_every="60s")
def profit_target_tile(account_id: int, is_percentage: bool, balance: float):
    """
    Display current profit (if positive, else 0).
    """
    
    # Get plan specifications for dynamic target
    specs = get_plan_specifications(account_id)
    target = specs['profit_target_pct'] if specs else 0.10  # Default to 10%
    
    latest_bar = get_latest_ohlc_bar(account_id)
    first_equity = get_first_equity(account_id)
    
    # Default to 0 if no data
    if not latest_bar or not first_equity:
        profit_ratio = 0.0
    else:
        current_equity = latest_bar['close']
        gain_ratio = (current_equity - first_equity) / first_equity
        # Only show profit if positive, otherwise 0
        profit_ratio = gain_ratio if gain_ratio > 0 else 0.0

    # Format value and right label based on display mode
    if is_percentage:
        display_value = f"{profit_ratio:.2%}"
        right_label = f"of {target:.0%}"
    else:
        dollar_value = profit_ratio * balance
        target_dollar = target * balance
        display_value = f"${dollar_value:,.2f}"
        right_label = f"of ${target_dollar:,.0f}"

    is_complete = profit_ratio >= target
    
    metric_tile(
        key="stat-3-tile",
        title="Profit Target",
        title_badge="Complete" if is_complete else "Incomplete",
        title_badge_color="green" if is_complete else "orange",
        value=display_value,
        right_label=right_label,
        progress=min(profit_ratio / target, 1.0) if target > 0 else 0.0,
    )

@st.fragment(run_every="60s")
def drawdown_tile(account_id: int, is_percentage: bool, balance: float):
    """
    Display current drawdown (if negative, else 0).
    If account has failed due to drawdown breach, show the limit as breached.
    """
    
    # Get plan specifications for dynamic limit
    specs = get_plan_specifications(account_id)
    dd_limit = specs['max_drawdown_pct'] if specs else 0.10  # Default to 10%
    
    # Check if account has failed
    account_status, _ = get_fresh_account_status(account_id)
    is_failed = account_status == 'failed'
    
    if is_failed:
        # Account failed - show the limit as the breached value
        dd_ratio = dd_limit
        is_breach = True
    else:
        # Active account - calculate current drawdown
        latest_bar = get_latest_ohlc_bar(account_id)
        first_equity = get_first_equity(account_id)
        
        # Default to 0 if no data
        if not latest_bar or not first_equity:
            dd_ratio = 0.0
        else:
            current_equity = latest_bar['close']
            gain_ratio = (current_equity - first_equity) / first_equity
            # Only show drawdown if negative, otherwise 0
            dd_ratio = abs(gain_ratio) if gain_ratio < 0 else 0.0
        
        is_breach = dd_ratio >= dd_limit

    # Format value and right label based on display mode
    if is_percentage:
        display_value = f"{dd_ratio:.2%}"
        right_label = f"of {dd_limit:.0%}"
    else:
        dollar_value = dd_ratio * balance
        limit_dollar = dd_limit * balance
        display_value = f"${dollar_value:,.2f}"
        right_label = f"of ${limit_dollar:,.0f}"
    
    metric_tile(
        key="stat-2-tile",
        title="Max Drawdown",
        title_badge="Breach" if is_breach else "On Track",
        title_badge_color="red" if is_breach else "green",
        value=display_value,
        right_label=right_label,
        progress=min(dd_ratio / dd_limit, 1.0) if dd_limit > 0 else 0.0,
    )

def get_historical_bars(account_id: int, bucket_seconds: int):
    """Get OHLC bars from the equity_ohlc_1min table for this account's trading period"""
    from veilon_core.repositories.db import execute_query
    
    # Get the trading period for this account
    period = get_trading_period_for_account(account_id)
    if not period:
        return []
    
    metaapi_account_id = period['metaapi_account_id']
    start_dt = period['start_time']
    end_dt = period['end_time'] if period['end_time'] else datetime.now(timezone.utc)
    
    if bucket_seconds == 60:  # 1m
        rows = execute_query(
            """
            SELECT
                bar_time AS ts,
                open,
                high,
                low,
                close
            FROM equity_ohlc_1min
            WHERE metaapi_account_id = %s
            AND bar_time >= %s
            AND bar_time <= %s
            ORDER BY bar_time ASC
            """,
            (metaapi_account_id, start_dt, end_dt),
            fetch_results=True,
        ) or []
    elif bucket_seconds == 300:  # 5m
        rows = execute_query(
            """
            WITH five_min_buckets AS (
                SELECT
                    bar_time,
                    open,
                    high,
                    low,
                    close,
                    to_timestamp(floor(extract(epoch from bar_time) / 300) * 300) AT TIME ZONE 'UTC' AS bucket
                FROM equity_ohlc_1min
                WHERE metaapi_account_id = %s
                AND bar_time >= %s
                AND bar_time <= %s
            )
            SELECT
                bucket AS ts,
                (ARRAY_AGG(open ORDER BY bar_time ASC))[1] AS open,
                MAX(high) AS high,
                MIN(low) AS low,
                (ARRAY_AGG(close ORDER BY bar_time DESC))[1] AS close
            FROM five_min_buckets
            GROUP BY bucket
            ORDER BY bucket ASC
            """,
            (metaapi_account_id, start_dt, end_dt),
            fetch_results=True,
        ) or []
    elif bucket_seconds == 900:  # 15m
        rows = execute_query(
            """
            WITH fifteen_min_buckets AS (
                SELECT
                    bar_time,
                    open,
                    high,
                    low,
                    close,
                    to_timestamp(floor(extract(epoch from bar_time) / 900) * 900) AT TIME ZONE 'UTC' AS bucket
                FROM equity_ohlc_1min
                WHERE metaapi_account_id = %s
                AND bar_time >= %s
                AND bar_time <= %s
            )
            SELECT
                bucket AS ts,
                (ARRAY_AGG(open ORDER BY bar_time ASC))[1] AS open,
                MAX(high) AS high,
                MIN(low) AS low,
                (ARRAY_AGG(close ORDER BY bar_time DESC))[1] AS close
            FROM fifteen_min_buckets
            GROUP BY bucket
            ORDER BY bucket ASC
            """,
            (metaapi_account_id, start_dt, end_dt),
            fetch_results=True,
        ) or []
    elif bucket_seconds == 1800:  # 30m
        rows = execute_query(
            """
            WITH thirty_min_buckets AS (
                SELECT
                    bar_time,
                    open,
                    high,
                    low,
                    close,
                    to_timestamp(floor(extract(epoch from bar_time) / 1800) * 1800) AT TIME ZONE 'UTC' AS bucket
                FROM equity_ohlc_1min
                WHERE metaapi_account_id = %s
                AND bar_time >= %s
                AND bar_time <= %s
            )
            SELECT
                bucket AS ts,
                (ARRAY_AGG(open ORDER BY bar_time ASC))[1] AS open,
                MAX(high) AS high,
                MIN(low) AS low,
                (ARRAY_AGG(close ORDER BY bar_time DESC))[1] AS close
            FROM thirty_min_buckets
            GROUP BY bucket
            ORDER BY bucket ASC
            """,
            (metaapi_account_id, start_dt, end_dt),
            fetch_results=True,
        ) or []
    elif bucket_seconds == 3600:  # 1h
        rows = execute_query(
            """
            WITH hourly_buckets AS (
                SELECT
                    bar_time,
                    open,
                    high,
                    low,
                    close,
                    to_timestamp(floor(extract(epoch from bar_time) / 3600) * 3600) AT TIME ZONE 'UTC' AS bucket
                FROM equity_ohlc_1min
                WHERE metaapi_account_id = %s
                AND bar_time >= %s
                AND bar_time <= %s
            )
            SELECT
                bucket AS ts,
                (ARRAY_AGG(open ORDER BY bar_time ASC))[1] AS open,
                MAX(high) AS high,
                MIN(low) AS low,
                (ARRAY_AGG(close ORDER BY bar_time DESC))[1] AS close
            FROM hourly_buckets
            GROUP BY bucket
            ORDER BY bucket ASC
            """,
            (metaapi_account_id, start_dt, end_dt),
            fetch_results=True,
        ) or []
    else:
        return []
    
    return rows

@st.fragment(run_every="60s")
def equity_ohlc_chart(account_id: int, bucket_seconds: int, is_percentage: bool, balance: float):
    """OHLC chart using only completed bars from equity_ohlc_1min table"""
    
    # Get all OHLC bars for this account's trading period
    bars = get_historical_bars(account_id, bucket_seconds)
    
    if not bars:
        st.info("No equity data available yet.")
        return
    
    df = pd.DataFrame(bars)
    
    # Process and render the chart
    df["ts"] = pd.to_datetime(df["ts"], errors="coerce", utc=True)
    for c in ["open", "high", "low", "close"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    
    df = df.dropna(subset=["ts", "open", "high", "low", "close"]).sort_values("ts")
    
    if df.empty:
        st.info("No equity data available yet.")
        return
    
    # Use first bar's open as baseline (actual starting equity)
    baseline = float(df["open"].iloc[0])
    
    # Calculate gains (ratios)
    df["open_gain"]  = (df["open"]  - baseline) / baseline
    df["high_gain"]  = (df["high"]  - baseline) / baseline
    df["low_gain"]   = (df["low"]   - baseline) / baseline
    df["close_gain"] = (df["close"] - baseline) / baseline
    
    # Convert to dollar values if needed
    if not is_percentage:
        df["open_display"]  = df["open_gain"] * balance
        df["high_display"]  = df["high_gain"] * balance
        df["low_display"]   = df["low_gain"] * balance
        df["close_display"] = df["close_gain"] * balance
    else:
        df["open_display"]  = df["open_gain"]
        df["high_display"]  = df["high_gain"]
        df["low_display"]   = df["low_gain"]
        df["close_display"] = df["close_gain"]
    
    # Create explicit bar positioning columns
    df["bar_low"] = df[["open_display", "close_display"]].min(axis=1)
    df["bar_high"] = df[["open_display", "close_display"]].max(axis=1)
    df["is_bullish"] = df["close"] >= df["open"]
    
    df["ts_key"] = df["ts"].dt.strftime("%Y-%m-%d %H:%M:%S")
    
    current_close = float(df["close_display"].iloc[-1])
    
    ymin = float(df["low_display"].min())
    ymax = float(df["high_display"].max())
    pad = max((ymax - ymin) * 0.1, abs(ymax * 0.005))
    y_domain = [ymin - pad, ymax + pad]
    
    candles = alt.Chart(df).encode(
        x=alt.X(
            "ts_key:N",
            sort=alt.SortField(field="ts", order="ascending"),
            axis=alt.Axis(
                title=None,
                labelAngle=0,
                labelOverlap="parity",
                labelExpr="slice(datum.value, 11, 16)"
            ),
        )
    )
    
    axis_format = "+.2%" if is_percentage else "$,.2f"
    
    wicks = candles.mark_rule(color="#A2A2A2").encode(
        y=alt.Y(
            "low_display:Q",
            scale=alt.Scale(domain=y_domain),
            axis=alt.Axis(title=None, format=axis_format, tickCount=8, grid=False, orient="left"),
        ),
        y2="high_display:Q",
        tooltip=[
            alt.Tooltip("ts:T", title="Time", format="%d/%m/%y %H:%M"),
            alt.Tooltip("open_gain:Q",  title="Open",  format="+.2%"),
            alt.Tooltip("high_gain:Q",  title="High",  format="+.2%"),
            alt.Tooltip("low_gain:Q",   title="Low",   format="+.2%"),
            alt.Tooltip("close_gain:Q", title="Close", format="+.2%"),
        ],
    )
    
    bodies = candles.mark_bar().encode(
        y=alt.Y("bar_low:Q"),
        y2=alt.Y2("bar_high:Q"),
        color=alt.condition(
            alt.datum.is_bullish,
            alt.value("#4BA88F"),
            alt.value("#B75A5A"),
        ),
        tooltip=[
            alt.Tooltip("ts:T", title="Time", format="%d/%m/%y %H:%M"),
            alt.Tooltip("open_gain:Q",  title="Open",  format="+.2%"),
            alt.Tooltip("high_gain:Q",  title="High",  format="+.2%"),
            alt.Tooltip("low_gain:Q",   title="Low",   format="+.2%"),
            alt.Tooltip("close_gain:Q", title="Close", format="+.2%"),
        ],
    )
    
    current_price_line = alt.Chart(pd.DataFrame({"y": [current_close]})).mark_rule(
        stroke="#81A4DD",
        strokeWidth=1,
        strokeDash=[1, 1],
        opacity=0.9,
    ).encode(
        y=alt.Y("y:Q", scale=alt.Scale(domain=y_domain))
    )
    
    chart = (
        (wicks + bodies + current_price_line)
        .configure(background="transparent")
        .configure_view(fill="transparent", strokeWidth=0)
    )
    
    st.altair_chart(chart, height=280, width="stretch")

@st.fragment(run_every="60s")
def equity_line_chart(account_id: int, bucket_seconds: int, is_percentage: bool, balance: float):
    """Line chart using only completed bars from equity_ohlc_1min table"""
    
    # Get all bars for this account's trading period
    bars = get_historical_bars(account_id, bucket_seconds)
    
    if not bars:
        st.info("No equity data available yet.")
        return
    
    df = pd.DataFrame(bars)[['ts', 'open', 'close']]
    
    df["ts"] = pd.to_datetime(df["ts"], errors="coerce", utc=True)
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["open"] = pd.to_numeric(df["open"], errors="coerce")
    df = df.dropna(subset=["ts", "close", "open"]).sort_values("ts")
    
    if df.empty:
        st.info("No equity data available yet.")
        return
    
    # Use first bar's open as baseline (actual starting equity)
    baseline = float(df["open"].iloc[0])
    
    if baseline == 0:
        st.info("Baseline equity is zero; cannot normalise.")
        return
    
    df["close_gain"] = (df["close"] - baseline) / baseline
    
    # Convert to dollar values if needed
    if not is_percentage:
        df["close_display"] = df["close_gain"] * balance
    else:
        df["close_display"] = df["close_gain"]
    
    df["ts_key"] = df["ts"].dt.strftime("%Y-%m-%d %H:%M:%S")
    
    current_close = float(df["close_display"].iloc[-1])
    
    ymin = float(df["close_display"].min())
    ymax = float(df["close_display"].max())
    pad = max((ymax - ymin) * 0.1, abs(ymax * 0.005))
    y_domain = [ymin - pad, ymax + pad]
    
    base = alt.Chart(df).encode(
        x=alt.X(
            "ts_key:N",
            sort=alt.SortField(field="ts", order="ascending"),
            axis=alt.Axis(
                title=None,
                labelAngle=0,
                labelOverlap="parity",
                labelExpr="slice(datum.value, 11, 16)",
                tickCount=6,
            ),
        )
    )
    
    axis_format = "+.2%" if is_percentage else "$,.2f"
    
    line = base.mark_line(
        interpolate="monotone",
        stroke="#81A4DD",
        strokeWidth=1,
    ).encode(
        y=alt.Y(
            "close_display:Q",
            scale=alt.Scale(domain=y_domain),
            axis=alt.Axis(title=None, format=axis_format, tickCount=8, grid=False, orient="left"),
        ),
        tooltip=[
            alt.Tooltip("ts:T", title="Time", format="%d/%m/%y %H:%M"),
            alt.Tooltip("close_gain:Q", title="Return", format="+.2%"),
        ],
    )
    
    current_price_line = alt.Chart(pd.DataFrame({"y": [current_close]})).mark_rule(
        stroke="#81A4DD",
        strokeWidth=1,
        strokeDash=[1, 1],
        opacity=0.9,
    ).encode(
        y=alt.Y("y:Q", scale=alt.Scale(domain=y_domain)),
    )
    
    chart = (
        (line + current_price_line)
        .properties(height=250, width="container")
        .configure(background="transparent")
        .configure_view(fill="transparent", strokeWidth=0)
    )
    
    st.altair_chart(chart, height=280, width="stretch")

def veilon_radar_plotly(metrics: dict[str, float], score_out_of_100: int) -> go.Figure:
    labels = list(metrics.keys())
    values = list(metrics.values())

    fig = go.Figure()

    fig.add_trace(go.Scatterpolar(
        r=values + [values[0]],
        theta=labels + [labels[0]],
        fill="toself",
        name="Veilon Score",
        line=dict(
            color="#979696",
            width=1
        ),
        fillcolor="rgba(129, 164, 221, 0.20)",
        marker=dict(size=1),
        text=[f"{v}" for v in values] + [f"{values[0]}"],
        textposition="top center",
        textfont=dict(
            size=11,
            color="rgba(255,255,255,0.85)"
        ),
        mode="lines+markers+text"
    ))

    fig.add_annotation(
        x=0.5, y=0.5, xref="paper", yref="paper",
        text=f"{score_out_of_100}",
        showarrow=False,
        font=dict(size=24),
    )

    fig.update_layout(
        width=220,
        height=250,
        margin=dict(l=20, r=20, t=20, b=20),
        showlegend=False,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(61,157,243,0.10)",
        polar=dict(
            bgcolor="rgba(0,0,0,0)",
            radialaxis=dict(
                visible=True,
                range=[0, 100],
                tick0=0,
                dtick=20,
                showline=False,
                gridcolor="rgba(255,255,255,0)",
                tickfont=dict(size=10, color="rgba(255,255,255,0)"),
                
            ),
            angularaxis=dict(
                showline=False,
                gridcolor="rgba(255,255,255,0.15)",
                tickfont=dict(size=12),
            ),
        ),
    )

    return fig

def accounts_page():
    hide_streamlit_header()

    # Back button
    with st.container(border=False, horizontal=True, vertical_alignment="center"):
        with st.container(
            border=False,
            horizontal=True,
            horizontal_alignment="left",
            vertical_alignment="bottom",
        ):
            if st.button(
                label="Dashboard",
                type="tertiary",
                icon=":material/arrow_back:",
            ):
                st.switch_page(DASHBOARD_PAGE)

    with st.container(border=False, horizontal=True, vertical_alignment="center"):
        with st.container(border=False):
            st.subheader("Accounts", anchor=False)
        st.space("stretch")
        with st.container(border=False, width=150):
            st.image("veilon_client/static/images/veilon_dark.png", width="stretch")

    user_id = get_user_id()
    accounts = get_user_accounts(user_id)

    label_to_account, labels, disabled = build_account_label_map(accounts)

    col11, col22 = st.columns([1,2])

    with col11:
        with st.container(border=False, horizontal=True, height=100, horizontal_alignment="left", vertical_alignment="bottom"):
            selection = st.selectbox(
                "Select Evaluation",
                options=labels,
                disabled=disabled,
                width="stretch",
            )

            if st.button("", type="secondary", icon=":material/add_circle:", width=40):
                st.switch_page(CHECKOUT_PAGE)
    
    with col22:
        with st.container(border=False, horizontal=True, height=100, horizontal_alignment="right", vertical_alignment="bottom"):
            value_format_input = st.segmented_control(
                    "Value Format", 
                    options=[":material/attach_money:", ":material/percent:"], 
                    selection_mode="single",
                    default=":material/attach_money:"
                )

    if value_format_input == ":material/attach_money:":
        value_format = "USD"
    else:
        value_format = "Percentage"

    selected_account = label_to_account.get(selection)
    
    # Get both IDs - account_id is primary, metaapi_account_id for deployment check
    account_id = selected_account.get("id") if selected_account else None
    metaapi_account_id = selected_account.get("metaapi_account_id") if selected_account else None
    account_balance = float(selected_account.get("balance", 100_000.0)) if selected_account else 100_000.0

    if not accounts:
        st.info("Purchase an evaluation to see your performance data, rewards and trade history.")
        return
    
    if not metaapi_account_id:
        st.info("This evaluation isn't linked to a broker account yet. Deploy/connect your MetaAPI account to see equity.")
        
        with st.form(key="connect-account-form", enter_to_submit=False):
            st.write("**Connect Account**")
            
            col1, col2 = st.columns(2)
            
            login_input = col1.text_input(
                "Login", 
                placeholder="Account Login",
                help="Your MT4/5 account login number"
            )
            password_input = col1.text_input(
                "Password", 
                type="password", 
                placeholder="Read-Only Password",
                help="Use your investor/read-only password for security"
            )
            server_input = col2.text_input(
                "Server", 
                placeholder="Server Name",
                help="Your broker's server name (e.g., 'BrokerName-Demo')"
            )
            platform_input = col2.selectbox(
                "Platform", 
                options=["MetaTrader4", "MetaTrader5"], 
                index=None, 
                placeholder="Select Platform"
            )
            
            st.space("xsmall")
            
            submit_button = st.form_submit_button("Connect & Deploy", width="stretch", type="secondary")
            
            if submit_button:
                if not all([login_input, password_input, server_input, platform_input]):
                    st.error("Please fill in all fields")
                else:
                    handle_account_deployment(
                        account_id=account_id,
                        login=login_input,
                        password=password_input,
                        server_name=server_input,
                        platform=platform_input,
                        account_name=selected_account.get('plan_name', 'Veilon Account')
                    )
        
        return

    is_percentage = (value_format == "Percentage")

    # FIXED: Use account_id instead of metaapi_account_id
    account_status, account_phase = get_fresh_account_status(account_id)
    
    # Check if account needs manual start (includes pending_start and pending_flat)
    if account_status in ('pending_flat', 'pending_start') and not account_phase:
        render_start_evaluation_button(account_id, metaapi_account_id)
        st.stop()

    col1, col2, col3 = st.columns(3)

    with col1:
        account_summary_tile(selected_account)

    with col2:
        # FIXED: Use account_id
        drawdown_tile(account_id, is_percentage, account_balance)
    
    with col3:
        # FIXED: Use account_id
        profit_target_tile(account_id, is_percentage, account_balance)

    with empty_tile(key="equity-chart", height=350):

        TF_OPTIONS = {
            "1m": 1 * 60,
            "5m": 5 * 60,
            "15m": 15 * 60,
            "30m": 30 * 60,
            "1h": 60 * 60,
        }

        with st.container(horizontal=True, vertical_alignment="top"):
            st.write("**Performance Chart**")
            st.space("stretch")
            chart_selection = st.segmented_control(
                "Chart Type", 
                options=[":material/show_chart:", ":material/candlestick_chart:"], 
                selection_mode="single", 
                label_visibility="collapsed",
                default=":material/show_chart:"
            )
            tf_selection = st.segmented_control(
                "Timeframe", 
                options=list(TF_OPTIONS.keys()),
                selection_mode="single",
                label_visibility="collapsed",
                default="5m"
            )

        bucket_seconds = TF_OPTIONS[tf_selection]

        if chart_selection is None or tf_selection is None:
            st.info("Ensure a chart type & timeframe has been selected.")
        else:
            # FIXED: Use account_id
            if chart_selection == ":material/candlestick_chart:":
                equity_ohlc_chart(account_id, bucket_seconds, is_percentage, account_balance)
            elif chart_selection == ":material/show_chart:":
                equity_line_chart(account_id, bucket_seconds, is_percentage, account_balance)

    with st.container(border=True, height=332):
        st.write("**Trade History**")
        

    # col4, col5 = st.columns(2)

    # with col4:
    #     with empty_tile(key="veilon-score-tile", height=332):
    #         st.write("**Veilon Score**")

    #         metrics = {
    #             "History": 72,
    #             "Profitability": 64,
    #             "Risk": 55,
    #             "Consistency": 70,
    #             "Discipline": 62,
    #         }

    #         score = round(sum(metrics.values()) / len(metrics))
    #         fig = veilon_radar_plotly(metrics, score_out_of_100=score)

    #         st.plotly_chart(fig, config={"displayModeBar": False})

    # with col5:
    #     empty_tile(key="tile-5", height=332)
        
if __name__ == "__main__":
    accounts_page()