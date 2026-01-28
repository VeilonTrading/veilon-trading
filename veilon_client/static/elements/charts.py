# static/elements/charts.py
import altair as alt
import pandas as pd

def daily_return_chart(data: pd.DataFrame) -> alt.Chart:
    """
    Build the daily return column chart.

    Expects `data` with:
      - Date: datetime-like
      - Gain: numeric (e.g. 0.01 = 1%)
    """
    df = data.copy()

    # Be defensive: ensure Date is datetime
    if not pd.api.types.is_datetime64_any_dtype(df["Date"]):
        df["Date"] = pd.to_datetime(df["Date"])

    x_axis = alt.X("Date:T", title=None)
    y_axis = alt.Y(
        "Gain:Q",
        title=None,
        axis=alt.Axis(format="%", grid=False),
    )

    color = alt.condition(
        alt.datum.Gain > 0,
        alt.value("#1da77e"),    # positive
        alt.value("#ef4444ba"),  # negative
    )

    chart = (
        alt.Chart(df)
        .mark_bar(cornerRadiusTopLeft=5, cornerRadiusTopRight=5)
        .encode(
            x=x_axis,
            y=y_axis,
            color=color,
        )
        .properties(
            height=250,
            background="#171717",
            padding={"top": 10, "bottom": 10, "left": 5, "right": 10},
        )
        .configure_view(strokeWidth=0)
    )

    return chart

def performance_chart(
    data: pd.DataFrame,
    dd_floor_pct: float = 0.10,       # 10% below initial balance
    profit_band_height_pct: float = 0.025,  # 2.5% band above profit target
    y_padding_pct: float = 0.02,      # 2% padding above/below domain
) -> alt.Chart:
    """
    Veilon performance chart, scaled proportionally to initial balance.

    Expects `data` with columns:
      - Date
      - Balance
      - Equity
      - Profit Target
      - Max Drawdown
      - Daily Drawdown

    Parameters are expressed as % of the initial Balance:
      dd_floor_pct          -> how far below initial balance the DD floor is
      profit_band_height_pct -> thickness of the profit target band
      y_padding_pct         -> vertical padding on y-domain
    """
    df = data.copy()

    # Ensure datetime
    if not pd.api.types.is_datetime64_any_dtype(df["Date"]):
        df["Date"] = pd.to_datetime(df["Date"])

    # Initial balance as anchor
    initial_balance = float(df["Balance"].iloc[0])

    # Derived levels from percentages
    dd_floor = initial_balance * (1 - dd_floor_pct)
    profit_band_height = initial_balance * profit_band_height_pct

    # Helper columns for shaded areas
    df["PT_Bottom"] = df["Profit Target"]
    df["PT_Top"] = df["Profit Target"] + profit_band_height

    df["DD_Bottom"] = dd_floor
    df["DD_Top"] = df[["Max Drawdown", "Daily Drawdown"]].max(axis=1)

    # Y-domain based on floors/targets with a bit of padding
    y_min = min(df["DD_Bottom"].min(), df[["Balance", "Equity"]].min().min())
    y_max = max(df["PT_Top"].max(), df[["Balance", "Equity"]].max().max())

    padding = (y_max - y_min) * y_padding_pct
    y_domain = [y_min - padding, y_max + padding]

    x_axis = alt.X("Date:T", title=None)
    y_scale = alt.Scale(domain=y_domain)

    # Drawdown shaded area
    dd_band = (
        alt.Chart(df)
        .mark_area(color="#ffa0a00d")
        .encode(
            x=x_axis,
            y=alt.Y("DD_Bottom:Q", scale=y_scale, title=None),
            y2="DD_Top:Q",
        )
    )

    # Profit target shaded area
    pt_band = (
        alt.Chart(df)
        .mark_area(color="#86ffdb0d")
        .encode(
            x=x_axis,
            y=alt.Y(
                "PT_Bottom:Q",
                scale=y_scale,
                title=None,
                axis=alt.Axis(format="$,.0f"),
            ),
            y2="PT_Top:Q",
        )
    )

    # Balance line
    balance_line = (
        alt.Chart(df)
        .mark_line(interpolate="monotone", strokeWidth=2, color="#358BCD")
        .encode(
            x=x_axis,
            y=alt.Y(
                "Balance:Q",
                scale=y_scale,
                title=None,
                axis=alt.Axis(format="$,.0f"),
            ),
        )
    )

    # Profit target line
    profit_line = (
        alt.Chart(df)
        .mark_line(interpolate="monotone", strokeWidth=2, color="#1da77e")
        .encode(
            x=x_axis,
            y=alt.Y(
                "Profit Target:Q",
                scale=y_scale,
                title=None,
                axis=alt.Axis(format="$,.0f"),
            ),
        )
    )

    # Drawdown top line
    drawdown_line = (
        alt.Chart(df)
        .mark_line(interpolate="monotone", strokeWidth=2, color="#ef4444ba")
        .encode(
            x=x_axis,
            y=alt.Y(
                "DD_Top:Q",
                scale=y_scale,
                title=None,
                axis=alt.Axis(format="$,.0f"),
            ),
        )
    )

    # Equity line (dashed)
    equity_line = (
        alt.Chart(df)
        .mark_line(
            interpolate="monotone",
            strokeDash=[4, 4],
            strokeWidth=2,
            color="#facc154a",
        )
        .encode(
            x=x_axis,
            y=alt.Y(
                "Equity:Q",
                scale=y_scale,
                title=None,
                axis=alt.Axis(format="$,.0f"),
            ),
        )
    )

    chart = (
        dd_band
        + pt_band
        + balance_line
        + equity_line
        + profit_line
        + drawdown_line
    ).properties(
        height=240,
        background="#17171700",
        padding={"top": 10, "bottom": 0, "left": 5, "right": 10},
    ).configure_axis(
        grid=False,
    ).configure_view(
        strokeWidth=0,
    )

    return chart

def veilon_score_bar(score: float, max_score: float = 100) -> alt.Chart:
    """
    Single vertical bar showing the Veilon Trader Score (0–100).

    Colors:
      - 0   → red
      - 50  → amber
      - 75+ → green-ish
    """
    df = pd.DataFrame(
        [{"Label": "Veilon Score", "Score": score}]
    )

    y_scale = alt.Scale(domain=[0, max_score])

    chart = (
        alt.Chart(df)
        .mark_bar(
            cornerRadiusTopLeft=5,
            cornerRadiusTopRight=5,
        )
        .encode(
            x=alt.X(
                "Label:N",
                title=None,
                axis=alt.Axis(labels=False, ticks=False, domain=False),  # hide x-axis
            ),
            y=alt.Y("Score:Q", stack=None, title=None, scale=y_scale),
            color=alt.Color(
                "Score:Q",
                legend=None,
                scale=alt.Scale(
                    domain=[0, 50, 75],
                    range=["#ef4444", "#fbbf24", "#1da77e"],  # red → amber → green
                ),
            ),
        )
        .properties(
            height=250,
            background="#171717",
            padding={"top": 10, "bottom": 10, "left": 5, "right": 20},
        )
        .configure_view(strokeWidth=0)
    )

    return chart


