import streamlit as st

from veilon_client.pages.routes import DASHBOARD_PAGE
from veilon_client.pages.routes import ACCOUNTS_PAGE
from veilon_client.static.elements.metrics import metric_tile, empty_tile

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

@st.dialog("**Request New Payout**", dismissible=False)
def request_payout_dialog(account_id, payout_amount):
    st.write("Payout method")
    st.write(account_id)
    st.write(payout_amount)
    col1, col2 = st.columns(2)
    if col1.button("Request", width="stretch"):
        st.rerun()
        st.toast(f"Payout Request of {payout_amount} Submitted")
    if col2.button("Cancel", width="stretch", ):
        st.rerun()

def payouts_page():
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
            st.subheader(f"Payouts", anchor=False)
        st.space("stretch")
        with st.container(border=False, width=150):
            st.image("veilon_client/static/images/veilon_dark.png", width="stretch")

        
    tile1, tile2, tile3 = st.columns(3)
    with tile1:
        metric_tile(
            key="total-payouts",
            title="Total Payouts",
            value="$1,500.00",
            value_size="1.5rem",
            title_padding_bottom="1.5rem",
            )
    with tile2:
        metric_tile(
            key="pending-payouts",
            title="Pending Payouts",
            value="$1,250.00",
            value_size="1.5rem",
            title_padding_bottom="1.5rem",
            )
    with tile3:
        metric_tile(
            key="next_payout_date",
            title="Next Payout Date",
            value="5th Feb 2026",
            value_size="1.5rem",
            title_padding_bottom="1.5rem",
            )

    col1, col2 = st.columns([2,1])
    with col1:
        with empty_tile(key="payout-history", height=332):
            st.write("**Payout History**")

            import pandas as pd

            payouts_data = {
                "Date": ["2026-01-15", "2026-01-02", "2025-12-18", "2025-12-03", "2025-11-20"],
                "Account": ["38", "38", "37", "36", "36"],
                "Amount": ["$1,250.00", "$890.50", "$2,100.00", "$750.00", "$1,500.00"],
                "Status": ["Pending", "Paid", "Paid", "Rejected", "Paid"],
                "Method": ["Revolut", "Rise", "Rise", "Revolut", "Crypto"]
            }

            df = pd.DataFrame(payouts_data)

            def transparent_background(col):
                return ['background-color: transparent' for _ in col]

            # Apply to all columns
            styled_df = df.style.apply(transparent_background, subset=['Date', 'Account', 'Amount', 'Status'])

            st.dataframe(
                key="payouts-history-df",
                data=styled_df,
                width="stretch",
                height=300,
                hide_index=True,
                selection_mode="off",
                column_config={
                    "Status": st.column_config.MultiselectColumn(
                        "Status",
                        options=["Pending", "Paid", "Rejected"],
                        color=["#FFBD6172", "#77DD9E72", "#FF696172"],
                        disabled=True,
                    )
                }
            )


    with col2:
        with empty_tile(key="payout-request", height=332):
            st.write("**Request New Payout**")

            with st.form(
                key="payout-request-form",
                enter_to_submit=False,
                border=False
            ):
                account_selection = st.selectbox(
                    label="Account",
                    options="1"
                )
                payout_amount = st.text_input(
                    label="Payout Amount",
                    value="$1000.00",
                    disabled=True
                )
                st.space("xsmall")
                if st.form_submit_button(
                    label="Request",
                    width="stretch",
                    ):
                        request_payout_dialog(account_selection, payout_amount)
                    

    with empty_tile(key="payout-chart", height=332):
        st.write("**Payout Chart**")
        
        import pandas as pd
        import altair as alt
        from datetime import datetime, timedelta
        
        # Dummy payout data - cumulative over time
        payout_history = {
            "date": [
                "2025-08-15", "2025-09-10", "2025-10-05", "2025-10-28",
                "2025-11-20", "2025-12-03", "2025-12-18", "2026-01-02"
            ],
            "amount": [500, 750, 1200, 800, 1500, 750, 2100, 890.50]
        }
        
        df = pd.DataFrame(payout_history)
        df["date"] = pd.to_datetime(df["date"], utc=True)
        df = df.sort_values("date")
        
        # Calculate cumulative sum
        df["cumulative"] = df["amount"].cumsum()
        
        df["date_key"] = df["date"].dt.strftime("%Y-%m-%d")
        
        current_total = float(df["cumulative"].iloc[-1])
        
        ymin = 0
        ymax = float(df["cumulative"].max())
        pad = max(ymax * 0.1, 100)
        y_domain = [ymin, ymax + pad]
        
        base = alt.Chart(df).encode(
            x=alt.X(
                "date_key:N",
                sort=alt.SortField(field="date", order="ascending"),
                axis=alt.Axis(
                    title=None,
                    labelAngle=0,
                    labelOverlap="parity",
                    labelExpr="slice(datum.value, 5, 10)",  # Show MM-DD
                    tickCount=6,
                ),
            )
        )
        
        line = base.mark_line(
            interpolate="monotone",
            stroke="#81A4DD",
            strokeWidth=1,
        ).encode(
            y=alt.Y(
                "cumulative:Q",
                scale=alt.Scale(domain=y_domain),
                axis=alt.Axis(title=None, format="$,.0f", tickCount=6, grid=False, orient="left"),
            ),
            tooltip=[
                alt.Tooltip("date:T", title="Date", format="%d/%m/%y"),
                alt.Tooltip("amount:Q", title="Payout", format="$,.2f"),
                alt.Tooltip("cumulative:Q", title="Total", format="$,.2f"),
            ],
        )
        
        # Add points at each payout
        points = base.mark_circle(
            size=40,
            color="#81A4DD",
        ).encode(
            y=alt.Y("cumulative:Q", scale=alt.Scale(domain=y_domain)),
            tooltip=[
                alt.Tooltip("date:T", title="Date", format="%d/%m/%y"),
                alt.Tooltip("amount:Q", title="Payout", format="$,.2f"),
                alt.Tooltip("cumulative:Q", title="Total", format="$,.2f"),
            ],
        )
        
        # Current total line
        current_line = alt.Chart(pd.DataFrame({"y": [current_total]})).mark_rule(
            stroke="#81A4DD",
            strokeWidth=1,
            strokeDash=[1, 1],
            opacity=0.9,
        ).encode(
            y=alt.Y("y:Q", scale=alt.Scale(domain=y_domain)),
        )
        
        chart = (
            (line + points + current_line)
            .properties(height=250, width="container")
            .configure(background="transparent")
            .configure_view(fill="transparent", strokeWidth=0)
        )
        
        st.altair_chart(chart, height=260, width="stretch")


if __name__ == "__main__":
    payouts_page()