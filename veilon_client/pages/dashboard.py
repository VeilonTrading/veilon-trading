import streamlit as st

from veilon_client.pages.routes import PAYOUTS_PAGE
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

@st.dialog("Logout", dismissible=False)
def logout_dialog():
    st.write("Are you sure you want to log out?")
    col1, col2 = st.columns(2)
    with col1:
        if st.button("Yes", type="secondary", width="stretch"):
            st.logout()
    with col2:
        if st.button("No", type="secondary", width="stretch"):
            st.rerun()

def dashboard_page():
    hide_streamlit_header()

    # # Back button
    # with st.container(border=False, horizontal=True, vertical_alignment="center"):
    #     with st.container(
    #         border=False,
    #         horizontal=True,
    #         horizontal_alignment="right",
    #         vertical_alignment="bottom",
    #     ):
    #         st.button(
    #         "Logout",
    #         key="logout-button",
    #         type="tertiary",
    #         icon=":material/logout:",
    #         on_click=logout_dialog,
    #         )

    with st.container(border=False, horizontal=True, vertical_alignment="center"):
        with st.container(border=False):
            st.subheader(f"Welcome, {getattr(st.user, 'given_name', '')}", anchor=False)
        st.space("stretch")
        with st.container(border=False, width=150):
            st.image("veilon_client/static/images/veilon_dark.png", width="stretch")


    with empty_tile(
        key="dashboard-accounts-tile",
        height=332
    ):
        st.write("**Accounts**")

        st.space("stretch")

        if st.button(
            label="Go to Accounts",
            type="tertiary",
            #icon=":material/arrow_back:",
        ):
            st.switch_page(ACCOUNTS_PAGE)
        
    with empty_tile(
        key="dashboard-payouts-tile",
        height=332
    ):
        st.write("**Payouts**")

        st.space("stretch")

        if st.button(
            label="Go to Payouts",
            type="tertiary",
            #icon=":material/arrow_back:",
        ):
            st.switch_page(PAYOUTS_PAGE)


if __name__ == "__main__":
    dashboard_page()