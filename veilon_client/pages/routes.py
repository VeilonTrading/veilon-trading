# routes.py
import streamlit as st

def _dashboard_page():
    from veilon_client.pages.dashboard import dashboard_page
    dashboard_page()

def _accounts_page():
    from veilon_client.pages.accounts import accounts_page
    accounts_page()

def _checkout_page():
    from veilon_client.pages.new_account import new_account_page
    new_account_page()

def _payouts_page():
    from veilon_client.pages.payouts import payouts_page
    payouts_page()

DASHBOARD_PAGE = st.Page(_dashboard_page,title="Dashboard")
ACCOUNTS_PAGE = st.Page(_accounts_page, title="Accounts")
CHECKOUT_PAGE  = st.Page(_checkout_page, title="New Account")
PAYOUTS_PAGE  = st.Page(_payouts_page, title="Payouts")

PAGES = [DASHBOARD_PAGE, ACCOUNTS_PAGE, CHECKOUT_PAGE, PAYOUTS_PAGE]