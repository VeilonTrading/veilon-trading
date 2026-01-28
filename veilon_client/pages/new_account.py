import streamlit as st
from streamlit_extras.stylable_container import stylable_container
import veilon_client.static.elements.layout as layouts
from veilon_client.pages.routes import DASHBOARD_PAGE
from veilon_client.pages.routes import ACCOUNTS_PAGE
from veilon_client.pages.footer import render_footer
from veilon_core.repositories.plans import get_plan_by_account_size
from veilon_core.repositories.coupons import get_active_coupon_by_code
from veilon_core.repositories.db import execute_query
from veilon_core.repositories.users import get_user_by_email, get_or_create_user_from_oidc
import stripe
import time

# Initialize Stripe
stripe.api_key = "sk_test_51SUrJdDgd8xHlmy8vynadqa6yberuIbvkqm8AMnjbz1ukorwuezTw8TZ6G6SuP0LnxRkM1uphH1CJLrCuCPPJsnU00bnPFyALv"


def get_user_id():
    """
    Resolve current user via Google / OIDC.
    If they don't exist yet in `users`, create a minimal user row.
    Then fetch their active accounts.
    """
    email = st.user.email.strip().lower()
    given_name = getattr(st.user, "given_name", None)
    family_name = getattr(st.user, "family_name", None)

    user = get_or_create_user_from_oidc(
        email=email,
        given_name=given_name,
        family_name=family_name,
    )
    user_id = user["id"]

    return user_id


def get_plan_from_db(account_size: int, eval_type: str = "1-Step"):
    """
    Fetch plan details from database based on account size and evaluation type.
    """
    try:
        # Convert string like "$5,000" to int 5000
        if isinstance(account_size, str):
            account_size = int(account_size.replace("$", "").replace(",", ""))
        
        plans = execute_query(
            f"""
            SELECT id, account_size, base_price, stripe_price_id, stripe_product_id
            FROM plans
            WHERE account_size = {account_size}
            LIMIT 1;
            """
        )
        
        if plans:
            return plans[0]
        return None
    except Exception as e:
        st.error(f"Error fetching plan: {e}")
        return None


def create_stripe_checkout_session(plan: dict, user_id: int, user_email: str):
    """
    Create a Stripe Checkout Session for the selected plan.
    Returns the checkout URL or None if failed.
    """
    try:
        # Validate plan has Stripe IDs
        if not plan.get('stripe_price_id'):
            st.error("This plan is not configured for payment. Please contact support.")
            return None
        
        # Base URL for Streamlit Cloud
        base_url = "https://veilontrading.streamlit.app"
        
        # Create the checkout session
        checkout_session = stripe.checkout.Session.create(
            payment_method_types=['card'],
            line_items=[{
                'price': plan['stripe_price_id'],
                'quantity': 1,
            }],
            mode='payment',
            success_url=f"{base_url}/_checkout_page?payment_success=true",
            cancel_url=f"{base_url}/_checkout_page?payment_canceled=true",
            client_reference_id=str(user_id),
            customer_email=user_email,
            metadata={
                'user_id': str(user_id),
                'plan_id': str(plan['id']),
                'account_size': str(int(plan['account_size'])),
            }
        )
        
        return checkout_session.url
        
    except stripe.error.StripeError as e:
        st.error(f"Payment error: {str(e)}")
        return None
    except Exception as e:
        st.error(f"Error creating checkout: {str(e)}")
        return None


def new_account_page():
    # Check for payment status in URL params
    query_params = st.query_params
    
    if query_params.get("payment_success") == "true":
        st.success("✅ Payment successful! Your evaluation account is being set up...")
        st.info("⏳ Redirecting to dashboard...")
        time.sleep(2)
        st.switch_page(DASHBOARD_PAGE)
    
    if query_params.get("payment_canceled") == "true":
        st.warning("⚠️ Payment was canceled. You can try again when you're ready.")
        st.query_params.clear()
    
    # Back button
    with st.container(border=False, horizontal=True, vertical_alignment="center"):
        with st.container(
            border=False,
            horizontal=True,
            horizontal_alignment="left",
            vertical_alignment="bottom",
        ):
            if st.button(
                label="Back",
                type="tertiary",
                icon=":material/arrow_back:",
            ):
                st.switch_page(ACCOUNTS_PAGE)

    # Header
    with st.container(border=False, horizontal=True, vertical_alignment="center"):
        with st.container(border=False):
            st.subheader("New Evaluation", anchor=False)
        st.space("stretch")
        with st.container(border=False, width=150):
            st.image("veilon_client/static/images/veilon_dark.png", width="stretch")

    st.caption("Demonstrate your trading performance with a new evaluation account.")
    st.space("small")
    
    # Initialize session state for form values
    if 'eval_type' not in st.session_state:
        st.session_state.eval_type = None
    if 'eval_balance' not in st.session_state:
        st.session_state.eval_balance = None
    if 'terms_accepted' not in st.session_state:
        st.session_state.terms_accepted = False
    if 'checkout_url' not in st.session_state:
        st.session_state.checkout_url = None
    
    with st.container(border=False):
        # Selection inputs
        with st.container(border=False, horizontal=True, vertical_alignment="center"):
            eval_type = st.selectbox(
                label="Evaluation Type",
                options=["1-Step Evaluation"],
                help="Choose your desired evaluation type.",
                placeholder="Select Evaluation Type",
                index=0 if st.session_state.eval_type else None,
                key="eval_type_select"
            )

            eval_balance = st.selectbox(
                label="Evaluation Balance",
                options=["$2,500", "$5,000", "$10,000", "$25,000", "$50,000"],
                help="Choose the size of the evaluation account.",
                placeholder="Select Evaluation Size",
                index=None,
                key="eval_balance_select"
            )
        
        # Update session state
        if eval_type:
            st.session_state.eval_type = eval_type
        if eval_balance:
            st.session_state.eval_balance = eval_balance
        
        st.space("xxsmall")

        # Terms and Purchase button
        with st.container(border=False, horizontal=True, vertical_alignment="center"):
            terms_bool = st.checkbox(
                label=":gray[By purchasing, I agree to the Veilon [__Terms of Service__](https://veilontrading.com/terms-of-service).]",
                key="terms_checkbox"
            )
            st.session_state.terms_accepted = terms_bool
            
            st.space("stretch")
            
            # Determine if purchase button should be enabled
            can_purchase = (
                st.session_state.eval_type is not None 
                and st.session_state.eval_balance is not None
                and st.session_state.terms_accepted
            )
            
            purchase_button = st.button(
                "Purchase",
                width=200,
                icon=":material/shopping_cart:",
                disabled=not can_purchase,
                type="secondary"
            )
            
            # Handle purchase button click
            if purchase_button and can_purchase:
                with st.spinner("Creating checkout session..."):
                    # Get user info
                    user_id = get_user_id()
                    user_email = st.user.email.strip().lower()
                    
                    # Get plan details
                    account_size_int = int(st.session_state.eval_balance.replace("$", "").replace(",", ""))
                    plan = get_plan_from_db(account_size_int, "1-Step")
                    
                    if plan:
                        # Create Stripe checkout session
                        checkout_url = create_stripe_checkout_session(plan, user_id, user_email)
                        
                        if checkout_url:
                            st.session_state.checkout_url = checkout_url
                            st.rerun()
                    else:
                        st.error("Unable to process purchase. Please try again.")
        
        # Show checkout button if URL is ready
        if st.session_state.checkout_url:
            st.divider()
            st.success("✅ Checkout session created!")
            
            # DEBUG - show URL (remove after testing)
            st.code(st.session_state.checkout_url)
            
            st.link_button(
                "🔒 Complete Payment on Stripe", 
                st.session_state.checkout_url, 
                type="primary",
                use_container_width=True
            )
            
            # Clear button
            if st.button("Cancel", type="tertiary"):
                st.session_state.checkout_url = None
                st.rerun()


if __name__ == "__main__":
    new_account_page()
