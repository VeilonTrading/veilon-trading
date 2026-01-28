"""
Stripe Payment Processor - Development Version

This script polls Stripe for completed checkout sessions and creates orders/accounts.
Run this in VS Code alongside your Streamlit app during development.

Usage:
    python veilon_core/services/stripe_processor.py

The script will:
1. Check Stripe every 10 seconds for new completed checkout sessions
2. Create orders and accounts in your database
3. Log all activity to console and file
"""

import sys
import os
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# Now import everything else
from stripe import checkout, error as stripe_error
import stripe as stripe_api
import time
from datetime import datetime, timedelta
import logging
from veilon_core.repositories.db import execute_query

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('stripe_processor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Initialize Stripe
stripe_api.api_key = "sk_test_51SUrJdDgd8xHlmy8vynadqa6yberuIbvkqm8AMnjbz1ukorwuezTw8TZ6G6SuP0LnxRkM1uphH1CJLrCuCPPJsnU00bnPFyALv"

# Track processed sessions to avoid duplicates
processed_sessions = set()


def check_duplicate_order(stripe_session_id):
    """Check if order already exists for this Stripe session."""
    try:
        existing_orders = execute_query(
            f"SELECT id FROM orders WHERE stripe_session_id = '{stripe_session_id}' LIMIT 1"
        )
        return len(existing_orders) > 0
    except Exception as e:
        logger.error(f"Error checking duplicate order: {e}")
        return False


def create_order_and_account(session):
    """
    Create order and account records after successful payment.
    
    Args:
        session: Stripe checkout session object
        
    Returns:
        dict: {'success': bool, 'order_id': int, 'account_id': int, 'message': str}
    """
    try:
        session_id = session.id
        
        # Check if already processed in this run
        if session_id in processed_sessions:
            logger.debug(f"Session {session_id} already processed in this run")
            return {'success': True, 'message': 'Already processed in this run'}
        
        # Check if already exists in database
        if check_duplicate_order(session_id):
            logger.info(f"Order already exists for session {session_id}, skipping")
            processed_sessions.add(session_id)
            return {'success': True, 'message': 'Order already exists in database'}
        
        # Extract metadata
        user_id = int(session.metadata.get('user_id'))
        plan_id = int(session.metadata.get('plan_id'))
        # Handle account_size as either int or float string
        account_size = int(float(session.metadata.get('account_size')))
        
        # Validate metadata
        if not all([user_id, plan_id, account_size]):
            logger.error(f"Missing metadata in session {session_id}")
            return {'success': False, 'message': 'Missing required metadata'}
        
        # Get payment details
        amount_paid = session.amount_total / 100  # Convert cents to dollars
        currency = session.currency.upper()
        customer_email = session.customer_details.get('email') if session.customer_details else None
        payment_intent = session.payment_intent
        
        logger.info(f"Creating order for user_id={user_id}, plan_id={plan_id}, amount=${amount_paid}, account_size={account_size}")
        
        # 1. Create the order
        order_rows = execute_query(
            f"""
            INSERT INTO orders (
                user_id,
                plan_id,
                price,
                currency,
                success,
                status,
                stripe_session_id,
                stripe_payment_intent,
                customer_email,
                expiry_date,
                created_at
            )
            VALUES (
                {user_id},
                {plan_id},
                {amount_paid},
                '{currency}',
                TRUE,
                'paid',
                '{session_id}',
                '{payment_intent}',
                {f"'{customer_email}'" if customer_email else 'NULL'},
                NOW() + INTERVAL '30 days',
                NOW()
            )
            RETURNING id;
            """
        )
        
        order_id = order_rows[0]["id"]
        logger.info(f"✅ Order created: order_id={order_id}")
        
        # 2. Create the account with balance
        account_rows = execute_query(
            f"""
            INSERT INTO accounts (
                metaapi_account_id,
                user_id,
                order_id,
                plan_id,
                balance,
                platform,
                broker,
                server,
                leverage,
                login,
                status,
                created_at
            )
            VALUES (
                NULL,
                {user_id},
                {order_id},
                {plan_id},
                {account_size},
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                'pending_setup',
                NOW()
            )
            RETURNING id;
            """
        )
        
        account_id = account_rows[0]["id"]
        logger.info(f"✅ Account created: account_id={account_id}, balance={account_size}")
        
        # 3. Log account creation event
        execute_query(
            f"""
            INSERT INTO account_events (
                account_id,
                event_type,
                event_status,
                actor_type,
                actor_id,
                occurred_at,
                payload
            )
            VALUES (
                {account_id},
                'account_created',
                'success',
                'system',
                NULL,
                NOW(),
                '{{"order_id": {order_id}, "plan_id": {plan_id}, "account_size": {account_size}, "stripe_session_id": "{session_id}"}}'::jsonb
            );
            """
        )
        logger.info(f"✅ Account creation event logged")
        
        # 4. Link the account back to the order
        execute_query(
            f"""
            UPDATE orders
            SET account_id = {account_id}
            WHERE id = {order_id};
            """
        )
        
        # Mark as processed
        processed_sessions.add(session_id)
        
        logger.info(f"✅ Successfully processed payment: order_id={order_id}, account_id={account_id}, user_id={user_id}, balance={account_size}")
        
        return {
            'success': True,
            'order_id': order_id,
            'account_id': account_id,
            'message': 'Order and account created successfully'
        }
        
    except Exception as e:
        logger.error(f"❌ Error creating order and account: {str(e)}", exc_info=True)
        return {
            'success': False,
            'message': f'Error: {str(e)}'
        }


def poll_stripe_sessions(lookback_minutes=30):
    """
    Poll Stripe for completed checkout sessions from the last N minutes.
    
    Args:
        lookback_minutes: How far back to look for sessions (default: 30 minutes)
    """
    try:
        # Calculate timestamp for lookback period
        lookback_time = int((datetime.now() - timedelta(minutes=lookback_minutes)).timestamp())
        
        # Fetch completed checkout sessions
        sessions = checkout.Session.list(
            limit=100,
            created={'gte': lookback_time},
        )
        
        logger.debug(f"Found {len(sessions.data)} sessions in last {lookback_minutes} minutes")
        
        completed_count = 0
        processed_count = 0
        
        for session in sessions.data:
            # Only process completed sessions with successful payment
            if session.status == 'complete' and session.payment_status == 'paid':
                completed_count += 1
                
                # Process the session
                result = create_order_and_account(session)
                
                if result['success'] and 'order_id' in result:
                    processed_count += 1
                    logger.info(f"✅ Processed session {session.id}: order_id={result['order_id']}, account_id={result['account_id']}")
                elif result['message'] != 'Already processed in this run' and result['message'] != 'Order already exists in database':
                    logger.error(f"❌ Failed to process session {session.id}: {result['message']}")
        
        if completed_count > 0:
            logger.info(f"📊 Found {completed_count} completed sessions, processed {processed_count} new orders")
        
    except stripe_error.StripeError as e:
        logger.error(f"Stripe API error: {str(e)}")
    except Exception as e:
        logger.error(f"Error polling Stripe: {str(e)}", exc_info=True)


def run_processor(poll_interval=10, lookback_minutes=30):
    """
    Run the payment processor continuously.
    
    Args:
        poll_interval: Seconds between polls (default: 10)
        lookback_minutes: How far back to check for sessions (default: 30)
    """
    logger.info("=" * 80)
    logger.info("🚀 Starting Stripe Payment Processor (Development Mode)")
    logger.info("=" * 80)
    logger.info(f"Poll interval: {poll_interval} seconds")
    logger.info(f"Lookback period: {lookback_minutes} minutes")
    logger.info(f"Stripe API Key: {stripe_api.api_key[:20]}...")
    logger.info("=" * 80)
    logger.info("Press Ctrl+C to stop")
    logger.info("=" * 80)
    
    try:
        while True:
            logger.debug(f"🔍 Polling Stripe at {datetime.now().strftime('%H:%M:%S')}")
            poll_stripe_sessions(lookback_minutes=lookback_minutes)
            time.sleep(poll_interval)
            
    except KeyboardInterrupt:
        logger.info("\n" + "=" * 80)
        logger.info("🛑 Stopping Stripe Payment Processor")
        logger.info(f"📊 Processed {len(processed_sessions)} unique sessions in this run")
        logger.info("=" * 80)


if __name__ == "__main__":
    # Run the processor
    # Poll every 10 seconds, check last 30 minutes of sessions
    run_processor(poll_interval=10, lookback_minutes=30)