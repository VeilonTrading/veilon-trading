# account_deployment_handler.py

"""
Account Deployment Handler for Streamlit Dashboard
Handles the complete flow of deploying an account with manual start button support
"""
import streamlit as st
from datetime import datetime
from typing import Dict, Any, Optional

from veilon_core.services.metaapi_deploy import deploy_account_sync, AccountDeploymentError
from veilon_core.repositories.db import execute_query
from veilon_core.services.improved_account_flow import get_improved_lifecycle_manager


def check_mt5_login_ownership(login: str, current_user_id: int) -> Dict[str, Any]:
    """
    Check if an MT5 login is already used by another user.
    
    Same user can reuse the same MT5 login across multiple evaluations.
    Different users cannot use the same MT5 login.
    
    Returns:
        {
            'allowed': True/False,
            'reason': str (if not allowed),
        }
    """
    # Check if this login exists in any account owned by a different user
    rows = execute_query(
        """
        SELECT 
            a.id AS account_id,
            a.user_id,
            u.email
        FROM accounts a
        JOIN users u ON u.id = a.user_id
        WHERE a.login = %s
        AND a.metaapi_account_id IS NOT NULL
        AND a.user_id != %s
        LIMIT 1
        """,
        (login, current_user_id),
        fetch_results=True
    )
    
    if rows:
        # Different user owns this login
        return {
            'allowed': False,
            'reason': "This MT5 login is already registered to another user."
        }
    
    # Either not used, or used by same user - allowed
    return {
        'allowed': True,
        'reason': None
    }


def handle_account_deployment(
    account_id: int,
    login: str,
    password: str,
    server_name: str,
    platform: str,
    account_name: str
) -> bool:
    """
    Handle the complete account deployment flow with manual start button support
    
    Flow:
    1. Check MT5 login ownership (block if different user owns it)
    2. Deploy account to MetaAPI (establishes connection, checks positions)
    3. Update accounts table with deployment info
    4. Use ImprovedAccountLifecycleManager to handle stream start
       - If flat: Start stream + create period + begin evaluation
       - If has positions: Set pending_flat status, show "Start Evaluation" button
    
    Returns:
        True if deployment successful (regardless of whether eval started)
    """
    
    # Get current user_id for this account
    account_rows = execute_query(
        "SELECT user_id FROM accounts WHERE id = %s",
        (account_id,),
        fetch_results=True
    )
    
    if not account_rows:
        st.error("Account not found")
        return False
    
    current_user_id = account_rows[0]['user_id']
    
    # Step 1: Check MT5 login ownership
    with st.spinner("Checking account ownership..."):
        ownership_check = check_mt5_login_ownership(login, current_user_id)
        
        if not ownership_check['allowed']:
            st.error(f"❌ {ownership_check['reason']}")
            log_account_event(
                account_id=account_id,
                event_type='deployment_blocked',
                event_data={
                    'login': login,
                    'reason': 'login_belongs_to_another_user'
                },
                event_status='failed'
            )
            return False
    
    # Step 2: Deploy to MetaAPI
    with st.spinner("Deploying account to MetaAPI..."):
        try:
            result = deploy_account_sync(
                login=login,
                password=password,
                server_name=server_name,
                platform=platform,
                account_name=account_name
            )
            
            metaapi_account_id = result['metaapi_account_id']
            has_open_positions = result['has_open_positions']
            positions = result.get('positions', [])
            account_info = result.get('account_info', {})
            
            st.success(f"✓ Account deployed successfully (ID: {metaapi_account_id})")
            
            # Step 3: Update accounts table
            broker = account_info.get('broker')
            leverage = account_info.get('leverage')
            platform_normalized = 'mt5' if platform.lower() in ['metatrader5', 'mt5'] else 'mt4'
            
            execute_query(
                """
                UPDATE accounts
                SET metaapi_account_id = %s,
                    platform = %s,
                    broker = %s,
                    login = %s,
                    server = %s,
                    leverage = %s,
                    is_enabled = TRUE,
                    updated_at = NOW()
                WHERE id = %s
                """,
                (metaapi_account_id, platform_normalized, broker, login, server_name, leverage, account_id)
            )
            
            st.success("✓ Account information saved to database")
            
            # Step 4: Log deployment event
            log_account_event(
                account_id=account_id,
                event_type='account_deployed',
                event_data={
                    'metaapi_account_id': metaapi_account_id,
                    'login': login,
                    'server': server_name,
                    'platform': platform_normalized,
                    'broker': broker,
                    'leverage': leverage,
                    'has_open_positions': has_open_positions,
                    'positions_count': len(positions)
                }
            )
            
            # Step 5: Handle initial deployment via lifecycle manager
            lifecycle_manager = get_improved_lifecycle_manager()
            
            deployment_result = lifecycle_manager.handle_initial_deployment(
                metaapi_account_id=metaapi_account_id,
                has_open_positions=has_open_positions,
                positions=positions
            )
            
            if not deployment_result['success']:
                st.error(f"Failed to initialize account: {deployment_result.get('reason')}")
                return False
            
            # Step 6: Show deployment success message
            st.success("✓ Account connected successfully!")
            
            if deployment_result.get('has_open_positions'):
                st.warning(
                    f"⚠ Note: {deployment_result.get('positions_count', 0)} open position(s) detected. "
                    "Close them before starting your evaluation."
                )
                
                if positions:
                    with st.expander("View Open Positions"):
                        for pos in positions:
                            st.write(
                                f"**{pos.get('symbol')}** - {pos.get('type')} - "
                                f"{pos.get('volume')} lots - P/L: ${pos.get('profit', 0):.2f}"
                            )
            
            st.info(
                "📌 Click **'🚀 Start Evaluation'** on the dashboard when you're ready to begin."
            )
            
            log_account_event(
                account_id=account_id,
                event_type='account_connected',
                event_data={
                    'metaapi_account_id': metaapi_account_id,
                    'has_open_positions': deployment_result.get('has_open_positions', False),
                    'positions_count': deployment_result.get('positions_count', 0)
                }
            )
            
            # Force rerun to refresh dashboard
            st.rerun()
            return True
            
        except AccountDeploymentError as e:
            st.error(f"Deployment failed: {str(e)}")
            log_account_event(
                account_id=account_id,
                event_type='deployment_failed',
                event_data={'error': str(e)},
                event_status='failed'
            )
            return False
            
        except Exception as e:
            st.error(f"Unexpected error: {str(e)}")
            import traceback
            st.error(traceback.format_exc())
            return False


def log_account_event(
    account_id: int,
    event_type: str,
    event_data: Dict[str, Any],
    event_status: str = 'completed',
    actor_type: str = 'system',
    actor_id: Optional[int] = None
) -> None:
    """
    Log an event to the account_events table
    
    Args:
        account_id: The account ID from accounts table
        event_type: Type of event (e.g., 'account_deployed', 'equity_stream_started')
        event_data: JSON data for the event
        event_status: Status of the event (completed, pending, failed)
        actor_type: Who triggered the event (system, user, admin)
        actor_id: Optional ID of the actor
    """
    import json
    from datetime import datetime, date
    
    def json_serializer(obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")
    
    query = """
        INSERT INTO account_events (
            account_id,
            event_type,
            event_status,
            actor_type,
            actor_id,
            payload
        ) VALUES (%s, %s, %s, %s, %s, %s)
    """
    
    execute_query(
        query,
        (account_id, event_type, event_status, actor_type, actor_id, json.dumps(event_data, default=json_serializer))
    )


if __name__ == "__main__":
    print("This module is designed to be imported into the Streamlit dashboard")
    print("Example usage:")
    print("""
    from veilon_core.services.account_deployment_handler import handle_account_deployment
    
    if submit_button:
        success = handle_account_deployment(
            account_id=123,
            login='12345678',
            password='readonly_password',
            server_name='BrokerServer-Demo',
            platform='MetaTrader5',
            account_name='My Trading Account'
        )
    """)