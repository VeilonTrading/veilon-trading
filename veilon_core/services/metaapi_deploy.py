# metaapi_deploy.py

"""
MetaAPI Account Deployment Module
Handles deployment of MT4/5 accounts to MetaAPI cloud
"""
import os
import asyncio
import logging
import traceback
import certifi
from typing import Optional, Dict, Any


# SSL certificates - REQUIRED for MetaAPI connection
os.environ["SSL_CERT_FILE"] = certifi.where()
os.environ["REQUESTS_CA_BUNDLE"] = certifi.where()

from metaapi_cloud_sdk import MetaApi

# Suppress noisy MetaAPI SDK logs
for logger_name in [
    'metaapi_cloud_sdk',
    'socketio', 
    'engineio',
    'websockets',
    'urllib3',
]:
    logging.getLogger(logger_name).setLevel(logging.WARNING)

TOKEN = 'eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9.eyJfaWQiOiIxNzM2NDVlM2U5MThkZjE2NjQzZmFjZDI0NTBlMGVmMiIsImFjY2Vzc1J1bGVzIjpbeyJpZCI6InRyYWRpbmctYWNjb3VudC1tYW5hZ2VtZW50LWFwaSIsIm1ldGhvZHMiOlsidHJhZGluZy1hY2NvdW50LW1hbmFnZW1lbnQtYXBpOnJlc3Q6cHVibGljOio6KiJdLCJyb2xlcyI6WyJyZWFkZXIiLCJ3cml0ZXIiXSwicmVzb3VyY2VzIjpbIio6JFVTRVJfSUQkOioiXX0seyJpZCI6Im1ldGFhcGktcmVzdC1hcGkiLCJtZXRob2RzIjpbIm1ldGFhcGktYXBpOnJlc3Q6cHVibGljOio6KiJdLCJyb2xlcyI6WyJyZWFkZXIiLCJ3cml0ZXIiXSwicmVzb3VyY2VzIjpbIio6JFVTRVJfSUQkOioiXX0seyJpZCI6Im1ldGFhcGktcnBjLWFwaSIsIm1ldGhvZHMiOlsibWV0YWFwaS1hcGk6d3M6cHVibGljOio6KiJdLCJyb2xlcyI6WyJyZWFkZXIiLCJ3cml0ZXIiXSwicmVzb3VyY2VzIjpbIio6JFVTRVJfSUQkOioiXX0seyJpZCI6Im1ldGFhcGktcmVhbC10aW1lLXN0cmVhbWluZy1hcGkiLCJtZXRob2RzIjpbIm1ldGFhcGktYXBpOndzOnB1YmxpYzoqOioiXSwicm9sZXMiOlsicmVhZGVyIiwid3JpdGVyIl0sInJlc291cmNlcyI6WyIqOiRVU0VSX0lEJDoqIl19LHsiaWQiOiJtZXRhc3RhdHMtYXBpIiwibWV0aG9kcyI6WyJtZXRhc3RhdHMtYXBpOnJlc3Q6cHVibGljOio6KiJdLCJyb2xlcyI6WyJyZWFkZXIiLCJ3cml0ZXIiXSwicmVzb3VyY2VzIjpbIio6JFVTRVJfSUQkOioiXX0seyJpZCI6InJpc2stbWFuYWdlbWVudC1hcGkiLCJtZXRob2RzIjpbInJpc2stbWFuYWdlbWVudC1hcGk6cmVzdDpwdWJsaWM6KjoqIl0sInJvbGVzIjpbInJlYWRlciIsIndyaXRlciJdLCJyZXNvdXJjZXMiOlsiKjokVVNFUl9JRCQ6KiJdfSx7ImlkIjoiY29weWZhY3RvcnktYXBpIiwibWV0aG9kcyI6WyJjb3B5ZmFjdG9yeS1hcGk6cmVzdDpwdWJsaWM6KjoqIl0sInJvbGVzIjpbInJlYWRlciIsIndyaXRlciJdLCJyZXNvdXJjZXMiOlsiKjokVVNFUl9JRCQ6KiJdfSx7ImlkIjoibXQtbWFuYWdlci1hcGkiLCJtZXRob2RzIjpbIm10LW1hbmFnZXItYXBpOnJlc3Q6ZGVhbGluZzoqOioiLCJtdC1tYW5hZ2VyLWFwaTpyZXN0OnB1YmxpYzoqOioiXSwicm9sZXMiOlsicmVhZGVyIiwid3JpdGVyIl0sInJlc291cmNlcyI6WyIqOiRVU0VSX0lEJDoqIl19LHsiaWQiOiJiaWxsaW5nLWFwaSIsIm1ldGhvZHMiOlsiYmlsbGluZy1hcGk6cmVzdDpwdWJsaWM6KjoqIl0sInJvbGVzIjpbInJlYWRlciJdLCJyZXNvdXJjZXMiOlsiKjokVVNFUl9JRCQ6KiJdfV0sImlnbm9yZVJhdGVMaW1pdHMiOmZhbHNlLCJ0b2tlbklkIjoiMjAyMTAyMTMiLCJpbXBlcnNvbmF0ZWQiOmZhbHNlLCJyZWFsVXNlcklkIjoiMTczNjQ1ZTNlOTE4ZGYxNjY0M2ZhY2QyNDUwZTBlZjIiLCJpYXQiOjE3NjY4ODQwMzh9.TxlTccBIbkoaxc39Ud1QbAuIeyi9YMcKxQaAMolJCjjptHRoMWy1DMzDhYo5wd5RtNbv3jrG2Y81Qx2mDC5C0UIjHIwRMADZuLtWtiFSPTPdfeqQBhk2tR12tzDrr8TZSMNH-lLTYx_MDTTDQkvJi2iDNy4U1V9kJxJ2qhgHPFhJYUN635r3A4M_hS2SvygUA6-zpiBlmEBiEk7CCxNe8l2ERVdMcYUwpFdfp_8rLKT94mVKAG6CspZYOgtAAbqEjcryZ9TStQ4_adhGU3UQt0n_AjNgPxwdxErKFEDI4UpJIeOfVZgbnSW_47W0sufRfTwz99RJNBHJvwNTCzsMeOR3FPMNiEtEnVD2lC4QzKg9ag_aOZ79hlGJ_NXLA_RnYD5rCnoRzxeiERdVgr2_29_ZZguGqaBXrUp8D7DKFsAMH_cyo7AbwHOi8tLRR5XxhzjtmDCJuhRyWT8ILEcl6Pety9ZN99Ekjx1C4SKfpsjmRmGu46J3yskdPD-0f0DyDqA3PZ2_VPtPQZbC7OVmwQAYHxchJLST7TH1GUNt_xZCImWYpVLO0u1NY4WnwFUVjRA4loDimEqiZ-t_iXdWojjRhB6fppcLMh7a2OmJkS_wZv6rZGiF1uUAcuhtWizh6ZwuTJ1GMgm9KAOc6vgfP71q-y1BDOlJpNbwRGT0JOs'


class AccountDeploymentError(Exception):
    """Custom exception for account deployment errors"""
    pass


async def deploy_account(
    login: str,
    password: str,
    server_name: str,
    platform: str,
    account_name: Optional[str] = None,
    magic: int = 1000
) -> Dict[str, Any]:
    """
    Deploy a MT4/5 account to MetaAPI cloud with proper error handling and cleanup
    
    Args:
        login: MT4/5 account login
        password: MT4/5 account password (read-only investor password recommended)
        server_name: MT4/5 broker server name
        platform: Either 'mt4' or 'mt5'
        account_name: Optional name for the account (defaults to login)
        magic: Magic number for the account (default 1000)
        
    Returns:
        Dict containing deployment info
            
    Raises:
        AccountDeploymentError: If deployment fails
    """
    token = os.getenv('METAAPI_TOKEN') or TOKEN
    if not token:
        raise AccountDeploymentError("METAAPI_TOKEN not configured")
    
    platform = platform.lower()
    if platform not in ['mt4', 'mt5']:
        if platform == 'metatrader4':
            platform = 'mt4'
        elif platform == 'metatrader5':
            platform = 'mt5'
        else:
            raise AccountDeploymentError(f"Invalid platform: {platform}. Must be 'mt4' or 'mt5'")
    
    if not account_name:
        account_name = f"account_{login}"
    
    api = MetaApi(token)
    account = None
    connection = None
    is_new_account = False
    
    try:
        # Check if account already exists by login
        print(f"Checking for existing account with login {login}")
        accounts = await api.metatrader_account_api.get_accounts_with_infinite_scroll_pagination()
        
        for item in accounts:
            if item.login == login and item.type.startswith('cloud'):
                account = item
                print(f"Found existing account: {account.id}")
                break
        
        # Create account if it doesn't exist
        if not account:
            print(f'Creating new {platform.upper()} account in MetaApi')
            account = await api.metatrader_account_api.create_account(
                {
                    'name': account_name,
                    'type': 'cloud',
                    'login': login,
                    'password': password,
                    'server': server_name,
                    'platform': platform,
                    'application': 'MetaApi',
                    'magic': magic,
                }
            )
            print(f"Account created with ID: {account.id}")
            is_new_account = True
        
        # Deploy account (idempotent - safe to call if already deployed)
        print('Deploying account...')
        await account.deploy()
        
        # Wait for connection to broker with retries
        print('Waiting for API server to connect to broker...')
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                await account.wait_connected()
                print('✓ Connected to broker')
                break
            except Exception as e:
                retry_count += 1
                if retry_count >= max_retries:
                    print(f"Failed to connect after {max_retries} attempts")
                    raise
                print(f"Connection attempt {retry_count} failed, retrying...")
                await asyncio.sleep(5)
        
        # Connect to MetaApi streaming
        connection = account.get_streaming_connection()
        await connection.connect()
        
        # Wait for synchronization
        print('Waiting for SDK to synchronize to terminal state...')
        try:
            await connection.wait_synchronized({'timeoutInSeconds': 300})
            print('✓ Synchronized to terminal state')
        except Exception as e:
            print(f"Warning: Synchronization timeout - continuing anyway.")
        
        # Access terminal state
        terminal_state = connection.terminal_state
        
        positions = terminal_state.positions
        has_open_positions = len(positions) > 0
        
        print(f"Account deployed successfully")
        print(f"Connected: {terminal_state.connected}")
        print(f"Connected to broker: {terminal_state.connected_to_broker}")
        print(f"Open positions: {len(positions)}")
        
        # Extract account information
        account_info = None
        if terminal_state.account_information:
            acc_info = terminal_state.account_information
            if isinstance(acc_info, dict):
                account_info = {
                    'balance': acc_info.get('balance'),
                    'equity': acc_info.get('equity'),
                    'currency': acc_info.get('currency'),
                    'leverage': acc_info.get('leverage'),
                    'broker': acc_info.get('broker') or acc_info.get('name'),
                    'server': acc_info.get('server'),
                }
            else:
                account_info = {
                    'balance': getattr(acc_info, 'balance', None),
                    'equity': getattr(acc_info, 'equity', None),
                    'currency': getattr(acc_info, 'currency', None),
                    'leverage': getattr(acc_info, 'leverage', None),
                    'broker': getattr(acc_info, 'broker', None) or getattr(acc_info, 'name', None),
                    'server': getattr(acc_info, 'server', None),
                }
            print(f"Account info: balance={account_info.get('balance')}, equity={account_info.get('equity')}")
        
        # Parse positions
        positions_list = []
        for pos in positions:
            try:
                if isinstance(pos, dict):
                    pos_dict = {
                        'id': pos.get('id') or pos.get('positionId'),
                        'symbol': pos.get('symbol'),
                        'type': pos.get('type'),
                        'volume': pos.get('volume'),
                        'open_price': pos.get('openPrice') or pos.get('open_price'),
                        'current_price': pos.get('currentPrice') or pos.get('current_price'),
                        'profit': pos.get('profit', 0),
                        'open_time': pos.get('time') or pos.get('openTime')
                    }
                else:
                    pos_dict = {
                        'id': pos.id,
                        'symbol': pos.symbol,
                        'type': pos.type,
                        'volume': pos.volume,
                        'open_price': pos.open_price,
                        'current_price': pos.current_price,
                        'profit': pos.profit,
                        'open_time': pos.time.isoformat() if hasattr(pos, 'time') else None
                    }
                positions_list.append(pos_dict)
            except Exception as pos_error:
                print(f"Warning: Failed to parse position: {pos_error}")
                continue
        
        result = {
            'metaapi_account_id': account.id,
            'status': 'deployed',
            'connected': terminal_state.connected,
            'connected_to_broker': terminal_state.connected_to_broker,
            'has_open_positions': has_open_positions,
            'positions': positions_list,
            'account_info': account_info
        }
        
        if connection:
            await connection.close()
        
        return result
        
    except Exception as err:
        # Only cleanup (undeploy) if we created a NEW account and it failed
        if account and is_new_account:
            try:
                print(f"Error occurred, cleaning up account {account.id}...")
                await account.undeploy()
                print(f"✓ Account {account.id} undeployed successfully")
            except Exception as cleanup_error:
                print(f"⚠ Failed to cleanup account: {cleanup_error}")
        
        if connection:
            try:
                await connection.close()
            except:
                pass
        
        error_msg = "Account deployment failed"
        
        if hasattr(err, 'details'):
            if err.details == 'E_SRV_NOT_FOUND':
                error_msg = f"Server file not found for '{server_name}'. Please check the server name."
            elif err.details == 'E_AUTH':
                error_msg = f"Authentication failed. Please check your login and password."
            elif err.details == 'E_SERVER_TIMEZONE':
                error_msg = f"Failed to detect broker settings. Please try again later."
        
        print(f"Error: {error_msg}")
        raise AccountDeploymentError(f"{error_msg}: {str(err)}")


def deploy_account_sync(
    login: str,
    password: str,
    server_name: str,
    platform: str,
    account_name: Optional[str] = None,
    magic: int = 1000
) -> Dict[str, Any]:
    """Synchronous wrapper for deploy_account"""
    return asyncio.run(deploy_account(
        login=login,
        password=password,
        server_name=server_name,
        platform=platform,
        account_name=account_name,
        magic=magic
    ))


if __name__ == "__main__":
    test_login = os.getenv('LOGIN') or input("Enter MT login: ")
    test_password = os.getenv('PASSWORD') or input("Enter MT password: ")
    test_server = os.getenv('SERVER') or input("Enter MT server: ")
    test_platform = os.getenv('PLATFORM', 'mt5')
    
    try:
        result = deploy_account_sync(
            login=test_login,
            password=test_password,
            server_name=test_server,
            platform=test_platform
        )
        
        print("\n=== Deployment Result ===")
        print(f"MetaAPI Account ID: {result['metaapi_account_id']}")
        print(f"Status: {result['status']}")
        print(f"Has Open Positions: {result['has_open_positions']}")
        print(f"Number of Positions: {len(result['positions'])}")
        if result['positions']:
            print("\nOpen Positions:")
            for pos in result['positions']:
                print(f"  - {pos['symbol']} {pos['type']}: {pos['volume']} lots, P/L: ${pos['profit']}")
        
    except AccountDeploymentError as e:
        print(f"Deployment failed: {e}")