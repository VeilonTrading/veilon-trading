"""
Revised Account Flow - Manual Start Button (More Economical)

UPDATED APPROACH:
Instead of background monitoring pending_flat accounts, show a "Start Evaluation" button.
When clicked, temporarily start stream to check if flat, then proceed or show error.

FLOWS:
=====

1. INITIAL DEPLOYMENT (Evaluation Start)
   - User connects MT account
   - MetaAPI connection established (check positions via API)
   - IF positions exist: Show "Start Evaluation" button
   - User clicks button → Temp start stream → Check flat → Proceed or error
   - IF no positions: Auto-start immediately

2. FUNDED STAGE START (Admin approval)
   - Admin approves passed evaluation
   - Show "Start Funded Stage" button
   - Admin clicks → Temp start stream → Check flat → Proceed or error

This saves resources by only checking when user explicitly requests start.
"""

from typing import Optional, Dict
import asyncio
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class ImprovedAccountLifecycleManager:
    """
    Manages account lifecycle with manual start buttons for pending accounts
    Much more economical than background monitoring
    """
    
    def __init__(self):
        from veilon_core.services.equity_stream_manager import get_service
        self.stream_service = get_service()
    
    # ==========================================
    # FLOW 1: INITIAL DEPLOYMENT
    # ==========================================
    
    def handle_initial_deployment(
        self, 
        metaapi_account_id: str,
        has_open_positions: bool,
        positions: list
    ) -> Dict[str, any]:
        """
        Called during initial MT account connection
        
        ALWAYS sets status to pending_start - user must click "Start Evaluation" button
        This provides a consistent UX regardless of whether positions exist
        
        Args:
            metaapi_account_id: MetaAPI account ID
            has_open_positions: Result from MetaAPI connection check (used for logging only)
            positions: List of open positions from MetaAPI (used for logging only)
        
        Returns:
            Dict with status and UI instructions
        """
        try:
            logger.info(f"📋 Account {metaapi_account_id} deployed - has_open_positions={has_open_positions}")
            
            # Always set status to pending_start - user must click button to begin
            self._set_account_status_by_metaapi(metaapi_account_id, 'pending_start')
            
            return {
                'success': True,
                'can_start': False,  # Never auto-start
                'needs_manual_start': True,  # Always show "Start Evaluation" button
                'has_open_positions': has_open_positions,
                'positions_count': len(positions),
                'positions': positions,
                'message': 'Click "Start Evaluation" when ready to begin.'
            }
                
        except Exception as e:
            logger.error(f"Error handling initial deployment: {e}")
            return {
                'success': False,
                'can_start': False,
                'needs_manual_start': False,
                'reason': 'error',
                'error': str(e)
            }
    
    # ==========================================
    # MANUAL START: EVALUATION (Button Click)
    # ==========================================
    
    async def attempt_start_evaluation(self, account_id: int, metaapi_account_id: str) -> Dict[str, any]:
        """
        Called when user clicks "Start Evaluation" button
        
        Process:
        1. Start stream temporarily
        2. Wait for first ticks (max 30 seconds)
        3. Check if equity == balance
        4. If flat: Create period and continue stream
        5. If not flat: Stop stream and return error
        
        Args:
            account_id: Veilon account ID (for database operations)
            metaapi_account_id: MetaAPI account ID (for stream operations)
        
        Returns:
            Dict with result (success/error with message)
        """
        try:
            logger.info(f"🎯 Attempting to start evaluation for account {account_id} ({metaapi_account_id})")
            
            # Step 1: Start stream temporarily
            stream_started = self.stream_service.start_stream(metaapi_account_id)
            if not stream_started:
                return {
                    'success': False,
                    'error_message': 'Failed to start equity stream. Please try again.'
                }
            
            # Step 2: Wait for first ticks (with timeout)
            logger.info(f"⏳ Waiting for equity data...")
            for i in range(30):  # Wait max 30 seconds
                await asyncio.sleep(1)
                
                if self._has_equity_data(metaapi_account_id):
                    break
            else:
                # Timeout - no data received
                logger.error(f"Timeout waiting for equity data")
                self.stream_service.stop_stream(metaapi_account_id)
                return {
                    'success': False,
                    'error_message': 'Could not connect to account. Please check your MT4/5 terminal is running and connected.'
                }
            
            # Step 3: Check if flat
            is_flat = self._is_account_flat(metaapi_account_id)
            
            if not is_flat:
                # Not flat - stop stream and return error
                equity, balance = self._get_equity_balance(metaapi_account_id)
                logger.warning(f"❌ Account {account_id} ({metaapi_account_id}) has open positions (equity={equity}, balance={balance})")
                self.stream_service.stop_stream(metaapi_account_id)
                
                return {
                    'success': False,
                    'error_message': f'Account has open positions (Equity: ${equity:,.2f}, Balance: ${balance:,.2f}). Please close all positions before starting.'
                }
            
            # Step 4: Flat - create period and continue stream
            logger.info(f"✅ Account {account_id} ({metaapi_account_id}) is flat - starting evaluation")
            
            # Use account_id directly instead of looking it up
            period_id = self._create_trading_period(account_id, metaapi_account_id, is_funded=False)
            self._set_account_status(account_id, 'active', phase='phase_1')
            
            return {
                'success': True,
                'period_id': period_id,
                'success_message': 'Evaluation started successfully!'
            }
            
        except Exception as e:
            logger.error(f"Error starting evaluation: {e}")
            # Make sure to stop stream on error
            try:
                self.stream_service.stop_stream(metaapi_account_id)
            except:
                pass
            
            return {
                'success': False,
                'error_message': f'Error starting evaluation: {str(e)}'
            }
    
    # ==========================================
    # MANUAL START: FUNDED STAGE (Button Click)
    # ==========================================
    
    async def attempt_start_funded_stage(self, account_id: int, metaapi_account_id: str) -> Dict[str, any]:
        """
        Called when admin clicks "Start Funded Stage" button
        
        Same process as attempt_start_evaluation but for funded accounts
        
        Args:
            account_id: Veilon account ID (for database operations)
            metaapi_account_id: MetaAPI account ID (for stream operations)
        """
        try:
            logger.info(f"🎯 Attempting to start funded stage for account {account_id} ({metaapi_account_id})")
            
            # Start stream temporarily
            stream_started = self.stream_service.start_stream(metaapi_account_id)
            if not stream_started:
                return {
                    'success': False,
                    'error_message': 'Failed to start equity stream. Please try again.'
                }
            
            # Wait for first ticks
            for i in range(30):
                await asyncio.sleep(1)
                if self._has_equity_data(metaapi_account_id):
                    break
            else:
                self.stream_service.stop_stream(metaapi_account_id)
                return {
                    'success': False,
                    'error_message': 'Could not connect to account. Please check MT terminal is running.'
                }
            
            # Check if flat
            is_flat = self._is_account_flat(metaapi_account_id)
            
            if not is_flat:
                equity, balance = self._get_equity_balance(metaapi_account_id)
                logger.warning(f"❌ Account {account_id} ({metaapi_account_id}) has open positions")
                self.stream_service.stop_stream(metaapi_account_id)
                
                return {
                    'success': False,
                    'error_message': f'Trader has open positions (Equity: ${equity:,.2f}, Balance: ${balance:,.2f}). Cannot start funded stage until all positions are closed.'
                }
            
            # Flat - start funded stage
            logger.info(f"✅ Account {account_id} ({metaapi_account_id}) is flat - starting funded stage")
            
            period_id = self._create_trading_period(account_id, metaapi_account_id, is_funded=True)
            self._set_account_status(account_id, 'active', phase='funded')
            
            return {
                'success': True,
                'period_id': period_id,
                'success_message': 'Funded stage started successfully!'
            }
            
        except Exception as e:
            logger.error(f"Error starting funded stage: {e}")
            try:
                self.stream_service.stop_stream(metaapi_account_id)
            except:
                pass
            
            return {
                'success': False,
                'error_message': f'Error: {str(e)}'
            }
    
    # ==========================================
    # FLOW 3: FUNDED PROFIT CAP HIT
    # ==========================================
    
    async def handle_profit_cap_hit(self, account_id: int, metaapi_account_id: str, gain_pct: float) -> bool:
        """
        Handle when funded account hits 10% profit cap
        
        Process:
        1. Close all positions via MetaAPI
        2. End trading period
        3. Stop stream
        4. Set status to 'awaiting_withdrawal'
        
        Trader must request withdrawal before continuing
        """
        try:
            logger.info(f"🎯 Profit cap hit for account {account_id} ({metaapi_account_id}) ({gain_pct:.2%})")
            
            # Close all positions
            await self._close_all_positions(metaapi_account_id)
            
            # End trading period
            self._end_trading_period(
                metaapi_account_id,
                reason=f"Profit cap reached: {gain_pct:.2%}"
            )
            
            # Stop stream
            self.stream_service.stop_stream(metaapi_account_id)
            
            # Set status to awaiting withdrawal
            self._set_account_status(account_id, 'awaiting_withdrawal')
            
            logger.info(f"✅ Account {account_id} stopped - awaiting withdrawal")
            return True
            
        except Exception as e:
            logger.error(f"Error handling profit cap: {e}")
            return False
    
    # ==========================================
    # FLOW 4: FUNDED WITHDRAWAL PROCESSED
    # ==========================================
    
    async def handle_withdrawal_processed(self, account_id: int, metaapi_account_id: str) -> Dict[str, any]:
        """
        Handle after withdrawal is processed - restart funded cycle
        
        Same as start_funded_stage - need to check if flat before restarting
        """
        logger.info(f"💰 Withdrawal processed for account {account_id} - restarting cycle")
        return await self.attempt_start_funded_stage(account_id, metaapi_account_id)
    
    # ==========================================
    # HELPER METHODS
    # ==========================================
    
    def _has_equity_data(self, metaapi_account_id: str) -> bool:
        """Check if we have any RECENT equity data for this account (last 60 seconds)"""
        from veilon_core.repositories.db import execute_query
        
        rows = execute_query(
            """
            SELECT 1 FROM equity_balance_ticks 
            WHERE metaapi_account_id = %s 
            AND ts > NOW() - INTERVAL '60 seconds'
            LIMIT 1
            """,
            (metaapi_account_id,),
            fetch_results=True
        )
        return bool(rows)

    def _is_account_flat(self, metaapi_account_id: str) -> bool:
        """Check if equity == balance (no open positions) using RECENT data only"""
        from veilon_core.repositories.db import execute_query
        
        rows = execute_query(
            """
            SELECT equity::float8, balance::float8
            FROM equity_balance_ticks
            WHERE metaapi_account_id = %s
            AND ts > NOW() - INTERVAL '60 seconds'
            ORDER BY ts DESC
            LIMIT 1
            """,
            (metaapi_account_id,),
            fetch_results=True
        )
        
        if not rows:
            return False
        
        equity = float(rows[0]['equity'])
        balance = float(rows[0]['balance'])
        
        # Consider flat if difference is less than $0.01
        return abs(equity - balance) < 0.01

    def _get_equity_balance(self, metaapi_account_id: str) -> tuple[float, float]:
        """Get latest RECENT equity and balance values"""
        from veilon_core.repositories.db import execute_query
        
        rows = execute_query(
            """
            SELECT equity::float8, balance::float8
            FROM equity_balance_ticks
            WHERE metaapi_account_id = %s
            AND ts > NOW() - INTERVAL '60 seconds'
            ORDER BY ts DESC
            LIMIT 1
            """,
            (metaapi_account_id,),
            fetch_results=True
        )
        
        if not rows:
            return (0.0, 0.0)
        
        return (float(rows[0]['equity']), float(rows[0]['balance']))
    
    def _create_trading_period(self, account_id: int, metaapi_account_id: str, is_funded: bool) -> int:
        """Create new trading period
        
        Args:
            account_id: Veilon account ID
            metaapi_account_id: MetaAPI account ID
            is_funded: True for funded stage, False for evaluation
        """
        from veilon_core.repositories.db import execute_query
        
        # Determine period_type based on is_funded flag
        period_type = 'funded' if is_funded else 'phase_1'
        
        # Create the trading period with correct schema
        rows = execute_query(
            """
            INSERT INTO trading_periods (account_id, metaapi_account_id, period_type, start_time, status)
            VALUES (%s, %s, %s, NOW(), 'active')
            RETURNING trading_period_id
            """,
            (account_id, metaapi_account_id, period_type),
            fetch_results=True
        )
        
        if rows:
            logger.info(f"Created trading period {rows[0]['trading_period_id']} for account {account_id} ({period_type})")
            return rows[0]['trading_period_id']
        else:
            logger.error(f"Failed to create trading period for account {account_id}")
            return None
    
    def _end_trading_period(self, metaapi_account_id: str, reason: str):
        """End current trading period"""
        from veilon_core.repositories.db import execute_query
        
        execute_query(
            """
            UPDATE trading_periods
            SET end_time = NOW(), 
                end_reason = %s,
                status = 'completed'
            WHERE metaapi_account_id = %s 
            AND end_time IS NULL
            AND status = 'active'
            """,
            (reason, metaapi_account_id)
        )
        logger.info(f"Ended trading period for {metaapi_account_id}: {reason}")
    
    def _set_account_status(self, account_id: int, status: str, phase: Optional[str] = None):
        """Update account status by account_id"""
        from veilon_core.repositories.db import execute_query
        
        if phase:
            execute_query(
                "UPDATE accounts SET status = %s, phase = %s WHERE id = %s",
                (status, phase, account_id)
            )
        else:
            execute_query(
                "UPDATE accounts SET status = %s WHERE id = %s",
                (status, account_id)
            )
    
    def _set_account_status_by_metaapi(self, metaapi_account_id: str, status: str, phase: Optional[str] = None):
        """Update account status by metaapi_account_id (used during deployment)"""
        from veilon_core.repositories.db import execute_query
        
        if phase:
            execute_query(
                "UPDATE accounts SET status = %s, phase = %s WHERE metaapi_account_id = %s",
                (status, phase, metaapi_account_id)
            )
        else:
            execute_query(
                "UPDATE accounts SET status = %s WHERE metaapi_account_id = %s",
                (status, metaapi_account_id)
            )
    
    async def _close_all_positions(self, metaapi_account_id: str):
        """Close all positions via MetaAPI"""
        try:
            from metaapi_cloud_sdk import MetaApi
            import os
            
            # Get MetaAPI token
            token = os.getenv('METAAPI_TOKEN')
            if not token:
                logger.error("METAAPI_TOKEN not found in environment")
                return False
            
            # Get MetaAPI connection
            api = MetaApi(token=token)
            account = await api.metatrader_account_api.get_account(metaapi_account_id)
            
            # Connect to account
            connection = account.get_rpc_connection()
            await connection.connect()
            await connection.wait_synchronized()
            
            # Get all open positions
            positions = await connection.get_positions()
            
            if not positions:
                logger.info(f"No open positions to close for {metaapi_account_id}")
                return True
            
            # Close each position
            close_failures = 0
            for position in positions:
                try:
                    await connection.close_position(position['id'])
                    logger.info(f"Closed position {position['id']} for {metaapi_account_id}")
                except Exception as e:
                    logger.error(f"Failed to close position {position['id']}: {e}")
                    close_failures += 1
            
            # Consider success if we closed at least some positions
            return close_failures == 0
            
        except Exception as e:
            logger.error(f"Error closing positions for {metaapi_account_id}: {e}")
            return False


# Singleton
_improved_manager: Optional[ImprovedAccountLifecycleManager] = None

def get_improved_lifecycle_manager() -> ImprovedAccountLifecycleManager:
    global _improved_manager
    if _improved_manager is None:
        _improved_manager = ImprovedAccountLifecycleManager()
    return _improved_manager