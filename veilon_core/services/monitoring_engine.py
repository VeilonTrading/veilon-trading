from pathlib import Path
import sys

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import os
import sys
import time
import logging
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Constants
CHECK_INTERVAL_SECONDS = 60


class EvaluationMonitor:
    """
    Monitors active accounts for profit target and drawdown breaches.
    
    Uses OHLC data (high/low) to accurately detect intra-minute limit hits.
    Limits are pulled from plan_specifications table per account.
    """
    
    def __init__(self):
        self.running = False
        logger.info("EvaluationMonitor initialized")
        logger.info(f"  Limits: Per-plan from plan_specifications table")
        logger.info(f"  Check Interval: {CHECK_INTERVAL_SECONDS}s")
    
    def get_active_accounts(self) -> List[Dict[str, Any]]:
        """Get all accounts with active trading periods and their plan specifications"""
        from veilon_core.repositories.db import execute_query
        
        # Join accounts -> trading_periods -> plan_specifications
        # Match phase: period_type 'phase_1' -> phase_number 1, 'funded' -> phase_type 'funded'
        rows = execute_query(
            """
            SELECT 
                a.id AS account_id,
                a.metaapi_account_id,
                a.status,
                a.phase,
                a.plan_id,
                tp.trading_period_id,
                tp.period_type,
                tp.start_time,
                ps.name AS plan_name,
                ps.phase_type,
                ps.profit_target_pct,
                ps.max_drawdown_pct,
                ps.daily_drawdown_pct,
                ps.min_trading_days,
                ps.profit_split_pct
            FROM accounts a
            JOIN trading_periods tp ON tp.metaapi_account_id = a.metaapi_account_id
            JOIN plan_specifications ps ON ps.plan_id = a.plan_id
                AND (
                    -- Match phase_1 period to phase_number 1
                    (tp.period_type = 'phase_1' AND ps.phase_number = 1)
                    -- Match funded period to phase_type 'funded'
                    OR (tp.period_type = 'funded' AND ps.phase_type = 'funded')
                )
            WHERE a.status = 'active'
            AND a.is_enabled = TRUE
            AND tp.status = 'active'
            AND tp.end_time IS NULL
            """,
            fetch_results=True
        )
        
        return rows or []
    
    def get_period_baseline(self, metaapi_account_id: str, period_start: datetime) -> Optional[float]:
        """
        Get the starting equity for a trading period.
        Uses the first OHLC bar's open price after period start.
        """
        from veilon_core.repositories.db import execute_query
        
        rows = execute_query(
            """
            SELECT open
            FROM equity_ohlc_1min
            WHERE metaapi_account_id = %s
            AND bar_time >= %s
            ORDER BY bar_time ASC
            LIMIT 1
            """,
            (metaapi_account_id, period_start),
            fetch_results=True
        )
        
        if not rows:
            return None
        
        return float(rows[0]['open'])
    
    def get_latest_ohlc(self, metaapi_account_id: str, period_start: datetime) -> Optional[Dict]:
        """Get the most recent OHLC bar for an account"""
        from veilon_core.repositories.db import execute_query
        
        rows = execute_query(
            """
            SELECT bar_time, open, high, low, close
            FROM equity_ohlc_1min
            WHERE metaapi_account_id = %s
            AND bar_time >= %s
            ORDER BY bar_time DESC
            LIMIT 1
            """,
            (metaapi_account_id, period_start),
            fetch_results=True
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
    
    def get_period_high_low(self, metaapi_account_id: str, period_start: datetime) -> Optional[Dict]:
        """
        Get the highest high and lowest low across all bars in the period.
        This ensures we catch any limit hits even if current bar is back within range.
        """
        from veilon_core.repositories.db import execute_query
        
        rows = execute_query(
            """
            SELECT 
                MAX(high) AS period_high,
                MIN(low) AS period_low
            FROM equity_ohlc_1min
            WHERE metaapi_account_id = %s
            AND bar_time >= %s
            """,
            (metaapi_account_id, period_start),
            fetch_results=True
        )
        
        if not rows or rows[0]['period_high'] is None:
            return None
        
        return {
            'period_high': float(rows[0]['period_high']),
            'period_low': float(rows[0]['period_low'])
        }
    
    def check_account(self, account: Dict[str, Any]) -> Optional[str]:
        """
        Check a single account for profit target or drawdown breach.
        Uses plan-specific limits from plan_specifications table.
        
        Returns:
            'profit_target' if hit profit target
            'drawdown_breach' if hit drawdown limit
            None if neither
        """
        metaapi_account_id = account['metaapi_account_id']
        period_start = account['start_time']
        period_type = account['period_type']
        plan_name = account.get('plan_name', 'Unknown')
        
        # Get plan-specific limits
        # Database stores as 10.00 meaning 10%, so divide by 100 to get decimal
        profit_target_pct = float(account['profit_target_pct']) / 100  # 10.00 -> 0.10
        max_drawdown_pct = float(account['max_drawdown_pct']) / 100    # 10.00 -> 0.10
        
        # Get baseline (starting equity)
        baseline = self.get_period_baseline(metaapi_account_id, period_start)
        if not baseline:
            logger.debug(f"[{metaapi_account_id}] No baseline data yet")
            return None
        
        # Get period high/low
        period_data = self.get_period_high_low(metaapi_account_id, period_start)
        if not period_data:
            logger.debug(f"[{metaapi_account_id}] No OHLC data yet")
            return None
        
        period_high = period_data['period_high']
        period_low = period_data['period_low']
        
        # Calculate gain/loss percentages
        high_gain_pct = (period_high - baseline) / baseline
        low_gain_pct = (period_low - baseline) / baseline
        
        # Get current close for logging
        latest = self.get_latest_ohlc(metaapi_account_id, period_start)
        current_close = latest['close'] if latest else baseline
        current_gain_pct = (current_close - baseline) / baseline
        
        logger.debug(
            f"[{metaapi_account_id}] ({plan_name}) baseline=${baseline:.2f}, "
            f"current=${current_close:.2f} ({current_gain_pct:+.2%}), "
            f"high=${period_high:.2f} ({high_gain_pct:+.2%}), "
            f"low=${period_low:.2f} ({low_gain_pct:+.2%}) | "
            f"limits: PT=+{profit_target_pct:.0%}, DD=-{max_drawdown_pct:.0%}"
        )
        
        # Check profit target (high touched target %)
        if high_gain_pct >= profit_target_pct:
            logger.info(
                f"🎯 PROFIT TARGET HIT: {metaapi_account_id} ({plan_name}) "
                f"(high={high_gain_pct:+.2%}, target=+{profit_target_pct:.0%})"
            )
            return 'profit_target'
        
        # Check drawdown breach (low touched -limit %)
        if low_gain_pct <= -max_drawdown_pct:
            logger.info(
                f"💥 DRAWDOWN BREACH: {metaapi_account_id} ({plan_name}) "
                f"(low={low_gain_pct:+.2%}, limit=-{max_drawdown_pct:.0%})"
            )
            return 'drawdown_breach'
        
        return None
    
    def handle_profit_target(self, account: Dict[str, Any]):
        """Handle profit target being hit"""
        from veilon_core.repositories.db import execute_query
        
        account_id = account['account_id']
        metaapi_account_id = account['metaapi_account_id']
        period_type = account['period_type']
        trading_period_id = account['trading_period_id']
        plan_name = account.get('plan_name', 'Unknown')
        profit_target_pct = float(account['profit_target_pct'])  # Store as-is from DB (10.00 = 10%)
        
        logger.info(f"Processing profit target for account {account_id} ({plan_name}, {period_type})")
        
        if period_type == 'phase_1':
            # Evaluation passed - set to in_review
            execute_query(
                """
                UPDATE accounts 
                SET status = 'passed', 
                    in_review = TRUE,
                    passed_at = NOW(),
                    updated_at = NOW()
                WHERE id = %s
                """,
                (account_id,)
            )
            
            # End trading period
            execute_query(
                """
                UPDATE trading_periods
                SET end_time = NOW(),
                    end_reason = 'profit_target_hit',
                    status = 'completed'
                WHERE trading_period_id = %s
                """,
                (trading_period_id,)
            )
            
            # Stop the equity stream
            self._stop_stream(metaapi_account_id)
            
            # Log event
            self._log_event(
                account_id=account_id,
                event_type='evaluation_passed',
                event_data={
                    'reason': 'profit_target_hit',
                    'period_type': period_type,
                    'trading_period_id': trading_period_id,
                    'plan_name': plan_name,
                    'profit_target_pct': profit_target_pct
                }
            )
            
            logger.info(f"✅ Account {account_id} PASSED evaluation - now in review")
            
        elif period_type == 'funded':
            # Funded account hit profit cap - await withdrawal
            execute_query(
                """
                UPDATE accounts 
                SET status = 'awaiting_withdrawal',
                    updated_at = NOW()
                WHERE id = %s
                """,
                (account_id,)
            )
            
            # End trading period
            execute_query(
                """
                UPDATE trading_periods
                SET end_time = NOW(),
                    end_reason = 'profit_cap_hit',
                    status = 'completed'
                WHERE trading_period_id = %s
                """,
                (trading_period_id,)
            )
            
            # Stop the equity stream
            self._stop_stream(metaapi_account_id)
            
            # Log event
            self._log_event(
                account_id=account_id,
                event_type='profit_cap_hit',
                event_data={
                    'reason': 'profit_cap_hit',
                    'period_type': period_type,
                    'trading_period_id': trading_period_id,
                    'plan_name': plan_name,
                    'profit_target_pct': profit_target_pct
                }
            )
            
            logger.info(f"💰 Account {account_id} hit profit cap - awaiting withdrawal")
    
    def handle_drawdown_breach(self, account: Dict[str, Any]):
        """Handle drawdown limit being breached"""
        from veilon_core.repositories.db import execute_query
        
        account_id = account['account_id']
        metaapi_account_id = account['metaapi_account_id']
        period_type = account['period_type']
        trading_period_id = account['trading_period_id']
        plan_name = account.get('plan_name', 'Unknown')
        max_drawdown_pct = float(account['max_drawdown_pct'])  # Store as-is from DB (10.00 = 10%)
        
        logger.info(f"Processing drawdown breach for account {account_id} ({plan_name}, {period_type})")
        
        # Mark account as failed and disable it
        execute_query(
            """
            UPDATE accounts 
            SET status = 'failed',
                is_enabled = FALSE,
                closed_at = NOW(),
                updated_at = NOW()
            WHERE id = %s
            """,
            (account_id,)
        )
        
        # End trading period
        execute_query(
            """
            UPDATE trading_periods
            SET end_time = NOW(),
                end_reason = 'drawdown_breach',
                status = 'failed'
            WHERE trading_period_id = %s
            """,
            (trading_period_id,)
        )
        
        # Stop the equity stream
        self._stop_stream(metaapi_account_id)
        
        # Log event
        self._log_event(
            account_id=account_id,
            event_type='evaluation_failed',
            event_data={
                'reason': 'drawdown_breach',
                'period_type': period_type,
                'trading_period_id': trading_period_id,
                'plan_name': plan_name,
                'max_drawdown_pct': max_drawdown_pct
            }
        )
        
        logger.info(f"❌ Account {account_id} FAILED - drawdown breach")
    
    def _stop_stream(self, metaapi_account_id: str):
        """Stop the equity stream for an account"""
        try:
            from veilon_core.services.equity_stream_manager import get_service
            service = get_service()
            service.stop_stream(metaapi_account_id)
            logger.info(f"Stream stopped for {metaapi_account_id}")
        except Exception as e:
            logger.error(f"Failed to stop stream for {metaapi_account_id}: {e}")
    
    def _log_event(self, account_id: int, event_type: str, event_data: Dict):
        """Log an event to account_events table"""
        from veilon_core.repositories.db import execute_query
        import json
        
        try:
            execute_query(
                """
                INSERT INTO account_events (
                    account_id, event_type, event_status, actor_type, payload
                ) VALUES (%s, %s, 'completed', 'system', %s)
                """,
                (account_id, event_type, json.dumps(event_data))
            )
        except Exception as e:
            logger.error(f"Failed to log event: {e}")
    
    def run_check_cycle(self):
        """Run a single check cycle across all active accounts"""
        logger.debug("Starting check cycle...")
        
        accounts = self.get_active_accounts()
        logger.debug(f"Found {len(accounts)} active accounts")
        
        for account in accounts:
            try:
                result = self.check_account(account)
                
                if result == 'profit_target':
                    self.handle_profit_target(account)
                elif result == 'drawdown_breach':
                    self.handle_drawdown_breach(account)
                    
            except Exception as e:
                logger.error(f"Error checking account {account.get('account_id')}: {e}")
                import traceback
                logger.error(traceback.format_exc())
        
        logger.debug("Check cycle complete")
    
    def run(self):
        """Main run loop"""
        self.running = True
        logger.info("=" * 50)
        logger.info("Evaluation Monitor Service Started")
        logger.info("=" * 50)
        
        while self.running:
            try:
                self.run_check_cycle()
            except KeyboardInterrupt:
                logger.info("Received interrupt, shutting down...")
                self.running = False
                break
            except Exception as e:
                logger.error(f"Error in check cycle: {e}")
                import traceback
                logger.error(traceback.format_exc())
            
            # Wait for next cycle
            if self.running:
                time.sleep(CHECK_INTERVAL_SECONDS)
        
        logger.info("Evaluation Monitor Service Stopped")
    
    def stop(self):
        """Stop the monitor"""
        self.running = False


def main():
    """Entry point"""
    monitor = EvaluationMonitor()
    
    try:
        monitor.run()
    except KeyboardInterrupt:
        monitor.stop()


if __name__ == "__main__":
    main()