"""
Equity Stream Service
=====================

Standalone service that manages equity streams for active accounts.
Runs independently, checking database for accounts that need streams started/stopped.

Streams are started when:
- accounts.is_enabled = TRUE
- accounts.status = 'active'
- accounts.metaapi_account_id IS NOT NULL
- trading_periods has an active period (end_time IS NULL)

Streams are stopped when:
- accounts.is_enabled = FALSE
- accounts.status NOT IN ('active')
- trading_periods.end_time IS NOT NULL

Usage:
    python -m veilon_core.services.equity_stream_service

Or run directly:
    python equity_stream_service.py
"""

from pathlib import Path
import sys

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import os
import time
import logging
import certifi
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, Set

# Suppress noisy third-party SDK logs BEFORE any MetaAPI imports
for logger_name in [
    'metaapi_cloud_sdk',
    'socketio', 
    'engineio',
    'websockets',
    'urllib3',
]:
    logging.getLogger(logger_name).setLevel(logging.WARNING)

# SSL certificates
os.environ["SSL_CERT_FILE"] = certifi.where()
os.environ["REQUESTS_CA_BUNDLE"] = certifi.where()

# Configure logging for THIS service
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Constants
CHECK_INTERVAL_SECONDS = 10  # Check every 10 seconds for quick response
STREAM_HEALTH_CHECK_SECONDS = 60  # Full health check every 60 seconds


class EquityStreamService:
    """
    Standalone service that manages equity streams based on database state.
    
    Monitors accounts table and starts/stops streams as needed.
    """
    
    def __init__(self):
        self.running = False
        self._active_streams: Set[str] = set()  # Track which streams we've started
        self._manager = None
        self._last_health_check = datetime.now(timezone.utc)
        
        logger.info("EquityStreamService initialized")
        logger.info(f"  Check Interval: {CHECK_INTERVAL_SECONDS}s")
        logger.info(f"  Health Check Interval: {STREAM_HEALTH_CHECK_SECONDS}s")
    
    def _get_manager(self):
        """Lazy-load the stream manager to avoid import issues"""
        if self._manager is None:
            from veilon_core.services.equity_stream_manager import get_service
            self._manager = get_service()
            logger.info("Stream manager initialized")
        return self._manager
    
    def get_accounts_needing_streams(self) -> List[Dict[str, Any]]:
        """
        Get all accounts that should have active streams.
        
        Criteria:
        - is_enabled = TRUE
        - status = 'active'
        - metaapi_account_id IS NOT NULL
        - Has an active trading period (end_time IS NULL)
        """
        from veilon_core.repositories.db import execute_query
        
        rows = execute_query(
            """
            SELECT DISTINCT
                a.id AS account_id,
                a.metaapi_account_id,
                a.status,
                a.is_enabled,
                tp.trading_period_id,
                tp.period_type,
                tp.start_time
            FROM accounts a
            JOIN trading_periods tp ON tp.metaapi_account_id = a.metaapi_account_id
            WHERE a.is_enabled = TRUE
            AND a.status = 'active'
            AND a.metaapi_account_id IS NOT NULL
            AND tp.status = 'active'
            AND tp.end_time IS NULL
            """,
            fetch_results=True
        )
        
        return rows or []
    
    def get_accounts_needing_stream_stop(self) -> List[Dict[str, Any]]:
        """
        Get metaapi_account_ids that should NOT have active streams.
        
        A stream should be stopped only if NO accounts with that metaapi_account_id
        need an active stream (handles multiple accounts sharing same MT5 login).
        """
        from veilon_core.repositories.db import execute_query
        
        if not self._active_streams:
            return []
        
        # Convert set to list for SQL
        active_ids = list(self._active_streams)
        placeholders = ','.join(['%s'] * len(active_ids))
        
        # Find metaapi_account_ids that have NO accounts needing streams
        # (i.e., all accounts with this metaapi_id are either disabled or not active)
        rows = execute_query(
            f"""
            SELECT DISTINCT a.metaapi_account_id
            FROM accounts a
            WHERE a.metaapi_account_id IN ({placeholders})
            AND NOT EXISTS (
                -- Check if ANY account with this metaapi_id needs a stream
                SELECT 1 FROM accounts a2
                JOIN trading_periods tp ON tp.metaapi_account_id = a2.metaapi_account_id
                WHERE a2.metaapi_account_id = a.metaapi_account_id
                AND a2.is_enabled = TRUE
                AND a2.status = 'active'
                AND tp.status = 'active'
                AND tp.end_time IS NULL
            )
            """,
            tuple(active_ids),
            fetch_results=True
        )
        
        # Return in expected format
        return [{'metaapi_account_id': r['metaapi_account_id'], 'account_id': None, 'status': None, 'is_enabled': None} for r in (rows or [])]
    
    def start_stream(self, metaapi_account_id: str) -> bool:
        """Start a stream for an account"""
        try:
            manager = self._get_manager()
            result = manager.start_stream(metaapi_account_id)
            
            if result:
                self._active_streams.add(metaapi_account_id)
                logger.info(f"✅ Stream STARTED for {metaapi_account_id}")
            else:
                logger.warning(f"⚠️ Stream already running for {metaapi_account_id}")
                self._active_streams.add(metaapi_account_id)  # Track it anyway
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to start stream for {metaapi_account_id}: {e}")
            return False
    
    def stop_stream(self, metaapi_account_id: str) -> bool:
        """Stop a stream for an account"""
        try:
            manager = self._get_manager()
            result = manager.stop_stream(metaapi_account_id)
            
            self._active_streams.discard(metaapi_account_id)
            
            if result:
                logger.info(f"🛑 Stream STOPPED for {metaapi_account_id}")
            else:
                logger.warning(f"⚠️ Stream was not running for {metaapi_account_id}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to stop stream for {metaapi_account_id}: {e}")
            return False
    
    def sync_streams(self):
        """
        Synchronize streams with database state.
        
        - Start streams for accounts that need them
        - Stop streams for accounts that no longer need them
        """
        # Get accounts that should have streams
        accounts_need_stream = self.get_accounts_needing_streams()
        needed_ids = {a['metaapi_account_id'] for a in accounts_need_stream}
        
        # Get metaapi_account_ids that should NOT have streams
        accounts_need_stop = self.get_accounts_needing_stream_stop()
        stop_ids = {a['metaapi_account_id'] for a in accounts_need_stop}
        
        # Start streams that aren't running
        for account in accounts_need_stream:
            metaapi_id = account['metaapi_account_id']
            if metaapi_id not in self._active_streams:
                logger.info(f"📡 Starting stream for account {account['account_id']} ({metaapi_id})")
                self.start_stream(metaapi_id)
        
        # Stop streams that shouldn't be running
        for metaapi_id in stop_ids:
            logger.info(f"📴 Stopping stream for {metaapi_id} - no active accounts need it")
            self.stop_stream(metaapi_id)
        
        # Also stop any streams we're tracking that aren't in the needed list
        orphaned = self._active_streams - needed_ids
        for metaapi_id in orphaned:
            if metaapi_id not in stop_ids:  # Don't double-stop
                logger.info(f"📴 Stopping orphaned stream for {metaapi_id}")
                self.stop_stream(metaapi_id)
    
    def health_check(self):
        """
        Perform health check on active streams.
        
        Checks if streams are actually receiving data.
        """
        from veilon_core.repositories.db import execute_query
        
        logger.debug(f"Running health check on {len(self._active_streams)} active streams")
        
        for metaapi_id in list(self._active_streams):
            # Check if we've received ticks in the last 5 minutes
            rows = execute_query(
                """
                SELECT MAX(ts) AS last_tick
                FROM equity_balance_ticks
                WHERE metaapi_account_id = %s
                AND ts > NOW() - INTERVAL '5 minutes'
                """,
                (metaapi_id,),
                fetch_results=True
            )
            
            if not rows or rows[0]['last_tick'] is None:
                logger.warning(f"⚠️ No recent ticks for {metaapi_id} - stream may be stale")
                # Could restart stream here if needed
    
    def run_cycle(self):
        """Run a single sync cycle"""
        logger.debug("Running sync cycle...")
        
        try:
            self.sync_streams()
            
            # Periodic health check
            now = datetime.now(timezone.utc)
            if (now - self._last_health_check).total_seconds() >= STREAM_HEALTH_CHECK_SECONDS:
                self.health_check()
                self._last_health_check = now
                
        except Exception as e:
            logger.error(f"Error in sync cycle: {e}")
            import traceback
            logger.error(traceback.format_exc())
        
        logger.debug(f"Sync cycle complete. Active streams: {len(self._active_streams)}")
    
    def run(self):
        """Main run loop"""
        self.running = True
        logger.info("=" * 50)
        logger.info("Equity Stream Service Started")
        logger.info("=" * 50)
        
        # Initial sync
        self.run_cycle()
        
        while self.running:
            try:
                time.sleep(CHECK_INTERVAL_SECONDS)
                self.run_cycle()
                
            except KeyboardInterrupt:
                logger.info("Received interrupt, shutting down...")
                self.running = False
                break
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                import traceback
                logger.error(traceback.format_exc())
        
        # Cleanup - stop all streams
        logger.info("Stopping all active streams...")
        for metaapi_id in list(self._active_streams):
            self.stop_stream(metaapi_id)
        
        logger.info("Equity Stream Service Stopped")
    
    def stop(self):
        """Stop the service"""
        self.running = False


def main():
    """Entry point"""
    service = EquityStreamService()
    
    try:
        service.run()
    except KeyboardInterrupt:
        service.stop()


if __name__ == "__main__":
    main()