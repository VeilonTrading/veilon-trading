import os
import certifi
import asyncio
import threading
import random
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Tuple
from dataclasses import dataclass

os.environ["SSL_CERT_FILE"] = certifi.where()
os.environ["REQUESTS_CA_BUNDLE"] = certifi.where()

from metaapi_cloud_sdk import RiskManagement, EquityBalanceListener

TOKEN = 'eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9.eyJfaWQiOiIxNzM2NDVlM2U5MThkZjE2NjQzZmFjZDI0NTBlMGVmMiIsImFjY2Vzc1J1bGVzIjpbeyJpZCI6InRyYWRpbmctYWNjb3VudC1tYW5hZ2VtZW50LWFwaSIsIm1ldGhvZHMiOlsidHJhZGluZy1hY2NvdW50LW1hbmFnZW1lbnQtYXBpOnJlc3Q6cHVibGljOio6KiJdLCJyb2xlcyI6WyJyZWFkZXIiLCJ3cml0ZXIiXSwicmVzb3VyY2VzIjpbIio6JFVTRVJfSUQkOioiXX0seyJpZCI6Im1ldGFhcGktcmVzdC1hcGkiLCJtZXRob2RzIjpbIm1ldGFhcGktYXBpOnJlc3Q6cHVibGljOio6KiJdLCJyb2xlcyI6WyJyZWFkZXIiLCJ3cml0ZXIiXSwicmVzb3VyY2VzIjpbIio6JFVTRVJfSUQkOioiXX0seyJpZCI6Im1ldGFhcGktcnBjLWFwaSIsIm1ldGhvZHMiOlsibWV0YWFwaS1hcGk6d3M6cHVibGljOio6KiJdLCJyb2xlcyI6WyJyZWFkZXIiLCJ3cml0ZXIiXSwicmVzb3VyY2VzIjpbIio6JFVTRVJfSUQkOioiXX0seyJpZCI6Im1ldGFhcGktcmVhbC10aW1lLXN0cmVhbWluZy1hcGkiLCJtZXRob2RzIjpbIm1ldGFhcGktYXBpOndzOnB1YmxpYzoqOioiXSwicm9sZXMiOlsicmVhZGVyIiwid3JpdGVyIl0sInJlc291cmNlcyI6WyIqOiRVU0VSX0lEJDoqIl19LHsiaWQiOiJtZXRhc3RhdHMtYXBpIiwibWV0aG9kcyI6WyJtZXRhc3RhdHMtYXBpOnJlc3Q6cHVibGljOio6KiJdLCJyb2xlcyI6WyJyZWFkZXIiLCJ3cml0ZXIiXSwicmVzb3VyY2VzIjpbIio6JFVTRVJfSUQkOioiXX0seyJpZCI6InJpc2stbWFuYWdlbWVudC1hcGkiLCJtZXRob2RzIjpbInJpc2stbWFuYWdlbWVudC1hcGk6cmVzdDpwdWJsaWM6KjoqIl0sInJvbGVzIjpbInJlYWRlciIsIndyaXRlciJdLCJyZXNvdXJjZXMiOlsiKjokVVNFUl9JRCQ6KiJdfSx7ImlkIjoiY29weWZhY3RvcnktYXBpIiwibWV0aG9kcyI6WyJjb3B5ZmFjdG9yeS1hcGk6cmVzdDpwdWJsaWM6KjoqIl0sInJvbGVzIjpbInJlYWRlciIsIndyaXRlciJdLCJyZXNvdXJjZXMiOlsiKjokVVNFUl9JRCQ6KiJdfSx7ImlkIjoibXQtbWFuYWdlci1hcGkiLCJtZXRob2RzIjpbIm10LW1hbmFnZXItYXBpOnJlc3Q6ZGVhbGluZzoqOioiLCJtdC1tYW5hZ2VyLWFwaTpyZXN0OnB1YmxpYzoqOioiXSwicm9sZXMiOlsicmVhZGVyIiwid3JpdGVyIl0sInJlc291cmNlcyI6WyIqOiRVU0VSX0lEJDoqIl19LHsiaWQiOiJiaWxsaW5nLWFwaSIsIm1ldGhvZHMiOlsiYmlsbGluZy1hcGk6cmVzdDpwdWJsaWM6KjoqIl0sInJvbGVzIjpbInJlYWRlciJdLCJyZXNvdXJjZXMiOlsiKjokVVNFUl9JRCQ6KiJdfV0sImlnbm9yZVJhdGVMaW1pdHMiOmZhbHNlLCJ0b2tlbklkIjoiMjAyMTAyMTMiLCJpbXBlcnNvbmF0ZWQiOmZhbHNlLCJyZWFsVXNlcklkIjoiMTczNjQ1ZTNlOTE4ZGYxNjY0M2ZhY2QyNDUwZTBlZjIiLCJpYXQiOjE3NjY4ODQwMzh9.TxlTccBIbkoaxc39Ud1QbAuIeyi9YMcKxQaAMolJCjjptHRoMWy1DMzDhYo5wd5RtNbv3jrG2Y81Qx2mDC5C0UIjHIwRMADZuLtWtiFSPTPdfeqQBhk2tR12tzDrr8TZSMNH-lLTYx_MDTTDQkvJi2iDNy4U1V9kJxJ2qhgHPFhJYUN635r3A4M_hS2SvygUA6-zpiBlmEBiEk7CCxNe8l2ERVdMcYUwpFdfp_8rLKT94mVKAG6CspZYOgtAAbqEjcryZ9TStQ4_adhGU3UQt0n_AjNgPxwdxErKFEDI4UpJIeOfVZgbnSW_47W0sufRfTwz99RJNBHJvwNTCzsMeOR3FPMNiEtEnVD2lC4QzKg9ag_aOZ79hlGJ_NXLA_RnYD5rCnoRzxeiERdVgr2_29_ZZguGqaBXrUp8D7DKFsAMH_cyo7AbwHOi8tLRR5XxhzjtmDCJuhRyWT8ILEcl6Pety9ZN99Ekjx1C4SKfpsjmRmGu46J3yskdPD-0f0DyDqA3PZ2_VPtPQZbC7OVmwQAYHxchJLST7TH1GUNt_xZCImWYpVLO0u1NY4WnwFUVjRA4loDimEqiZ-t_iXdWojjRhB6fppcLMh7a2OmJkS_wZv6rZGiF1uUAcuhtWizh6ZwuTJ1GMgm9KAOc6vgfP71q-y1BDOlJpNbwRGT0JOs'
DOMAIN = os.getenv("DOMAIN") or "agiliumtrade.agiliumtrade.ai"

def log(msg: str):
    print(f"[{datetime.now(timezone.utc).isoformat()}] {msg}")


# ===========================
# EQUITY MONITOR (FIXED)
# ===========================

@dataclass
class AccountMetrics:
    metaapi_account_id: str
    period_start: datetime
    first_equity: float
    current_equity: float
    peak_equity: float
    trough_equity: float
    current_gain_pct: float
    peak_gain_pct: float
    trough_gain_pct: float
    current_drawdown_pct: float
    last_updated: datetime


class EquityMonitor:
    """
    Runs alongside equity stream to calculate real-time metrics.
    Updates a cached metrics table on every tick.
    
    FIXED: Uses simplified balance-based drawdown (not trailing drawdown)
    """
    
    def __init__(self):
        self._metrics: Dict[str, AccountMetrics] = {}
        self._lock = asyncio.Lock()
    
    async def initialize_account(self, metaapi_account_id: str):
        """Load initial state from database for an account"""
        from veilon_core.repositories.db import execute_query
        
        log(f"[MONITOR] Initializing metrics for {metaapi_account_id}")
        
        # Get the current trading period start time
        period_rows = await asyncio.to_thread(
            execute_query,
            """
            SELECT start_time 
            FROM trading_periods 
            WHERE metaapi_account_id = %s
            AND end_time IS NULL
            ORDER BY start_time DESC 
            LIMIT 1
            """,
            (metaapi_account_id,),
            fetch_results=True
        )
        
        if not period_rows:
            log(f"[MONITOR] No active trading period for {metaapi_account_id}")
            raise ValueError(f"No active trading period for {metaapi_account_id}")
        
        period_start = period_rows[0]['start_time']
        
        # Get first equity value for this period
        first_equity_rows = await asyncio.to_thread(
            execute_query,
            """
            SELECT equity::float8 
            FROM equity_balance_ticks 
            WHERE metaapi_account_id = %s
            AND ts >= %s
            ORDER BY ts ASC 
            LIMIT 1
            """,
            (metaapi_account_id, period_start),
            fetch_results=True
        )
        
        if not first_equity_rows:
            log(f"[MONITOR] No equity data for period starting {period_start}, will wait for first tick")
            return
        
        first_equity = float(first_equity_rows[0]['equity'])
        
        async with self._lock:
            self._metrics[metaapi_account_id] = AccountMetrics(
                metaapi_account_id=metaapi_account_id,
                period_start=period_start,
                first_equity=first_equity,
                current_equity=first_equity,
                peak_equity=first_equity,
                trough_equity=first_equity,
                current_gain_pct=0.0,
                peak_gain_pct=0.0,
                trough_gain_pct=0.0,
                current_drawdown_pct=0.0,
                last_updated=datetime.now(timezone.utc)
            )
        
        log(f"[MONITOR] Initialized metrics for {metaapi_account_id} - first_equity={first_equity}")
    
    async def process_tick(self, metaapi_account_id: str, equity: float):
        """Update metrics on every new tick"""
        async with self._lock:
            # Initialize if not already done
            if metaapi_account_id not in self._metrics:
                from veilon_core.repositories.db import execute_query
                period_rows = await asyncio.to_thread(
                    execute_query,
                    """
                    SELECT start_time 
                    FROM trading_periods 
                    WHERE metaapi_account_id = %s
                    AND end_time IS NULL
                    ORDER BY start_time DESC 
                    LIMIT 1
                    """,
                    (metaapi_account_id,),
                    fetch_results=True
                )
                
                if not period_rows:
                    log(f"[MONITOR] Cannot process tick - no active trading period for {metaapi_account_id}")
                    return
                
                period_start = period_rows[0]['start_time']
                
                # Initialize with this first equity value
                self._metrics[metaapi_account_id] = AccountMetrics(
                    metaapi_account_id=metaapi_account_id,
                    period_start=period_start,
                    first_equity=equity,
                    current_equity=equity,
                    peak_equity=equity,
                    trough_equity=equity,
                    current_gain_pct=0.0,
                    peak_gain_pct=0.0,
                    trough_gain_pct=0.0,
                    current_drawdown_pct=0.0,
                    last_updated=datetime.now(timezone.utc)
                )
                log(f"[MONITOR] Initialized metrics on first tick for {metaapi_account_id} - first_equity={equity}")
            
            metrics = self._metrics[metaapi_account_id]
            
            # Update current equity
            metrics.current_equity = equity
            
            # Calculate gain from start
            if metrics.first_equity > 0:
                metrics.current_gain_pct = (equity - metrics.first_equity) / metrics.first_equity
            else:
                metrics.current_gain_pct = 0.0
            
            # Update peak equity and gain if we hit a new high
            if equity > metrics.peak_equity:
                metrics.peak_equity = equity
                metrics.peak_gain_pct = metrics.current_gain_pct
            
            # Update trough equity and gain if we hit a new low
            if equity < metrics.trough_equity:
                metrics.trough_equity = equity
                metrics.trough_gain_pct = metrics.current_gain_pct
            
            # FIXED: Simplified balance-based drawdown
            # Drawdown is only shown when gain is negative
            # If gain >= 0, drawdown = 0
            # If gain < 0, drawdown = abs(gain)
            if metrics.current_gain_pct < 0:
                metrics.current_drawdown_pct = abs(metrics.current_gain_pct)
            else:
                metrics.current_drawdown_pct = 0.0
            
            metrics.last_updated = datetime.now(timezone.utc)
            
            # Persist to database
            await self._persist_metrics(metrics)
    
    async def _persist_metrics(self, metrics: AccountMetrics):
        """Write metrics to a cached table for fast frontend queries"""
        from veilon_core.repositories.db import execute_query
        
        await asyncio.to_thread(
            execute_query,
            """
            INSERT INTO account_metrics_cache (
                metaapi_account_id,
                period_start,
                first_equity,
                current_equity,
                peak_equity,
                trough_equity,
                current_gain_pct,
                peak_gain_pct,
                trough_gain_pct,
                current_drawdown_pct,
                last_updated
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (metaapi_account_id) 
            DO UPDATE SET
                current_equity = EXCLUDED.current_equity,
                peak_equity = EXCLUDED.peak_equity,
                trough_equity = EXCLUDED.trough_equity,
                current_gain_pct = EXCLUDED.current_gain_pct,
                peak_gain_pct = EXCLUDED.peak_gain_pct,
                trough_gain_pct = EXCLUDED.trough_gain_pct,
                current_drawdown_pct = EXCLUDED.current_drawdown_pct,
                last_updated = EXCLUDED.last_updated
            """,
            (
                metrics.metaapi_account_id,
                metrics.period_start,
                metrics.first_equity,
                metrics.current_equity,
                metrics.peak_equity,
                metrics.trough_equity,
                metrics.current_gain_pct,
                metrics.peak_gain_pct,
                metrics.trough_gain_pct,
                metrics.current_drawdown_pct,
                metrics.last_updated
            )
        )

# ===========================
# OHLC AGGREGATOR
# ===========================

class OHLCAggregator:
    """Maintains 1-minute OHLC bars from tick data"""
    
    def __init__(self):
        self._current_bars: Dict[str, dict] = {}
        self._lock = asyncio.Lock()
        self._flush_task: Optional[asyncio.Task] = None
    
    def start_flush_task(self):
        """Start background task to flush completed bars"""
        if self._flush_task is None or self._flush_task.done():
            self._flush_task = asyncio.create_task(self._periodic_flush())
            log("[OHLC] Started periodic flush task")
    
    async def _periodic_flush(self):
        """Periodically check and flush completed bars"""
        while True:
            try:
                await asyncio.sleep(60)  # Check every minute
                await self._flush_completed_bars()
            except asyncio.CancelledError:
                break
            except Exception as e:
                log(f"[OHLC] Flush task error: {e}")
    
    async def _flush_completed_bars(self):
        """Flush all bars that are older than 1 minute"""
        now = datetime.now(timezone.utc)
        async with self._lock:
            keys_to_delete = []
            
            for key, bar in self._current_bars.items():
                bar_time = bar['bar_time']
                # If bar is more than 1 minute old, persist it
                if now - bar_time >= timedelta(minutes=1):
                    await self._persist_bar(bar)
                    keys_to_delete.append(key)
            
            # Remove persisted bars
            for key in keys_to_delete:
                del self._current_bars[key]
            
            if keys_to_delete:
                log(f"[OHLC] Flushed {len(keys_to_delete)} completed bars")
    
    async def process_tick(self, metaapi_account_id: str, equity: float, timestamp: datetime):
        """Update current bar with new tick"""
        # Round down to the minute
        bar_time = timestamp.replace(second=0, microsecond=0)
        
        async with self._lock:
            key = f"{metaapi_account_id}:{bar_time.isoformat()}"
            
            if key not in self._current_bars:
                # New bar
                self._current_bars[key] = {
                    'metaapi_account_id': metaapi_account_id,
                    'bar_time': bar_time,
                    'open': equity,
                    'high': equity,
                    'low': equity,
                    'close': equity
                }
            else:
                # Update existing bar
                bar = self._current_bars[key]
                bar['high'] = max(bar['high'], equity)
                bar['low'] = min(bar['low'], equity)
                bar['close'] = equity
            
            # Check if we should persist immediately (bar is complete)
            now = datetime.now(timezone.utc)
            if now - bar_time >= timedelta(minutes=1):
                await self._persist_bar(self._current_bars[key])
                del self._current_bars[key]
    
    async def _persist_bar(self, bar: dict):
        """Write completed bar to database and update account profit"""
        from veilon_core.repositories.db import execute_query
        
        metaapi_account_id = bar['metaapi_account_id']
        
        try:
            # Insert/update OHLC bar
            await asyncio.to_thread(
                execute_query,
                """
                INSERT INTO equity_ohlc_1min (metaapi_account_id, bar_time, open, high, low, close)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (metaapi_account_id, bar_time) DO UPDATE SET
                    high = GREATEST(equity_ohlc_1min.high, EXCLUDED.high),
                    low = LEAST(equity_ohlc_1min.low, EXCLUDED.low),
                    close = EXCLUDED.close
                """,
                (
                    metaapi_account_id,
                    bar['bar_time'],
                    bar['open'],
                    bar['high'],
                    bar['low'],
                    bar['close']
                )
            )
            
            # Update account's current_profit column
            await self._update_account_current_profit(metaapi_account_id)
            
        except Exception as e:
            log(f"[OHLC] Failed to persist bar for {metaapi_account_id} at {bar['bar_time']}: {e}")
    
    async def _update_account_current_profit(self, metaapi_account_id: str):
        """
        Update the current_profit column for an account based on latest OHLC data.
        Called after each OHLC bar is flushed.
        """
        from veilon_core.repositories.db import execute_query
        
        try:
            await asyncio.to_thread(
                execute_query,
                """
                WITH period_data AS (
                    SELECT 
                        tp.start_time,
                        (
                            SELECT open FROM equity_ohlc_1min 
                            WHERE metaapi_account_id = %s 
                            AND bar_time >= tp.start_time
                            ORDER BY bar_time ASC 
                            LIMIT 1
                        ) AS first_equity,
                        (
                            SELECT close FROM equity_ohlc_1min 
                            WHERE metaapi_account_id = %s 
                            ORDER BY bar_time DESC 
                            LIMIT 1
                        ) AS latest_equity
                    FROM trading_periods tp
                    WHERE tp.metaapi_account_id = %s
                    AND tp.status = 'active'
                    AND tp.end_time IS NULL
                    LIMIT 1
                )
                UPDATE accounts
                SET 
                    current_profit = COALESCE(
                        (SELECT latest_equity - first_equity FROM period_data),
                        0
                    ),
                    updated_at = NOW()
                WHERE metaapi_account_id = %s
                """,
                (metaapi_account_id, metaapi_account_id, metaapi_account_id, metaapi_account_id)
            )
        except Exception as e:
            log(f"[OHLC] Failed to update current_profit for {metaapi_account_id}: {e}")

# ===========================
# EQUITY BALANCE LISTENER
# ===========================

async def insert_equity_tick(metaapi_account_id: str, equity: float, balance: float):
    from veilon_core.repositories.db import execute_query
    sql = """
        INSERT INTO equity_balance_ticks (ts, metaapi_account_id, equity, balance)
        VALUES (%s, %s, %s, %s);
    """
    params = (datetime.now(timezone.utc), metaapi_account_id, equity, balance)
    await asyncio.to_thread(execute_query, sql, params)


class DBEquityBalanceListener(EquityBalanceListener):
    def __init__(
        self, 
        account_id: str, 
        monitor: Optional[EquityMonitor] = None,
        ohlc_aggregator: Optional[OHLCAggregator] = None
    ):
        super().__init__(account_id)
        self._account_id = account_id
        self.monitor = monitor
        self.ohlc_aggregator = ohlc_aggregator
        self.disconnected_event = asyncio.Event()
        self.connected_event = asyncio.Event()
        self.last_tick_at: Optional[datetime] = None

    async def on_equity_or_balance_updated(self, equity_balance_data):
        equity = equity_balance_data.get("equity")
        balance = equity_balance_data.get("balance")
        log(f"[STREAM {self._account_id}] equity={equity} balance={balance}")

        if equity is None or balance is None:
            return

        timestamp = datetime.now(timezone.utc)
        self.last_tick_at = timestamp

        try:
            # Insert tick to database
            await insert_equity_tick(self._account_id, float(equity), float(balance))
            
            # CRITICAL: Update metrics immediately after saving tick
            if self.monitor:
                await self.monitor.process_tick(self._account_id, float(equity))
            
            # Update OHLC
            if self.ohlc_aggregator:
                await self.ohlc_aggregator.process_tick(self._account_id, float(equity), timestamp)
                
        except Exception as e:
            log(f"[STREAM {self._account_id}] Processing failed: {e}")
            import traceback
            log(f"[STREAM {self._account_id}] Traceback: {traceback.format_exc()}")

    async def on_connected(self):
        log(f"[STREAM {self._account_id}] ✅ CONNECTED EVENT FIRED")
        self.connected_event.set()
        self.disconnected_event.clear()

    async def on_disconnected(self):
        log(f"[STREAM {self._account_id}] ❌ DISCONNECTED EVENT FIRED")
        self.disconnected_event.set()

    async def on_error(self, error: Exception):
        log(f"[STREAM {self._account_id}] ERROR: {error}")


# ===========================
# STREAM MANAGER
# ===========================

class EquityStreamManagerAsync:
    """
    This object MUST live on the background asyncio loop thread.
    """
    def __init__(self, token: str, domain: str):
        self.token = token
        self.domain = domain
        
        # Create initial SDK connection
        self._recreate_sdk()
        
        # Create equity monitor and OHLC aggregator
        self.monitor = EquityMonitor()
        self.ohlc_aggregator = OHLCAggregator()
        log("[MANAGER] EquityMonitor and OHLCAggregator created")
        
        # Start OHLC flush task
        self.ohlc_aggregator.start_flush_task()

        self._lock = asyncio.Lock()
        self._streams: Dict[str, Tuple[asyncio.Task, asyncio.Event, Optional[str]]] = {}

    def _recreate_sdk(self):
        """Recreate the SDK connection objects from scratch"""
        log("[MANAGER] Creating new SDK connection")
        self.rm = RiskManagement(self.token, {"domain": self.domain})
        self.api = self.rm.risk_management_api

    async def start_equity_stream(self, account_id: str) -> bool:
        async with self._lock:
            if account_id in self._streams and not self._streams[account_id][0].done():
                log(f"[MANAGER] Stream already running for {account_id}")
                return False

            log(f"[MANAGER] Starting stream for {account_id}")
            
            # Initialize metrics for this account
            try:
                await self.monitor.initialize_account(account_id)
            except Exception as e:
                log(f"[MANAGER] Metrics initialization error (will retry on first tick): {e}")
            
            stop_event = asyncio.Event()
            task = asyncio.create_task(self._run(account_id, stop_event))
            self._streams[account_id] = (task, stop_event, None)
            return True

    async def stop_equity_stream(self, account_id: str) -> bool:
        async with self._lock:
            if account_id not in self._streams:
                log(f"[MANAGER] No running stream for {account_id}")
                return False
            task, stop_event, _ = self._streams[account_id]
            log(f"[MANAGER] Stopping stream for {account_id}")
            stop_event.set()

        try:
            await asyncio.wait_for(task, timeout=10)
        except asyncio.TimeoutError:
            log(f"[MANAGER] Stop timeout -> cancelling task for {account_id}")
            task.cancel()
            try:
                await asyncio.wait_for(task, timeout=5)
            except Exception:
                pass
        finally:
            async with self._lock:
                self._streams.pop(account_id, None)
            log(f"[MANAGER] Stream stopped for {account_id}")

        return True

    async def _run(self, account_id: str, stop_event: asyncio.Event) -> None:
        listener = DBEquityBalanceListener(
            account_id, 
            monitor=self.monitor,
            ohlc_aggregator=self.ohlc_aggregator
        )
        listener_id: Optional[str] = None

        backoff = 2.0
        backoff_max = 60.0
        consecutive_failures = 0
        max_failures_before_sdk_reset = 3

        async def detach_listener():
            nonlocal listener_id
            if listener_id:
                log(f"[STREAM {account_id}] Removing listener id={listener_id}")
                try:
                    self.api.remove_equity_balance_listener(listener_id)
                except Exception as e:
                    log(f"[STREAM {account_id}] Remove failed: {e}")
                finally:
                    listener_id = None

        try:
            while not stop_event.is_set():
                try:
                    # Check if we need to recreate SDK connection
                    if consecutive_failures >= max_failures_before_sdk_reset:
                        log(f"[STREAM {account_id}] {consecutive_failures} consecutive failures - recreating SDK connection")
                        await detach_listener()
                        
                        self._recreate_sdk()
                        
                        consecutive_failures = 0
                        backoff = 2.0
                        
                        # Give the new SDK connection time to initialize
                        await asyncio.sleep(2)

                    await detach_listener()

                    log(f"[STREAM {account_id}] 🔄 Attaching listener (backoff={backoff:.1f}s, failures={consecutive_failures})")
                    listener.connected_event.clear()
                    listener.disconnected_event.clear()
                    listener.last_tick_at = None
                    log(f"[STREAM {account_id}] 📝 last_tick_at reset to None")

                    listener_id = await asyncio.wait_for(
                        self.api.add_equity_balance_listener(listener, account_id),
                        timeout=20
                    )

                    log(f"[STREAM {account_id}] ✓ Listener attached id={listener_id}")

                    async with self._lock:
                        if account_id in self._streams:
                            task, ev, _ = self._streams[account_id]
                            self._streams[account_id] = (task, ev, listener_id)

                    # Success! Reset failure counter and backoff
                    consecutive_failures = 0
                    backoff = 2.0
                    
                    # Wait for initial connection before entering watchdog loop
                    log(f"[STREAM {account_id}] ⏳ Waiting for connection event (30s timeout)...")
                    connection_established = False
                    try:
                        await asyncio.wait_for(listener.connected_event.wait(), timeout=30)
                        log(f"[STREAM {account_id}] ✅ Connection event received!")
                        connection_established = True
                    except asyncio.TimeoutError:
                        log(f"[STREAM {account_id}] ⏱️ Connection timeout after 30s")
                    
                    # Always set timestamp to give grace period
                    listener.last_tick_at = datetime.now(timezone.utc)
                    log(f"[STREAM {account_id}] 📝 last_tick_at set to NOW: {listener.last_tick_at.isoformat()}")
                    
                    if not connection_established:
                        # If we didn't connect, increment failures and retry
                        consecutive_failures += 1
                        log(f"[STREAM {account_id}] ❌ Connection not established - will retry")
                        await asyncio.sleep(2)
                        continue

                    # Enter watchdog loop
                    log(f"[STREAM {account_id}] 👀 Entering watchdog loop (120s timeout)")
                    while not stop_event.is_set():
                        watchdog_seconds = 120
                        now = datetime.now(timezone.utc)
                        last = listener.last_tick_at
                        
                        # Check if stream has gone stale
                        if last is not None:
                            seconds_since_last_tick = (now - last).total_seconds()
                            if seconds_since_last_tick > watchdog_seconds:
                                log(f"[STREAM {account_id}] ⏰ No ticks for {watchdog_seconds}s (last: {seconds_since_last_tick:.0f}s ago) -> reattach")
                                consecutive_failures += 1
                                break

                        if listener.disconnected_event.is_set():
                            log(f"[STREAM {account_id}] ❌ Disconnect event detected -> reattaching")
                            consecutive_failures += 1
                            break

                        await asyncio.sleep(1.0)

                except asyncio.TimeoutError:
                    consecutive_failures += 1
                    log(f"[STREAM {account_id}] Attach timed out (failure {consecutive_failures})")
                    sleep_for = min(backoff, backoff_max) * (0.8 + 0.4 * random.random())
                    await asyncio.sleep(sleep_for)
                    backoff = min(backoff * 2.0, backoff_max)

                except asyncio.CancelledError:
                    raise

                except Exception as e:
                    consecutive_failures += 1
                    log(f"[STREAM {account_id}] Runner error (failure {consecutive_failures}): {e}")
                    sleep_for = min(backoff, backoff_max) * (0.8 + 0.4 * random.random())
                    await asyncio.sleep(sleep_for)
                    backoff = min(backoff * 2.0, backoff_max)

        finally:
            await detach_listener()


# ===========================
# SYNC SERVICE FACADE
# ===========================

class EquityStreamService:
    """
    Sync facade for Streamlit.
    Owns the background event loop and the async manager.
    """
    def __init__(self, token: str, domain: str):
        self.token = token
        self.domain = domain

        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._manager: Optional[EquityStreamManagerAsync] = None

        self._ready = threading.Event()

        self._start_thread()

    def _start_thread(self):
        def runner():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            self._loop = loop

            def init_manager():
                try:
                    self._manager = EquityStreamManagerAsync(self.token, self.domain)
                    log("[SERVICE] Async manager created inside running loop")
                finally:
                    self._ready.set()

            loop.call_soon(init_manager)
            loop.run_forever()

        self._thread = threading.Thread(target=runner, name="equity-stream-loop", daemon=True)
        self._thread.start()
        if not self._ready.wait(timeout=10):
            raise RuntimeError("Equity stream loop did not initialize in time")

    def start_stream(self, account_id: str) -> bool:
        assert self._loop and self._manager
        fut = asyncio.run_coroutine_threadsafe(self._manager.start_equity_stream(account_id), self._loop)
        return fut.result(timeout=10)

    def stop_stream(self, account_id: str) -> bool:
        assert self._loop and self._manager
        fut = asyncio.run_coroutine_threadsafe(self._manager.stop_equity_stream(account_id), self._loop)
        return fut.result(timeout=20)


# ===========================
# PROCESS SINGLETON
# ===========================

_service: Optional[EquityStreamService] = None

def get_service() -> EquityStreamService:
    global _service
    if _service is None:
        _service = EquityStreamService(TOKEN, DOMAIN)
        log("[SERVICE] EquityStreamService created (loop thread started)")
    return _service