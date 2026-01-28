# veilon_core/services/equity_monitor.py
import asyncio
from datetime import datetime, timezone
from typing import Optional, Dict
from dataclasses import dataclass

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
    """
    
    def __init__(self):
        self._metrics: Dict[str, AccountMetrics] = {}
        self._lock = asyncio.Lock()
    
    async def initialize_account(self, metaapi_account_id: str):
        """Load initial state from database for an account"""
        from veilon_core.repositories.db import execute_query
        
        # Get the current trading period start time
        period_row = execute_query(
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
        
        if not period_row:
            raise ValueError(f"No active trading period for {metaapi_account_id}")
        
        period_start = period_row[0]['start_time']
        
        # Get first equity value for this period
        first_equity_row = execute_query(
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
        
        if not first_equity_row:
            raise ValueError(f"No equity data for period starting {period_start}")
        
        first_equity = float(first_equity_row[0]['equity'])
        
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
    
    async def process_tick(self, metaapi_account_id: str, equity: float):
        """Update metrics on every new tick"""
        async with self._lock:
            if metaapi_account_id not in self._metrics:
                # Initialize if not already done
                await self.initialize_account(metaapi_account_id)
            
            metrics = self._metrics[metaapi_account_id]
            
            # Update current equity
            metrics.current_equity = equity
            
            # Calculate gain from start
            metrics.current_gain_pct = (equity - metrics.first_equity) / metrics.first_equity
            
            # Update peak equity and gain if we hit a new high
            if equity > metrics.peak_equity:
                metrics.peak_equity = equity
                metrics.peak_gain_pct = metrics.current_gain_pct
            
            # Update trough equity and gain if we hit a new low
            if equity < metrics.trough_equity:
                metrics.trough_equity = equity
                metrics.trough_gain_pct = metrics.current_gain_pct
            
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
    
    async def get_metrics(self, metaapi_account_id: str) -> Optional[AccountMetrics]:
        """Get current metrics for an account"""
        async with self._lock:
            return self._metrics.get(metaapi_account_id)
    
    async def remove_account(self, metaapi_account_id: str):
        """Remove account from monitoring (e.g., when stream stops)"""
        async with self._lock:
            if metaapi_account_id in self._metrics:
                del self._metrics[metaapi_account_id]


# Singleton instance
_monitor_instance: Optional[EquityMonitor] = None

def get_equity_monitor() -> EquityMonitor:
    """Get or create the singleton equity monitor instance"""
    global _monitor_instance
    if _monitor_instance is None:
        _monitor_instance = EquityMonitor()
    return _monitor_instance