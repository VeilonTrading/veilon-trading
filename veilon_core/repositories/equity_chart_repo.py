from typing import Any, Dict, Iterable, Optional
from veilon_core.repositories.db import execute_query


def get_equity_series(
    metaapi_account_id: str,
    start_time: str | None = None,  # 'YYYY-MM-DD HH:MI:SS' or date
    end_time: str | None = None,
) -> list[dict]:
    """
    Returns ordered equity time series for a MetaApi account.

    Output rows:
      - broker_time
      - average_equity
      - last_equity
      - average_balance
      - last_balance
    """
    sql = """
        SELECT
            broker_time,
            average_equity,
            last_equity,
            average_balance,
            last_balance
        FROM equity_chart_points
        WHERE metaapi_account_id = %s
    """

    params: list[Any] = [metaapi_account_id]

    if start_time:
        sql += " AND broker_time >= %s::timestamptz"
        params.append(start_time)

    if end_time:
        sql += " AND broker_time <= %s::timestamptz"
        params.append(end_time)

    sql += " ORDER BY broker_time ASC"

    rows = execute_query(sql, tuple(params), fetch_results=True)
    return rows or []


def _extract_time_field(obj: Any) -> Optional[str]:
    if obj is None:
        return None
    if isinstance(obj, str):
        return obj
    if isinstance(obj, dict):
        return obj.get("brokerTime") or obj.get("time")
    return None


def _normalise_equity_record(
    metaapi_account_id: str,
    rec: Dict[str, Any],
) -> Optional[Dict[str, Any]]:

    start_broker_time = rec.get("startBrokerTime") or _extract_time_field(rec.get("startTime"))
    end_broker_time   = rec.get("endBrokerTime") or _extract_time_field(rec.get("endTime"))

    broker_time = rec.get("brokerTime") or _extract_time_field(rec.get("time"))
    broker_time = broker_time or start_broker_time or end_broker_time
    if broker_time is None:
        return None

    return {
        "metaapi_account_id": metaapi_account_id,
        "broker_time":  broker_time,
        "start_broker_time": start_broker_time,
        "end_broker_time": end_broker_time,

        "average_balance": rec.get("averageBalance"),
        "average_equity":  rec.get("averageEquity"),
        "balance_sum":     rec.get("balanceSum"),
        "equity_sum":      rec.get("equitySum"),

        "last_balance": rec.get("lastBalance"),
        "last_equity":  rec.get("lastEquity"),
        "max_balance":  rec.get("maxBalance"),
        "max_equity":   rec.get("maxEquity"),
        "min_balance":  rec.get("minBalance"),
        "min_equity":   rec.get("minEquity"),

        "start_balance": rec.get("startBalance"),
        "start_equity":  rec.get("startEquity"),

        "duration":        rec.get("duration"),
        "currency_digits": rec.get("currencyDigits"),

        "min_equity_time":   _extract_time_field(rec.get("minEquityTime")),
        "max_equity_time":   _extract_time_field(rec.get("maxEquityTime")),
        "min_balance_time":  _extract_time_field(rec.get("minBalanceTime")),
        "max_balance_time":  _extract_time_field(rec.get("maxBalanceTime")),
    }


def save_equity_chart_records(
    metaapi_account_id: str,
    records: Iterable[Dict[str, Any]],
) -> None:
    sql = """
    INSERT INTO equity_chart_points (
        metaapi_account_id,
        broker_time,
        start_broker_time,
        end_broker_time,
        average_balance,
        average_equity,
        balance_sum,
        equity_sum,
        last_balance,
        last_equity,
        max_balance,
        max_equity,
        min_balance,
        min_equity,
        start_balance,
        start_equity,
        duration,
        currency_digits,
        min_equity_time,
        max_equity_time,
        min_balance_time,
        max_balance_time
    )
    VALUES (
        %s, %s, %s, %s,
        %s, %s, %s, %s,
        %s, %s, %s, %s,
        %s, %s, %s, %s,
        %s, %s,
        %s, %s, %s, %s
    )
    ON CONFLICT (metaapi_account_id, broker_time) DO UPDATE SET
        start_broker_time = EXCLUDED.start_broker_time,
        end_broker_time   = EXCLUDED.end_broker_time,
        average_balance   = EXCLUDED.average_balance,
        average_equity    = EXCLUDED.average_equity,
        balance_sum       = EXCLUDED.balance_sum,
        equity_sum        = EXCLUDED.equity_sum,
        last_balance      = EXCLUDED.last_balance,
        last_equity       = EXCLUDED.last_equity,
        max_balance       = EXCLUDED.max_balance,
        max_equity        = EXCLUDED.max_equity,
        min_balance       = EXCLUDED.min_balance,
        min_equity        = EXCLUDED.min_equity,
        start_balance     = EXCLUDED.start_balance,
        start_equity      = EXCLUDED.start_equity,
        duration          = EXCLUDED.duration,
        currency_digits   = EXCLUDED.currency_digits,
        min_equity_time   = EXCLUDED.min_equity_time,
        max_equity_time   = EXCLUDED.max_equity_time,
        min_balance_time  = EXCLUDED.min_balance_time,
        max_balance_time  = EXCLUDED.max_balance_time;
    """

    for rec in records:
        norm = _normalise_equity_record(metaapi_account_id, rec)
        if norm is None:
            continue

        params = tuple(norm.values())
        execute_query(sql, params, False)