from veilon_core.repositories.db import execute_query

def get_trades_by_account_id(account_id) -> list[dict]:
    if account_id is None:
        return []

    return execute_query(
        """
        SELECT *
        FROM trades
        WHERE account_id = %s
        ORDER BY open_time ASC;
        """,
        (str(account_id),),  # 👈 explicit cast
    ) or []