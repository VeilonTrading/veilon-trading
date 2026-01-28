from veilon_core.repositories.db import execute_query

def get_plan_by_account_size(account_size: int) -> dict | None:
    rows = execute_query(
        """
        SELECT
            id,
            name,
            code,
            account_size,
            price,
            stripe_link
        FROM plans
        WHERE account_size = %s
          AND is_active = TRUE
        LIMIT 1;
        """,
        (account_size,),
    )
    return rows[0] if rows else None
