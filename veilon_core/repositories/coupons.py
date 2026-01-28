from veilon_core.repositories.db import execute_query

def get_active_coupon_by_code(code: str) -> dict | None:
    if not code:
        return None

    rows = execute_query(
        """
        SELECT
            id,
            code,
            description,
            discount_type,
            discount_value,
            max_uses,
            max_uses_per_user,
            min_order_amount,
            valid_from,
            valid_until,
            is_active,
            stripe_coupon_id
        FROM coupons
        WHERE LOWER(TRIM(code)) = LOWER(TRIM(%s))
          AND is_active = TRUE
          AND (valid_from IS NULL OR valid_from <= NOW())
          AND (valid_until IS NULL OR valid_until >= NOW());
        """,
        (code.strip(),),
    )
    return rows[0] if rows else None
