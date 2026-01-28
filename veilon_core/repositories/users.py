from veilon_core.repositories.db import execute_query

def get_user_by_email(email: str) -> dict | None:
    rows = execute_query(
        """
        SELECT *
        FROM users 
        WHERE LOWER(TRIM(email)) = LOWER(TRIM(%s));
        """,
        (email.strip().lower(),),
    )
    return rows[0] if rows else None


def get_or_create_user_from_oidc(
    email: str,
    given_name: str | None = None,
    family_name: str | None = None,
) -> dict:
    """
    Used for Google / OIDC logins.

    - If a user with this email already exists, return that row.
      (Covers both legacy password registrations and prior OIDC logins.)
    - If not, insert a new user with first_name, last_name, email.
      country/password/marketing are left NULL / default.
    """
    email_norm = email.strip().lower()

    # 1) Try existing user
    existing = get_user_by_email(email_norm)
    if existing is not None:
        return existing

    # 2) Insert minimal new user record
    rows = execute_query(
        """
        INSERT INTO users (
            first_name,
            last_name,
            email,
            country,
            password_hash,
            password_hint,
            marketing
        )
        VALUES (
            %s,        -- first_name
            %s,        -- last_name
            %s,        -- email
            NULL,      -- country
            NULL,      -- password_hash (no password for OIDC)
            NULL,      -- password_hint
            NULL       -- marketing (or FALSE if you prefer)
        )
        RETURNING
            id,
            first_name,
            last_name,
            email,
            country,
            password_hash,
            password_hint,
            marketing;
        """,
        (given_name, family_name, email_norm),
    )
    return rows[0]
