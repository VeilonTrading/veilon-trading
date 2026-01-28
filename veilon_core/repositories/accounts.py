from __future__ import annotations
from typing import Any, Optional, Sequence
from veilon_core.repositories.db import execute_query
from psycopg2.extras import Json
import streamlit as st
import pandas as pd

def derive_status(row) -> str:
    """
    Canonical account status resolver.
    Precedence: Closed > In Review > Disabled > Funded > Phase
    """

    # 1. Closed overrides everything
    if pd.notna(row.get("closed_at")):
        return "Closed"

    # 2. In-review overrides enabled/disabled
    if row.get("in_review") is True:
        return "In Review"

    # 3. Disabled (only if not closed / in-review)
    if not row.get("is_enabled", True):
        return "Disabled"

    # 4. Funded
    if row.get("is_funded") is True or pd.notna(row.get("funded_at")):
        return "Funded"

    # 5. Phase fallback
    phase = row.get("phase")
    return f"Phase {int(phase)}" if pd.notna(phase) else "Phase 1"


def accounts_table(
    user_id: Optional[int] = None,
    status: Optional[str] = None,      # "Phase 1" | "Funded" | "In Review" | "Closed" | "Disabled"
    plan_id: Optional[int] = None,
):
    accounts_rows = execute_query(
        """
        SELECT *
        FROM accounts
        WHERE (%s IS NULL OR user_id = %s)
          AND (%s IS NULL OR plan_id = %s)
        ;
        """,
        (user_id, user_id, plan_id, plan_id),
    )

    accounts_df = pd.DataFrame(accounts_rows)

    if accounts_df.empty:
        st.info("No accounts found.")
        # Keep selection state consistent
        if st.session_state.get("has_accounts_selection") or st.session_state.get("selected_account_ids"):
            st.session_state["has_accounts_selection"] = False
            st.session_state["selected_account_ids"] = []
        return

    accounts_df["status"] = accounts_df.apply(derive_status, axis=1)

    if status is not None:
        accounts_df = accounts_df[accounts_df["status"] == status]

        if accounts_df.empty:
            st.info("No accounts match the selected status.")
            st.session_state["has_accounts_selection"] = False
            st.session_state["selected_account_ids"] = []
            return



    DISPLAY_COLUMNS = [
        "id",
        "user_id",
        "order_id",
        "plan_id",
        "balance",
        "status",
        "created_at",
        "funded_at",
        "closed_at",
        "notes",
    ]

    COLUMN_LABELS = {
        "id": "Account ID",
        "user_id": "User ID",
        "order_id": "Order ID",
        "plan_id": "Plan ID",
        "balance": "Balance",
        "status": "Status",
        "created_at": "Opened At",
        "funded_at": "Funded At",
        "closed_at": "Closed At",
        "notes": "Notes",
    }

    df = accounts_df.loc[:, DISPLAY_COLUMNS].copy()
    df = df.rename(columns=COLUMN_LABELS)

    table = st.dataframe(
        df,
        key="accounts_df",
        on_select="rerun",
        selection_mode=["single-row"],
        hide_index=True,
        column_config={
            "Balance": st.column_config.NumberColumn("Balance", format="dollar"),
            "Status": st.column_config.MultiselectColumn(
                "Status",
                options=["Phase 1", "Funded", "In Review", "Closed", "Disabled"],
                color=["#D6EAF8", "#D5F5E3", "#FDEBD0", "#F5B7B1", "#E5E7E9"],
            ),
            "Opened At": st.column_config.DatetimeColumn("Opened At", format="DD/MM/YY hh:mm:ss"),
            "Closed At": st.column_config.DatetimeColumn("Closed At", format="DD/MM/YY hh:mm:ss"),
            "Funded At": st.column_config.DatetimeColumn("Funded At", format="DD/MM/YY hh:mm:ss"),
        },
    )

    selected_rows = table.selection.get("rows", [])
    current_is_selected = bool(selected_rows)

    selected_ids = df.iloc[selected_rows]["Account ID"].tolist() if selected_rows else []

    # ---- Sync + force rerun once on change ----
    state_changed = (
        current_is_selected != st.session_state.get("has_accounts_selection", False)
        or selected_ids != st.session_state.get("selected_account_ids", [])
    )

    if state_changed:
        st.session_state["has_accounts_selection"] = current_is_selected
        st.session_state["selected_account_ids"] = selected_ids
        st.rerun()


def _one(rows: Sequence[dict], err: str) -> dict:
    if not rows:
        raise ValueError(err)
    return rows[0]


def account_get(account_id: int) -> dict:
    rows = execute_query(
        """
        SELECT *
        FROM accounts
        WHERE id = %s;
        """,
        (account_id,),
    )
    return _one(rows, f"Account {account_id} not found.")


def account_event_log(
    account_id: int,
    *,
    event_type: str,
    actor_type: str = "system",
    actor_id: Optional[int] = None,
    event_status: Optional[str] = None,
    payload: Optional[dict[str, Any]] = None,
) -> dict:
    rows = execute_query(
        """
        INSERT INTO account_events (account_id, event_type, event_status, actor_type, actor_id, payload)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING id, account_id, event_type, occurred_at;
        """,
        (account_id, event_type, event_status, actor_type, actor_id, Json(payload or {})),
    )
    return _one(rows, "Failed to write account event.")


def account_create(
    user_id: int,
    plan_id: int,
    *,
    is_enabled: bool = True,
    actor_type: str = "system",
    actor_id: Optional[int] = None,
) -> dict:
    rows = execute_query(
        """
        INSERT INTO accounts (user_id, plan_id, is_enabled, balance, phase)
        SELECT %s, p.id, %s, p.account_size, 1
        FROM plans p
        WHERE p.id = %s
        RETURNING id, user_id, plan_id, is_enabled, balance, phase;
        """,
        (user_id, is_enabled, plan_id),
    )
    account = _one(rows, f"Plan {plan_id} not found. Account was not created.")

    account_event_log(
        account["id"],
        event_type="account.created",
        actor_type=actor_type,
        actor_id=actor_id,
        payload={
            "user_id": user_id,
            "plan_id": plan_id,
            "is_enabled": is_enabled,
            "initial_balance": str(account["balance"]),
            "initial_phase": account["phase"],
        },
    )

    return account


def account_toggle_active(account_id: int) -> dict:
    """
    Toggle an account's is_enabled flag atomically in SQL.
    """
    rows = execute_query(
        """
        UPDATE accounts
        SET is_enabled = NOT COALESCE(is_enabled, FALSE)
        WHERE id = %s
        RETURNING id, is_enabled;
        """,
        (account_id,),
    )
    account = _one(rows, f"Account {account_id} not found.")

    account_event_log(
        account_id,
        event_type="account.is_enabled.toggled",
        actor_type="system",
        actor_id=None,
        payload={"is_enabled": account["is_enabled"]},
    )

    return account


def account_set_note(account_id: int, note: str, admin_user_id: int) -> dict:
    rows = execute_query(
        """
        UPDATE accounts
        SET notes = %s,
            notes_updated_at = NOW(),
            notes_updated_by_user_id = %s
        WHERE id = %s
        RETURNING id, notes, notes_updated_at, notes_updated_by_user_id;
        """,
        (note, admin_user_id, account_id),
    )
    account = _one(rows, f"Account {account_id} not found.")

    account_event_log(
        account_id,
        event_type="account.note.set",
        actor_type="admin",
        actor_id=admin_user_id,
        payload={"note": note},
    )

    return account


def account_set_balance(account_id: int, new_balance: float) -> dict:
    """
    Hard set the balance.
    """
    rows = execute_query(
        """
        UPDATE accounts
        SET balance = %s
        WHERE id = %s
        RETURNING id, balance;
        """,
        (new_balance, account_id),
    )
    account = _one(rows, f"Account {account_id} not found.")

    account_event_log(
        account_id,
        event_type="account.balance.set",
        actor_type="system",
        actor_id=None,
        payload={"new_balance": new_balance},
    )

    return account


def account_adjust_balance(account_id: int, delta: float) -> dict:
    """
    Adjust balance by a signed delta:
    +100 = deposit, -200 = withdrawal.
    """
    rows = execute_query(
        """
        UPDATE accounts
        SET balance = COALESCE(balance, 0) + %s
        WHERE id = %s
        RETURNING id, balance;
        """,
        (delta, account_id),
    )
    account = _one(rows, f"Account {account_id} not found.")

    account_event_log(
        account_id,
        event_type="account.balance.adjusted",
        actor_type="system",
        actor_id=None,
        payload={"delta": delta, "new_balance": float(account["balance"])},
    )

    return account


def account_change_phase(account_id: int, new_phase: int) -> dict:
    rows = execute_query(
        """
        UPDATE accounts
        SET phase = %s
        WHERE id = %s
        RETURNING id, phase;
        """,
        (new_phase, account_id),
    )
    account = _one(rows, f"Account {account_id} not found.")

    account_event_log(
        account_id,
        event_type="account.phase.changed",
        actor_type="system",
        actor_id=None,
        payload={"new_phase": new_phase},
    )

    return account


def account_close(account_id: int, *, close_reason: Optional[str] = None) -> dict:
    rows = execute_query(
        """
        UPDATE accounts
        SET closed_at = NOW()
        WHERE id = %s
        RETURNING id, closed_at;
        """,
        (account_id,),
    )
    account = _one(rows, f"Account {account_id} not found.")

    account_event_log(
        account_id,
        event_type="account.closed",
        actor_type="system",
        actor_id=None,
        payload={"close_reason": close_reason},
    )

    return account


def account_reopen(account_id: int) -> dict:
    rows = execute_query(
        """
        UPDATE accounts
        SET closed_at = NULL
        WHERE id = %s
        RETURNING id, closed_at;
        """,
        (account_id,),
    )
    account = _one(rows, f"Account {account_id} not found.")

    account_event_log(
        account_id,
        event_type="account.reopened",
        actor_type="system",
        actor_id=None,
        payload={},
    )

    return account


def account_set_in_review(
    account_id: int,
    in_review: bool,
    *,
    resolution: Optional[str] = None,  # "approved" | "rejected" | None
    reason: Optional[str] = None,
    actor_type: str = "admin",
    actor_id: Optional[int] = None,
) -> dict:
    rows = execute_query(
        """
        UPDATE accounts
        SET in_review = %s
        WHERE id = %s
        RETURNING id, in_review;
        """,
        (in_review, account_id),
    )
    account = _one(rows, f"Account {account_id} not found.")

    account_event_log(
        account_id,
        event_type="account.review.updated",
        actor_type=actor_type,
        actor_id=actor_id,
        payload={
            "in_review": in_review,
            "resolution": resolution,
            "reason": reason,
        },
    )

    return account

def get_accounts_for_user(user_id: int) -> list[dict]:
    return execute_query(
        """
        SELECT
            a.id,
            a.user_id,
            a.plan_id,
            a.order_id,
            a.balance,
            a.created_at,
            a.closed_at,
            a.metaapi_account_id,
            a.passed_at,
            a.funded_at,
            a.is_enabled,
            a.in_review,
            p.name AS plan_name
        FROM accounts a
        LEFT JOIN plans p ON a.plan_id = p.id
        WHERE a.user_id = %s
        ORDER BY a.created_at DESC;
        """,
        (user_id,),
        fetch_results=True,
    ) or []
