import psycopg2
from psycopg2.extras import RealDictCursor

host = "analytiq-test-database.c102eee68lij.eu-west-2.rds.amazonaws.com"
port = 5432
dbname = "postgres"
user = "blackboxresearch"
password = "!Audacious2011"


def execute_query(query, params=None, fetch_results=True):
    """
    Executes a SQL query and optionally fetches results.

    - For SELECT queries (or any that return rows), with fetch_results=True:
      -> returns a list of rows (possibly empty).
    - For INSERT/UPDATE/DELETE or when fetch_results=False:
      -> executes and returns [] / None without trying to fetch.
    """
    try:
        with psycopg2.connect(
            host=host,
            port=port,
            database=dbname,
            user=user,
            password=password,
        ) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params)

                # If we don't want results, or the statement doesn't return rows,
                # just let the context manager commit and exit.
                if not fetch_results or cursor.description is None:
                    return [] if fetch_results else None

                rows = cursor.fetchall()
                return rows if rows is not None else []

    except psycopg2.Error as e:
        print(f"Database error: {e}")
        # Better to see real errors now than silently swallow them
        return [] if fetch_results else None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return [] if fetch_results else None