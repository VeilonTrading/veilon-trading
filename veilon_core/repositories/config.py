import os
from pathlib import Path

from dotenv import load_dotenv

# -------------------------------------------------------------------
# Locate and load .env at project root: veilon/.env
# -------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent  # .../veilon
ENV_PATH = BASE_DIR / ".env"

load_dotenv(ENV_PATH)


# -------------------------------------------------------------------
# DATABASE CONFIG (required)
# -------------------------------------------------------------------
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

_required_db = {
    "DB_HOST": DB_HOST,
    "DB_PORT": DB_PORT,
    "DB_NAME": DB_NAME,
    "DB_USER": DB_USER,
    "DB_PASSWORD": DB_PASSWORD,
}

_missing_db = [k for k, v in _required_db.items() if not v]
if _missing_db:
    raise RuntimeError(
        f"Missing required DB environment variables: {', '.join(_missing_db)}. "
        "Check your .env file."
    )


# -------------------------------------------------------------------
# AUTH / GOOGLE OAUTH CONFIG (optional here)
# -------------------------------------------------------------------
AUTH_REDIRECT_URI = os.getenv("AUTH_REDIRECT_URI")
AUTH_COOKIE_SECRET = os.getenv("AUTH_COOKIE_SECRET")
AUTH_CLIENT_ID = os.getenv("AUTH_CLIENT_ID")
AUTH_CLIENT_SECRET = os.getenv("AUTH_CLIENT_SECRET")
AUTH_SERVER_METADATA_URL = os.getenv("AUTH_SERVER_METADATA_URL")

AUTH_CONFIG = {
    "redirect_uri": AUTH_REDIRECT_URI,
    "cookie_secret": AUTH_COOKIE_SECRET,
    "client_id": AUTH_CLIENT_ID,
    "client_secret": AUTH_CLIENT_SECRET,
    "server_metadata_url": AUTH_SERVER_METADATA_URL,
}

# NOTE: no hard failure here. Your auth layer should validate AUTH_CONFIG
# and raise with a clean error if anything critical is missing.


# -------------------------------------------------------------------
# OPTIONAL: MetaAPI Token
# -------------------------------------------------------------------
METAAPI_TOKEN = 'eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9.eyJfaWQiOiIxNzM2NDVlM2U5MThkZjE2NjQzZmFjZDI0NTBlMGVmMiIsImFjY2Vzc1J1bGVzIjpbeyJpZCI6InRyYWRpbmctYWNjb3VudC1tYW5hZ2VtZW50LWFwaSIsIm1ldGhvZHMiOlsidHJhZGluZy1hY2NvdW50LW1hbmFnZW1lbnQtYXBpOnJlc3Q6cHVibGljOio6KiJdLCJyb2xlcyI6WyJyZWFkZXIiLCJ3cml0ZXIiXSwicmVzb3VyY2VzIjpbIio6JFVTRVJfSUQkOioiXX0seyJpZCI6Im1ldGFhcGktcmVzdC1hcGkiLCJtZXRob2RzIjpbIm1ldGFhcGktYXBpOnJlc3Q6cHVibGljOio6KiJdLCJyb2xlcyI6WyJyZWFkZXIiLCJ3cml0ZXIiXSwicmVzb3VyY2VzIjpbIio6JFVTRVJfSUQkOioiXX0seyJpZCI6Im1ldGFhcGktcnBjLWFwaSIsIm1ldGhvZHMiOlsibWV0YWFwaS1hcGk6d3M6cHVibGljOio6KiJdLCJyb2xlcyI6WyJyZWFkZXIiLCJ3cml0ZXIiXSwicmVzb3VyY2VzIjpbIio6JFVTRVJfSUQkOioiXX0seyJpZCI6Im1ldGFhcGktcmVhbC10aW1lLXN0cmVhbWluZy1hcGkiLCJtZXRob2RzIjpbIm1ldGFhcGktYXBpOndzOnB1YmxpYzoqOioiXSwicm9sZXMiOlsicmVhZGVyIiwid3JpdGVyIl0sInJlc291cmNlcyI6WyIqOiRVU0VSX0lEJDoqIl19LHsiaWQiOiJtZXRhc3RhdHMtYXBpIiwibWV0aG9kcyI6WyJtZXRhc3RhdHMtYXBpOnJlc3Q6cHVibGljOio6KiJdLCJyb2xlcyI6WyJyZWFkZXIiLCJ3cml0ZXIiXSwicmVzb3VyY2VzIjpbIio6JFVTRVJfSUQkOioiXX0seyJpZCI6InJpc2stbWFuYWdlbWVudC1hcGkiLCJtZXRob2RzIjpbInJpc2stbWFuYWdlbWVudC1hcGk6cmVzdDpwdWJsaWM6KjoqIl0sInJvbGVzIjpbInJlYWRlciIsIndyaXRlciJdLCJyZXNvdXJjZXMiOlsiKjokVVNFUl9JRCQ6KiJdfSx7ImlkIjoiY29weWZhY3RvcnktYXBpIiwibWV0aG9kcyI6WyJjb3B5ZmFjdG9yeS1hcGk6cmVzdDpwdWJsaWM6KjoqIl0sInJvbGVzIjpbInJlYWRlciIsIndyaXRlciJdLCJyZXNvdXJjZXMiOlsiKjokVVNFUl9JRCQ6KiJdfSx7ImlkIjoibXQtbWFuYWdlci1hcGkiLCJtZXRob2RzIjpbIm10LW1hbmFnZXItYXBpOnJlc3Q6ZGVhbGluZzoqOioiLCJtdC1tYW5hZ2VyLWFwaTpyZXN0OnB1YmxpYzoqOioiXSwicm9sZXMiOlsicmVhZGVyIiwid3JpdGVyIl0sInJlc291cmNlcyI6WyIqOiRVU0VSX0lEJDoqIl19LHsiaWQiOiJiaWxsaW5nLWFwaSIsIm1ldGhvZHMiOlsiYmlsbGluZy1hcGk6cmVzdDpwdWJsaWM6KjoqIl0sInJvbGVzIjpbInJlYWRlciJdLCJyZXNvdXJjZXMiOlsiKjokVVNFUl9JRCQ6KiJdfV0sImlnbm9yZVJhdGVMaW1pdHMiOmZhbHNlLCJ0b2tlbklkIjoiMjAyMTAyMTMiLCJpbXBlcnNvbmF0ZWQiOmZhbHNlLCJyZWFsVXNlcklkIjoiMTczNjQ1ZTNlOTE4ZGYxNjY0M2ZhY2QyNDUwZTBlZjIiLCJpYXQiOjE3NjY4ODQwMzh9.TxlTccBIbkoaxc39Ud1QbAuIeyi9YMcKxQaAMolJCjjptHRoMWy1DMzDhYo5wd5RtNbv3jrG2Y81Qx2mDC5C0UIjHIwRMADZuLtWtiFSPTPdfeqQBhk2tR12tzDrr8TZSMNH-lLTYx_MDTTDQkvJi2iDNy4U1V9kJxJ2qhgHPFhJYUN635r3A4M_hS2SvygUA6-zpiBlmEBiEk7CCxNe8l2ERVdMcYUwpFdfp_8rLKT94mVKAG6CspZYOgtAAbqEjcryZ9TStQ4_adhGU3UQt0n_AjNgPxwdxErKFEDI4UpJIeOfVZgbnSW_47W0sufRfTwz99RJNBHJvwNTCzsMeOR3FPMNiEtEnVD2lC4QzKg9ag_aOZ79hlGJ_NXLA_RnYD5rCnoRzxeiERdVgr2_29_ZZguGqaBXrUp8D7DKFsAMH_cyo7AbwHOi8tLRR5XxhzjtmDCJuhRyWT8ILEcl6Pety9ZN99Ekjx1C4SKfpsjmRmGu46J3yskdPD-0f0DyDqA3PZ2_VPtPQZbC7OVmwQAYHxchJLST7TH1GUNt_xZCImWYpVLO0u1NY4WnwFUVjRA4loDimEqiZ-t_iXdWojjRhB6fppcLMh7a2OmJkS_wZv6rZGiF1uUAcuhtWizh6ZwuTJ1GMgm9KAOc6vgfP71q-y1BDOlJpNbwRGT0JOs'
DOMAIN = os.getenv("DOMAIN") or "agiliumtrade.agiliumtrade.ai"