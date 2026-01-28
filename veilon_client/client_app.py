import sys
from pathlib import Path

# Ensure project root (/.../veilon-trading) is on sys.path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import streamlit as st
from veilon_client.pages.auth import render_login_screen, is_logged_in
from veilon_client.pages.routes import PAGES
from pathlib import Path

# Anchor paths to this module location (deployment-safe)
APP_ROOT = Path(__file__).resolve().parent  # .../veilon_client
CSS_PATH = APP_ROOT / "static" / "css" / "app.css"

def load_css():
    if CSS_PATH.exists():
        st.markdown(
            f"<style>{CSS_PATH.read_text()}</style>",
            unsafe_allow_html=True,
        )
    else:
        st.warning(f"Missing CSS: {CSS_PATH}")


def main():
    st.set_page_config(
        layout="centered",
        menu_items={
            "Get Help": "https://www.veilontrading.com/help",
            "Report a bug": "mailto:bug@veilontrading.com",
        },
    )

    # Ensure CSS loads after page config
    load_css()

    # --- Auth gate ---
    if not is_logged_in():
        render_login_screen()
        st.stop()   # hard stop, don't render dashboard

    nav = st.navigation(pages=PAGES, position="hidden")
    nav.run()


if __name__ == "__main__":
    main()