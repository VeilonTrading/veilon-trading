# static/elements/layout.py

import streamlit as st
from streamlit_extras.stylable_container import stylable_container

# Same tokens for now – later you can centralise these in theme.py
secondary_background = "#171717"
light_text_color     = "#E8E8E8"
border_color         = "#3c3c3c"


def tile(
    key: str,
    height: int = 65,
    border: bool = True,
):
    """
    Base card container used across the app.

    Example:
        with tile("overview-chart", height=300, border=True):
            st.caption("Performance")
            st.altair_chart(...)

    """
    border_style = f"1px solid {border_color};" if border else "none;"

    with stylable_container(
        key=key,
        css_styles=f"""
        {{
            background-color: {secondary_background};
            font-family: "Source Sans Pro", sans-serif;
            font-weight: 400;
            border-radius: 0.5rem;
            border: {border_style};
            padding: calc(1em - 1px);
            color: {light_text_color};
        }}
        """
    ):
        # inner container = where Streamlit widgets go
        return st.container(border=False, height=height)