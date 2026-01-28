import streamlit as st
from st_social_media_links import SocialMediaIcons as socials
from streamlit_extras.bottom_container import bottom

def render_footer():
    social_media_links = [
        "https://x.com/veilontrading",
        "https://www.instagram.com/veilontrading",
    ]

    with bottom():
        with st.container(border=False, horizontal=True):
            social_media_icons = socials(
                social_media_links,
                colors=["#e8e8e8", "#e8e8e8"],
            )
            social_media_icons.render()