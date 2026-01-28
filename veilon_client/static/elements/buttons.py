import streamlit as st
from streamlit_extras.stylable_container import stylable_container

def button(label, key, color, icon, disabled, on_click, width):
    with stylable_container(
        key=key,
        css_styles=f'''
            button {{
                background-color:{color};
                color: white;
                border-radius: 0.5rem;
            }}
            ''',
        ):
            return st.button(label=label, 
                        key=key,
                        icon=icon, 
                        use_container_width=True,
                        disabled=disabled,
                        on_click=on_click,
                        width=width)