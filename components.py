import streamlit as st
from logic import get_filtered_df

def search_tab_ui(df, tab_id, mode="global"):
    """
    Renders a searchable table unique to its tab.
    tab_id: ensures widget keys don't collide.
    """
    st.subheader(f"Search Mode: {mode.title()}")
    
    # 1. Inputs (Unique per tab)
    col_to_search = None
    if mode == "column":
        col_to_search = st.selectbox("Select Column", df.columns, key=f"col_{tab_id}")
    
    search_query = st.text_input(f"Enter search for {tab_id}...", key=f"input_{tab_id}")

    # 2. Process (Delegate to logic.py)
    filtered_df = get_filtered_df(df, search_query, mode, col_to_search)

    # 3. Output
    st.dataframe(filtered_df, width='stretch')
    st.info(f"Found {len(filtered_df)} results in this tab.")
