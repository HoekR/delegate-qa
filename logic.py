import pandas as pd

def get_filtered_df(df, query, mode="global", column=None):
    if not query:
        return df

    query = str(query).lower()

    if mode == "global":
        # Search across all columns
        mask = df.astype(str).apply(lambda x: x.str.lower().str.contains(query)).any(axis=1)

    elif mode == "column" and column:
        # Search only in a specific column
        if column not in df.columns:
            raise ValueError(f"Column '{column}' not found in DataFrame.")
        mask = df[column].astype(str).str.lower().str.contains(query)

    elif mode == "strict":
        # Exact match logic
        mask = df.astype(str).apply(lambda x: x.str.lower() == query).any(axis=1)

    return df[mask]
