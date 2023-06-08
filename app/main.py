import pandas as pd
import streamlit as st

from home_scrape.sql import get_engine, get_values

SCHEMA = "scrape"
TABLE = "houses"
MODELS = ["linear regression", "Ridge regression"]


@st.cache_data
def fetch_data() -> pd.DataFrame:
    vals = get_values(get_engine(), table=TABLE, schema=SCHEMA)
    df = pd.DataFrame(vals)
    df.drop(columns=["index"], inplace=True)
    return df


def main():
    orig_df = fetch_data()
    target_col, feature_col = st.columns(2)
    target = target_col.selectbox("Choose a target variable", orig_df.columns.to_list())
    cols = feature_col.multiselect(
        "Choose features to use in an 'X' vector", orig_df.columns.to_list()
    )
    df = orig_df[cols + [target]]
    st.write(f"Number of data points: {len(orig_df)}")
    st.dataframe(df)

    if st.button("Clean data"):
        df.dropna(inplace=True)
        st.success(f"Cleaned dataframe. Current number of datapoints: {len(df)}")

    col1, col2 = st.columns(2)
    models = col1.multiselect("Choose models to test", MODELS)
    if models:
        tabs = col2.tabs(models)

    for tab in tabs:
        print(tab)


if __name__ == "__main__":
    st.set_page_config(layout="wide")
    main()
