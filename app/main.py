import pandas as pd
import streamlit as st

from home_scrape.sql import get_engine, get_values

# from sklearn.linear_model import LinearRegression


SCHEMA = "scrape"
TABLE = "houses"
MODELS = ["linear regression", "Ridge regression"]


# @st.cache_data
def fetch_data() -> pd.DataFrame:
    return pd.DataFrame(get_values(get_engine(), table=TABLE, schema=SCHEMA))


def main():
    st.header("Create your dataset")
    orig_df = fetch_data()
    with st.expander("Choose columns of interest"):
        target_col, feature_col = st.columns(2)
        target_col.text(f"Number of rows in data: {len(orig_df)}")
        feature_col.text(f"Number of columns in data: {len(orig_df.columns)}")
        target = target_col.selectbox(
            "Choose a target variable", orig_df.columns.to_list()
        )
        feature_cols = feature_col.multiselect(
            "Choose features to use in an 'X' vector", orig_df.columns.to_list()
        )
        df = orig_df[feature_cols + [target]]
        df.dropna(inplace=True)
        st.dataframe(df.head(10))
    with st.expander("Choose data type for each column"):
        dtypes = ["numeric", "str"]
        orig_column_types = {k: [None] for k in df.columns}
        t = {
            k: st.column_config.SelectboxColumn(
                options=dtypes,
                width="medium",
                help="Choose the preferred data type",
                required=True,
            )
            for k in orig_column_types.keys()
        }
        column_types = st.data_editor(orig_column_types, column_config=t)
        convert = st.button("Convert data..")
        if convert:
            for k, v in column_types.items():
                if "str" in v:
                    df[k] = df[k].astype("string")
                elif "numeric" in v:
                    df[k] = pd.to_numeric(df[k], "coerce")

            st.success("Completed conversion successfully!")
            st.write(df.dtypes)
    with st.expander("Get insights about the data"):
        pass
        # pr = df.profile_report()
        # st_profile_report(pr)

    # for x in feature_cols:
    #    ax = df.plot(kind="scatter", x=x, y=target)
    #    st.pyplot(ax.figure)
    # st.dataframe(df)

    # X = df[feature_cols]
    # y = df[target]

    st.header("Train models")
    models = st.multiselect("Choose models to use", MODELS)
    if models:
        tabs = st.tabs(models)

        for tab in tabs:
            tab.write("hej")


if __name__ == "__main__":
    # st.set_page_config(layout="wide")
    main()
