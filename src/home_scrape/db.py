"""Module to help with interactions with the database."""
import logging

import pandas as pd
from sqlalchemy import Engine, text

from home_scrape.sql import get_metadata

logging.basicConfig(level=logging.INFO)


def update_columns_in_table(
    engine: Engine, df: pd.DataFrame, table: str, schema: str
) -> None:
    """Check current columns in SCHEMA.TABLE, update with new columns if present in df.

    Args:
        engine (Engine): SQL database engine to use for connection
        df (pd.DataFrame): Dataframe with data where columns are compared with existing
                           columns in database.
        table (str): Name of table of interest in database.
    Returns:
        None
        schema (str): Name of schema where table is located in database.
    """
    with engine.begin() as connection:
        db_columns = pd.read_sql_query(
            text(f"SELECT * FROM {schema + '.' + table} limit 1"), connection
        ).columns.str.lower()
    new_columns = set(df.columns) - set(db_columns)
    if not new_columns:
        return
    query = ""
    for column in new_columns:
        query += f"ALTER TABLE {schema + '.' + table} ADD COLUMN {column} text;"
    with engine.begin() as connection:
        _ = connection.execute(text(query))


def table_exists(engine: Engine, schema: str, table: str) -> bool:
    """Check if SCHEMA.TABLE exists in database given a db engine."""
    return (
        True if schema + "." + table in get_metadata(engine, schema).tables else False
    )


def add_data_to_table(
    engine: Engine, df: pd.DataFrame, table: str, schema: str
) -> None:
    """Add data in dataframe to table.

    If SCHEMA.TABLE doesn't exist it will be created.
    If SCHEMA.TABLE already exists, data will be added.
    If new columns exists in the dataframe, they will be added to the database.

    Args:
        engine (Engine): SQL database engine to use for connection.
        df (pd.DataFrame): Dataframe with data to add to the database.
        table (str): Name of table of interest in database.
        schema (str): Name of schema where table is located in database.
    Returns:
        None
    """
    if table_exists(engine, schema, table):
        update_columns_in_table(engine, df, table=table, schema=schema)
        logging.info(f"Table {schema + '.' + table} exists, adding data..")
    else:
        logging.info(f"Table {schema + '.' + table} is created..")

    logging.info(f"Adding {len(df)} rows to {schema + '.' + table}...")
    df.to_sql(table, engine, schema=schema, if_exists="append")
