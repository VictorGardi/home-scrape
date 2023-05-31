"""Module for simple actions in sql database."""
from typing import List, Dict, Union, Any
from urllib.parse import quote_plus

from sqlalchemy import MetaData, create_engine, text
from sqlalchemy.engine import Engine

from home_scrape.login import _get_credentials

def get_table_name(table: str, schema: str) -> str:
    """Get name of table from schema and table.
    Args:
        table (str): name of table
        schema (str): Schema where the table is located.
    Returns:
        str: table name combined with schema name
    """
    return table if not schema else schema + "." + table

def get_engine() -> Engine:
    """
    Get engine from environment variables.
    The following env variables need to be set globally or in a .env file:
        - PG_USER
        - PG_PW
        - PG_HOST
        - PG_DB
        - PG_PORT
    Returns:
        Engine: Database engine
    """
    return _create_engine(**_get_credentials())


def _create_engine(
    user: str, password: str, host: str, database: str, port: int = 5432
) -> Engine:
    """Create engine for a given database and user.
    Args:
        user (str): username
        password (str): password
        host (str): name or ip of host
        database (str): name of database
        port (int): port
    Returns:
        Engine: Database engine
    """
    connection_str = (
        f"postgresql://{user}:{quote_plus(password)}@{host}:{port}/{database}"
    )
    engine = create_engine(connection_str, connect_args={"connect_timeout": 10})
    return engine


def insert_rows_in_table(
    engine: Engine, table: str, row: Union[Dict, List[Dict]], schema: str = ""
) -> None:
    """Insert row in table.
    Args:
        engine (Engine): Database engine
        table (str): name of table
        row (Dict): Key value pairs corresponding to column name and value in the table
        schema (str): Schema where the table is located. Optional. Defaults to "".
    """
    stmt = get_table(engine=engine, table=table, schema=schema).insert().values(row)
    with engine.begin() as connection:
        connection.execute(stmt)


def get_values(
    engine: Engine, table: str, columns: str = "*", schema: str = "", where: str = ""
) -> List[Any]:
    """
    Get values from table.
    Filtering can be done on both columns and rows.
    Filter on columns
    If you want to select multiple columns (but not all) 'columns' should have the
    following format: columns = "column1, column2, column3".
    Filter on rows
    If you want to select rows given certain conditions 'where' should have the
    following format: where = "column1 = 'value1'" or where = "column2 = value2".
    Args:
        engine (Engine): Database engine
        table (str): name of table
        column (str): column to get values from. Optional. Defaults to "*".
        schema (str): Schema where the table is located. Optional. Defaults to "".
        where (str): SQL where statement to filter rows. Optional. Defaults to "".
    Returns:
        List[Any]: List of values in the table
    """
    query = f"SELECT {columns} FROM {get_table_name(table, schema)}"
    query = query if not where else query + f" WHERE {where}"

    with engine.begin() as connection:
        cursor = connection.execute(text(query))

    if columns == "*" or len(columns.split(",")) > 1:
        results = [dict(row._mapping) for row in cursor]
    else:
        results = [item[0] for item in cursor.fetchall()]
    return results


def get_table(*, engine: Engine, table: str, schema: str = "") -> Any:
    """Get table object given a table name.
    Args:
        table (str): name of table
        schema (str): Schema where the table is located. Optional. Defaults to "".
    Returns:
        Any: Class object that represents a table.
    """
    metadata = get_metadata(engine, schema)
    table_name = get_table_name(table, schema)
    return metadata.tables[table_name]


def get_metadata(engine: Engine, schema: str = "") -> MetaData:
    """
    Get metadata for the database engine.
    This includes table names and descriptions.
    Args:
        engine (Engine): Database engine
        schema (str): Schema that the metadata should reflect. Optional. Defaults to "".
                      Note that not all tables might be reflected in the metadata
                      if several schemas exist. If that's the case, include the schema
                      argument to get the tables in a specific schema.
    Returns:
        MetaData: Metadata for the database engine
    """
    metadata = MetaData()
    if not schema:
        metadata.reflect(engine)
    else:
        metadata.reflect(engine, schema=schema)
    return metadata


if __name__ == "__main__":
    engine = get_engine()
