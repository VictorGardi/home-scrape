"""Module to scrape real estate objects from hemnet."""

import logging
from uuid import uuid4

from home_scrape.sql import get_engine, get_values, get_metadata
import requests
from sqlalchemy import Engine, text
from unidecode import unidecode as ud
from bs4 import BeautifulSoup
from requests.exceptions import HTTPError
import pandas as pd

BASE_URL = (
    "https://www.hemnet.se/bostader?item_types%5B%5D=villa"
    "&location_ids%5B%5D=17755&location_ids%5B%5D=17754&page="
)

SCHEMA = "scrape"
TABLE = "houses"

logging.basicConfig(level=logging.INFO)

class RequestError(Exception):
    """Custom exception to be raised when request fails."""

    pass


def get_request(url: str) -> requests.Response:
    """Get request for url.

    Args:
        url (str): url of interest

    Returns:
        Response
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36"
        )
    }
    try:
        r = requests.get(url, headers=headers)
        r.raise_for_status()
        return r
    except HTTPError as exc:
        raise RequestError(f"Request failed for url {url} with error {exc}") from exc


def get_soup(url: str) -> BeautifulSoup:
    """Get soup from BeautifulSoup given an url.

    Args:
        url (str): url of interest

    Returns:
       Soup
    """
    try:
        r = get_request(url)
        return BeautifulSoup(r.text, "html.parser")
    except RequestError as exc:
        raise RuntimeError from exc

def get_table_info(soup: BeautifulSoup, class_name: str) -> dict:
    keys, values = [], []
    for dl in soup.findAll("dl", {"class": class_name}):
        for dt in dl.findAll("dt"):
            keys.append(dt.text.strip())
        for dd in dl.findAll("dd"):
            values.append(ud(dd.text.strip()).replace(" m2", "").replace(" rum", "").replace(" kr/ar", ""))
    info = dict(zip(keys, values))
    info.pop("", None)
    return info

def get_house_information(soup: BeautifulSoup) -> dict:
    """Get information about a house given soup.

    Args:
        soup (BeautifulSoup): Soup for the given house

    Returns:
        Dictionary with KPI's for the house
    """
    price = ud(soup.find("div", attrs={"class": "property-info__price-container"}).p.text).replace(" kr", "")
    living_area = ud(soup.find( "div", attrs={ "class": "property-attributes-table__row qa-living-area-attribute" },).dd.text).replace(" m2", "")
    info = get_table_info(soup, class_name="property-attributes-table__area")

    address = soup.find("div", attrs={"class": "property-address"}).h1.text
    city_region = soup.find("span", attrs={"class": "property-address__area"}).text
    try:
        city, region = city_region.split(", ")
    except ValueError:
        city, region = None, city_region
    info.update({
            "stad": city,
            "region": region,
            "adress": address,
            "pris": price ,
            "Boarea": living_area,
        })

    return info

def main(n_pages: int) -> None:
    """Scraping hemnet."""
    engine = get_engine() 
    if SCHEMA + "." + TABLE in get_metadata(engine, SCHEMA).tables:
        logging.info(f"Table {SCHEMA + '.' + TABLE} exists, adding data..")
        urls = get_values(engine, TABLE, schema=SCHEMA, columns="url")
    else:
        logging.info(f"Table {SCHEMA + '.' + TABLE} does not exist, creating..")
        urls = []
    for page in range(n_pages):
        items = []
        url = BASE_URL + str(page)
        try:
            soup = get_soup(url)
        except RuntimeError:
            logging.warning(f"Request failed for url: {url}")
            continue
        houses = soup.find_all(
            "li", attrs={"class": "normal-results__hit js-normal-list-item"}
        )
        for i, house in enumerate(houses):
            if i % 10 == 0:
                logging.info(f"Done with scraping of {i*(page+1)} houses")
            try:
                house_url = house.find(
                    "a", attrs={"class": "js-listing-card-link listing-card"}
                )["href"]
            except Exception as e:
                logging.warning(f"No URL exists for the house. Failed due to {e}")
                continue

            if house_url in urls:
                logging.warning(f"House already exists in db. URL: {house_url}")
                continue

            try:
                soup = get_soup(house_url)
            except RuntimeError:
                logging.warning(f"Request failed for url: {url}")
                continue
            try:
                info = get_house_information(soup)
                info.update({"url": house_url})
                items.append(info)
            except Exception as e:
                logging.warning(f"Fetching information for url: {url} failed due to {e}")
                continue
        if not items:
            continue
        df = pd.DataFrame(items)
        try:
            df.columns = df.columns.str.lower()
        except Exception as e:
            print(e)
            breakpoint()
        if SCHEMA + "." + TABLE in get_metadata(engine, SCHEMA).tables:
            df = update_columns_in_table(engine, df, table=TABLE, schema=SCHEMA)
        df.to_sql(TABLE, engine, schema=SCHEMA, if_exists="append")


def update_columns_in_table(engine: Engine, df: pd.DataFrame, table: str, schema: str) -> pd.DataFrame:
    with engine.begin() as connection:
        db_columns = pd.read_sql_query(f"SELECT * FROM {schema + '.' + table} limit 1", connection).columns.str.lower()
    new_columns = set(df.columns) - set(db_columns)
    if not new_columns:
        return df
    query = ''   
    for column in new_columns:
        #old_col = column
        #if " " in column:
        #    column = column.replace(" ", "_")
        #    df.rename(columns={old_col: column})
        query += f"ALTER TABLE {schema + '.' + table} ADD COLUMN {column} text;" 
    with engine.begin() as connection:
        _ = connection.execute(text(query))
    return df


if __name__ == "__main__":
    main(n_pages=30)
