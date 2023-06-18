"""Module to scrape real estate objects from hemnet."""

import logging
from typing import Union

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.exceptions import HTTPError
from unidecode import unidecode as ud

from home_scrape.db import add_data_to_table, table_exists
from home_scrape.sql import get_engine, get_values

BASE_URL = (
    "https://www.hemnet.se/bostader?item_types%5B%5D=villa"
    "&location_ids%5B%5D=17755&location_ids%5B%5D=17754&page="
)

SCHEMA = "houses"
TABLE_RAW = "raw"
TABLE_TRANSFORMED = "transformed"

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
    """Get information from hemnet table and collect into a dict."""
    keys, values = [], []
    for dl in soup.findAll("dl", {"class": class_name}):
        for dt in dl.findAll("dt"):
            keys.append(dt.text.strip())
        for dd in dl.findAll("dd"):
            values.append(
                ud(dd.text.strip())
                .replace(" m2", "")
                .replace(" rum", "")
                .replace(" kr/ar", "")
            )
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
    price = ud(
        soup.find("div", attrs={"class": "property-info__price-container"}).p.text
    ).replace(" kr", "")
    living_area = ud(
        soup.find(
            "div",
            attrs={"class": "property-attributes-table__row qa-living-area-attribute"},
        ).dd.text
    ).replace(" m2", "")
    info = get_table_info(soup, class_name="property-attributes-table__area")

    address = soup.find("div", attrs={"class": "property-address"}).h1.text
    city_region = soup.find("span", attrs={"class": "property-address__area"}).text
    try:
        city, region = city_region.split(", ")
    except ValueError:
        city, region = None, city_region
    info.update(
        {
            "stad": city,
            "region": region,
            "adress": address,
            "pris": price,
            "Boarea": living_area,
        }
    )

    return info


def extract(n_pages: int) -> None:
    """Scraping hemnet."""
    engine = get_engine()
    urls = (
        get_values(engine, TABLE_RAW, schema=SCHEMA, columns="url")
        if table_exists(engine, SCHEMA, TABLE_RAW)
        else []
    )
    for page in range(n_pages):
        logging.info(f"Scraping houses from page {page}")
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
        for house in houses:
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
                logging.warning(
                    f"Fetching information for url: {url} failed due to {e}"
                )
                continue
        if not items:
            continue
        df = pd.DataFrame(items)
        try:
            df.columns = df.columns.str.lower()
        except Exception as e:
            print(e)
        add_data_to_table(engine, df, TABLE_RAW, SCHEMA)


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Transform data into something usable."""
    df.drop(columns=["index"], inplace=True)
    df["antal rum"] = df["antal rum"].str.replace(",", ".")
    df["pris"] = df["pris"].str.replace(" ", "")
    df.uteplats = _str_col_to_bool(df.uteplats, "ja")
    df.balkong = _str_col_to_bool(df.balkong, "ja")
    return df


def pipeline() -> None:
    """Pipeline to run the transformation of the data."""
    engine = get_engine()
    urls_raw = get_values(engine, TABLE_RAW, columns="url", schema=SCHEMA)
    urls_transformed = (
        get_values(get_engine(), TABLE_TRANSFORMED, columns="url", schema=SCHEMA)
        if table_exists(engine, TABLE_TRANSFORMED, SCHEMA)
        else []
    )
    new_urls = list(set(urls_raw) - set(urls_transformed))
    df = pd.DataFrame(
        get_values(
            engine, TABLE_RAW, schema=SCHEMA, where=f"url IN {_list_to_tuple(new_urls)}"
        )
    )
    df = transform(df)
    add_data_to_table(engine, df, TABLE_TRANSFORMED, SCHEMA)


def _str_col_to_bool(col: pd.Series, qry: str) -> pd.Series:
    """Convert pandas str column to bool given a qry.

    Args:
        col (pd.Series): str column to convert to bool
        qry (str): string value that should be converted to True.
                   All other values will be set to False. Note that this
                   functionality is NOT case-sensitive. All values in column,
                   and qry, will be converted to lower case before converting.

    Returns:
        pd.Series: bool column.
    """
    return col.str.lower() == qry.lower()


def _list_to_tuple(ls: list) -> Union[tuple, str]:
    """Convert list to sql friendly tuple to use in IN statements."""
    return tuple(ls) if len(ls) > 1 else "('" + str(ls[0]) + "')"


if __name__ == "__main__":
    # extract(n_pages=30)
    pipeline()
