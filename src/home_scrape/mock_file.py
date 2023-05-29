"""Module to scrape real estate objects from hemnet."""

import logging

import requests
import unidecode
from bs4 import BeautifulSoup
from requests.exceptions import HTTPError

BASE_URL = (
    "https://www.hemnet.se/bostader?item_types%5B%5D=villa"
    "&location_ids%5B%5D=17755&location_ids%5B%5D=17754&page="
)


class RequestError(Exception):
    """Custom Exmeption to be raised when request fails."""

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


def get_house_information(soup: BeautifulSoup) -> dict:
    """Get information about a house given soup.

    Args:
        soup (BeautifulSoup): Soup for the given house

    Returns:
        Dictionary with KPI's for the house
    """
    # breakpoint()
    return {
        "address": soup.find("div", attrs={"class": "property-address"}).h1.text,
        "price": unidecode.unidecode(
            soup.find("div", attrs={"class": "property-info__price-container"}).p.text
        ),
        "land_area": unidecode.unidecode(
            soup.find(
                "div",
                attrs={
                    "class": "property-attributes-table__row qa-land-area-attribute"
                },
            ).dd.text
        ).replace(" m2", ""),
        "living_area": unidecode.unidecode(
            soup.find(
                "div",
                attrs={
                    "class": "property-attributes-table__row qa-living-area-attribute"
                },
            ).dd.text
        ).replace(" m2", ""),
        "n_rooms": "då",
    }


def main():
    """Scraping hemnet."""
    for page in range(24):
        url = BASE_URL + str(page)
        try:
            soup = get_soup(url)
        except RuntimeError:
            logging.warning(f"Request failed for url: {url}")
            continue
        houses = soup.find_all(
            "li", attrs={"class": "normal-results__hit js-normal-list-item"}
        )
        # print(containers)
        for house in houses:
            house_url = house.find(
                "a", attrs={"class": "js-listing-card-link listing-card"}
            )["href"]
            print(house_url)
            try:
                soup = get_soup(house_url)
            except RuntimeError:
                logging.warning(f"Request failed for url: {url}")
                continue
            info = get_house_information(soup)
            print(info)
        break


if __name__ == "__main__":
    main()
