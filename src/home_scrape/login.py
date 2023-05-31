"""Module to login to database. """

"""Module to get credentials for database."""
import os
from typing import Dict

from dotenv import dotenv_values, find_dotenv

CREDS = {
    "PG_USER": "user",
    "PG_PW": "password",
    "PG_DB": "database",
    "PG_HOST": "host",
    "PG_PORT": "port",
}


def _get_credentials() -> Dict:
    """
    Get login credentials.
    Looks for environment variables PG_USER, PG_PW, PG_DB, PG_HOST and PG_PORT.
    If they don't exist, it tries to find a .env file to read variables from.
    Raises:
        (RuntimeError): If no environment variables are found
    Returns:
        Dict: login credentials
    """
    if all(item in os.environ.keys() for item in CREDS.keys()):
        credentials = {v: os.environ[k] for k, v in CREDS.items()}
    else:
        try:
            credentials = _load_dotenv()
        except (FileNotFoundError, KeyError) as e:
            raise RuntimeError from e

    return credentials


def _load_dotenv() -> Dict:
    """
    Load credentials from .env file.
    Raises:
        (FileNotFoundError): If no .env file is found
        (KeyError): If not all environment variables are set in the .env file
    Returns:
        Dict: login credentials
    """
    dotenv_path = find_dotenv(usecwd=True)
    env_creds = dotenv_values(dotenv_path)
    if not env_creds:
        raise FileNotFoundError("No .env file found")
    if all(item in env_creds.keys() for item in CREDS.keys()):
        credentials = {v: env_creds[k] for k, v in CREDS.items()}
    else:
        raise KeyError(f"{CREDS.keys()} must be set in the .env file")

    return credentials


if __name__ == "__main__":
    c = _get_credentials()
