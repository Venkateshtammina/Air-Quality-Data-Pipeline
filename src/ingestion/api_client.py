import time

import requests

from config import (
    API_URL,
    RESOURCE_ID,
    MAX_RETRIES,
    RETRY_DELAY_SECONDS
)


def fetch_page(offset, limit):
    """
    Fetch one page from the OpenCity CKAN API.

    Retries transient request failures.
    """

    params = {
        "resource_id": RESOURCE_ID,
        "limit": limit,
        "offset": offset
    }

    for attempt in range(1, MAX_RETRIES + 1):

        try:

            print(
                f"Requesting offset={offset}, "
                f"limit={limit}, "
                f"attempt={attempt}"
            )

            response = requests.get(
                API_URL,
                params=params,
                timeout=30
            )

            response.raise_for_status()

            data = response.json()

            if not data.get("success", False):
                raise RuntimeError(
                    "API returned success=false"
                )

            return data

        except (
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
            requests.exceptions.HTTPError
        ) as e:

            print(
                f"API request failed on attempt "
                f"{attempt}: {e}"
            )

            if attempt == MAX_RETRIES:

                print(
                    "Maximum retries reached."
                )

                return None

            delay = RETRY_DELAY_SECONDS ** attempt

            print(
                f"Retrying in {delay} seconds..."
            )

            time.sleep(delay)

    return None


def validate_api_response(data):
    """
    Validate the structure of an API response.
    """

    if not isinstance(data, dict):
        return False

    if not data.get("success", False):
        return False

    if "result" not in data:
        return False

    if "records" not in data["result"]:
        return False

    return True