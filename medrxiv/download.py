import requests
import re


def download_file(date: str, regex: str) -> list | None:
    """Download data from medrxiv api and process data by filtering for keyword.

    Args:
        date (str): Date to download data from. In the format of YYYY-MM-DD.
        regex (str): Regex to filter data by.

    Returns:
        list | None: List of dictionary if data is available, None otherwise.
    """
    cursor = 0
    data_list = []
    # Get initial data where cursor is 0
    initial_res = requests.get(
        f"https://api.medrxiv.org/details/medrxiv/{date}/{date}/{cursor}/json"
    )
    # Case of no data available
    if initial_res.json()["messages"][0]["status"] == "no posts found":
        return None
    elif initial_res.json()["messages"][0]["status"] == "ok":
        initial_collection = initial_res.json()["collection"]
        for article in initial_collection:
            if re.search(regex, article["title"]) or re.search(
                regex, article["abstract"]
            ):
                data_list.append(article)
        remaining_pages = (int(initial_res.json()["messages"][0]["total"]) - 1) // 100
        for _ in range(remaining_pages):
            cursor += 1
            iter_res = requests.get(
                f"https://api.medrxiv.org/details/medrxiv/{date}/{date}/{cursor*100}/json"
            )
            iter_collection = iter_res.json()["collection"]
            for iter_article in iter_collection:
                if re.search(regex, iter_article["title"]) or re.search(
                    regex, iter_article["abstract"]
                ):
                    data_list.append(iter_article)
        return data_list
    else:
        raise Exception(f"Unknown respone: {initial_res.json()}")
