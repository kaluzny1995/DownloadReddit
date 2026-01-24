import os
import logging
import datetime as dt
from typing import List, Any


def setup_logger(name, log_file, level=logging.INFO):
    """ Setup logger """

    handler = logging.FileHandler(log_file)
    handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger


def chunk_list(elements: List[Any], number: int) -> List[List[Any]]:
    """ Returns given number of striped chunks from elements list. """
    chunks = []
    for i in range(0, number):
        chunks.append(elements[i::number])
    return chunks


def filter_reddits_by_dates(reddit_jsons: List[dict[str, object]],
                            start_date: dt.datetime, end_date: dt.datetime = None) -> List[dict[str, object]]:
    """ Filters the provided reddits JSON by provided dates interval """
    return list(filter(lambda rj: end_date > dt.datetime.fromtimestamp(rj['created_utc']) >= start_date, reddit_jsons))


def get_recent_file_date(jsons_folder: str) -> dt.datetime | None:
    """ Returns the latest file date (in order to determine the missing periods for INCREMENTAL load) """
    file_names = os.listdir(jsons_folder)
    dates = list(sorted(map(lambda j: dt.datetime.fromisoformat(j.split("_")[-1].split(".")[0]), file_names)))
    return None if len(dates) == 0 else dates[-1]


def collect_authors(reddit_jsons: List[dict[str, object | List[dict[str, object]]]]) -> List[str]:
    """ Returns a unique list of all found authors of given reddits and inner subreddits JSON """
    authors = list([])

    for reddit_json in reddit_jsons:
        authors.append(reddit_json['author'])
        if reddit_json.get("comments", None) is not None and len(reddit_json['comments']) > 0:
            authors.extend(collect_authors(reddit_json['comments']))
        elif reddit_json.get("replies", None) is not None and len(reddit_json['replies']) > 0:
            authors.extend(collect_authors(reddit_json['replies']))

    return list(filter(lambda a: a != "[deleted]", set(authors)))
