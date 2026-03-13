import os
import logging
import json
import datetime as dt
from dateutil.relativedelta import relativedelta
from typing import List, Any, Dict


def setup_logger(name, log_file, level=logging.INFO):
    """ Setup logger """

    handler = logging.FileHandler(log_file)
    handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger


def date_range(start_date, end_date=None, interval="d"):
    if end_date is None:
        end_date = dt.datetime.now()

    if interval == "h":
        sd = start_date.replace(minute=0, second=0)
        ed = end_date.replace(minute=0, second=0)
        for i in range(int((ed - sd).total_seconds()//3600) + 1):
            date = sd + dt.timedelta(hours=i)
            yield date, sd + dt.timedelta(hours=i+1)
    elif interval == "d":
        sd = start_date.replace(hour=0, minute=0, second=0)
        ed = end_date.replace(hour=0, minute=0, second=0)
        for i in range((ed - sd).days + 1):
            date = sd + dt.timedelta(days=i)
            yield date, sd + dt.timedelta(days=i+1)
    elif interval == "m":
        sd = start_date.replace(day=1, hour=0, minute=0, second=0)
        ed = end_date.replace(day=1, hour=0, minute=0, second=0)
        for i in range((ed.year - sd.year) * 12 + (ed.month - sd.month) + 1):
            date = sd + relativedelta(months=i)
            yield date, sd + relativedelta(months=i+1)
    elif interval == "y":
        sd = start_date.replace(month=1, day=1, hour=0, minute=0, second=0)
        ed = end_date.replace(month=1, day=1, hour=0, minute=0, second=0)
        for i in range(ed.year - sd.year + 1):
            date = sd + relativedelta(years=i)
            yield date, sd + relativedelta(years=i+1)
    else:
        raise ValueError(f"Unknown interval '{interval}'. Should be 'h', 'd', 'm' or 'y'.")


def chunk_list(elements: List[Any], number: int) -> List[List[Any]]:
    """ Returns given number of striped chunks from elements list. """
    chunks = []
    for i in range(0, number):
        chunks.append(elements[i::number])
    return chunks


def filter_reddits_by_dates(reddit_jsons: List[Dict[str, Any]],
                            start_date: dt.datetime, end_date: dt.datetime = None) -> List[Dict[str, Any]]:
    """ Filters the provided reddits JSON by provided dates interval """
    return list(filter(lambda rj: end_date > dt.datetime.fromtimestamp(rj['created_utc']) >= start_date, reddit_jsons))


def get_recent_file_date(jsons_folder: str) -> dt.datetime | None:
    """ Returns the latest file date (in order to determine the missing periods for INCREMENTAL load) """
    file_names = os.listdir(jsons_folder)
    dates = list(sorted(map(get_file_date_from_file_name, file_names)))
    return None if len(dates) == 0 else dates[-1]


def get_file_date_from_file_name(file_name: str) -> dt.datetime:
    """ Returns the file date from given file name """
    return dt.datetime.fromisoformat(file_name.split("_")[-1].split(".")[0])


def collect_authors(reddit_jsons: List[Dict[str, Any]]) -> List[str]:
    """ Returns a unique list of all found authors of given reddits and inner subreddits JSON """
    authors = list([])

    for reddit_json in reddit_jsons:
        authors.append(reddit_json['author'])
        if reddit_json.get("comments", None) is not None and len(reddit_json['comments']) > 0:
            authors.extend(collect_authors(reddit_json['comments']))
        elif reddit_json.get("replies", None) is not None and len(reddit_json['replies']) > 0:
            authors.extend(collect_authors(reddit_json['replies']))

    return list(filter(lambda a: a != "[deleted]", set(authors)))


def save_jsons(jsons: List[Dict[str, Any]], output_folder: str, output_file_pattern: str,
               start_date: dt.datetime, end_date: dt.datetime, logger: logging.Logger | None = None) -> None:
    """ Saves JSON data to the provided output folder under provided date range """
    if logger is None:
        logger = setup_logger(name="utils",
                              log_file=f"logs/yars/utils_{dt.datetime.now().isoformat()}.log")

    output_file = output_file_pattern.format(start_date=start_date.isoformat(), end_date=end_date.isoformat())
    output_json_file = f"{output_folder}/{output_file}"

    try:
        with open(output_json_file, "w", encoding="utf-8") as json_file:
            json.dump(jsons, json_file, indent=4)
        print(f"File {output_json_file} saved.")
        logger.info(f"File {output_json_file} saved.")
    except Exception as e:
        logger.error(f"Error exporting to JSON: {e}")
        print(f"Error exporting to JSON: {e}")

