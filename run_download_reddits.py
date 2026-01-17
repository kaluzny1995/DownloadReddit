import os
import argparse
import json
import datetime as dt
from typing import List
from tqdm import tqdm

from utils import setup_logger
from yars import YARS, date_range, export_to_json


logger = setup_logger(name="download_reddits",
                      log_file=f"logs/download_reddits/download_reddits_{dt.datetime.now().isoformat()}.log")


def get_config(file_name: str) -> dict[str, int | str | object]:
    """ Returns the default application parameters """
    with open(file_name, "r") as f:
        config = json.load(f)
    return config


def parse_args(defaults: dict[str, int | str | object]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reddits downloader Python 3.11 application.")

    parser.add_argument("phrase", type=str, help="phrase to search for reddits")
    parser.add_argument("-l", "--limit", type=int, required=False, default=defaults['limit'],
                        help=f"limit of searched reddits, default: {defaults['limit']}")
    parser.add_argument("-i", "--interval", type=str, required=False, choices=["h", "d", "m", "y"], default=defaults['interval'],
                        help=f"interval between dates of searching period, default: {defaults['interval']}")
    parser.add_argument("-d", "--start_date", type=str, required=False, default=defaults['start_date'],
                        help=f"start date of reddits search, default: {defaults['start_date']}")
    parser.add_argument("--download_authors", required=False, default=defaults['is_author_downloaded'],
                        help=f"flag whether to download reddit authors information, default: {defaults['is_author_downloaded']}",
                        action="store_true")
    parser.add_argument("--previous_day", required=False, default=defaults['is_date_to_previous_day'],
                        help=f"flag whether to download reddits till the previous day, default: {defaults['is_date_to_previous_day']}",
                        action="store_true")

    return parser.parse_args()


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
    """ Returns a unique list of all found authors of given reddits and inner subreddits json """
    authors = list([])

    for reddit_json in reddit_jsons:
        authors.append(reddit_json['author'])
        if reddit_json.get("comments", None) is not None and len(reddit_json['comments']) > 0:
            authors.extend(collect_authors(reddit_json['comments']))
        elif reddit_json.get("replies", None) is not None and len(reddit_json['replies']) > 0:
            authors.extend(collect_authors(reddit_json['replies']))

    return list(filter(lambda a: a != "[deleted]", set(authors)))


def save_jsons(jsons: List[dict[str, object]], output_folder: str, output_file_pattern: str,
               start_date: dt.datetime, end_date: dt.datetime) -> None:
    output_file = output_file_pattern.format(start_date=start_date.isoformat(), end_date=end_date.isoformat())
    output_json_file = f"{output_folder}/{output_file}"
    export_to_json(jsons, filename=output_json_file)
    print(f"File {output_json_file} saved.")
    logger.info(f"File {output_json_file} saved.")


def main():
    print("---- Reddits downloader ----\n")

    config = get_config("config.json")
    args = parse_args(config)

    phrase = args.phrase
    limit = args.limit
    date_interval = args.interval
    default_start_date = args.start_date
    is_author_downloaded = args.download_authors
    is_date_to_previous_day = args.previous_day

    website_url = config['website_url']
    output_reddits_folder = config['reddits_folder_pattern'].format(phrase=phrase)
    output_authors_folder = config['authors_folder_pattern'].format(phrase=phrase)
    output_reddits_file_pattern = config['reddits_file_pattern'].format(phrase=phrase)
    output_authors_file_pattern = config['authors_file_pattern'].format(phrase=phrase)

    # Show parameters
    print("Searched phrase:", phrase)
    print("Max searched:", limit)
    print("Date interval:", date_interval)
    print("Reddits folder:", output_reddits_folder)
    print("Authors folder:", output_authors_folder)
    print("Download author details:", is_author_downloaded)
    print("Search until previous day:", is_date_to_previous_day, "\n")

    # Create folders if not exist
    if not os.path.exists(output_reddits_folder):
        os.makedirs(output_reddits_folder)
    if not os.path.exists(output_authors_folder):
        os.makedirs(output_authors_folder)

    recent_date = get_recent_file_date(output_reddits_folder)
    load_type = "HISTORICAL" if recent_date is None else "INCREMENTAL"
    date_from = default_start_date if recent_date is None else recent_date
    date_to = dt.datetime.now() if not is_date_to_previous_day else dt.datetime.now() - dt.timedelta(days=1)

    print("Load type:", load_type)
    print("Start date:", date_from)
    print("End date:", date_to, "\n")

    downloader = YARS()

    # Getting posts headers
    print(f"Searching reddits with phrase '{phrase}'.\n")
    logger.info(f"Searching reddits with phrase '{phrase}'.")
    reddit_headers = downloader.search_reddit(query=phrase, limit=limit)

    # Restriction to the newest only for INCREMENTAL load
    if load_type == "INCREMENTAL":
        reddit_headers = list(filter(lambda rh: dt.datetime.fromtimestamp(rh['created_utc']) >= date_from, reddit_headers))

    # Getting posts details
    permalinks = list(map(lambda rh: rh['link'].split(website_url)[1], reddit_headers))
    print(f"Found {len(permalinks)} results.")
    logger.info(f"Found {len(permalinks)} results.")
    if len(permalinks) == 0:
        logger.info("No new reddits found. Finishing.")
        raise Exception("No new reddits available. Finishing.")

    print("Downloading reddits:")
    logger.info("Downloading reddits.")
    reddit_details = list(map(lambda p: downloader.scrape_post_details(p), tqdm(permalinks)))
    logger.info("Reddit details downloaded.")

    # Saving into separate jsons
    for sd, ed in date_range(date_from, date_to, interval=date_interval):
        # Filtering reddits details by dates interval
        reddits_interval = filter_reddits_by_dates(reddit_details, sd, ed)

        # Saving reddits details into json file
        save_jsons(reddits_interval, output_reddits_folder, output_reddits_file_pattern, sd, ed)

        if is_author_downloaded:
            # Getting posts authors
            authors = collect_authors(reddits_interval)
            print(f"\nFound {len(authors)} different authors for period {sd} -- {ed}.")
            logger.info(f"Found {len(authors)} different authors for period {sd} -- {ed}.")
            print(f"Downloading authors details for period {sd} -- {ed}")
            logger.info(f"Downloading authors details for period {sd} -- {ed}")
            author_details = list(map(lambda a: downloader.scrape_user_data(a, limit=1), tqdm(authors)))

            # Saving authors details into json file
            save_jsons(author_details, output_authors_folder, output_authors_file_pattern, sd, ed)

    print("\nDone.")
    logger.info("Done.")


if __name__ == "__main__":
    main()
