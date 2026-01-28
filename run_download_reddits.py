import multiprocessing
import os
import argparse
import json
import datetime as dt
from multiprocessing import Queue
from typing import List, Dict, Any
from tqdm import tqdm

import utils
from yars import YARS, date_range, export_to_json


logger = utils.setup_logger(name="download_reddits",
                            log_file=f"logs/download_reddits/download_reddits_{dt.datetime.now().isoformat()}.log")


def get_config(file_name: str) -> Dict[str, Any]:
    """ Returns the default application parameters """
    with open(file_name, "r") as f:
        config = json.load(f)
    return config


def parse_args(defaults: Dict[str, Any]) -> argparse.Namespace:
    """ Parses command line arguments """
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
    parser.add_argument("--use_multiprocessing", required=False, default=defaults['is_multiprocessing_used'],
                        help=f"flag whether to use multiprocessing while downlading reddits and authors, default: {defaults['is_multiprocessing_used']}",
                        action="store_true")
    parser.add_argument("--num_processes", type=int, required=False, default=defaults['num_processes'],
                        help=f"number of processes if multiprocessing is used, default: {defaults['num_processes']}")

    return parser.parse_args()


def _download_reddits_details(downloader: YARS, permalinks: List[str], num: int, queue: Queue) -> None:
    """ Partially downloads reddits details (utilizes multiprocessing) """
    print(f"P{num + 1}: Starting downloading reddits details.")
    logger.info(f"P{num + 1}: Starting downloading reddits details.")

    details = list([])
    for i, permalink in enumerate(permalinks):
        result = downloader.scrape_post_details(permalink)
        if isinstance(result, dict):
            details.append(result)
        else:
            print(f"P{num + 1}: Something went wrong.")
            logger.warning(f"P{num + 1}: Something went wrong.")

        if len(details) % 10 == 0 and len(details) > 0:
            print(f"P{num + 1}: Downloaded {len(details)} out of {len(permalinks)} reddits.")
            logger.info(f"P{num + 1}: Downloaded {len(details)} out of {len(permalinks)} reddits.")

    print(f"P{num + 1}: Finished downloading reddits details. Downloaded: {len(details)}.")
    logger.info(f"P{num + 1}: Finished downloading reddits details. Downloaded: {len(details)}.")

    queue.put((details, num))


def _download_authors_details(downloader: YARS, names: List[str], num: int, queue: Queue) -> None:
    """ Partially downloads authors details (utilizes multiprocessing) """
    print(f"P{num + 1}: Starting downloading authors details.")
    logger.info(f"P{num + 1}: Starting downloading authors details.")

    details = list([])
    for i, name in enumerate(names):
        result = downloader.scrape_user_data(name, limit=1)
        if isinstance(result, list):
            details.append(result)
        else:
            print(f"P{num + 1}: Something went wrong.")
            logger.warning(f"P{num + 1}: Something went wrong.")

        if len(details) % 10 == 0 and len(details) > 0:
            print(f"P{num + 1}: Downloaded {len(details)} out of {len(names)} authors.")
            logger.info(f"P{num + 1}: Downloaded {len(details)} out of {len(names)} authors.")

    print(f"P{num + 1}: Finished downloading authors details. Downloaded: {len(details)}.")
    logger.info(f"P{num + 1}: Finished downloading authors details. Downloaded: {len(details)}.")

    queue.put((details, num))


def save_jsons(jsons: List[Dict[str, Any]], output_folder: str, output_file_pattern: str,
               start_date: dt.datetime, end_date: dt.datetime) -> None:
    output_file = output_file_pattern.format(start_date=start_date.isoformat(), end_date=end_date.isoformat())
    output_json_file = f"{output_folder}/{output_file}"
    export_to_json(jsons, filename=output_json_file, logger=logger)
    print(f"File {output_json_file} saved.")
    logger.info(f"File {output_json_file} saved.")


def main():
    print("---- Reddits downloader ----\n")

    config = get_config("config.json")
    args = parse_args(config)

    phrase = args.phrase
    limit = args.limit
    date_interval = args.interval
    default_start_date = dt.datetime.strptime(args.start_date, "%Y-%m-%d")
    is_author_downloaded = args.download_authors
    is_date_to_previous_day = args.previous_day
    is_multiprocessing_used = args.use_multiprocessing
    num_processes = 1 if not is_multiprocessing_used else args.num_processes

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
    print("Search until previous day:", is_date_to_previous_day)
    print("Use multiprocessing:", is_multiprocessing_used)
    print("Number of processes:", num_processes, "\n")

    # Create folders if not exist
    if not os.path.exists(output_reddits_folder):
        os.makedirs(output_reddits_folder)
    if not os.path.exists(output_authors_folder):
        os.makedirs(output_authors_folder)

    recent_date = utils.get_recent_file_date(output_reddits_folder)
    load_type = "HISTORICAL" if recent_date is None else "INCREMENTAL"
    date_from = default_start_date if recent_date is None else recent_date
    date_to = dt.datetime.now() if not is_date_to_previous_day \
        else dt.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - dt.timedelta(seconds=1)

    print("Load type:", load_type)
    print("Start date:", date_from)
    print("End date:", date_to, "\n")

    if date_from > date_to:
        logger.info("Recent (start) file date is bigger than end date. Nothing to download. Finishing.")
        raise Exception("Recent (start) file date is bigger than end date. Nothing to download.")

    downloader = YARS()

    # Getting posts headers
    print(f"Searching reddits with phrase '{phrase}'.\n")
    logger.info(f"Searching reddits with phrase '{phrase}'.")
    reddit_headers = downloader.search_reddit(query=phrase, limit=limit)

    # Restriction to the newest only for INCREMENTAL load
    if load_type == "INCREMENTAL":
        reddit_headers = list(filter(lambda rh: date_to > dt.datetime.fromtimestamp(rh['created_utc']) >= date_from, reddit_headers))

    # Getting posts details
    permalinks = list(map(lambda rh: rh['link'].split(website_url)[1], reddit_headers))
    print(f"Found {len(permalinks)} results.")
    logger.info(f"Found {len(permalinks)} results.")

    print("Downloading reddits.")
    logger.info("Downloading reddits.")
    # Using multiprocessing only if applicable and number of reddits to download is >= quadratic number of processes
    if is_multiprocessing_used and len(permalinks) >= num_processes ** 2:
        reddit_details = list([])
        queue = multiprocessing.Queue()
        for i, chunk in enumerate(utils.chunk_list(permalinks, num_processes)):
            p = multiprocessing.Process(target=_download_reddits_details, args=(downloader, chunk, i, queue))
            p.start()

        for i in range(num_processes):
            results, num = queue.get()
            if isinstance(results, list):
                reddit_details.extend(results)

    else:
        reddit_details = list(map(lambda pl: downloader.scrape_post_details(pl), tqdm(permalinks)))
    print(f"Reddit details downloaded. Total: {len(reddit_details)}.")
    logger.info(f"Reddit details downloaded. Total: {len(reddit_details)}.")


    # Saving into separate JSONs
    for sd, ed in date_range(date_from, date_to, interval=date_interval):
        # Filtering reddits details by dates interval
        reddits_interval = utils.filter_reddits_by_dates(reddit_details, sd, ed)

        # Saving reddits details into JSON file
        save_jsons(reddits_interval, output_reddits_folder, output_reddits_file_pattern, sd, ed)

        if is_author_downloaded:
            # Getting posts authors
            authors = utils.collect_authors(reddits_interval)

            print(f"\nFound {len(authors)} different authors for period {sd} -- {ed}.")
            logger.info(f"Found {len(authors)} different authors for period {sd} -- {ed}.")
            print(f"Downloading authors details for period {sd} -- {ed}.")
            logger.info(f"Downloading authors details for period {sd} -- {ed}.")
            # Using multiprocessing only if applicable and number of authors to download is >= quadratic number of processes
            if is_multiprocessing_used and len(authors) >= num_processes ** 2:
                author_details = list([])
                queue = multiprocessing.Queue()
                for i, chunk in enumerate(utils.chunk_list(authors, num_processes)):
                    p = multiprocessing.Process(target=_download_authors_details, args=(downloader, chunk, i, queue))
                    p.start()

                for i in range(num_processes):
                    results, num = queue.get()
                    if isinstance(results, list):
                        author_details.extend(results)

            else:
                author_details = list(map(lambda a: downloader.scrape_user_data(a, limit=1), tqdm(authors)))
            print(f"Downloading authors details for period {sd} -- {ed} finished.")
            logger.info(f"Downloading authors details for period {sd} -- {ed} finished.")

            # Saving authors details into JSON file
            save_jsons(author_details, output_authors_folder, output_authors_file_pattern, sd, ed)

    print("\nDone.")
    logger.info("Done.")


if __name__ == "__main__":
    main()
