import multiprocessing
import os
import logging
import argparse
import datetime as dt
from multiprocessing import Queue
from typing import List
from tqdm import tqdm

import util
import yars
from model import EloadType, AppConfig, DownloadParams, LoadParams


def parse_args(defaults: AppConfig) -> argparse.Namespace:
    """ Parses command line arguments """
    parser = argparse.ArgumentParser(description="Reddits downloader Python 3.11 application.")

    parser.add_argument("phrase", type=str, help="phrase to search for reddits")
    parser.add_argument("-l", "--limit", type=int, required=False, default=defaults.limit,
                        help=f"limit of searched reddits, default: {defaults.limit}")
    parser.add_argument("-i", "--interval", type=str, required=False, choices=["h", "d", "m", "y"], default=defaults.interval,
                        help=f"interval between dates of searching period, default: {defaults.interval}")
    parser.add_argument("-d", "--start_date", type=str, required=False, default=defaults.start_date,
                        help=f"start date of reddits search, default: {defaults.start_date}")
    parser.add_argument("--no_authors_download", required=False, default=defaults.is_no_authors_download,
                        help=f"flag whether to skip reddit authors information downloading, default: {defaults.is_no_authors_download}",
                        action="store_true")
    parser.add_argument("--include_today", required=False, default=defaults.is_today_included,
                        help=f"flag whether to download reddits until the current datetime, ie. moment of script launch, default: {defaults.is_today_included}",
                        action="store_true")
    parser.add_argument("--no_multiprocessing", required=False, default=defaults.is_no_multiprocessing_used,
                        help=f"flag whether not to use multiprocessing while downloading reddits and authors, default: {defaults.is_no_multiprocessing_used}",
                        action="store_true")
    parser.add_argument("--num_processes", type=int, required=False, default=defaults.num_processes,
                        help=f"number of processes if multiprocessing is used, default: {defaults.num_processes}")

    return parser.parse_args()


def show_params(download_params: DownloadParams, logger: logging.Logger) -> None:
    """ Shows the parameters and its values """
    print("Searched phrase:", download_params.phrase)
    print("Max searched:", download_params.limit)
    print("Date interval:", download_params.date_interval)
    print("Reddits folder:", download_params.output_reddits_folder)
    print("Authors folder:", download_params.output_authors_folder)
    print("Download author details:", download_params.is_author_downloaded)
    print("Search until previous day:", download_params.is_date_to_previous_day)
    print("Use multiprocessing:", download_params.is_multiprocessing_used)
    print("Number of processes:", download_params.num_processes, "\n")

    logger.info(f"Searched phrase: {download_params.phrase}")
    logger.info(f"Max searched: {download_params.limit}")
    logger.info(f"Date interval: {download_params.date_interval}")
    logger.info(f"Reddits folder: {download_params.output_reddits_folder}")
    logger.info(f"Authors folder: {download_params.output_authors_folder}")
    logger.info(f"Download author details: {download_params.is_author_downloaded}")
    logger.info(f"Search until previous day: {download_params.is_date_to_previous_day}")
    logger.info(f"Use multiprocessing: {download_params.is_multiprocessing_used}")
    logger.info(f"Number of processes: {download_params.num_processes}")


def create_folders(download_params: DownloadParams):
    """ Creates folders for JSONs if not exist """
    if not os.path.exists(download_params.output_reddits_folder):
        os.makedirs(download_params.output_reddits_folder)
    if not os.path.exists(download_params.output_authors_folder):
        os.makedirs(download_params.output_authors_folder)


def show_load_params(load_params: LoadParams, logger: logging.Logger):
    print("Load type:", load_params.load_type.value)
    print("Start date:", load_params.date_from)
    print("End date:", load_params.date_to, "\n")

    logger.info(f"Load type: {load_params.load_type.value}")
    logger.info(f"Start date: {load_params.date_from}")
    logger.info(f"End date: {load_params.date_to}")


def _download_reddits_details(downloader: yars.YARS, permalinks: List[str], num: int,
                              queue: Queue, logger: logging.Logger) -> None:
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


def _download_authors_details(downloader: yars.YARS, names: List[str], num: int,
                              queue: Queue, logger: logging.Logger) -> None:
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


def main():
    config = AppConfig.from_json()
    args = parse_args(config)

    logger = util.setup_logger(name=f"download_reddits_{args.phrase}",
                               log_file=f"logs/download_reddits/download_reddits_{args.phrase}_{dt.datetime.now().isoformat()}.log")

    print("---- Reddits downloader app ----\n")
    logger.info("---- Reddits downloader app ----")

    download_params = DownloadParams.from_argparse_namespace_and_config(args, config)

    # Show parameters
    show_params(download_params, logger=logger)

    # Create folders if not exist
    create_folders(download_params)

    load_params = LoadParams.from_download_params(download_params)

    # Show load params
    show_load_params(load_params, logger=logger)

    if load_params.date_from > load_params.date_to:
        logger.info("Recent (start) file date is bigger than end date. Nothing to download. Finishing.")
        raise Exception("Recent (start) file date is bigger than end date. Nothing to download.")

    downloader = yars.YARS(logger=logger)

    # Getting posts headers
    print(f"Searching reddits with phrase '{download_params.phrase}'.\n")
    logger.info(f"Searching reddits with phrase '{download_params.phrase}'.")
    reddit_headers = downloader.search_reddit(query=download_params.phrase, limit=download_params.limit)

    # Restriction to the newest only for INCREMENTAL load
    if load_params.load_type == EloadType.INCREMENTAL:
        reddit_headers = list(filter(lambda rh: load_params.date_to > dt.datetime.fromtimestamp(rh['created_utc']) >= load_params.date_from, reddit_headers))

    # Getting posts details
    permalinks = list(map(lambda rh: rh['link'].split(config.website_url)[1], reddit_headers))
    print(f"Found {len(permalinks)} results.")
    logger.info(f"Found {len(permalinks)} results.")

    print("Downloading reddits.")
    logger.info("Downloading reddits.")
    # Using multiprocessing only if applicable and number of reddits to download is >= quadratic number of processes
    if download_params.is_multiprocessing_used and len(permalinks) >= download_params.num_processes ** 2:
        reddit_details = list([])
        queue = multiprocessing.Queue()
        for i, chunk in enumerate(util.chunk_list(permalinks, download_params.num_processes)):
            p = multiprocessing.Process(target=_download_reddits_details, args=(downloader, chunk, i, queue, logger))
            p.start()

        for i in range(download_params.num_processes):
            results, num = queue.get()
            if isinstance(results, list):
                reddit_details.extend(results)

    else:
        reddit_details = list(map(lambda pl: downloader.scrape_post_details(pl), tqdm(permalinks)))
    print(f"Reddit details downloaded. Total: {len(reddit_details)}.")
    logger.info(f"Reddit details downloaded. Total: {len(reddit_details)}.")


    # Saving into separate JSONs
    for sd, ed in util.date_range(load_params.date_from, load_params.date_to, interval=download_params.date_interval):
        # Filtering reddits details by dates interval
        reddits_interval = util.filter_reddits_by_dates(reddit_details, sd, ed)

        # Saving reddits details into JSON file
        util.save_jsons(reddits_interval,
                        download_params.output_reddits_folder, download_params.output_reddits_file_pattern,
                        sd, ed, logger=logger)

        if download_params.is_author_downloaded:
            # Getting posts authors
            authors = util.collect_authors(reddits_interval)

            print(f"\nFound {len(authors)} different authors for period {sd} -- {ed}.")
            logger.info(f"Found {len(authors)} different authors for period {sd} -- {ed}.")
            print(f"Downloading authors details for period {sd} -- {ed}.")
            logger.info(f"Downloading authors details for period {sd} -- {ed}.")
            # Using multiprocessing only if applicable and number of authors to download is >= quadratic number of processes
            if download_params.is_multiprocessing_used and len(authors) >= download_params.num_processes ** 2:
                author_details = list([])
                queue = multiprocessing.Queue()
                for i, chunk in enumerate(util.chunk_list(authors, download_params.num_processes)):
                    p = multiprocessing.Process(target=_download_authors_details, args=(downloader, chunk, i, queue, logger))
                    p.start()

                for i in range(download_params.num_processes):
                    results, num = queue.get()
                    if isinstance(results, list):
                        author_details.extend(results)

            else:
                author_details = list(map(lambda a: downloader.scrape_user_data(a, limit=1), tqdm(authors)))
            print(f"Downloading authors details for period {sd} -- {ed} finished.")
            logger.info(f"Downloading authors details for period {sd} -- {ed} finished.")

            # Saving authors details into JSON file
            util.save_jsons(author_details,
                            download_params.output_authors_folder, download_params.output_authors_file_pattern,
                            sd, ed, logger=logger)

    print("\nDone.")
    logger.info("Done.")


if __name__ == "__main__":
    main()
