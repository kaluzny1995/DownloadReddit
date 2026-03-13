import argparse
import datetime as dt
from pydantic import BaseModel

from config import AppConfig


class DownloadParams(BaseModel):
    phrase: str
    limit: int
    date_interval: str
    default_start_date: dt.datetime
    output_reddits_folder: str
    output_authors_folder: str
    output_reddits_file_pattern: str
    output_authors_file_pattern: str
    is_author_downloaded: bool
    is_date_to_previous_day: bool
    is_multiprocessing_used: bool
    num_processes: int

    class ConfigDict:
        frozen = True

    @staticmethod
    def from_argparse_namespace_and_config(args: argparse.Namespace, config: AppConfig) -> 'DownloadParams':
        return DownloadParams(
            phrase=args.phrase,
            limit=args.limit,
            date_interval=args.interval,
            default_start_date=dt.datetime.strptime(args.start_date, "%Y-%m-%d"),
            output_reddits_folder=config.reddits_folder_pattern.format(phrase=args.phrase),
            output_authors_folder=config.authors_folder_pattern.format(phrase=args.phrase),
            output_reddits_file_pattern=config.reddits_file_pattern.format(phrase=args.phrase),
            output_authors_file_pattern=config.authors_file_pattern.format(phrase=args.phrase),
            is_author_downloaded=not args.no_authors_download,
            is_date_to_previous_day = not args.include_today,
            is_multiprocessing_used = not args.no_multiprocessing,
            num_processes = 1 if args.no_multiprocessing else args.num_processes
        )