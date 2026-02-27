import json
from pydantic import BaseModel


class AppConfig(BaseModel):
    """ App config class """
    limit: int
    interval: str
    start_date: str
    website_url: str
    reddits_folder_pattern: str
    authors_folder_pattern: str
    reddits_file_pattern: str
    authors_file_pattern: str
    is_no_authors_download: bool
    is_today_included: bool
    is_no_multiprocessing_used: bool
    num_processes: int

    class Config:
        frozen = True

    @staticmethod
    def from_json() -> 'AppConfig':
        with open("config.json") as f:
            config = AppConfig(**json.load(f))
        return config
