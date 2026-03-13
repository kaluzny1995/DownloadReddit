import datetime as dt
from pydantic import BaseModel

import util
from model import EloadType, DownloadParams


class LoadParams(BaseModel):
    load_type: EloadType
    date_from: dt.datetime
    date_to: dt.datetime

    class ConfigDict:
        frozen = True

    @staticmethod
    def from_download_params(download_params: DownloadParams):
        recent_date = util.get_recent_file_date(download_params.output_reddits_folder)
        return LoadParams(
            load_type=EloadType.HISTORICAL if recent_date is None else EloadType.INCREMENTAL,
            date_from=download_params.default_start_date if recent_date is None else recent_date,
            date_to=dt.datetime.now() if not download_params.is_date_to_previous_day \
                else dt.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - dt.timedelta(seconds=1)
        )
