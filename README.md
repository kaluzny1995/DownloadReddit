# Reddits scraper and downloader

A Python 3.11 application for reddits scraping and downloading with application of [YARS](yars/README.md) solution.

## Installation
Before downloading the repo you have to install [Anaconda](https://anaconda.org/). After installation establish new Anaconda Python 3.11 environment, like _download_reddit_311_ with required packages:

    conda create -n download_reddit_311 python=3.11 conda-forge::uv==0.9.13 conda-forge::beautifulsoup4==4.13.4 anaconda::colorama==0.4.6 anaconda::flask==3.1.0 anaconda::lxml==5.3.0 conda-forge::markupsafe==3.0.2 anaconda::pygments==2.19.1 anaconda::python-dateutil==2.9.0post0 anaconda::requests==2.32.4 anaconda::tqdm==4.67.1 anaconda::urllib3==2.5.0 conda-forge::websockets==15.0.1

Finally download the repo.

## Running application
Activate the Anaconda environment via: `conda activate download_reddit_311` and run the app using the following command: `python run_download_reddits.py "phrase"`. 

Details available below:
```
---- Reddits downloader ----

usage: run_download_reddits.py [-h] [-l LIMIT] [-i {h,d,m,y}] [-d START_DATE] [--download_authors] [--previous_day] phrase

Reddits downloader Python 3.11 application.

positional arguments:
  phrase                phrase to search for reddits

options:
  -h, --help            show this help message and exit
  -l LIMIT, --limit LIMIT
                        limit of searched reddits, default: 10000
  -i {h,d,m,y}, --interval {h,d,m,y}
                        interval between dates of searching period, default: d
  -d START_DATE, --start_date START_DATE
                        start date of reddits search, default: 2020-01-01
  --download_authors    flag whether to download reddit authors information, default: False
  --previous_day        flag whether to download reddits till the previous day, default: False
```
The application searches reddits matching the given _phrase_ and returns max _limit_ number of entries. The dowloaded reddits are then saved into separate .json files in dependence of _interval_ parameter. 'y' denotes year, 'm' - month, 'd' - day and 'h' - hour. The .json files contain only the entries with creation datetime only from certain year, month, day or hour.
The solution may also download reddit authors info, like personal details and their Reddit rating measures. 