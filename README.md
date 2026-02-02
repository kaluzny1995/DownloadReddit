# Reddits scraper and downloader

A Python 3.11 application for reddits scraping and downloading with application of [YARS](yars/README.md) solution.

## Installation
Before downloading the repo you have to install [Anaconda](https://anaconda.org/). After installation clone the repo. Go into the downloaded repo directory.

    cd DownloadReddit

Then establish new Anaconda Python 3.11 environment, like _download_reddit_311_ with required packages:

    conda create -n download_reddit_311 python=3.11 --file "requirements.txt"

Activate the Anaconda environment via: `conda activate download_reddit_311`. The application is now ready to be used.

## Running the application
Running the help command: `python run_download_reddits.py -h` yields the following: 

```
---- Reddits downloader ----

usage: run_download_reddits.py [-h] [-l LIMIT] [-i {h,d,m,y}] [-d START_DATE] [--download_authors] [--previous_day] [--use_multiprocessing] [--num_processes NUM_PROCESSES] phrase

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
  --use_multiprocessing
                        flag whether to use multiprocessing while downlading reddits and authors, default: False
  --num_processes NUM_PROCESSES
                        number of processes if multiprocessing is used, default: 8

```
The application searches reddits by provided _phrase_ and stores found results in separate JSON files.

### Parameters overview
1. **phrase** -- _required_ -- word or sentence fragment the downloaded reddit texts or titles should contain
2. **-l, --limit** -- _optional_ -- **10000** by default -- max number of returned results
3. **-i, --interval** -- _optional_ -- **"d"** by default -- period of time (between two date times) from which the searched results are: _"y"_ denotes year, _"m"_ - month, _"d"_ - day and _"h"_ - hour. The downloaded results are saved into separate JSON files for separate periods. For example with 'y' (year) interval a result with date time of creation _2021-03-21T09:12:54_ with be stored in `reddits_phrase_2021-01-01T00:00:00_2022-01-01T00:00:00.json` file.
4. **-s, --start_date** -- _optional_ -- **"2020-01-01"** by default -- earliest date time of reddits searching
5. **--download_authors** -- _optional_ -- **False** by default -- flag whether to download authors data. If unset the application will not perform any authors data downloading and saving them into JSON files
6. **--previous_day** -- _optional_ -- **False** by default -- flag whether to set up the latest date time of downloaded reddits to the end of previous day. For example if the downloading started on _2021-09-02T03:00:00_ then the latest result date would be _2021-09-01T23:59:59_
7. **--use_multiprocessing** -- _optional_ -- **False** by default -- flag whether to utilize multiprocess approach for results downloading. If set up the application will divide the list of reddit permalinks to download them from to separate processes. For 2xQuadCore processors the number should not be larger than 8
8. **--num_processes** -- _optional_ -- **8** -- number of processes for multiprocess approach, not applicable if _use multiprcessing_ flag is unset

### Command examples

#### Simple
    python run_download_reddits.py "corgi"
The application will download all reddits having "corgi" word inside without authors info store the results in separate JSON files for separate days.

#### Year interval and 2022-01-01 start date
    python run_download_reddits.py "corgi" -i="y" -s="2022-01-01"
The application will download "corgi" reddits which are not older than 1st of January 2022 and store the result in separate JSON file for separate year (instead of day)

#### Downloading authors
    python run_download_reddits.py "corgi" --download_authors
The application will download not only all "corgi" reddits but also information about reddit authors and store the results into JSON files separate for reddits and authors

#### Multiprocesssing
    python run_download_reddits.py "corgi" --download_authors --use_multiprocessing
The application will download the "corgi" reddit and information about author however using multiprocess approach

### Testing
To perform application unit testing simply run the command `pytest` in main project directory. The output should look like the following:
```
================================ test session starts ================================
platform linux -- Python 3.11.14, pytest-9.0.2, pluggy-1.5.0
rootdir: /home/jakub/PycharmProjects/DownloadReddit
configfile: pyproject.toml
collected 8 items                                                                                                                                                                      

test/test_utils.py ........                                                    [100%]

================================= 8 passed in 0.03s =================================
```
## Dataflow
![Dataflow diagram](/assets/images/reddits_dataflow_download.png)
The illustration above shows the solution dataflow diagram. The dash-frame highlighted area denotes the downloading reddits stages.

### Stages of reddits downloading
1. **Search reddits and authors data** -- searching relevant reddits and preparing them for download
2. **Download raw data** -- downloading searched raw reddits/authors via JSON like objects
3. **JSON files persistence** -- storing and persistence of downloaded data into separate JSON files

## Stored JSON file structure
![JSON file structure](/assets/images/reddits_files_structure.png)
The illustration above shows how the persisted JSON files should look like in appropriate folders.