from __future__ import annotations
from .sessions import RandomUserAgentSession
import time
import datetime as dt
import random
import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

from utils import setup_logger


logger = setup_logger(name="yars",
                      log_file=f"logs/yars/YARS_{dt.datetime.now().isoformat()}.log")


class YARS:
    __slots__ = ("headers", "session", "proxy", "timeout")

    def __init__(self, proxy=None, timeout=10, random_user_agent=True):
        self.session = RandomUserAgentSession() if random_user_agent else requests.Session()
        self.proxy = proxy
        self.timeout = timeout

        retries = Retry(
            total=5,
            backoff_factor=2,  # Exponential backoff
            status_forcelist=[429, 500, 502, 503, 504],
        )

        self.session.mount("https://", HTTPAdapter(max_retries=retries))

        if proxy:
            self.session.proxies.update({"http": proxy, "https": proxy})
    def handle_search(self,url, params, after=None, before=None):
        if after:
            params["after"] = after
        if before:
            params["before"] = before

        response = None
        try:
            response = self.session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            logger.info("Search request successful")
        except Exception as e:
            if response is not None:
                if response.status_code != 200:
                    logger.info("Search request unsuccessful due to: %s", e)
                    return []
            else:
                logger.info("Search request unsuccessful due to: %s", e)
                return []

        data = response.json()
        results = []
        for post in data["data"]["children"]:
            post_data = post["data"]
            results.append(
                {
                    "author": post_data["author"],
                    "title": post_data["title"],
                    "link": f"https://www.reddit.com{post_data['permalink']}",
                    "description": post_data.get("selftext", "")[:269],
                    "created": post_data.get("created", 0.),
                    "created_utc": post_data.get("created_utc", 0.),
                }
            )
        logger.info("Search Results Returned %d Results", len(results))
        return results
    def search_reddit(self, query, limit=10, after=None, before=None):
        url = "https://www.reddit.com/search.json"
        params = {"q": query, "limit": limit, "sort": "relevance", "type": "link"}
        return self.handle_search(url, params, after, before)
    def search_subreddit(self, subreddit, query, limit=10, after=None, before=None, sort="relevance"):
        url = f"https://www.reddit.com/r/{subreddit}/search.json"
        params = {"q": query, "limit": limit, "sort": "relevance", "type": "link","restrict_sr":"on"}
        return self.handle_search(url, params, after, before)

    def scrape_post_details(self, permalink):
        url = f"https://www.reddit.com{permalink}.json"

        response = None
        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            logger.info("Post details request successful : %s", url)
        except Exception as e:
            logger.info("Post details request unsuccessful: %s", e)
            if response is not None:
                if response.status_code != 200:
                    logger.error(f"Failed to fetch post data: {response.status_code}")
                    return None
            else:
                logger.error(f"Failed to fetch post data: {e}")
                return None

        post_data = response.json()
        if not isinstance(post_data, list) or len(post_data) < 2:
            logger.info("Unexpected post data structre")
            logger.error("Unexpected post data structure")
            return None

        main_post = post_data[0]["data"]["children"][0]["data"]
        entry_id = main_post["id"]
        name = main_post["name"]
        permalink = main_post["permalink"]
        author = main_post["author"]
        title = main_post["title"]
        body = main_post.get("selftext", "")
        created = main_post.get("created", 0.)
        created_utc = main_post.get("created_utc", 0.)
        comments = self._extract_comments(post_data[1]["data"]["children"])

        logger.info("Successfully scraped post: %s", title)
        return {
            "id": entry_id,
            "name": name,
            "permalink": permalink,
            "author": author,
            "title": title,
            "body": body,
            "created": created,
            "created_utc": created_utc,
            "likes": main_post.get("likes", 0),
            "ups": main_post.get("ups", 0),
            "downs": main_post.get("downs", 0),
            "score": main_post.get("score", 0),
            "upvote_ratio": main_post.get("upvote_ratio", 1.),
            "gilded": main_post.get("gilded", 0),
            "subreddit_id": main_post.get("subreddit_id", ""),
            "subreddit_name": main_post.get("subreddit", ""),
            "num_comments": main_post.get("num_comments", 0),
            "comments": comments
        }

    def _extract_comments(self, comments):
        logger.info("Extracting comments")
        extracted_comments = []
        for comment in comments:
            if isinstance(comment, dict) and comment.get("kind") == "t1":
                comment_data = comment.get("data", {})
                extracted_comment = {
                    "id": comment_data.get("id", ""),
                    "parent_id": comment_data.get("parent_id", ""),
                    "name": comment_data.get("name", ""),
                    "permalink": comment_data.get("permalink", ""),
                    "author": comment_data.get("author", ""),
                    "body": comment_data.get("body", ""),
                    "created": comment_data.get("created", 0.),
                    "created_utc": comment_data.get("created_utc", 0.),
                    "depth_level": comment_data.get("depth", 0),
                    "controversiality": comment_data.get("controversiality", 0),
                    "likes": comment_data.get("likes",0),
                    "ups": comment_data.get("ups",0),
                    "downs": comment_data.get("downs",0),
                    "score": comment_data.get("score",0),
                    "upvote_ratio": comment_data.get("upvote_ratio", 1.),
                    "gilded": comment_data.get("gilded", 0),
                    "subreddit_id": comment_data.get("subreddit_id", ""),
                    "subreddit_name": comment_data.get("subreddit", ""),
                    "replies": [],
                }

                replies = comment_data.get("replies", "")
                if isinstance(replies, dict):
                    extracted_comment["replies"] = self._extract_comments(
                        replies.get("data", {}).get("children", [])
                    )
                extracted_comments.append(extracted_comment)
        logger.info("Successfully extracted comments")
        return extracted_comments

    def scrape_user_data(self, username, limit=10):
        logger.info("Scraping user data for %s, limit: %d", username, limit)
        base_url = f"https://www.reddit.com/user/{username}/.json"
        params = {"limit": limit, "after": None}
        all_items = []
        count = 0

        while count < limit:
            response = None
            try:
                response = self.session.get(
                    base_url, params=params, timeout=self.timeout
                )
                response.raise_for_status()

                logger.info("User data request successful")
            except Exception as e:
                logger.info("User data request unsuccessful: %s", e)
                if response is not None:
                    if response.status_code != 200:
                        logger.error(f"Failed to fetch data for user {username}: {response.status_code}")
                else:
                    logger.error(f"Failed to fetch data for user {username}: {e}")
                break
            try:
                data = response.json()
            except ValueError:
                logger.error(f"Failed to parse JSON response for user {username}.")
                break

            if "data" not in data or "children" not in data["data"]:
                logger.error(f"No 'data' or 'children' field found in response for user {username}.")
                logger.info("No 'data' or 'children' field found in response")
                break

            items = data["data"]["children"]
            if not items:
                logger.error(f"No more items found for user {username}.")
                logger.info("No more items found for user")
                break

            for item in items:
                kind = item["kind"]
                item_data = item["data"]
                if kind == "t3":
                    post_url = f"https://www.reddit.com{item_data.get('permalink', '')}"
                    all_items.append(
                        {
                            "type": "post",
                            "subreddit": item_data.get("subreddit", ""),
                            "title": item_data.get("title", ""),
                            "author": item_data.get("author", ""),
                            "author_flair_background_color": item_data.get("author_flair_background_color", None),
                            "author_flair_css_class": item_data.get("author_flair_css_class", None),
                            "author_flair_richtext": item_data.get("author_flair_richtext", None),
                            "author_flair_template_id": item_data.get("author_flair_template_id", None),
                            "author_flair_text": item_data.get("author_flair_text", None),
                            "author_flair_text_color": item_data.get("author_flair_text_color", None),
                            "author_flair_type": item_data.get("author_flair_type", ""),
                            "author_fullname": item_data.get("author_fullname", ""),
                            "author_is_blocked": item_data.get("author_is_blocked", ""),
                            "author_patreon_flair": item_data.get("author_patreon_flair", ""),
                            "author_premium": item_data.get("author_premium", ""),
                            "created": item_data.get("created", ""),
                            "created_utc": item_data.get("created_utc", ""),
                            "url": post_url,
                        }
                    )
                elif kind == "t1":
                    comment_url = (
                        f"https://www.reddit.com{item_data.get('permalink', '')}"
                    )
                    all_items.append(
                        {
                            "type": "comment",
                            "subreddit": item_data.get("subreddit", ""),
                            "body": item_data.get("body", ""),
                            "author": item_data.get("author", ""),
                            "author_flair_background_color": item_data.get("author_flair_background_color", None),
                            "author_flair_css_class": item_data.get("author_flair_css_class", None),
                            "author_flair_richtext": item_data.get("author_flair_richtext", None),
                            "author_flair_template_id": item_data.get("author_flair_template_id", None),
                            "author_flair_text": item_data.get("author_flair_text", None),
                            "author_flair_text_color": item_data.get("author_flair_text_color", None),
                            "author_flair_type": item_data.get("author_flair_type", ""),
                            "author_fullname": item_data.get("author_fullname", ""),
                            "author_is_blocked": item_data.get("author_is_blocked", ""),
                            "author_patreon_flair": item_data.get("author_patreon_flair", ""),
                            "author_premium": item_data.get("author_premium", ""),
                            "created": item_data.get("created", ""),
                            "created_utc": item_data.get("created_utc", ""),
                            "url": comment_url,
                        }
                    )
                count += 1
                if count >= limit:
                    break

            params["after"] = data["data"].get("after")
            if not params["after"]:
                break

            time.sleep(random.uniform(1, 2))
            logger.info("Sleeping for random time")

        logger.info("Successfully scraped user data for %s", username)
        return all_items

    def fetch_subreddit_posts(
        self, subreddit, limit=10, category="hot", time_filter="all"
    ):
        logger.info(
            "Fetching subreddit/user posts for %s, limit: %d, category: %s, time_filter: %s",
            subreddit,
            limit,
            category,
            time_filter,
        )
        if category not in ["hot", "top", "new", "userhot", "usertop", "usernew"]:
            raise ValueError("Category for Subredit must be either 'hot', 'top', or 'new' or for User must be 'userhot', 'usertop', or 'usernew'")

        batch_size = min(100, limit)
        total_fetched = 0
        after = None
        all_posts = []

        while total_fetched < limit:
            if category == "hot":
                url = f"https://www.reddit.com/r/{subreddit}/hot.json"
            elif category == "top":
                url = f"https://www.reddit.com/r/{subreddit}/top.json"
            elif category == "new":
                url = f"https://www.reddit.com/r/{subreddit}/new.json"
            elif category == "userhot":
                url = f"https://www.reddit.com/user/{subreddit}/submitted/hot.json"
            elif category == "usertop":
                url = f"https://www.reddit.com/user/{subreddit}/submitted/top.json"
            else:
                url = f"https://www.reddit.com/user/{subreddit}/submitted/new.json"

            params = {
                "limit": batch_size,
                "after": after,
                "raw_json": 1,
                "t": time_filter,
            }
            response = None
            try:
                response = self.session.get(url, params=params, timeout=self.timeout)
                response.raise_for_status()
                logger.info("Subreddit/user posts request successful")
            except Exception as e:
                logger.info("Subreddit/user posts request unsuccessful: %s", e)
                if response is not None:
                    if response.status_code != 200:
                        logger.info(f"Failed to fetch posts for subreddit/user {subreddit}: {response.status_code}")
                        break
                else:
                    logger.info(f"Failed to fetch posts for subreddit/user: {subreddit}: {e}")

            data = response.json()
            posts = data["data"]["children"]
            if not posts:
                break

            for post in posts:
                post_data = post["data"]
                post_info = {
                    "title": post_data["title"],
                    "author": post_data["author"],
                    "permalink": post_data["permalink"],
                    "score": post_data["score"],
                    "num_comments": post_data["num_comments"],
                    "created_utc": post_data["created_utc"],
                }
                if post_data.get("post_hint") == "image" and "url" in post_data:
                    post_info["image_url"] = post_data["url"]
                elif "preview" in post_data and "images" in post_data["preview"]:
                    post_info["image_url"] = post_data["preview"]["images"][0][
                        "source"
                    ]["url"]
                if "thumbnail" in post_data and post_data["thumbnail"] != "self":
                    post_info["thumbnail_url"] = post_data["thumbnail"]

                all_posts.append(post_info)
                total_fetched += 1
                if total_fetched >= limit:
                    break

            after = data["data"].get("after")
            if not after:
                break

            time.sleep(random.uniform(1, 2))
            logger.info("Sleeping for random time")

        logger.info("Successfully fetched subreddit posts for %s", subreddit)
        return all_posts
