import praw
import pandas as pd
from datetime import datetime, timezone
import time
import yaml


class RedditDataCollector:
    def __init__(self, config_path="config/config.yaml"):
        # Load configuration
        with open(config_path, "r") as file:
            self.config = yaml.safe_load(file)

        # Initialize Reddit client
        self.reddit = praw.Reddit(
            client_id=self.config["api_credentials"]["reddit"]["client_id"],
            client_secret=self.config["api_credentials"]["reddit"]["client_secret"],
            user_agent=self.config["api_credentials"]["reddit"]["user_agent"],
        )

        self.subreddits = ["Chicago", "ChicagoSocial", "ChicagoFood", "Chicagomusic"]

    def collect_posts(self, time_filter="month", limit=1000):
        """Collect posts related to nightlife from specified subreddits"""
        posts_data = []

        for subreddit_name in self.subreddits:
            subreddit = self.reddit.subreddit(subreddit_name)

            # Search for relevant keywords
            search_queries = [
                "bar",
                "club",
                "nightlife",
                "restaurant",
                "venue",
                "concert",
                "live music",
                "drinks",
                "dancing",
            ]

            for query in search_queries:
                try:
                    for post in subreddit.search(
                        query, time_filter=time_filter, limit=limit
                    ):
                        post_data = {
                            "post_id": post.id,
                            "subreddit": subreddit_name,
                            "title": post.title,
                            "text": post.selftext,
                            "score": post.score,
                            "upvote_ratio": post.upvote_ratio,
                            "num_comments": post.num_comments,
                            "created_utc": datetime.fromtimestamp(post.created_utc),
                            "query_term": query,
                        }
                        posts_data.append(post_data)

                        # Collect comments
                        post.comments.replace_more(limit=0)
                        self._process_comments(post, posts_data)

                except Exception as e:
                    print(
                        f"Error collecting from {subreddit_name} with query {query}: {e}"
                    )

                # Respect Reddit's API rate limits
                time.sleep(2)

        return pd.DataFrame(posts_data)

    def _process_comments(self, post, posts_data, depth=1):
        """Process comments for a given post"""
        for comment in post.comments:
            if isinstance(comment, praw.models.Comment):
                comment_data = {
                    "post_id": post.id,
                    "comment_id": comment.id,
                    "parent_id": comment.parent_id,
                    "text": comment.body,
                    "score": comment.score,
                    "created_utc": datetime.fromtimestamp(comment.created_utc),
                    "depth": depth,
                }
                posts_data.append(comment_data)

    def save_to_csv(self, df, filename):
        """Save collected data to CSV"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        df.to_csv(f"data/raw/reddit_{filename}_{timestamp}.csv", index=False)
