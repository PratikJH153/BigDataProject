from data_collection.reddit_collector import RedditDataCollector
import pandas as pd


def main():
    collector = RedditDataCollector()

    # Collect posts and comments
    print("Collecting Reddit data...")
    posts_df = collector.collect_posts(time_filter="month", limit=500)

    # Basic data analysis
    print("\nCollection Summary:")
    print(f"Total posts and comments collected: {len(posts_df)}")
    print(f"Unique subreddits: {posts_df['subreddit'].nunique()}")

    # Save the data
    collector.save_to_csv(posts_df, "nightlife_data")
    print("\nData saved successfully!")


if __name__ == "__main__":
    main()
