from src.data_collection.reddit_collector import RedditDataCollector
from src.processing.reddit_processing import RedditDataProcessor

# Collect data
collector = RedditDataCollector()
data_df = collector.collect_posts()

# Process data
processor = RedditDataProcessor()
processed_df = processor.process_data(data_df)


collector.save_to_csv(processed_df, "nightlife_data")
print("\nData saved successfully!")

# View results
print(f"Total posts collected: {len(data_df)}")
print(f"Posts with venue mentions: {processed_df['has_venue_mention'].sum()}")
