import pandas as pd
import re
from collections import Counter


class RedditDataProcessor:
    def __init__(self):
        self.venue_keywords = [
            "bar",
            "club",
            "restaurant",
            "venue",
            "lounge",
            "pub",
            "tavern",
            "nightclub",
        ]

    def extract_venues(self, text):
        """Extract potential venue names from text"""
        if pd.isna(text):
            return []

        # Split text into sentences
        sentences = text.split(".")
        venues = []

        for sentence in sentences:
            # Look for patterns like "at [Venue Name]" or "to [Venue Name]"
            venue_patterns = re.findall(r"(?:at|to|in)\s+([A-Z][A-Za-z\s&]+)", sentence)
            venues.extend(venue_patterns)

        return venues

    def process_data(self, df):
        """Process the Reddit data to extract meaningful information"""
        # Combine title and text for posts
        df["full_text"] = df["title"].fillna("") + " " + df["text"].fillna("")

        # Extract venues
        df["mentioned_venues"] = df["full_text"].apply(self.extract_venues)

        # Calculate basic metrics
        df["text_length"] = df["full_text"].str.len()
        df["has_venue_mention"] = df["mentioned_venues"].apply(lambda x: len(x) > 0)

        return df
