"""
This file constructs the text data that includes sponsor description and video level data.

Sponsors:
    - description (creator level, not video level)
    (from videos_sponsors.csv)

Sponsored videos content (in a separate file):
    - video title
    - video description
    - video topics
    - video labels from vca
    - video transcript

"""

import os
import pandas as pd
import json

PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(PATH,"VideoDownloads/")

# Load the main data
data = pd.read_csv(os.path.join(DATA_PATH,'pooled_us_jul2024.csv'))
sponsor_df = data[data.sponsored==1][['sponsor_id','sponsor_name']].drop_duplicates().copy() # finding unique sponsors

### Build the sponsor data

# Load the sponsor data
sponsor_meta = pd.read_csv(os.path.join(DATA_PATH,'videos_sponsors.csv'))

# Merge the sponsor data
sponsor_df['description'] = sponsor_df.merge(sponsor_meta[['creator_name','creator_description']].drop_duplicates(),left_on='sponsor_name',right_on='creator_name',how='inner')['creator_description']

print(sponsor_df.head())
print(sponsor_df.shape)

# Save the sponsor data
sponsor_df.to_csv(os.path.join(DATA_PATH,'sponsor_description.csv'),index=False)