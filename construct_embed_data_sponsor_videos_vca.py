"""
This file constructs the text data that includes sponsor description and video level data.

Sponsors:
    - description (creator level, not video level)
    (from videos_sponsors.csv)

Sponsored videos content:
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
data2embed = data[data.sponsored==1].copy()
### Build the sponsor data

# Load the sponsor data
sponsors = pd.read_csv(os.path.join(DATA_PATH,'videos_sponsors.csv'))

# Merge the sponsor data
#data2embed['sponsor_description'] = 
temp = data2embed[['sponsor_name']].merge(sponsors[['creator_name','creator_description']].drop_duplicates(),left_on='sponsor_name',right_on='creator_name',how='inner')
print(temp.shape)
print(temp.notnull().sum())
print(temp.sponsor_name.nunique())
