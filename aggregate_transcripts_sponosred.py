import os
import pandas as pd
import numpy as np
from tqdm import tqdm

PATH = "E:/Facebook/transcripts/sponsored/"

# load the transcripts
def read_transcript(file):
    with open(file, 'r') as f:
        transcript = f.read()
        return transcript
     
# create a dictionary of video_id and transcript
transcript_dict = {}
print("Creating transcript dictionary")
for root, dirs, files in os.walk(PATH):
    for file in files:
        video_id = file.split(".")[0]
        transcript_dict[video_id] = read_transcript(os.path.join(root, file))

# data frame
df = pd.DataFrame(transcript_dict.items(), columns=["video_id", "transcript"])
df['video_id'] = df['video_id'].astype(np.int64)

# transcript length
df['length'] = df['transcript'].apply(lambda x: len(x.split()))
df.length.describe()

df.loc[df.length<3,'transcript'] = None


# sponsored data
data = pd.read_csv(r"C:\Users\doosti\OneDrive - Chapman University\Research\Research Projects\Facebook\Tubular\revision_2024\pooled_us_jul2024.csv")
data['video_id'] = data['video_id'].astype(np.int64)    

# check the number of matching video_id
print("Number of matching video_id: ", data.merge(df, on='video_id', how='inner').shape)
print('Number of matching new_id: ', data.merge(df, right_on='video_id', left_on = 'new_id', how='inner').shape)
# adding one to video_id in df
df['new_id'] = df['video_id'] + 1
print("Number of matching video_id: ", data.merge(df, right_on='new_id', left_on = 'video_id', how='inner').shape)
# subtracting one from video_id in df
df['new_id'] = df['video_id'] - 1
print("Number of matching video_id: ", data.merge(df, right_on='new_id', left_on = 'video_id', how='inner').shape)

counter = 0
for vid in df.video_id.values:
    if vid not in data.video_id.values:
        if vid+1 not in data.video_id.values:
            if vid-1 not in data.video_id.values:
                print(vid)
                counter+=1
#df.to_csv("sponsored_transcripts.csv", index=False)