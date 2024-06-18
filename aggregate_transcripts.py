import os
import pandas as pd
from tqdm import tqdm


# load the transcripts
def read_transcript(file):
    with open(file, 'r') as f:
        transcript = f.read()
        return transcript
     
# Set the path to the directory containing the data
creators = pd.read_csv("videos_creators.csv")
sponsors = pd.read_csv("videos_sponsors.csv")

# modifying the columns
ccols = ['video_id', 'creator_id', 'creator_name', 'video_title',
       'video_description', 'video_url', 'video_topics']
scols = ['video_id', 'creator_id', 'creator_name', 'title',
       'description', 'url', 'topics']
creators = creators[ccols].copy()
creators.columns = scols
creators['type'] = "creator"
sponsors['type'] = "sponsor"

# Combine the data
combined = pd.concat([creators, sponsors], ignore_index=True)

print(combined.head())
print(combined.shape)

# create a dictionary of video_id and transcript file
transcript_dict = {}
print("Creating transcript dictionary")
for root, dirs, files in os.walk("transcripts"):
    for file in files:
        video_id = file.split(".")[0]
        transcript_dict[video_id] = os.path.join(root, file)
for root, dirs, files in os.walk("transcripts2"):
    for file in files:
        video_id = file.split(".")[0]
        transcript_dict[video_id] = os.path.join(root, file)

# transcript dictionary
transcript_text_dict = {}
for video_id, video_file in transcript_dict.items():
    #print(video_id, video_file)
    if os.path.exists(video_file):
        #print(f"Transcript file found for video {video_id}")
        transcript_text = read_transcript(video_file)
        transcript_text_dict[video_id] = transcript_text
    else:
        #print(f"Transcript file not found for video {video_id}")
        pass

# Aggregate the transcripts
print('Aggregating transcripts')
transcripts = []
for i, row in tqdm(combined.iterrows(), desc='Creators and Sponsors'):
    video_id = str(row["video_id"])
    transcript = transcript_text_dict.get(video_id,None)    
    transcripts.append(transcript)

combined["transcript"] = transcripts

# Save the data
combined.to_csv("videos_transcripts.csv", index=False)

