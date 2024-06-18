import os
import pandas as pd
from tqdm import tqdm


# load the transcripts
def read_transcript(file):
    with open(file, 'r') as f:
        transcript = f.read()
        return transcript
    

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
     
# Set the path to the directory containing the data
creators = pd.read_csv("videos_creators.csv")
sponsors = pd.read_csv("videos_sponsors.csv")

# Aggregate the transcripts
print('Aggregating transcripts for creators')
transcripts = []
for i, row in tqdm(creators.iterrows(), desc='Creators'):
    video_id = str(row["video_id"])
    transcript = transcript_text_dict.get(video_id,None)    
    transcripts.append(transcript)

creators["transcript"] = transcripts
creators['type'] = "creator"

print('Aggregating transcripts for sponsors')
transcripts = []
for i, row in tqdm(sponsors.iterrows(), desc='Sponsors'):
    video_id = str(row["video_id"])
    transcript = transcript_text_dict.get(video_id,None)    
    transcripts.append(transcript)

sponsors["transcript"] = transcripts
sponsors['type'] = "sponsor"

# Combine the data
combined = pd.concat([creators, sponsors], ignore_index=True)

print(combined.head())
print(combined.shape)

# Save the data
combined.to_csv("videos_transcripts.csv", index=False)

