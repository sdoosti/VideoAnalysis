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
    if vid not in data.new_id.values:
        if vid+1 not in data.new_id.values:
            if vid-1 not in data.new_id.values:
                print(vid)
                counter+=1


vca = pd.read_csv(r"C:\Users\doosti\OneDrive - Chapman University\Research\Research Projects\Facebook\Tubular\revision_2020\google\vca_types_17000_feb2021.csv")
print("Number of matching video_id: ", data.merge(vca, on='video_id', how='inner').shape)

data['transcript'] = data.merge(df, left_on='new_id', right_on='video_id', how='left').transcript

sponsored = data[(data.sponsor_name.notnull()) & (data.transcript.notnull())][['sponsor_name','transcript']]
sponsored['terms'] = sponsored.sponsor_name.apply(lambda x: x.split())

def is_in_text(terms, text):
    if ' '.join(terms).lower() in text.lower():
        return True
    for term in terms:
        if term.lower() in ['the','a','an','of','and','or','in','on','at','to','for','with','by','from','as','is']:
            continue
        if term.lower() in text.lower().split():
            return True
    return False

def term_in_text(terms, text):
    found = []
    if ' '.join(terms).lower() in text.lower():
        return terms
    for term in terms:
        if term.lower() in ['the','a','an','of','and','or','in','on','at','to','for','with','by','from','as','is']:
            continue
        if term.lower() in text.lower().split():
            found.append(term)
    return found

def sponsored_by(text):
    if 'sponsored' in text.lower().split():
        return True
    elif 'brought to you by' in text.lower():
        return True
    return False

sponsored['is_in_text'] = sponsored.apply(lambda x: is_in_text(x.terms, x.transcript), axis=1)
print(sponsored['is_in_text'].sum())
sponsored['found_terms'] = sponsored.apply(lambda x: term_in_text(x.terms, x.transcript), axis=1)
sponsored['sponsored_by'] = sponsored.transcript.apply(sponsored_by)
print(sponsored['sponsored_by'].sum())

temp = sponsored[sponsored.is_in_text==True][['sponsor_name','found_terms','transcript']]

# function to show 20 rows of a data frame at a time every time by pressing enter
def display(df):
    for i in range(0, len(df), 20):
        print(df[i:i+20])
        input("Press Enter to continue...")

display(temp)
# check these sponsors: Real Street, Capital One, Before I Fall Movie, "Kiss Him, Not Me", NBC SPorts Washington, Fidelity, Be The Match, 200576, 200717, 204485, 207797

temp['found'] = temp.found_terms.apply(lambda x: ' '.join(x))
temp2 = temp[temp.found=="Live"].copy()

for ind in temp2.index:
    print(temp2.loc[ind,'sponsor_name'])
    for i,term in enumerate(temp2.loc[ind,'transcript'].split()):
        if term.lower() == 'live':
            print(temp2.loc[ind,'transcript'].split()[i-5:i+5])
    print("\n\n")

for ind in temp[temp.found=="Real"].index:
    print(temp.loc[ind,'sponsor_name'])
    for i,term in enumerate(temp.loc[ind,'transcript'].split()):
        if term.lower() == 'real':
            print(temp.loc[ind,'transcript'].split()[i-5:i+5])
    print("\n\n")

for ind in [200576, 200717, 204485, 207797]:
    print(temp.loc[ind,'sponsor_name'])
    for i, term in enumerate(temp.loc[ind,'transcript'].split()):
        if term.lower() == temp.loc[ind,'found'].lower():
            print(temp.loc[ind,'transcript'].split()[i-5:i+5])
    print("\n\n")

# finding:
# "Live" is not sponsor name
# non of the above sponsors are in the transcript


#sponsored[['sponsor_name','found_terms','transcript']].to_csv("sponsored_transcripts.csv", index=False)
#df.to_csv("sponsored_transcripts.csv", index=False)