"""
File: video_download.py
Description: This file includes the function to downlod transcripts facebook videos
Created by: Shahryar Doosti (doosti@chapman.edu)
Date: 6/2/2024
"""

import os
import pandas as pd
import yt_dlp as youtube_dl
import logging
import datetime
import time

PATH = os.path.dirname(__file__)
print(PATH)
OUTPUT_PATH = os.path.join(PATH, 'transcripts','Sponsors')

# Get today's date
#today = datetime.date.today()

# Configure logging
logging.basicConfig(filename=os.path.join(PATH, f'download_transcript_sponsors.log'), level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def download_transcript(url, video_id=False, output_path=OUTPUT_PATH, verbose=True):
    """
    Download a transcript from the given URL.

    Parameters
    ----------
    url : str
        The URL of the video to download.
    video_id : str, optional
        The ID of the video. If not provided, it will be extracted from the URL.
    output_path : str, optional
        The path to save the downloaded video.
    verbose : bool, optional
        Whether to print the status of the download operation.

    Returns the status of the download operation.
    -------
    """
    if video_id is False:
        video_id = url.split('=')[-1] 

    ydl_opts = {
        'skip_download': True,                                # Skip downloading the video
        'write-sub': True,                                   # Write the subtitles to a file
        'write-auto-sub': True,                              # Write automatic subtitles (if available)
        'writeauto_sub': True,                                # Write automatic subtitles (if available)
        'subtitleslangs': ['en'],                             # Download subtitles in English
        'list-subs': True,                                   # List the available subtitles
        'outtmpl': output_path+f"/{video_id}.srt",        # Include the specified path
        'sub-format': 'srt',        # Specify the subtitle format
        'noplaylist': True,                                   # Disable downloading playlists, only single video
    }
    print(output_path+f"/{video_id}.srt")
    try:
        with youtube_dl.YoutubeDL(ydl_opts) as ydl:
            info_dict = ydl.extract_info(url, download=False)
            subtitle_url = ydl.prepare_filename(info_dict).rsplit('.', 1)[0] + '.en.srt'
            ydl.download([url])    
        logging.info(f"Subtitle download completed: {url}")
        if verbose: 
            print(f"Subtitle download completed: {url}")
            print(f"Subtitle saved to: {subtitle_url}")
        return True
    except youtube_dl.utils.DownloadError as e:
        logging.error(f"Download error: {video_id} - {e}")
        if verbose: print(f"Download error: {e}")
        return False
    except youtube_dl.utils.ExtractorError as e:
        logging.error(f"Extractor error: {video_id} - {e}")
        if verbose: print(f"Extractor error: {e}")
        return False
    except youtube_dl.utils.PostProcessingError as e:
        logging.error(f"Post-processing error: {video_id} - {e}")
        if verbose: print(f"Post-processing error: {e}")
        return False
    except Exception as e:
        logging.error(f"An unexpected error occurred: {video_id} - {e}")
        if verbose: print(f"An unexpected error occurred: {e}")
        return False


def sponsor_data(file_path = os.path.join(PATH, 'VideoDownloads','videos_sponsors.csv')):
    """
    Load data from a CSV file.

    Parameters
    ----------
    file_path : str
        The path to the CSV file.

    Returns
    -------
    pandas.DataFrame
        The loaded data as a pandas DataFrame.
    """
    return pd.read_csv(file_path)

def main():
    # data = sponsor_data()
    # for k,row in data.iterrows():
    #     #path = os.path.join(OUTPUT_PATH, f"{row.video_id}.mp4")
    #     # if os.path.isfile(path):
    #     #     logging.info(f"Video already exists: {row.video_id}")
    #     #     continue
    #     print(f"------------ {k+1} - id:{row.video_id}")
    #     try:
    #         _ = download_transcript(row.video_url, row.video_id)
    #     except Exception:
    #         print("Not successful")
    #     time.sleep(1)
    url = "https://www.facebook.com/cnn/videos/981577819286073"
    try:
        _ = download_transcript(url)
    except Exception:
        print("Not successful")

if __name__ == '__main__':
    main()


