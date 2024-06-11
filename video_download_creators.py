"""
File: video_download.py
Description: This file includes the function to downlod and save facebook videos
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
OUTPUT_PATH = os.path.join(PATH, 'videos','Creators')

# Get today's date
#today = datetime.date.today()

# Configure logging
logging.basicConfig(filename=os.path.join(PATH, f'download_videos_creators.log'), level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def download_video(url, video_id=False, output_path=OUTPUT_PATH, verbose=True, format='b[filesize<2M] / w'):
    """
    Download a video from the given URL.

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
    format : str, optional
        The format of the video to download.
        e.g., 'bestvideo[height<=50M]+bestaudio[height<=50M]'
              'bestvideo[filesize<=720]+bestaudio[filesize<=720]'
              'best'

    Returns the status of the download operation.
    -------
    """
    ydl_opts = {
        'format': format,
        'outtmpl': output_path,  # Include the specified path
        'noplaylist': True,      # Disable downloading playlists, only single video
    }
    if video_id is False:
        video_id = url.split('/')[-1]
    try:
        with youtube_dl.YoutubeDL(ydl_opts) as ydl:
            ydl.download([url])
        logging.info(f"Download completed: {video_id}")
        if verbose: print(f"Download completed: {url}")
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


def creator_data(file_path = os.path.join(PATH, 'VideoDownloads','videos_creators.csv')):
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
    data = creator_data()
    for k,row in data.iterrows():
        path = os.path.join(OUTPUT_PATH, f"{row.video_id}.mp4")
        if os.path.isfile(path):
            continue
        print(f"------------ {k+1} - id:{row.video_id}")
        try:
            _ = download_video(row.video_url, row.video_id, path)
        except Exception:
            print("Not successful")
        time.sleep(1)

if __name__ == '__main__':
    main()


