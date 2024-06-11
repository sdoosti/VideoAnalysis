"""
File: video_download_creators_server.py
Description: This file includes the function to downlod and save facebook videos for creators
(Server Version)
Created by: Shahryar Doosti (doosti@chapman.edu)
Date: 6/2/2024
"""

import os
import pandas as pd
import yt_dlp as youtube_dl
import logging
import datetime
import time
import multiprocessing
from multiprocessing import Pool

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


def creator_data(file_path = os.path.join(PATH, 'videos_creators.csv')):
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
    return pd.read_csv(file_path)[['video_id','video_url']]

def main(videos):
    # Use multiprocessing Pool to download subtitles concurrently
    with Pool(multiprocessing.cpu_count()) as pool:
        pool.starmap(download_video, videos)

if __name__ == '__main__':
    data = creator_data()
    videos = [(r[1],r[0],os.path.join(OUTPUT_PATH, f"{r[0]}.mp4")) for r in data.to_numpy()] # (url, video_id, output_path)
    vidoes = [x for x in videos if not os.path.isfile(x[2])]
    # reverse the order of the list (to start from the end of the list)
    videos = videos[::-1]
    main(videos)


