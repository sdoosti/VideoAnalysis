"""
This python script is used to analyze the logging data from the log file for video download process
"""

import sys
import os
import re
import json
import argparse
import datetime
import time
import logging
import logging.handlers
import pandas as pd

PATH = os.path.dirname(__file__)

def get_args():
    """
    Get the command line arguments
    """
    parser = argparse.ArgumentParser(description='Analyze the logging data from the log file for video download process')
    parser.add_argument('-f', '--file', help='The log file to analyze', required=True)
    parser.add_argument('-o', '--output', help='The output file to store the analysis result', required=True)
    parser.add_argument('-v', '--verbose', help='Print the log messages', action='store_true')
    return parser.parse_args()

def analyze_log_file(log_file):
    """
    Analyze the log file
    """
    # Define the regular expression pattern
    pattern = re.compile(r'(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d{3} - (?P<level>\w+) - (?P<message>.*)')

    # Define the data list
    data = []
    # Errors
    errors = []

    # Open the log file
    with open(log_file, 'r') as f:
        # Read the lines
        lines = f.readlines()

        # Iterate the lines
        for line in lines:
            # Match the pattern
            match = pattern.match(line)

            # Check if the match is found
            if match:
                # Get the matched groups
                groups = match.groupdict()

                # Get the timestamp
                timestamp = groups['timestamp']

                # Get the level
                level = groups['level']

                # Get the message
                message = groups['message']

                # Check if the message contains the video download information
                if 'Download completed' in message:
                    # Get the video id
                    video_id = message.split(' ')[-1]

                    # Get the timestamp
                    timestamp = datetime.datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')

                    # Append the data
                    data.append({
                        'timestamp': timestamp,
                        'level': level,
                        'video_id': video_id,
                    })
                elif 'ERROR' in level:
                    errors.append(message)  

    # Return the data
    return data, errors

def analyze_data(data):
    """
    Analyze the data
    """
    # Create a dataframe
    df = pd.DataFrame(data)

    # Get the start time
    start_time = df['timestamp'].min()

    # Get the end time
    end_time = df['timestamp'].max()

    # Get the total number of videos downloaded
    total_videos = df['video_id'].nunique()

    # Get the total download time
    total_time = end_time - start_time

    # Get the average download time
    average_time = total_time / total_videos

    # Get the download time for each video
    download_time = df.groupby('video_id')['timestamp'].apply(lambda x: x.max() - x.min())

    # Get the minimum download time
    min_time = download_time.min()

    # Get the maximum download time
    max_time = download_time.max()

    # Get the average download time
    average_time = download_time.mean()

    # Get the median download time
    median_time = download_time.median()

    # Get the download time for each video
    download_time = download_time.to_dict()

    # Return the analysis result
    return {
        'start_time': start_time,
        'end_time': end_time,
        'total_videos': total_videos,
        'total_time': total_time,
        'average_time': average_time,
        'min_time': min_time,
        'max_time': max_time,
        'average_time': average_time,
        'median_time': median_time,
        #'download_time': download_time,
    }

def save_output(output_file, analysis_result):
    """
    Save the output
    """
    # Open the output file
    with open(output_file, 'w') as f:
        # Write the analysis result
        json.dump(analysis_result, f, indent=4)

def main():
    """
    Main function
    """
    # Get the command line arguments
    args = get_args()

    # Get the log file
    log_file = args.file
    #log_file = "E:/Facebook/download_videos_sponsors.log"

    # Get the output file
    output_file = args.output
    #output_file = "E:/Facebook/sponsors_output.json"

    # Get the verbose flag
    verbose = args.verbose

    # Analyze the log file
    data, errors = analyze_log_file(log_file)

    # Analyze the data
    analysis_result = analyze_data(data)

    analysis_result['errors'] = len(errors)
    analysis_result['success_rate'] = analysis_result['total_videos'] / (analysis_result['total_videos'] + analysis_result['errors'])
    # the success rate formatted as a percentage
    analysis_result['success_rate'] = "{:.1%}".format(analysis_result['success_rate'])

    # convert datetime to string
    analysis_result['start_time'] = analysis_result['start_time'].strftime('%Y-%m-%d %H:%M:%S')
    analysis_result['end_time'] = analysis_result['end_time'].strftime('%Y-%m-%d %H:%M:%S')
    analysis_result['total_time'] = str(analysis_result['total_time'])
    analysis_result['average_time'] = str(analysis_result['average_time'])
    analysis_result['min_time'] = str(analysis_result['min_time'])
    analysis_result['max_time'] = str(analysis_result['max_time'])
    analysis_result['average_time'] = str(analysis_result['average_time'])
    analysis_result['median_time'] = str(analysis_result['median_time'])
    #analysis_result['download_time'] = {k: str(v) for k, v in analysis_result['download_time'].items()}

    # Print the analysis result
    if verbose:
        print(json.dumps(analysis_result, indent=4))

    # Save the output
    save_output(output_file, analysis_result)

if __name__ == '__main__':
    main()
