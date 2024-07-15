"""
This file extracts the audio from a video file from a .mp4 file.
It also extracts the transcript from the video and saves it as a .txt file.

Hybrid parallelization is used to transcribe the audio chunks in parallel (also at video level)

Creator: Shahryar Doosti (doosti@chapman.edu)
"""

import os
from moviepy.editor import VideoFileClip
import speech_recognition as sr
from pydub import AudioSegment
import io
import numpy as np
import logging
import argparse
from tqdm import tqdm


PATH = os.path.dirname(__file__)

def get_args():
    """
    Get the command line arguments
    """
    parser = argparse.ArgumentParser(description='Video files and options')
    parser.add_argument('-f', '--file', help='The path to a txt file including video files', required=True)
    parser.add_argument('-o', '--output', help='The output path to store the transcripts', required=True)
    parser.add_argument('-v', '--verbose', help='Print the log messages', action='store_true')
    return parser.parse_args()

# configure logging
logging.basicConfig(filename=os.path.join(PATH, f'transcribe.log'), level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


# Step 1: Extract audio from video
# Done in the process_video function

# Step 2: Split audio into chunks
def split_audio(audio, chunk_length_ms):
    #audio = AudioSegment.from_wav(audio_file) # removed this line to avoid creating the audio file
    chunks = [audio[i:i + chunk_length_ms] for i in range(0, len(audio), chunk_length_ms)]
    return chunks

# Step 3: Transcribe audio chunks (with parallelization)
def transcribe_chunk(chunk, index, video_id):
    recognizer = sr.Recognizer()
    chunk_io = io.BytesIO()
    chunk.export(chunk_io, format="wav")
    chunk_io.seek(0)

    with sr.AudioFile(chunk_io) as source:
        audio_data = recognizer.record(source)
        try:
            transcript = recognizer.recognize_google(audio_data)
            return (index, transcript)
        except sr.UnknownValueError as e:
            logging.error(f"Chunk {index} could not be understood: {video_id} - {e}")
            return (index, "")
        except sr.RequestError as e:
            logging.error(f"Could not request results from Google Speech Recognition service for chunk {index}: {video_id} - {e}")
            return (index, "")

def transcribe_audio_chunks(chunks, video_id):
    # with ThreadPoolExecutor() as executor:
    #     futures = [executor.submit(transcribe_chunk, chunk, i, video_id) for i, chunk in enumerate(chunks)]
    #     futures = [transcribe_chunk(chunk, i, video_id) for i, chunk in enumerate(chunks)]
    #     results = [future.result() for future in futures]
    #     results.sort(key=lambda x: x[0])  # Sort by index to maintain order
    #     full_transcript = " ".join([result[1] for result in results])
    #     return full_transcript\
    
    # no multi-threading
    results = [transcribe_chunk(chunk, i, video_id) for i, chunk in enumerate(chunks)]
    full_transcript = " ".join([result[1] for result in results])
    return full_transcript

# Step 4: Process video file
def process_video(video_file, output_file, verbose=False):
    # Extract audio from video
    video = VideoFileClip(video_file)
    video_id = os.path.splitext(video_file)[0].split('/')[-1]
    audio = video.audio

    try:
        audio_segment = AudioSegment.from_file(video_file, format="mp4")
    except IndexError as e:
        logging.error(f"Could not extract audio from video: {video_id} - {e}")
        return ""

    video.close()  # Close the video to free up resources

    # Split the audio into chunks of 1 minute (60000 milliseconds)
    audio_chunks = split_audio(audio_segment, 60000) # I passed the audio_segment instead of the audio_file

    # Transcribe the audio chunks
    full_transcript = transcribe_audio_chunks(audio_chunks, video_id)
    logging.info(f"Transcript Completed: {video_id}")
    #print(f"Transcript for {video_file}: ", full_transcript)

    transcript_file = os.path.join(output_file, video_id + '.txt')
    #transcript_file = os.path.splitext(video_file)[0] + '.txt'
    
    with open(transcript_file, 'w') as f:
        f.write(full_transcript)
    logging.info(f"Transcript saved to: {transcript_file}: {video_file}")
    
    if verbose:
        print(f"Transcript saved to: {transcript_file}")
        
    return full_transcript

# Step 5: Process all video files
def process_videos(video_files, output_file, verbose=False):
    for video_file in tqdm(video_files, desc="Processing video files") if verbose else video_files:
        transcript = process_video(video_file, output_file, verbose)
    
def main(video_files, output_file, verbose=False):
    if verbose:
        print(f"Processing {len(video_files)} video files")
    process_videos(video_files, output_file, verbose)


if __name__ == '__main__':
    # Step 0: Get the command line arguments
    args = get_args()
    video_file_path = args.file
    output_path = args.output
    verbose = args.verbose
    # open video files
    with open(video_file_path, 'r') as f:
        video_files = f.readlines()
        video_files = [video_file.replace("\\","/") for video_file in video_files]
        # exclude the files that the transcript already exists
        video_files = [video_file.strip() for video_file in video_files if not os.path.exists(os.path.join(output_path, os.path.splitext(video_file.strip())[0].split('/')[-1] + '.txt'))]
        #video_files = [video_file.strip() for video_file in video_files]
    print(f"Number of video files: {len(video_files)}")
    main(video_files[3:500],output_path, verbose)
