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
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import numpy as np
import logging


PATH = os.path.dirname(__file__)

# add arg parser for video files path and options and output


# configure logging
logging.basicConfig(filename=os.path.join(PATH, f'transcribe.log'), level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

videos = ['335673943535848.mp4',
          '354955018231049.mp4',
          '356330281398896.mp4',
          "129791957687477.mp4"]

video_files = [os.path.join(PATH, video) for video in videos]

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
        except sr.UnknownValueError:
            logging.error(f"Chunk {index} could not be understood: {video_id} - {e}")
            return (index, "")
        except sr.RequestError as e:
            logging.error(f"Could not request results from Google Speech Recognition service for chunk {index}: {video_id} - {e}")
            return (index, "")

def transcribe_audio_chunks(chunks, video_id):
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(transcribe_chunk, chunk, i, video_id) for i, chunk in enumerate(chunks)]
        results = [future.result() for future in futures]
        results.sort(key=lambda x: x[0])  # Sort by index to maintain order
        full_transcript = " ".join([result[1] for result in results])
        return full_transcript

# Step 4: Process video file
def process_video(video_file):
    # Extract audio from video
    video = VideoFileClip(video_file)
    video_id = os.path.splitext(video_file)[0]
    audio = video.audio

    audio_segment = AudioSegment.from_file(video_file, format="mp4")

    video.close()  # Close the video to free up resources

    # Split the audio into chunks of 1 minute (60000 milliseconds)
    audio_chunks = split_audio(audio_segment, 60000) # I passed the audio_segment instead of the audio_file

    # Transcribe the audio chunks
    full_transcript = transcribe_audio_chunks(audio_chunks, video_id)
    logging.info(f"Transcript Completed: {video_id}")
    #print(f"Transcript for {video_file}: ", full_transcript)

    return full_transcript

# Step 5: Process all video files
def process_videos(video_files):
    with ProcessPoolExecutor() as executor:
        results = executor.map(process_video, video_files)
        return list(results)
    
def main(video_files):
    # Process multiple videos in parallel
    all_transcripts = process_videos(video_files)
    for idx, transcript in enumerate(all_transcripts):
        #print(f"Transcript for video {idx+1}: {transcript}")

        video_file = video_files[idx]
        transcript_file = os.path.splitext(video_file)[0] + '.txt'
        
        with open(transcript_file, 'w') as f:
            f.write(transcript)
        logging.info(f"Transcript saved to: {transcript_file}: {video_file}")
        
        #print(f"Transcript saved to: {transcript_file}")

if __name__ == '__main__':
    main(video_files)
