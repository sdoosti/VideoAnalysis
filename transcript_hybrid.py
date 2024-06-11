"""
This file extracts the audio from a video file and saves it as a .wav file.
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

PATH = os.path.dirname(__file__)

videos = ['335673943535848.mp4',
          '354955018231049.mp4',
          '356330281398896.mp4',
          "129791957687477.mp4"]

video_files = [os.path.join(PATH, video) for video in videos]

# Step 1: Extract audio from video
def extract_audio(video_file):
    audio_file = os.path.splitext(video_file)[0] + '.wav'
    video = VideoFileClip(video_file)
    audio = video.audio
    #audio.write_audiofile(audio_file)
    return audio_file

# Step 2: Split audio into chunks
def split_audio(audio, chunk_length_ms):
    #audio = AudioSegment.from_wav(audio_file) # removed this line to avoid creating the audio file
    chunks = [audio[i:i + chunk_length_ms] for i in range(0, len(audio), chunk_length_ms)]
    return chunks

# # Step 3: Convert audio to text (no chunking)
# def audio_to_text(audio_file):
#     recognizer = sr.Recognizer()
#     audio = sr.AudioFile(audio_file)
    
#     with audio as source:
#         audio_data = recognizer.record(source)
    
#     try:
#         transcript = recognizer.recognize_google(audio_data)
#         return transcript
#     except sr.UnknownValueError:
#         return "Google Speech Recognition could not understand audio"
#     except sr.RequestError as e:
#         return f"Could not request results from Google Speech Recognition service; {e}"

# Step 3: Transcribe audio chunks (no parallelization)
# def transcribe_audio_chunks(chunks):
#     recognizer = sr.Recognizer()
#     full_transcript = ""

#     for i, chunk in enumerate(chunks):
#         # Save each chunk as a separate .wav file
#         # chunk_name = f"chunk{i}.wav"
#         # chunk.export(chunk_name, format="wav")
#         # Export chunk to a BytesIO object
#         chunk_io = io.BytesIO()
#         chunk.export(chunk_io, format="wav")
#         chunk_io.seek(0)

#         with sr.AudioFile(chunk_io) as source:
#         #with sr.AudioFile(chunk_name) as source:
#             audio_data = recognizer.record(source)
#             try:
#                 transcript = recognizer.recognize_google(audio_data)
#                 full_transcript += transcript + " "
#             except sr.UnknownValueError:
#                 print(f"Chunk {i} could not be understood")
#             except sr.RequestError as e:
#                 print(f"Could not request results from Google Speech Recognition service for chunk {i}; {e}")

#     return full_transcript

# Step 3: Transcribe audio chunks (with parallelization)
def transcribe_chunk(chunk, index):
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
            return (index, "")
        except sr.RequestError as e:
            return (index, "")

def transcribe_audio_chunks(chunks):
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(transcribe_chunk, chunk, i) for i, chunk in enumerate(chunks)]
        results = [future.result() for future in futures]
        results.sort(key=lambda x: x[0])  # Sort by index to maintain order
        full_transcript = " ".join([result[1] for result in results])
        return full_transcript

# Step 4: Process video file
def process_video(video_file):
    # Extract audio from video
    video = VideoFileClip(video_file)

    # Save audio as .wav file
    #audio_file = os.path.splitext(video_file)[0] + '.wav'
    audio = video.audio
    #audio.write_audiofile(audio_file)

    # Process audio in memory
    # audio = video.audio
    # # Convert audio to numpy array
    # audio_array = audio.to_soundarray(fps=16000) # Convert to 16kHz, required by pydub
    # # Convert numpy array to AudioSegment
    # audio_segment = AudioSegment(
    #     data=audio_array.astype(np.int16).tobytes(),
    #     sample_width=audio_array.dtype.itemsize,
    #     frame_rate=16000,
    #     channels=1
    # )
    # Save audio to a temporary buffer
    #audio_buffer = io.BytesIO(audio.to_bytestring())
    #audio.write_audiofile(audio_buffer, verbose=False)
    #audio_buffer.seek(0)
    # Load audio into pydub AudioSegment
    #audio_segment = AudioSegment.from_file(audio_buffer, format="wav")
    audio_segment = AudioSegment.from_file(video_file, format="mp4")


    video.close()  # Close the video to free up resources

    # Split the audio into chunks of 1 minute (60000 milliseconds)
    audio_chunks = split_audio(audio_segment, 60000) # I passed the audio_segment instead of the audio_file

    # Transcribe the audio chunks
    full_transcript = transcribe_audio_chunks(audio_chunks)
    print(f"Transcript for {video_file}: ", full_transcript)

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
        print(f"Transcript for video {idx+1}: {transcript}")

        video_file = video_files[idx]
        transcript_file = os.path.splitext(video_file)[0] + '.txt'
        
        with open(transcript_file, 'w') as f:
            f.write(transcript)
        
        print(f"Transcript saved to: {transcript_file}")

if __name__ == '__main__':
    main(video_files)
